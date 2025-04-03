package measures

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.H3DistanceUtils
import org.apache.spark.sql.functions.udf
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions.col

import java.io.Serializable

object extractTrips {
  object H3CoreSingleton extends Serializable {
    @transient lazy val instance: H3Core = H3Core.newInstance()
  }
  def getODMatrix(
      spark: SparkSession,
      data: DataFrame,
      resolution: Int = 8,
      outputPath: String
  ): Unit = {

    /** input data schema: caid / h3_region_id / local_time / stay_end_timestamp
      * / stay_duration / row_count_for_region / h3_index return individual user
      * level of trip data, with schema: caid / origin / destination / distance
      */

    val windowSpec = Window.partitionBy("caid").orderBy(col("local_time"))

    val dataWithNext = data
      .withColumn("next_h3_index", lead("h3_index", 1).over(windowSpec))
    val filteredData = dataWithNext.filter(col("next_h3_index").isNotNull)
    // add distance in kilometers
    val distanceUDF = udf[Double, String, String](H3DistanceUtils.distance)

    val dataWithDistance = filteredData.withColumn(
      "distance",
      distanceUDF(col("h3_index"), col("next_h3_index"))
    )

    val result = dataWithDistance
      .select(
        col("caid"),
        col("h3_index").alias("origin"),
        col("next_h3_index").alias("destination"),
        col("distance")
      )
      .filter(col("distance") =!= 0.0)

    // Get the OD matrix
    val h3 = H3Core.newInstance()
    val h3ToParentUDF = udf((h3Index: String) => {
      val h3 = H3CoreSingleton.instance

      h3.cellToParentAddress(h3Index, resolution)
    })
    val resultWithParent = result
      .withColumn("parent_origin", h3ToParentUDF(col("origin")))
      .withColumn("parent_destination", h3ToParentUDF(col("destination")))

    val odCount = resultWithParent
      .groupBy("parent_origin", "parent_destination")
      .count()

    val odCountDistance = odCount.withColumn(
      "distance",
      distanceUDF(col("parent_origin"), col("parent_destination"))
    )

    val tripName = "/trips.parquet"
    val ODName   = "/OD-Matrix.parquet"

    result.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath + tripName)

    odCountDistance.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath + ODName)
  }
  def getHomeWorkMatrix(
      spark: SparkSession,
      data: DataFrame,
      resolution: Int = 8,
      outputPath: String
  ): Unit = {
    val h3 = H3Core.newInstance()
    val h3ToParentUDF = udf((h3Index: String) => {
        val h3 = H3CoreSingleton.instance

        h3.cellToParentAddress(h3Index, resolution)
    })
    val distanceUDF = udf[Double, String, String](H3DistanceUtils.distance)

    val result = data
      .select(
        col("caid"),
        col("home_h3_index").alias("origin"),
        col("work_h3_index").alias("destination"),
      )
      .dropDuplicates("caid", "origin", "destination")
      .groupBy("origin", "destination")
      .agg(
        countDistinct("caid").alias("unique_count")
      ).filter(col("origin").isNotNull && col("destination").isNotNull)
   
    val resultWithParent = result
      .withColumn("origin", h3ToParentUDF(col("origin")))
      .withColumn("destination", h3ToParentUDF(col("destination")))
      .select(
        col("origin"),
        col("destination"),
        col("unique_count")
      )
      .groupBy("origin", "destination")
      .agg(
        sum("unique_count").alias("flow")
      ).filter(col("origin").isNotNull && col("destination").isNotNull)

    // Calculate distance
    val odDistance = resultWithParent.withColumn(
      "distance",
      distanceUDF(col("origin"), col("destination"))
    )

    val ODName = "/HW_OD_Matrix.parquet"

    odDistance.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath + ODName)

  }
}
