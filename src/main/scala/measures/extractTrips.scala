package measures

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.H3DistanceUtils

object extractTrips {
  def trip(spark: SparkSession, data: DataFrame): DataFrame ={
    /**
     * data schema:
     * caid / h3_region_id / local_time / stay_end_timestamp / stay_duration / row_count_for_region / h3_index
     * return individual user level of trip data, with schema:
     * caid / origin / destination / distance
     */

    val cleanedData = data.withColumn("caid", trim(col("caid").cast("string")))

    val windowSpec = Window.partitionBy("caid").orderBy(col("local_time"))
    val maxRowNumberSpec = windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val dataWithNext = cleanedData
      .withColumn("next_h3_region_stay_id", lead("h3_region_stay_id", 1).over(windowSpec))
      .withColumn("next_h3_index", lead("h3_index", 1).over(windowSpec))
    //.withColumn("row_number", row_number().over(windowSpec))

    val filteredData = dataWithNext.filter(col("next_h3_index").isNotNull)
    //add distance in kilometers
    val distanceUDF = udf[Double, String, String](H3DistanceUtils.distance)

    val dataWithDistance = filteredData.withColumn("distance", distanceUDF(col("h3_index"), col("next_h3_index")))


    val result = dataWithDistance.select(
      col("caid"),
      col("h3_index").alias("origin"),
      col("next_h3_index").alias("destination"),
      col("distance")
    ).filter(col("distance") =!= 0.0)

    val curDir = System.getProperty("user.dir")
    val relPath = "/data/intermediateResults/trips" //Whole folder
    val outputPath = curDir + relPath
    result.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath)
    result
  }
}
