import org.apache.spark.sql.{DataFrame, SparkSession, Row, functions => F}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.{StringType, LongType}
import org.apache.spark.sql.functions._
import com.uber.h3core.H3Core
import com.uber.h3core.util.LatLng

import scala.util.Try
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._


object stay_detection {
  
  val h3 = H3Core.newInstance()

  object Haversine {
    val R = 6371.0 // R, km

    def distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val dLat = math.toRadians(lat2 - lat1)
      val dLon = math.toRadians(lon2 - lon1)
      val a = math.pow(math.sin(dLat / 2), 2) + math.pow(math.sin(dLon / 2), 2) * math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2))
      val c = 2 * math.asin(math.sqrt(a))
      R * c
    }
  }

  val latLonToH3UDF = udf((lat: Double, lon: Double, resolution: Int) => {
    h3.latLngToCell(lat, lon, resolution)
  })




  def getStays(df: DataFrame, spark: SparkSession, temporal_threshold: Long = 300, spatial_threshold: Double = 300): (DataFrame) = {

    // Temporal filtering: If delta_t(duration) > temporal_threshold (default = 300sec), "temporal_stay" = 1
    // Define window specification to partition by 'caid' and order by 'utc_timestamp'
    val windowSpec: WindowSpec = Window.partitionBy("caid").orderBy("utc_timestamp")
    // Calculate time lag between consecutive events for each 'caid', time difference (delta_t) between consecutive events
    val dfWithLag = df
      .withColumn("lag_timestamp", lag("utc_timestamp", 1).over(windowSpec))
      .withColumn("delta_t", unix_timestamp(col("utc_timestamp")) - unix_timestamp(col("lag_timestamp")))
      .withColumn("temporal_stay", when(col("delta_t") > temporal_threshold, 1).otherwise(0)) // Mark as temporal stay if the time difference exceeds the threshold
    // Assign a unique stay ID based on the temporal stay
    val dfWithStayId = dfWithLag
      .withColumn("temporal_stay_id", sum("temporal_stay").over(windowSpec))


    // Spatial filtering: If distance > spatial_threshold (default = 300meters), "distance_threshold" = 1
    val sortedDF = dfWithStayId
      .orderBy("caid", "temporal_stay_id", "utc_timestamp")
    val stayWindowSpec = Window.partitionBy("caid", "temporal_stay_id").orderBy("utc_timestamp")
    val haversineUDF = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      Haversine.distance(lat1, lon1, lat2, lon2) * 1000 // km ---> meter
    })
    val dfWithDistance = sortedDF
      .withColumn("prev_latitude", lag("latitude", 1).over(stayWindowSpec))
      .withColumn("prev_longitude", lag("longitude", 1).over(stayWindowSpec))
      .withColumn("distance", haversineUDF(col("prev_latitude"), col("prev_longitude"), col("latitude"), col("longitude")))
      .withColumn("distance_threshold", when(col("distance") > spatial_threshold, 1).otherwise(0))


    // "stay" = 1, if ("temporal_stay" = 1) or ("distance_threshold" = 1).
    val dfWithStay = dfWithDistance
      .withColumn("stay", when(col("temporal_stay") === 1 || col("distance_threshold") === 1, 1).otherwise(0))
    // "stay_id"
    val dfWithFinalStayId = dfWithStay
      .withColumn("stay_id", sum("stay").over(windowSpec))


    // Group by stay ID for final aggregation, calculate "stay_duration" and mean lat&lon
    val finalResultDF = dfWithFinalStayId.groupBy("caid", "stay_id")
      .agg(
        min("utc_timestamp").alias("stay_start_timestamp"),
        max("utc_timestamp").alias("stay_end_timestamp"),
        mean("latitude").alias("latitude"),
        mean("longitude").alias("longitude"),
        count("*").alias("stay_count")
      )
      .withColumn("stay_duration", col("stay_end_timestamp").cast("long") - col("stay_start_timestamp").cast("long"))
      .drop("stay_end_timestamp")

    (finalResultDF)
  }





  val haversineDistance = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    Haversine.distance(lat1, lon1, lat2, lon2)
  })

  val speedFilter = udf((distance: Double, timeDiff: Double, speedThreshold: Double) => (distance / timeDiff) > speedThreshold)


  def mapToH3(
               data_i: DataFrame,
               spark: SparkSession,
               resolution: Int,
               temporal_threshold: Int = 3600, // second
               // filterPassing: Boolean = true,
               speed_threshold: Double): (DataFrame, DataFrame) = {

    val windowSpec = Window.partitionBy("caid").orderBy("stay_start_timestamp")
    val dfWithH3 = data_i.withColumn("h3_index", latLonToH3UDF(col("latitude"), col("longitude"), lit(resolution)))
    val dfWithLag = dfWithH3
      .withColumn("prev_h3_index", lag("h3_index", 1).over(windowSpec))
      .withColumn("prev_h3_stay_end_time", lag("stay_start_timestamp", 1).over(windowSpec))
      .persist()


    // Check if the user has entered a new H3 cell or if the stay time exceeds the threshold
    // "stay_index_h3" = 1 if 1)Null h3 index. 2)Entering a new h3. 3)time_diff > temporal_threshold (default: 3600sec)
    val DataWithStayIndex = dfWithLag
      .withColumn("stay_index_h3", when(
      (col("h3_index").isNull) ||
      (col("prev_h3_index") =!= col("h3_index")) ||
      (col("prev_h3_index") === col("h3_index") && (unix_timestamp(col("stay_start_timestamp")) - unix_timestamp(col("prev_h3_stay_end_time"))) > temporal_threshold),
      1).otherwise(0)
    )
    // "stay_index_h3"
    val DataWithCumulativeIndex = DataWithStayIndex.withColumn("stay_index_h3", sum(col("stay_index_h3")).over(windowSpec))
    // Calculate "stay_end_timestamp" for each stay
    val DataWithEndTimestamp = DataWithCumulativeIndex
      .withColumn("stay_end_timestamp",
      (unix_timestamp(col("stay_start_timestamp")) + col("stay_duration")).cast("timestamp")
    )


    // Group by 'caid' and 'stay_index_h3' to aggregate stays in the same H3 cell
    val aggregated = DataWithEndTimestamp.groupBy("caid", "stay_index_h3")
      .agg(
        min("stay_start_timestamp").alias("h3_stay_start_time"),
        max("stay_end_timestamp").alias("h3_stay_end_time"),
        min("h3_index").alias("h3_id"), // Same h3_index actually
        avg("latitude").alias("h3_stay_lat"), // Average latitude for stays in this H3 cell
        avg("longitude").alias("h3_stay_lon"), // Average longitude for stays in this H3 cell
        sum("stay_count").alias("row_count")
      )

    dfWithLag.unpersist()


    // Filtering passing points
    val passingWindowSpec = Window.partitionBy("caid").orderBy("h3_stay_start_time")
    val checkWithPreviousNext = aggregated
      .withColumn("prev_h3_id", lag("h3_id", 1).over(passingWindowSpec))
      .withColumn("prev_h3_stay_end_time", lag("h3_stay_end_time", 1).over(passingWindowSpec))
      .withColumn("next_h3_id", lead("h3_id", 1).over(passingWindowSpec))
      .withColumn("next_h3_stay_start_time", lead("h3_stay_start_time", 1).over(passingWindowSpec))
      .withColumn("distance", haversineDistance(
        col("h3_stay_lat"), col("h3_stay_lon"),
        lag("h3_stay_lat", 1).over(passingWindowSpec),
        lag("h3_stay_lon", 1).over(passingWindowSpec))) // distance: km
      //.withColumn("time_diff", (col("next_h3_stay_start_time") - col("prev_h3_stay_end_time")) / 3600.0)
      .withColumn("time_diff", ((col("next_h3_stay_start_time").cast("long") - col("prev_h3_stay_end_time").cast("long")) / 3600.0).cast("double")) // time_diff: hour
      .persist()

    val checkedData = checkWithPreviousNext.withColumn("passing", speedFilter(
      col("distance"),
      col("time_diff"),
      lit(speed_threshold)
    ))

    // Filter passing records
    val passing = checkedData.filter(col("passing") === true)
      .select("caid", "h3_id", "h3_stay_start_time", "h3_stay_end_time")
    // Filter stay records
    val stays = checkedData.filter(col("passing") === false)
      //.select("caid", "h3_id", "h3_stay_start_time", "h3_stay_end_time",
      //"stay_index_h3", "h3_stay_lat", "h3_stay_lon", "distance", "time_diff", "passing")

    checkWithPreviousNext.unpersist()
    (passing, stays)

  }





  val sequentialH3RegionDetectionUDF = udf((names: Seq[String]) => {
    //val h3 = H3Core.newInstance()
    val h3LookupDict = scala.collection.mutable.Map[String, String]()
    val result = scala.collection.mutable.ListBuffer[String]()

    names.foreach { h3_id =>
      try {
        val h3Index = java.lang.Long.parseLong(h3_id) // Convert string to long
        if (!h3LookupDict.contains(h3_id)) {
          // Get all neighbors (k-ring with distance 1) of the h3_id
          val elements = h3.gridDisk(h3Index, 1).asScala.map(_.toString) // Convert Long back to String

          // Assign each element in k-ring to the region represented by h3_id
          elements.foreach { i =>
            if (!h3LookupDict.contains(i)) {
              h3LookupDict(i) = h3_id
            }
          }
        }
        result += h3LookupDict(h3_id)
      } catch {
        case e: NumberFormatException =>
          println(s"Failed to parse H3 index: $h3_id")
      }
    }
    result.toList
  })




  // Function to map H3 IDs to regions based on proximity and clustering
  def getH3RegionMapping(df: DataFrame, spark: SparkSession): DataFrame = {

    // Compute mean latitude, mean longitude, and number of stays for each H3 cell per caid
    val aggregatedDf = df.groupBy("caid", "h3_id")
      .agg(
        mean("h3_stay_lat").alias("mean_h3_stay_lat"),
        mean("h3_stay_lon").alias("mean_h3_stay_lon"),
        count("h3_id").alias("num_stays") //, sum("row_count").alias("initial_row_count")
      )
      .orderBy(col("caid"), col("num_stays").desc)

    // Collect list of H3 IDs for each caid and detect regions
    val groupedDf = aggregatedDf.groupBy("caid")
      .agg(
        collect_list("h3_id").alias("list_of_h3_id") //, collect_list("initial_row_count").alias("list_of_row_counts")
      )
      .withColumn("h3_id_region", sequentialH3RegionDetectionUDF(col("list_of_h3_id")))

    // Explode the H3 region and H3 ID lists for each caid
    val explodedDfRegion = groupedDf.selectExpr("caid", "explode(h3_id_region) as h3_id_region")
      .withColumn("join_key", monotonically_increasing_id())
    val explodedDfList = groupedDf.selectExpr("caid", "explode(list_of_h3_id) as h3_id") // , "explode(list_of_row_counts) as initial_row_count"
      .withColumn("join_key", monotonically_increasing_id())


    // Join to map each H3 ID to its identified region
    explodedDfRegion.as("region")
      .join(explodedDfList.as("list"), Seq("join_key"))
      .select(
        col("list.caid"),
        col("list.h3_id"),
        col("region.h3_id_region") //, col("list.initial_row_count")
      )
  }



  def mergeH3Region(df: DataFrame, temporal_threshold: Int = 3600): DataFrame = {
    val windowSpec = Window.partitionBy("caid").orderBy("stay_index_h3")

    val dfWithLag = df
      .withColumn("lagged_h3_id_region", lag("h3_id_region", 1).over(windowSpec))
      .withColumn("prev_h3_stay_end_time", lag("h3_stay_end_time", 1).over(windowSpec))

    // mark region change or temporal threshold excess
    val dfWithRegionStayId = dfWithLag
      .withColumn("h3_region_stay_id", when(
        col("lagged_h3_id_region") =!= col("h3_id_region") ||
        (unix_timestamp(col("h3_stay_start_time")) - unix_timestamp(col("prev_h3_stay_end_time")) > temporal_threshold),
        1).otherwise(0))
      .withColumn("h3_region_stay_id", sum("h3_region_stay_id").over(windowSpec))

    // aggregate regional stays info
    val result = dfWithRegionStayId.groupBy("caid", "h3_region_stay_id")
      .agg(
        min("h3_stay_start_time").alias("stay_start_timestamp"),
        max("h3_stay_end_time").alias("stay_end_timestamp"),
        (unix_timestamp(max("h3_stay_end_time")) - unix_timestamp(min("h3_stay_start_time"))
          ).alias("stay_duration"),
        first("h3_id_region").alias("h3_id_region"),
        sum("row_count").alias("row_count_for_region")
      )

    result
  }
}