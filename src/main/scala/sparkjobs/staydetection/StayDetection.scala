package sparkjobs.staydetection

import com.uber.h3core.H3Core
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import scala.util.Try
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.Row

object StayDetection {
  
  val h3 = H3Core.newInstance()

  object Haversine {
    val R = 6372.8 // R, km

    def distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val dLat = math.toRadians(lat2 - lat1)
      val dLon = math.toRadians(lon2 - lon1)
      val a = math.pow(math.sin(dLat / 2), 2) + math.pow(math.sin(dLon / 2), 2) * math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2))
      val c = 2 * math.asin(math.sqrt(a))
      R * c
    }
  }

  val latLonToH3UDF = udf((lat: Double, lon: Double, resolution: Int) => {
    // val h3 = H3Core.newInstance()
    h3.latLngToCell(lat, lon, resolution)
    // val h3IdDecimal = h3.latLngToCell(lat, lon, resolution)  // Returns H3 ID as a decimal Long
    // val h3IdHex = java.lang.Long.toHexString(h3IdDecimal)  // Convert to hexadecimal string
    // h3IdHex
  })


  val h3ToGeoUDF = udf((h3Index: Long) => {
    //val h3 = H3Core.newInstance()
    val geoCoord = h3.cellToLatLng(h3Index)
    Map("lat" -> geoCoord.lat, "lon" -> geoCoord.lng)
  })

  def sequentialStayDetection(iterator: Iterator[Map[String, Double]], threshold: Double = 300): Iterator[Int] = {
    val firstRow = Try(iterator.next()).toOption
    if (firstRow.isEmpty) return Iterator(1)

    var centroidLat = firstRow.get("latitude")
    var centroidLon = firstRow.get("longitude")
    var stackCount = 1
    var result = List(1)

    iterator.foreach { row =>
      val xLat = row("latitude")
      val xLon = row("longitude")

      if (Haversine.distance(centroidLat, centroidLon, xLat, xLon) * 1000 > threshold) {
        centroidLat = xLat
        centroidLon = xLon
        stackCount = 1
        result = result :+ 1
      } else {
        centroidLat = (centroidLat * stackCount + xLat) / (stackCount + 1)
        centroidLon = (centroidLon * stackCount + xLon) / (stackCount + 1)
        stackCount += 1
        result = result :+ 0
      }
    }

    result.iterator
  }


def getStays(df: DataFrame, spark: SparkSession, delta_t: Long = 300, threshold: Double = 300): DataFrame = {
  // Define window specification to partition by 'caid' and order by 'utc_timestamp'
  val windowSpec: WindowSpec = Window.partitionBy("caid").orderBy("utc_timestamp")

  // Calculate time lag between consecutive events for each 'caid', time difference (delta_t) between consecutive events
  val dfWithLag = df.withColumn("lag_timestamp", lag("utc_timestamp", 1).over(windowSpec))
    .withColumn("delta_t", unix_timestamp(col("utc_timestamp")) - unix_timestamp(col("lag_timestamp")))
    .withColumn("temporal_stay", when(col("delta_t") > delta_t, 1).otherwise(0)) // Mark as temporal stay if the time difference exceeds the threshold

  // Assign a unique stay ID based on the temporal stay
  val dfWithStayId = dfWithLag.withColumn("temporal_stay_id", sum("temporal_stay").over(windowSpec))

  // Perform a flatMap transformation to apply the sequential stay detection
  val resultRDD = dfWithStayId.groupBy("caid", "temporal_stay_id")
    .agg(collect_list(struct("latitude", "longitude")).alias("group_data"))
    .rdd.flatMap { row =>
      val caid = row.get(0).toString
      val temporalStayId = row.getLong(1)
      val groupData = row.getAs[scala.collection.Seq[Row]]("group_data").toSeq.map { r =>
        Map(
          "latitude" -> r.getAs[Double]("latitude"),
          "longitude" -> r.getAs[Double]("longitude")
        )
      }
      sequentialStayDetection(groupData.iterator, threshold).map((caid, temporalStayId, _))
    }
  import spark.implicits._

  // Convert result RDD to a DataFrame and assign unique join keys
  val listDF = resultRDD.toDF("caid", "temporalStayId", "distance_threshold")
    .withColumn("join_key", monotonically_increasing_id())

  // Add join key to the original DataFrame
  val dfWithJoinKey = dfWithStayId.withColumn("join_key", monotonically_increasing_id())
  // Repartition the listDF using 'caid'
  val repartitionedListDF = listDF.repartition(col("caid"))
  var repartitioneddfWithJoinKey = dfWithJoinKey.repartition(col("caid"))
  // Join the DataFrame with the result DataFrame based on the join key
  val joinedDF = repartitioneddfWithJoinKey.as("df1").join(repartitionedListDF.as("df2"), "join_key").drop("join_key")
  // Drop duplicate 'caid' column
  val deduplicatedDF = joinedDF.drop(joinedDF.col("df2.caid"))
  // Determine if a location qualifies as a stay based on temporal or distance criteria
  val dfWithFinalStay = deduplicatedDF.withColumn("stay", when(col("temporal_stay") === 1 || col("distance_threshold") === 1, 1).otherwise(0))

  // Assign a unique stay ID to each stay event
  val dfWithFinalStayId = dfWithFinalStay.withColumn("stay_id", sum(col("stay")).over(windowSpec))

  // Select the relevant columns and group by stay ID for final aggregation
  val resultDF = dfWithFinalStayId.groupBy(col("caid"), col("stay_id"))
    .agg(
      min("utc_timestamp").alias("stay_start_timestamp"),
      (max("utc_timestamp") - min("utc_timestamp")).alias("stay_duration"),
      mean("latitude").alias("latitude"),
      mean("longitude").alias("longitude")
    )
  resultDF
}

def getRegionalUsers(stayGroup: DataFrame, boundary: Array[Double]): DataFrame = {

  // Calculate the median latitude and longitude (centroid) for each user
  val userCentroid = stayGroup.groupBy("caid")
    .agg(
      expr("percentile_approx(latitude, 0.5)").alias("average_latitude"),
      expr("percentile_approx(longitude, 0.5)").alias("average_longitude")
    )

  // Filter users based on their centroids falling within the region's boundaries
  val regionalUsers = userCentroid.filter(
    col("average_latitude").between(boundary(0), boundary(1)) &&
      col("average_longitude").between(boundary(2), boundary(3))
  )

  // Semi join the stays with regional users to get the stays of users in the region
  val regionalStays = stayGroup.join(regionalUsers, "caid")

  regionalStays
}


val haversineDistance = udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
  Haversine.distance(lat1, lon1, lat2, lon2)
})

val checkPassing = udf((prevH3Id: Long, nextH3Id: Long, prevEndTime: Long, nextStartTime: Long, speedThreshold: Double) => {
  //val h3 = H3Core.newInstance()
  val centroid1 = h3.cellToLatLng(prevH3Id)
  val centroid2 = h3.cellToLatLng(nextH3Id)
  val distance = Haversine.distance(centroid1.lat, centroid1.lng, centroid2.lat, centroid2.lng) / 1000.0
  val timeDiff = (nextStartTime - prevEndTime) / 3600.0
  (distance / timeDiff) > speedThreshold
})


def mapToH3(data_i: DataFrame, resolution: Int, temporalThreshold: Int = 3600, filterPassing: Boolean = true, speed_threshold: Double = 30.0): (DataFrame, DataFrame) = {
  // partitions by 'caid' and orders by the 'stay_start_timestamp'
  val windowSpec = Window.partitionBy("caid").orderBy("stay_start_timestamp")
  
  val DataWithH3 = data_i.withColumn("h3_index", latLonToH3UDF(col("latitude"), col("longitude"), lit(resolution)))
  // val DataWithH3 = data_i.withColumn("h3_index", LatLonToH3(col("latitude"), col("longitude"), lit(resolution)))

  val DataWithPrevH3 = DataWithH3.withColumn("prev_h3_index", lag("h3_index", 1).over(windowSpec))
  val DataWithPrevEndTime = DataWithPrevH3.withColumn("prev_h3_stay_end_time", lag("stay_start_timestamp", 1).over(windowSpec))

  // Check if the user has entered a new H3 cell or if the stay time exceeds the threshold
  val DataWithStayIndex = DataWithPrevEndTime.withColumn("stay_index_h3", when(
    (col("h3_index").isNull) || 
    (col("prev_h3_index") =!= col("h3_index")) || 
    (col("prev_h3_index") === col("h3_index") && (unix_timestamp(col("stay_start_timestamp")) - unix_timestamp(col("prev_h3_stay_end_time"))) > temporalThreshold),
    1).otherwise(0)
  )

  // Cumulative count to give each stay point a unique H3 stay index
  val DataWithCumulativeIndex = DataWithStayIndex.withColumn("stay_index_h3", sum(col("stay_index_h3")).over(windowSpec))

  // Calculate the end timestamp for each stay
  val DataWithEndTimestamp = DataWithCumulativeIndex.withColumn("stay_end_timestamp", col("stay_start_timestamp") + col("stay_duration"))

  // Group by 'caid' and 'stay_index_h3' to aggregate stays in the same H3 cell
  val aggregated = DataWithEndTimestamp.groupBy("caid", "stay_index_h3")
    .agg(
      min("stay_start_timestamp").alias("h3_stay_start_time"),
      max("stay_end_timestamp").alias("h3_stay_end_time"),
      min("h3_index").alias("h3_id"), 
      avg("latitude").alias("h3_stay_lat"), // Average latitude for stays in this H3 cell
      avg("longitude").alias("h3_stay_lon") // Average longitude for stays in this H3 cell
    )

  // If filterPassing is true, apply logic to filter passing points
  if (filterPassing) {
    val passingWindowSpec = Window.partitionBy("caid").orderBy("h3_stay_start_time")

    // Fetch the previous and next H3 cell stay for comparison
    val checkWithPreviousNext = aggregated
      .withColumn("prev_h3_id", lag("h3_id", 1).over(passingWindowSpec))
      .withColumn("prev_h3_stay_end_time", lag("h3_stay_end_time", 1).over(passingWindowSpec))
      .withColumn("next_h3_id", lead("h3_id", 1).over(passingWindowSpec))
      .withColumn("next_h3_stay_start_time", lead("h3_stay_start_time", 1).over(passingWindowSpec))

    // Use checkPassing to filter according to the speed limit
    val checkedData = checkWithPreviousNext.withColumn("passing", checkPassing(
      col("prev_h3_id"),
      col("next_h3_id"),
      col("prev_h3_stay_end_time"),
      col("next_h3_stay_start_time"), 
      lit(speed_threshold)
    ))

    // Filter passing records
    val passing = checkedData.filter(col("passing") === true).select("caid", "h3_id", "h3_stay_start_time", "h3_stay_end_time")

    // Filter stay records, excluding passing points
    val stays = checkedData.filter(col("passing") === false).select("caid", "h3_id", "h3_stay_start_time", "h3_stay_end_time", 
      "stay_index_h3", "h3_stay_lat", "h3_stay_lon")

    (passing, stays)
  } else {
    val emptyDF = data_i.sparkSession.emptyDataFrame
    (emptyDF, aggregated)
  }
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
      count("h3_id").alias("num_stays")
    )
    .orderBy(col("caid"), col("num_stays").desc)

  // Collect list of H3 IDs for each caid and detect regions
  val groupedDf = aggregatedDf.groupBy("caid")
    .agg(collect_list("h3_id").alias("list_of_h3_id"))
    .withColumn("h3_id_region", sequentialH3RegionDetectionUDF(col("list_of_h3_id")))

  // Explode the H3 region and H3 ID lists for each caid
  val explodedDfRegion = groupedDf.selectExpr("caid", "explode(h3_id_region) as h3_id_region")
    .withColumn("join_key", monotonically_increasing_id())
  val explodedDfList = groupedDf.selectExpr("caid", "explode(list_of_h3_id) as h3_id")
    .withColumn("join_key", monotonically_increasing_id())

  // Join to map each H3 ID to its identified region
  explodedDfRegion.as("region").join(explodedDfList.as("list"), Seq("join_key"))
    .select(col("list.caid"), col("list.h3_id"), col("region.h3_id_region"))
}



def mergeH3Region(df: DataFrame, temporalThreshold: Int = 3600): DataFrame = {
  val windowSpec = Window.partitionBy("caid").orderBy("stay_index_h3")
  
  // create lag col
  val dfWithLag = df
    .withColumn("lagged_h3_id_region", lag("h3_id_region", 1).over(windowSpec))
    .withColumn("prev_h3_stay_end_time", lag("h3_stay_end_time", 1).over(windowSpec))
  
  // mark region change or temporal threshold excess
  val dfWithRegionStayId = dfWithLag
    .withColumn("h3_region_stay_id", when(
      col("lagged_h3_id_region") =!= col("h3_id_region") ||
      (unix_timestamp(col("h3_stay_start_time")) - unix_timestamp(col("prev_h3_stay_end_time")) > temporalThreshold),
      1).otherwise(0))
    .withColumn("h3_region_stay_id", sum("h3_region_stay_id").over(windowSpec))

  // aggregate regional stays info 
  val result = dfWithRegionStayId.groupBy("caid", "h3_region_stay_id")
    .agg(
      min("h3_stay_start_time").alias("stay_start_timestamp"),
      max("h3_stay_end_time").alias("stay_end_timestamp"),
      (max("h3_stay_end_time") - min("h3_stay_start_time")).alias("stay_duration"),
      first("h3_id_region").alias("h3_id_region")
    )
  
  result
}

}