import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, LongType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.uber.h3core.H3Core
import com.uber.h3core.util.LatLng

import scala.annotation.varargs



object Main {

  val delta_t: Long = 300
  val stay_threshold: Double = 300
  val boundary = Array(33.5, 34.5, -118.8, -118.0) // LA, SF -> Array(37.5, 38.0, -122.5, -121.8)
  val speed_threshold: Double = 30.0 //
  val temporal_threshold: Int = 3600
  val resolution: Int = 9
  val filter_passing: Boolean = true
  val region_temporal_threshold: Int = 3600

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StayDetection")
      .master("local[*]")
      //.config("spark.driver.memory", "32g")
      //.config("spark.executor.memory", "32g")
      //.config("spark.executor.instances", "8")
      //.config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()
    
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet("./data/16.parquet")
      //.parquet("./data/data_201901A")
      //.repartition(200) //5000
      .withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp")))
      //.limit(300000)


    // dataDF.show(10)
    // dataDF.write.mode(SaveMode.Overwrite).parquet("./tem_output/0-initial_data.parquet")

    // 1 getStays
    dataDF = stay_detection.getStays(dataDF, spark, delta_t, stay_threshold)
    // dataDF.show(10)
    // dataDF.write.mode(SaveMode.Overwrite).parquet("./tem_output/1-stays.parquet")

    // 2 getRegionalUsers
    dataDF = stay_detection.getRegionalUsers(dataDF, boundary)
    // dataDF.show(10)
    // dataDF.write.mode(SaveMode.Overwrite).parquet("./tem_output/2-regional_users.parquet")

    // 3 mapToH3
    val (passing, stays) = stay_detection.mapToH3(dataDF, resolution, temporal_threshold, filter_passing, speed_threshold)
    // passing.show(10)
    // stays.show(10)
    // passing.write.mode(SaveMode.Overwrite).parquet("./tem_output/3-passing.parquet")
    // stays.write.mode(SaveMode.Overwrite).parquet("./tem_output/3-stays.parquet")

    // 4 getH3RegionMapping
    val h3RegionMapping = stay_detection.getH3RegionMapping(stays, spark)
    // h3RegionMapping.show(10)
    // h3RegionMapping.write.mode(SaveMode.Overwrite).parquet("./tem_output/4-h3_region_mapping.parquet")

    // 5 h3RegionMapping
    val staysJoined = stays
      .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
      .orderBy("caid", "stay_index_h3")
    // staysJoined.show(10)
    // staysJoined.write.mode(SaveMode.Overwrite).parquet("./tem_output/5-stays_joined.parquet")

    // 6 mergeH3Region
    val staysH3Region = stay_detection.mergeH3Region(staysJoined, region_temporal_threshold)
    // staysH3Region.show(10)
    // staysH3Region.write.mode(SaveMode.Overwrite).parquet("./tem_output/6-stays_h3_region.parquet")
  
  }



}