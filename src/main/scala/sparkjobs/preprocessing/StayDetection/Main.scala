import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, LongType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.uber.h3core.H3Core
import com.uber.h3core.util.LatLng

import scala.annotation.varargs

import org.apache.log4j.{Level, Logger}

object Main {

  val temporal_threshold_1: Long = 300 // second
  val spatial_threshold: Double = 300 // meter
  val speed_threshold: Double = 6.0 // km/h, if larger than speed_threshold --> passing
  val temporal_threshold_2: Int = 3600 // second
  val resolution: Int = 9
  val region_temporal_threshold: Int = 3600 // second

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StayDetection")
      .master("local[*]")
      .config("spark.driver.memory", "64g")
      .config("spark.executor.memory", "64g")
      //.config("spark.executor.instances", "8")
      //.config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()

    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet("./data/data_201901A") //   16.parquet
      //.repartition(20) //5000
      .withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp")))
      //.limit(300000)

      //dataDF.show(10)
      //dataDF.write.mode(SaveMode.Overwrite).parquet("./tem_output/0-initial_data.parquet")




      println("Processing getStays")
      // 1 getStays
      val (getStays) = stay_detection.getStays(dataDF, spark, temporal_threshold_1, spatial_threshold)
      val getStaysCount = getStays.count()
      println("getStays Count: " + getStaysCount)
      //// getStays.write.mode(SaveMode.Overwrite).parquet("./tem_output/1-stays.parquet").show(10)
      //getStays.write.mode(SaveMode.Overwrite).parquet("./tem_output/1-stays.parquet")

      // val rowCount = dataDF.count()
      // val colCount = dataDF.columns.length
      // println(s"DataFrame shape: ($rowCount, $colCount)")

      println("Processing mapToH3")
      // 2 mapToH3
      val (passing, stays) = stay_detection.mapToH3(getStays, spark, resolution, temporal_threshold_2, speed_threshold)
      // passing.show(10)
      // stays.show(10)
      //passing.write.mode(SaveMode.Overwrite).parquet("./tem_output/3-passing.parquet")
      //stays.write.mode(SaveMode.Overwrite).parquet("./tem_output/3-stays.parquet")

      println("Processing getH3RegionMapping")
      // 3 getH3RegionMapping
      val h3RegionMapping = stay_detection.getH3RegionMapping(stays, spark)
      // h3RegionMapping.show(10)
      //h3RegionMapping.write.mode(SaveMode.Overwrite).parquet("./tem_output/4-h3_region_mapping.parquet")

      println("Processing h3RegionMapping")
      // h3RegionMapping
      val staysJoined = stays
        .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
        .orderBy("caid", "stay_index_h3")
      // staysJoined.show(10)
      //staysJoined.write.mode(SaveMode.Overwrite).parquet("./tem_output/5-stays_joined.parquet")

      println("Processing mergeH3Region")
      // 4 mergeH3Region
      val staysH3Region = stay_detection.mergeH3Region(staysJoined, region_temporal_threshold)
      // staysH3Region.show(10)
      println("Writing document")
      staysH3Region.write.mode(SaveMode.Overwrite).parquet("./tem_output/6-stays_h3_region.parquet")

  }
}