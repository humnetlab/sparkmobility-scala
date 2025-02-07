package pipelines
import org.apache.spark.sql.SparkSession

import org.apache.spark.internal.Logging
import sparkjobs.staydetection.StayDetection
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Row}
import org.apache.spark.sql.SaveMode

import utils.RunMode
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromEnv
import sparkjobs.filtering.dataLoadFilter
import sparkjobs.filtering.h3Indexer
import org.apache.spark.sql.functions._
import sparkjobs.locations.locationType

class PipeExample extends Logging {
  // Class implementation goes here
  val runMode: RunMode = runModeFromEnv()

  val temporal_threshold_1: Long = 300 // second
  val spatial_threshold: Double  = 300 // meter
  val speed_threshold: Double =
    6.0 // km/h, if larger than speed_threshold --> passing
  val temporal_threshold_2: Int      = 3600 // second
  val resolution: Int                = 9
  val region_temporal_threshold: Int = 3600 // second
  val passing                        = true

  def getStaysTest(relativePath: String): Unit = {
    log.info("Creating spark session")
    val currentDir = System.getProperty("user.dir")
    val folderPath = s"$currentDir$relativePath"

    log.info("folder path: " + folderPath)
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)
      .limit(1000000)

    dataDF = dataLoadFilter.loadFilteredData(spark, dataDF)
    dataDF =
      dataDF.withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp")))
    // dataDF = h3Indexer.addIndex(dataDF, resolution = 10)
    // dataDF.show(10)

    /** Stay Detection */
    // dataDF = dataDF.withColumnRenamed("h3_id_region", "h3_index")

    log.info("Processing getStays")
    // dataDF = dataDF.limit(1000000)
    // 1 getStays
    val (getStays) = StayDetection.getStays(
      dataDF,
      spark,
      temporal_threshold_1,
      spatial_threshold
    )
    // val getStaysCount = getStays.count() This takes too much time to process the count
    // log.info("getStays Count: " + getStaysCount)
    log.info("Processing mapToH3")

    // 2 mapToH3
    val (passingResult, stays) = StayDetection.mapToH3(
      getStays,
      resolution,
      temporal_threshold_2,
      passing,
      speed_threshold
    )

    log.info("Processing getH3RegionMapping")
    // 3 getH3RegionMapping
    val h3RegionMapping = StayDetection.getH3RegionMapping(stays, spark)

    log.info("Processing h3RegionMapping")
    // h3RegionMapping
    val staysJoined = stays
      .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
      .orderBy("caid", "stay_index_h3")
    log.info("Processing mergeH3Region")

    // 4 mergeH3Region
    val staysH3Region =
      StayDetection.mergeH3Region(staysJoined, region_temporal_threshold)
    // staysH3Region.show(10)
    log.info("Writing document")
    staysH3Region.write
      .mode(SaveMode.Overwrite)
      .parquet("data/test/6-stays_h3_region.parquet")
    
  }

  def exampleFunction(param: String): String = {
    s"Hello, $param"
  }
  def getHomeWorkLocation(relativePath: String): (DataFrame, DataFrame) = {
    log.info("Creating spark session")
    val currentDir = System.getProperty("user.dir")
    val folderPath = s"$currentDir$relativePath"

    log.info("folder path: " + folderPath)
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)
    val toHexString = udf((index: Long) => java.lang.Long.toHexString(index))
    val indexDF = dataDF.withColumnRenamed("stay_start_timestamp", "local_time")
      .withColumnRenamed("h3_id_region", "h3_index")
      .withColumn("h3_index_hex", toHexString(col("h3_index")))
      .drop(col("h3_index"))
      .withColumnRenamed("h3_index_hex", "h3_index")

    val homeDF = locationType.homeLocation(indexDF)
    val workDF = locationType.workLocation(homeDF)

    (homeDF, workDF)
  }
}
