package pipelines

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Row}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import sparkjobs.filtering.dataLoadFilter
import sparkjobs.filtering.h3Indexer
import sparkjobs.locations.locationType
import sparkjobs.staydetection.StayDetection

import utils.RunMode
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromEnv
import utils.FileUtils

class PipeExample extends Logging {
  // Class implementation goes here
  val runMode: RunMode = runModeFromEnv()

  val temporal_threshold_1: Long = 300 // second
  val spatial_threshold: Double  = 300 // meter
  val speed_threshold: Double =
    6.0 // km/h, if larger than speed_threshold --> passing
  val temporal_threshold_2: Int      = 3600 // second
  val resolution: Int                = 8
  val region_temporal_threshold: Int = 3600 // second
  val passing                        = true

  def getStaysTest(fullPath: String): Unit = {
    log.info("Creating spark session")
    // val currentDir = System.getProperty("user.dir")
    val folderPath = s"$fullPath"

    log.info("folder path: " + folderPath)
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")
    Logger.getRootLogger.setLevel(Level.WARN)
    var dataDF = FileUtils.readParquetData(folderPath, spark)
    dataDF = dataDF.select(
      col("_c0").alias("caid"),
      col("_c2").alias("latitude"),
      col("_c3").alias("longitude"),
      col("_c5").alias("utc_timestamp")
    )
    // dataDF = dataDF.sample(withReplacement = false, fraction = 0.05, seed = 42)

    dataDF = dataLoadFilter.loadFilteredData(spark, dataDF)
    dataDF = dataDF.withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp")))

    /** Stay Detection */

    log.info("Processing getStays")
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
    // getStays.count() == 309430

    // 2 mapToH3
    val (passingResult, stays) = StayDetection.mapToH3(
      getStays,
      resolution,
      temporal_threshold_2,
      passing,
      speed_threshold
    )
    // 38330 stays
    val staysCached = stays.repartition(col("caid")).cache()

    staysCached.count()
    log.info("Processing getH3RegionMapping")
    // 3 getH3RegionMapping
    val h3RegionMapping = StayDetection.getH3RegionMapping(staysCached, spark)

    log.info("Processing h3RegionMapping")
    // h3RegionMapping
    val staysJoined = staysCached
      .join(broadcast(h3RegionMapping), Seq("caid", "h3_id"), "left")
    log.info("Processing mergeH3Region")
    
    // 4 mergeH3Region
    val staysH3Region =
      StayDetection.mergeH3Region(staysJoined, region_temporal_threshold)
    staysCached.unpersist()
    // staysH3Region.show(10)
    log.info("Writing document")
    staysH3Region.cache().write
      .parquet("/data_1/quadrant/output/stays_full.parquet")
  }

  def exampleFunction(param: String): String = {
    s"Hello, $param"
  }
  def appendNeededColumns(folderPath: String): Unit = {
    log.info("folder path: " + folderPath)
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")
    // var DAY = 0
    // var SECOND = 3
    val schema = StructType(Seq(
      StructField("caid", LongType, nullable = true),
      StructField("h3_region_stay_id", LongType, nullable = true),
      StructField("stay_start_timestamp", TimestampType, nullable = true),
      StructField("stay_end_timestamp", TimestampType, nullable = true),
      StructField("stay_duration", DayTimeIntervalType(0, 3)	, nullable = true),
      StructField("h3_id_region", StringType, nullable = true)
    ))

    var dataDF = spark.read
      .schema(schema)
      .parquet(folderPath)
    

    // val toHexString = udf((index: Long) => java.lang.Long.toHexString(index))
    var extendDF = dataDF
      .withColumn("local_time", col("stay_start_timestamp"))
      .withColumn("h3_index", expr("hex(cast(h3_id_region as bigint))"))
      .withColumn("day_of_week", dayofweek(col("local_time")))
      .withColumn("hour_of_day", hour(col("local_time")))
    extendDF.write
      .parquet("/data_1/quadrant/output/staysExtent.parquet")
  }
  def getHomeWorkLocation(folderPath: String): Unit = {
    log.info("Creating spark session")
    // val currentDir = System.getProperty("user.dir")
    // val folderPath = s"$currentDir$relativePath"

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
    // val workDF = locationType.workLocation(homeDF)
    
    log.info("Writing Home document")
    homeDF.coalesce(50).write
      .mode(SaveMode.Overwrite)
      .parquet("/home/christopher/humnetmobility/data/home.parquet")
    // log.info("Writing Work document")
    // workDF.repartition(16).write
    //   .option("compression", "snappy") 
    //   .mode(SaveMode.Overwrite)
    //   .parquet("/home/christopher/humnetmobility/data/work.parquet")
  }
}
