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
import sparkjobs.filtering.FilterParameters

import utils.RunMode
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromEnv
import utils.FileUtils
import scala.annotation.meta.param

class PipeExample extends Logging {
  // Class implementation goes here
  val runMode: RunMode = runModeFromEnv()

  def getStays(fullPath: String, outputPath: String, configFile: String = "src/main/resources/config/DefaultParameters.json"): Unit = {
    log.info("Creating spark session")
    val params = FilterParameters.fromJsonFile(configFile)
    val folderPath = s"$fullPath"

    log.info("folder path: " + folderPath)
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")
    Logger.getRootLogger.setLevel(Level.WARN)
    var dataDF = FileUtils.readParquetData(folderPath, spark)
    // dataDF = dataDF.sample(withReplacement = false, fraction = 0.1, seed = 42)

    // dataDF = dataDF.limit(1000000)
    dataDF = dataDF.select(
      col("_c0").alias("caid"),
      col("_c2").alias("latitude"),
      col("_c3").alias("longitude"),
      col("_c5").alias("utc_timestamp")
    )
    dataDF = dataLoadFilter.loadFilteredData(spark, dataDF, params)
    
    dataDF = dataDF.select(
      col("caid"),
      col("latitude"),
      col("longitude"),
      to_timestamp(col("utc_timestamp")).as("utc_timestamp")
    )

    /** Stay Detection */
    dataDF = dataDF.repartition(col("caid"))
    log.info("Processing getStays")
    // 1 getStays
    val (getStays) = StayDetection.getStays(
      dataDF,
      spark,
      params.deltaT,
      params.spatialThreshold
    ).cache()
    getStays.count()
    // val getStaysCount = getStays.count() This takes too much time to process the count
    // log.info("getStays Count: " + getStaysCount)
    log.info("Processing mapToH3")
    // getStays.count() == 309430

    // 2 mapToH3
    val (passingResult, stays) = StayDetection.mapToH3(
      getStays,
      params.hexResolution,
      params.regionalTemporalThreshold,
      params.passing,
      params.speedThreshold
    )
    // 38330 stays

    
    log.info("Processing getH3RegionMapping")
    // 3 getH3RegionMapping
    val h3RegionMapping = StayDetection.getH3RegionMapping(stays, spark)

    log.info("Processing h3RegionMapping")
    // h3RegionMapping
    val staysJoined = stays
      .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
    log.info("Processing mergeH3Region")
    
    // 4 mergeH3Region
    val staysH3Region =
      StayDetection.mergeH3Region(staysJoined, params.regionalTemporalThreshold)
    getStays.unpersist()
    // staysH3Region.show(10)
    log.info("Writing document")
    staysH3Region.write
      .parquet(outputPath)
    
  }
  def exampleFunction(param: String): String = {
    s"Hello, $param"
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
