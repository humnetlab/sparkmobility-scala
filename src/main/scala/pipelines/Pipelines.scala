/**
 * Implements data processing pipelines for mobility data analysis.
 * 
 * This class provides methods to process spatiotemporal data to identify stays,
 * extract home and work locations, and generate origin-destination matrices.
 * It leverages Spark for distributed processing of potentially large datasets.
 * 
 * The pipelines implemented include:
 * - Stay detection from raw mobility data
 * - Identification of home and work locations
 * - Origin-destination matrix calculation between home and work locations
 * - Full origin-destination matrix calculation for all locations
 * 
 * All methods utilize Apache Spark and rely on configuration parameters that can be
 * supplied through JSON configuration files.
 */
package pipelines

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Row}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import measures.extractTrips
import measures.dailyVisitedLocation
import measures.locationDistribution
import measures.stayDurationDistribution
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

class Pipelines extends Logging {
  // Class implementation goes here
  val runMode: RunMode = runModeFromEnv()

  def getStays(
      fullPath: String,
      outputPath: String,
      timeFormat: String,
      inputFormat: String,
      delim: String,
      ifHeader: String, 
      columnNames: Map[String, String] = Map("_c0" -> "caid", "_c2" -> "latitude", "_c3" -> "longitude", "_c5" -> "utc_timestamp"),
      configFile: String = "src/main/resources/config/DefaultParameters.json"
  ): Unit = {
    log.info("Creating spark session")
    val params     = FilterParameters.fromJsonFile(configFile)
    val folderPath = s"$fullPath"

    log.info("folder path: " + folderPath)
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    Logger.getRootLogger.setLevel(Level.WARN)
    var dataDF = if (inputFormat == "parquet") {
        FileUtils.readParquetData(folderPath, spark)
    } else if (inputFormat == "csv") {
        FileUtils.readCSVData(folderPath, delim, ifHeader, spark)
    } else {
        throw new IllegalArgumentException("Unsupported input format")
    }
    
    // var dataDF = FileUtils.readParquetData(folderPath, spark)
    // var dataDF = FileUtils.readCSVData(folderPath, spark)
    // dataDF = dataDF.sample(withReplacement = false, fraction = 0.1, seed = 42)

    // Rename columns using the columnNames map
    dataDF = dataDF.select(
      columnNames.map { case (originalCol, aliasCol) => col(originalCol).alias(aliasCol) }.toSeq: _*
    )

    if (timeFormat != "UNIX") {
      dataDF = dataDF
        .withColumn(
          "utc_timestamp",
          unix_timestamp(col("utc_timestamp"), timeFormat) // convert timestamp to Unix time
        )
    }
    dataDF = dataDF
      .withColumn("latitude", col("latitude").cast(DoubleType))
      .withColumn("longitude", col("longitude").cast(DoubleType))

    dataDF = dataLoadFilter.loadFilteredData(spark, dataDF, params)

    dataDF = dataDF.select(
      col("caid"),
      col("latitude"),
      col("longitude"),
      col("utc_timestamp")
    )

    /** Stay Detection */
    dataDF = dataDF.repartition(col("caid"))
    log.info("Processing getStays")
    // 1 getStays
    val (getStays) = StayDetection
      .getStays(
        dataDF,
        spark,
        params.deltaT,
        params.spatialThreshold
      )
      .cache()
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
      StayDetection.mergeH3Region(staysJoined, params)
    getStays.unpersist()
    // staysH3Region.show(10)
    log.info("Writing document")
    staysH3Region.write
      .mode("overwrite")
      .parquet(outputPath)

  }
  def exampleFunction(param: String): String = {
    // Example function implementation, functions as test
    s"Hello, $param"
  }
  def getHomeWorkLocation(
      folderPath: String,
      outputPath: String,
      configFile: String = "src/main/resources/config/DefaultParameters.json"
  ): Unit = {
    log.info("Creating spark session")
    val params              = FilterParameters.fromJsonFile(configFile)
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    Logger.getRootLogger.setLevel(Level.WARN)
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)

    // small patch remove
    dataDF = dataDF.select(
        col("caid"),
        col("h3_region_stay_id"),
        col("stay_start_timestamp"),
        col("stay_end_timestamp"),
        col("stay_duration"),
        col("h3_id_region"),
        from_utc_timestamp(col("local_time"), params.timeZone).alias("local_time"),
        col("h3_index"),
        dayofweek(from_utc_timestamp(col("local_time"), params.timeZone)).alias("day_of_week"),
        hour(from_utc_timestamp(col("local_time"), params.timeZone)).alias("hour_of_day")
      )
    //

    val homeDF = locationType.homeLocation(dataDF, params)
    val workDF = locationType.workLocation(homeDF, params)

    // log.info("Writing Home document")
    // homeDF.write
    //   .mode(SaveMode.Overwrite)
    //   .parquet(s"$outputPath/home_locations.parquet")
    log.info("Writing home and work labeled stays")
    workDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"$outputPath")
  }
  def getODMatrix(
      folderPath: String,
      outputPath: String,
      resolution: Int = 8,
  ): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF = FileUtils.readParquetData(folderPath, spark)
    val odMatrix = extractTrips.getHomeWorkMatrix(spark, dataDF, resolution, outputPath)
  }
  
  def getFullODMatrix(
      folderPath: String,
      outputPath: String,
      resolution: Int = 8,
  ): Unit = {
    /**
   * Generates a full origin-destination (OD) matrix from parquet data.
   *
   * This function reads parquet data from the specified folder, processes it to extract
   * trip information, and creates an origin-destination matrix at the specified resolution.
   * The resulting OD matrix is saved to the given output path.
   *
   * @param folderPath The path to the folder containing input parquet data files
   * @param outputPath The path where the generated OD matrix and full trips will be saved
   * @param resolution The spatial resolution for the OD matrix, default is 8
   */
    
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    Logger.getRootLogger.setLevel(Level.WARN)
    var dataDF = FileUtils.readParquetData(folderPath, spark)
    val odMatrix = extractTrips.getODMatrix(spark, dataDF, resolution, outputPath)
  }
  def getDailyVisitedLocation(folderPath: String, outputPath: String){
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)
    dailyVisitedLocation.visit(spark, dataDF, outputPath)
  }
  def getLocationDistribution(folderPath: String, outputPath: String){
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)
    locationDistribution.locate(spark, dataDF, outputPath)
  }
  def getStayDurationDistribution(folderPath: String, outputPath: String){
    /**
     * Computes the distribution of stay durations from mobility data and saves the results.
     *
     * This function reads mobility data from a Parquet file, processes it to calculate
     * the distribution of stay durations, and saves the results to the specified output path.
     *
     * @param folderPath The path to the folder containing Parquet files with mobility data
     * @param outputPath The path where the stay duration distribution results will be saved
     */
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)
    stayDurationDistribution.duration(spark, dataDF, outputPath)
  }
}
