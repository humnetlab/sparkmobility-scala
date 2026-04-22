/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pipelines

import measures.{
  DailyVisitedLocation,
  DepartureTimeDistribution,
  ExtractTrips,
  LocationDistribution,
  StayDurationDistribution
}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import sparkjobs.filtering.{DataLoadFilter, FilterParametersType}
import sparkjobs.locations.LocationType
import sparkjobs.staydetection.StayDetection
import utils.FileUtils
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromEnv

class Pipelines extends Logging {
  val runMode: RunMode = runModeFromEnv()

  def getStays(
      fullPath: String,
      outputPath: String,
      timeFormat: String,
      inputFormat: String,
      delim: String,
      ifHeader: String,
      columnNames: Map[String, String],
      params: FilterParametersType
  ): Unit = {
    log.info("Creating spark session")
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

    // Rename columns using the columnNames map
    dataDF = dataDF.select(
      columnNames.map { case (originalCol, aliasCol) =>
        col(originalCol).alias(aliasCol)
      }.toSeq: _*
    )

    // For non-UNIX inputs, parse the string as UTC wall-clock and hand off
    // epoch seconds (a Long) to DataLoadFilter.castTimestamp, which then takes
    // the timestamp_seconds branch — session-TZ-independent. Note: we do NOT
    // use Spark's unix_timestamp() here because it parses in the session TZ,
    // which would shift the window under non-UTC sessions. We also don't
    // round-trip through to_timestamp/from_unixtime: castTimestamp would
    // re-parse the session-TZ-formatted string as UTC and double-shift.
    if (timeFormat != "UNIX") {
      dataDF = dataDF.withColumn(
        "utc_timestamp",
        DataLoadFilter.parseCustomUtcUdf(timeFormat)(col("utc_timestamp"))
      )
    }
    dataDF = dataDF.na
      .drop(Seq("latitude", "longitude", "utc_timestamp"))
      .withColumn("latitude", col("latitude").cast(DoubleType))
      .withColumn("longitude", col("longitude").cast(DoubleType))

    dataDF = DataLoadFilter.loadFilteredData(dataDF, params)

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
    val getStays = StayDetection
      .getStays(
        dataDF,
        spark,
        params.deltaT,
        params.spatialThreshold
      )
    log.info("Processing mapToH3")

    // 2 mapToH3
    val (_, staysUncached) = StayDetection.mapToH3(
      getStays,
      params.hexResolution,
      params.regionalTemporalThreshold,
      params.passing,
      params.speedThreshold
    )
    val stays = staysUncached.cache()

    log.info("Processing getH3RegionMapping")
    // 3 getH3RegionMapping
    val h3RegionMapping = StayDetection.getH3RegionMapping(stays)

    log.info("Processing h3RegionMapping")
    // Let Spark pick the join strategy: its auto-broadcast threshold plus AQE
    // SMJ->Broadcast promotion handles the small-mapping case, while large
    // h3RegionMapping tables (seen on >1e8-row inputs) stay as SMJ. An
    // explicit broadcast() hint here would force BroadcastExchangeExec to
    // collect the right side to the driver and fail with maxResultSize.
    val staysJoined = stays
      .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
    log.info("Processing mergeH3Region")

    // 4 mergeH3Region
    val staysH3Region =
      StayDetection.mergeH3Region(staysJoined, params)
    stays.unpersist()
    log.info("Writing document")
    staysH3Region.write
      .mode("overwrite")
      .parquet(outputPath)

  }
  def getHomeWorkLocation(
      folderPath: String,
      outputPath: String,
      params: FilterParametersType
  ): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    Logger.getRootLogger.setLevel(Level.WARN)
    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet(folderPath)

    dataDF = dataDF.select(
      col("caid"),
      col("h3_region_stay_id"),
      col("stay_start_timestamp"),
      col("stay_end_timestamp"),
      col("stay_duration"),
      col("h3_id_region"),
      col("local_time"), // already local
      col("h3_index"),
      dayofweek(col("local_time")).as("day_of_week"),
      hour(col("local_time")).as("hour_of_day")
    )

    val homeDF = LocationType.homeLocation(dataDF, params)
    val workDF = LocationType.workLocation(homeDF, params)

    log.info("Writing home and work labeled stays")
    workDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"$outputPath")
  }
  def getODMatrix(
      folderPath: String,
      outputPath: String,
      resolution: Int
  ): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    val dataDF              = FileUtils.readParquetData(folderPath, spark)
    ExtractTrips.getHomeWorkMatrix(dataDF, resolution, outputPath)
  }

  def getFullODMatrix(
      folderPath: String,
      outputPath: String,
      resolution: Int
  ): Unit = {

    /** Generates a full origin-destination (OD) matrix from parquet data.
      *
      * This function reads parquet data from the specified folder, processes it
      * to extract trip information, and creates an origin-destination matrix at
      * the specified resolution. The resulting OD matrix is saved to the given
      * output path.
      *
      * @param folderPath
      *   The path to the folder containing input parquet data files
      * @param outputPath
      *   The path where the generated OD matrix and full trips will be saved
      * @param resolution
      *   The spatial resolution for the OD matrix
      */

    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    Logger.getRootLogger.setLevel(Level.WARN)
    val dataDF = FileUtils.readParquetData(folderPath, spark)
    ExtractTrips.getODMatrix(dataDF, resolution, outputPath)
  }

  def getDailyVisitedLocation(folderPath: String, outputPath: String): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    val dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = DailyVisitedLocation.visit(dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
  def getLocationDistribution(folderPath: String, outputPath: String): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    val dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = LocationDistribution.locate(dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
  def getStayDurationDistribution(
      folderPath: String,
      outputPath: String
  ): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    val dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = StayDurationDistribution.duration(dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
  def getDepartureTimeDistribution(
      folderPath: String,
      outputPath: String
  ): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    val dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = DepartureTimeDistribution.departureTime(dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
