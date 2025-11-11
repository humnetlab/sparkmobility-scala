/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pipelines

import measures.{
  dailyVisitedLocation,
  departureTimeDistribution,
  extractTrips,
  locationDistribution,
  stayDurationDistribution
}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import sparkjobs.filtering.{FilterParameters, dataLoadFilter}
import sparkjobs.locations.locationType
import sparkjobs.staydetection.StayDetection
import utils.FileUtils
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromEnv

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
      columnNames: Map[String, String] = Map(
        "_c0" -> "caid",
        "_c2" -> "latitude",
        "_c3" -> "longitude",
        "_c5" -> "utc_timestamp"
      ),
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
      columnNames.map { case (originalCol, aliasCol) =>
        col(originalCol).alias(aliasCol)
      }.toSeq: _*
    )

    if (timeFormat != "UNIX") {
      dataDF = dataDF
        .withColumn(
          "utc_timestamp",
          unix_timestamp(
            col("utc_timestamp"),
            timeFormat
          ) // convert timestamp to Unix time
        )
    }
    dataDF = dataDF.na
      .drop(Seq("latitude", "longitude", "utc_timestamp"))
      .withColumn("latitude", col("latitude").cast(DoubleType))
      .withColumn("longitude", col("longitude").cast(DoubleType))
      .withColumn("utc_epoch_s", col("utc_timestamp").cast(LongType))
      .withColumn(
        "utc_timestamp",
        to_timestamp(from_unixtime(col("utc_epoch_s")))
      ) // TimestampType (UTC)
      .drop("utc_epoch_s")

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

    // // Save intermediate getStays to a temporary parquet, free its cache, reload from disk and schedule deletion
    // val tmpGetStaysPath = s"${outputPath.stripSuffix("/")}/_tmp_getStays_${java.util.UUID.randomUUID().toString}"
    // log.info(s"Writing intermediate getStays to $tmpGetStaysPath")
    // getStays.write.mode("overwrite").parquet(tmpGetStaysPath)

    // // Free memory held by the cached DataFrame
    // try {
    //   getStays.unpersist()
    // } catch {
    //   case _: Throwable => log.warn("Warning: failed to unpersist original getStays (might not be cached)")
    // }

    // // Reload the DataFrame from disk (use this reloaded DF for downstream processing if you choose)
    // val getStaysReloaded = FileUtils.readParquetData(tmpGetStaysPath, spark)
    //   .repartition(col("caid"))
    //   .cache()
    // getStaysReloaded.count() // materialize to ensure it's persisted from disk

    // // Schedule deletion of the temporary file when the JVM exits (also attempts immediate delete at end)
    // def deleteTmpPath(): Unit = {
    //   try {
    //     val hadoopConf = spark.sparkContext.hadoopConfiguration
    //     val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //     val p = new org.apache.hadoop.fs.Path(tmpGetStaysPath)
    //     if (fs.exists(p)) {
    //       fs.delete(p, true)
    //       log.info(s"Deleted temporary path: $tmpGetStaysPath")
    //     }
    //   } catch {
    //     case e: Throwable => log.warn(s"Unable to delete temporary path $tmpGetStaysPath: ${e.getMessage}")
    //   }
    // }

    // sys.addShutdownHook {
    //   deleteTmpPath()
    // }

    // // Attempt immediate deletion at the end of the method will be performed after final write (registered above).
    // // Note: downstream code can use `getStaysReloaded` instead of the original `getStays` to ensure disk-backed data is used.

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
      col("local_time"), // already local
      col("h3_index"),
      dayofweek(col("local_time")).as("day_of_week"),
      hour(col("local_time")).as("hour_of_day")
    )

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
      resolution: Int
  ): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF              = FileUtils.readParquetData(folderPath, spark)
    extractTrips.getHomeWorkMatrix(spark, dataDF, resolution, outputPath)
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
    var dataDF = FileUtils.readParquetData(folderPath, spark)
    extractTrips.getODMatrix(spark, dataDF, resolution, outputPath)
  }

  def getDailyVisitedLocation(folderPath: String, outputPath: String) {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = dailyVisitedLocation.visit(spark, dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
  def getLocationDistribution(folderPath: String, outputPath: String) {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = locationDistribution.locate(spark, dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
  def getStayDurationDistribution(folderPath: String, outputPath: String) {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF            = stayDurationDistribution.duration(spark, dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
  def getDepartureTimeDistribution(folderPath: String, outputPath: String) {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "TimeGeoPipe")
    var dataDF              = FileUtils.readParquetData(folderPath, spark)
    val resultDF = departureTimeDistribution.departureTime(spark, dataDF)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
