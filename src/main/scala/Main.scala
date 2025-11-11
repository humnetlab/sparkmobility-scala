/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.timegeo
import org.apache.spark.internal.Logging
import pipelines.Pipelines
import utils.RunMode.RunMode
import utils.TestUtils.runModeFromEnv

object Main extends Logging {
  val runMode: RunMode = runModeFromEnv()

  // def repartitionParquet(spark: SparkSession, path: String): Unit = {
  //   val schema = StructType(Seq(
  //     StructField("id", StringType, nullable = false),       // _c0
  //     StructField("unknown", StringType, nullable = true),   // _c1
  //     StructField("lat", DoubleType, nullable = false),       // _c2
  //     StructField("lon", DoubleType, nullable = false),      // _c3
  //     StructField("altitude", DoubleType, nullable = true),      // _c4
  //     StructField("unixtime", LongType, nullable = false), // _c5
  //   ))
  //   var dataDF = FileUtils.readTextData(path, schema, spark)

  def main(args: Array[String]): Unit = {

    log.info("Creating spark session and running the job")
    var pipe = new Pipelines()

    // val basePath = "/Users/chris/Documents/quadrant/sample/"
    // val folders = new File(basePath).listFiles.filter(_.isDirectory)
    // val spark: SparkSession = createSparkSession(runMode, "FilterJobCSV")
    // folders.foreach { folder =>
    //   repartitionParquet(spark, folder.getAbsolutePath)
    // }
    var input: String =
      "/data_1/quadrant/output/output_fixed_timezones/work_locations.parquet"
    var output: String = "data_1/quadrant/output/output_fixed_timezones"
    // pipe.getStays(input, output)
    // pipe.getHomeWorkLocation(output, "/data_1/quadrant/output")
    pipe.getFullODMatrix(input, output, 8)
  }
}
