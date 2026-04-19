/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import utils.FileUtils._
import utils.RunMode.RunMode

object SparkFactory extends Logging {

  private val sparkConfigsPath = "/config/SparkConfig.json"

  // Baseline configs applied to every SparkSession regardless of run mode.
  // Enabling AQE + Kryo here (rather than only in PRODUCTION) so that local runs, tests,
  // and py4j-driven invocations all get the same query optimizer and serializer behavior.
  private val baselineConfigurations: Map[String, String] = Map(
    "spark.serializer"                        -> classOf[KryoSerializer].getName,
    "spark.sql.adaptive.enabled"              -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
    "spark.sql.adaptive.skewJoin.enabled"     -> "true"
  )

  private def getSparkConf(
      runMode: RunMode,
      configOverrides: Map[String, String]
  ): SparkConf = {

    runMode match {
      case RunMode.UNIT_TEST =>
        log.info("Setting up unit test spark configs")
        val unitTestSparkConfigurations = Map[String, String](
          "spark.master"                    -> "local[4]",
          "spark.executor.memory"           -> "4g",
          "spark.app.name"                  -> RunMode.UNIT_TEST.toString,
          "spark.sql.catalogImplementation" -> "in-memory",
          "spark.sql.shuffle.partitions"    -> "1",
          "spark.sql.warehouse.dir"         -> "target/spark-warehouse",
          "log4j.configuration"             -> "log4j.properties"
        )
        new SparkConf().setAll(
          baselineConfigurations ++ unitTestSparkConfigurations ++ configOverrides
        )
      case RunMode.PRODUCTION =>
        log.info("Setting up the production spark configs")
        new SparkConf().setAll(
          baselineConfigurations ++ readJsonAsMap(sparkConfigsPath) ++ configOverrides
        )
    }
  }

  def createSparkSession(
      runMode: RunMode,
      appName: String = "Sample App",
      configOverrides: Map[String, String] = Map.empty[String, String]
  ): SparkSession = {
    val spark = runMode match {
      case RunMode.UNIT_TEST => {
        SparkSession
          .builder()
          .appName(appName)
          .config(getSparkConf(runMode, configOverrides))
          .getOrCreate()
      }
      case RunMode.PRODUCTION => {
        SparkSession
          .builder()
          .appName(appName)
          // .enableHiveSupport()
          .config(getSparkConf(runMode, configOverrides))
          .getOrCreate()
      }
    }
    // SparkUdfs.registerUDFs()(implicitly(spark))
    // hadoopConfigurations(spark)
    spark
  }

//  def main(args: Array[String]): Unit = {
//    logger.info("Hello Spark Factory")
//    val runMode = runModeFromOS()
//    createSparkSession(runMode)
//  }

}
