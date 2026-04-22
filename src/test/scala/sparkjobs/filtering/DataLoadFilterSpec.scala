/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package sparkjobs.filtering

import munit.FunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.jdk.CollectionConverters._

class DataLoadFilterSpec extends FunSuite {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val tmp =
      Option(System.getenv("SPARK_TMPDIR")).getOrElse("/tmp/sparkmobility-test")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DataLoadFilterSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.local.dir", tmp)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def params(start: String, end: String): FilterParametersType =
    FilterParametersType(
      deltaT = 600,
      spatialThreshold = 200.0,
      speedThreshold = 33.0,
      temporalThreshold = 180,
      hexResolution = 10,
      regionalTemporalThreshold = 600,
      passing = false,
      startTimestamp = start,
      endTimestamp = end,
      longitude = Array(-123.0f, -122.0f),
      latitude = Array(37.0f, 38.0f),
      homeToWork = 7,
      workToHome = 17,
      workDistanceLimit = 500,
      workFreqCountLimit = 3,
      timeZone = "UTC"
    )

  private val schema = StructType(
    Seq(
      StructField("utc_timestamp", StringType, nullable = false),
      StructField("latitude", DoubleType, nullable = false),
      StructField("longitude", DoubleType, nullable = false)
    )
  )

  private def df(rows: Seq[(String, Double, Double)]) = {
    val javaRows = rows.map { case (t, lat, lon) => Row(t, lat, lon) }.asJava
    spark.createDataFrame(javaRows, schema)
  }

  test(
    "loadFilteredData respects UTC boundaries regardless of session timezone"
  ) {
    // Session TZ is America/Los_Angeles (UTC-8); if the filter used string
    // coercion via session TZ, it would slide the window by 8h and drop the row.
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try {
      val input = df(
        Seq(
          ("2024-01-01 00:30:00", 37.5, -122.5), // in window
          ("2023-12-31 23:30:00", 37.5, -122.5), // before window
          ("2024-01-01 02:00:00", 37.5, -122.5)  // after window
        )
      )
      val out = DataLoadFilter
        .loadFilteredData(
          input,
          params("2024-01-01 00:00:00", "2024-01-01 01:00:00")
        )
        .collect()
      assertEquals(out.length, 1, s"Expected one row, got ${out.length}")
    } finally {
      spark.conf.set("spark.sql.session.timeZone", "UTC")
    }
  }

  test("loadFilteredData parses Unix-epoch seconds in utc_timestamp") {
    // 2024-01-01 00:30:00 UTC = 1704069000
    val input = df(
      Seq(
        ("1704069000", 37.5, -122.5),
        ("1700000000", 37.5, -122.5) // 2023-11-14, outside window
      )
    )
    val out = DataLoadFilter
      .loadFilteredData(
        input,
        params("2024-01-01 00:00:00", "2024-01-01 01:00:00")
      )
      .collect()
    assertEquals(out.length, 1)
  }

  test("loadFilteredData filters by lat/lon bounding box") {
    val input = df(
      Seq(
        ("2024-01-01 00:30:00", 37.5, -122.5), // inside
        ("2024-01-01 00:30:00", 39.0, -122.5), // lat out
        ("2024-01-01 00:30:00", 37.5, -121.0)  // lon out
      )
    )
    val out = DataLoadFilter
      .loadFilteredData(
        input,
        params("2024-01-01 00:00:00", "2024-01-01 01:00:00")
      )
      .collect()
    assertEquals(out.length, 1)
  }
}
