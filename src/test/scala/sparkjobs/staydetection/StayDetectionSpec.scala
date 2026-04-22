/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package sparkjobs.staydetection

import munit.FunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.jdk.CollectionConverters._

class StayDetectionSpec extends FunSuite {

  // Shared SparkSession across tests. Starting a JVM + SparkContext per test is prohibitive;
  // we create one in beforeAll and tear it down in afterAll.
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val tmp =
      Option(System.getenv("SPARK_TMPDIR")).getOrElse("/tmp/sparkmobility-test")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StayDetectionSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.local.dir", tmp)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("sequentialStayDetection returns [1] for a single-row group") {
    val out = StayDetection
      .sequentialStayDetection(Iterator((37.0, -122.0)), threshold = 300)
      .toList
    assertEquals(out, List(1))
  }

  test("sequentialStayDetection marks a moved point as new stay") {
    // Two points ~1.5 km apart (> default 300m threshold) → second row should start a new stay (flag 1).
    val rows = Iterator((37.7749, -122.4194), (37.7850, -122.4350))
    val out =
      StayDetection.sequentialStayDetection(rows, threshold = 300).toList
    assertEquals(out, List(1, 1))
  }

  test("sequentialStayDetection merges co-located points into one stay") {
    // Three points within ~10m of each other → only the first row starts a new stay.
    val rows = Iterator(
      (37.7749, -122.4194),
      (37.77491, -122.41941),
      (37.77492, -122.41942)
    )
    val out =
      StayDetection.sequentialStayDetection(rows, threshold = 300).toList
    assertEquals(out, List(1, 0, 0))
  }

  test("getStays collapses a home-like burst into a single stay") {
    // 5 samples, all within ~10m of 37.7749,-122.4194, 60s apart. delta_t=600 means no temporal break.
    val base = java.sql.Timestamp.valueOf("2024-01-01 08:00:00")
    val rows = (0 until 5).map { i =>
      val ts  = new java.sql.Timestamp(base.getTime + i * 60 * 1000L)
      val lat = 37.7749 + i * 1e-5
      val lon = -122.4194 + i * 1e-5
      Row("user-1", lat, lon, ts)
    }
    val schema = StructType(
      Seq(
        StructField("caid", StringType, nullable = false),
        StructField("latitude", DoubleType, nullable = false),
        StructField("longitude", DoubleType, nullable = false),
        StructField("utc_timestamp", TimestampType, nullable = false)
      )
    )
    val df = spark.createDataFrame(rows.asJava, schema)

    val result = StayDetection
      .getStays(df, spark, delta_t = 600, threshold = 300)
      .collect()
    assertEquals(result.length, 1, s"Expected one stay, got ${result.length}")
    val row = result.head
    assertEquals(row.getAs[String]("caid"), "user-1")
    assert(
      row.getAs[Double]("latitude") > 37.77 && row.getAs[Double](
        "latitude"
      ) < 37.78
    )
    assert(
      row.getAs[Double]("longitude") < -122.41 && row.getAs[Double](
        "longitude"
      ) > -122.42
    )
  }
}
