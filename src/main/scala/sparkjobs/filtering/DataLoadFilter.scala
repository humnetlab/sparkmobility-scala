/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package sparkjobs.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

object DataLoadFilter {

  // Window boundaries and the utc_timestamp column are normalized to real UTC
  // instants (via lit(Timestamp) and timestamp_seconds/parseUtcUdf), so
  // filtering is independent of spark.sql.session.timeZone. startTimestamp /
  // endTimestamp in the JSON config and any ISO-string values in the
  // utc_timestamp column are interpreted as UTC wall-clock regardless of JVM
  // default zone.
  private val tsFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private def parseUtc(s: String): Timestamp =
    Timestamp.from(LocalDateTime.parse(s, tsFormat).toInstant(ZoneOffset.UTC))

  private val parseUtcUdf = udf { (s: String) =>
    if (s == null) null else parseUtc(s)
  }

  def castTimestamp(c: Column): Column =
    when(
      c.cast("string").rlike("^[0-9]+$"),
      timestamp_seconds(c.cast("long"))
    ).otherwise(parseUtcUdf(c.cast("string")))

  def loadFilteredData(
      df: DataFrame,
      params: FilterParametersType
  ): DataFrame = {
    val start = parseUtc(params.startTimestamp)
    val end   = parseUtc(params.endTimestamp)

    df.withColumn("utc_timestamp", castTimestamp(col("utc_timestamp")))
      .filter(col("utc_timestamp").between(lit(start), lit(end)))
      .filter(col("latitude").between(params.latitude(0), params.latitude(1)))
      .filter(
        col("longitude").between(params.longitude(0), params.longitude(1))
      )
  }
}
