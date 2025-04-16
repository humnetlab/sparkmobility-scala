// src/main/scala/filter/dataProcessor.scala
package sparkjobs.filtering

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import sparkjobs.filtering.FilterParameters._
import scala.annotation.meta.param
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset


object dataLoadFilter {
  def castToUTCTimestamp(df: DataFrame, columnName: String, timeZone: String = "UTC"): DataFrame = {
    df.withColumn(
      columnName,
      when(
        col(columnName).cast("string").rlike("^[0-9]+$"), // Check if it's a Unix timestamp
        to_utc_timestamp(from_unixtime(col(columnName).cast("long")), timeZone) // Convert Unix to UTC timestamp
      ).otherwise(
        to_utc_timestamp(col(columnName).cast("timestamp"), timeZone) // Cast to UTC timestamp if not Unix
      )
    )
  }
 
  def loadFilteredData(spark: SparkSession, df: DataFrame, params: FilterParametersType): DataFrame = {

    val dfCast = castToUTCTimestamp(df, "utc_timestamp")
    val (startTimeUnix, endTimeUnix) = timeFrame(spark, params)
    val bottomLatitude = params.latitude(0)
    val topLatitude = params.latitude(1)
    val leftLongitude = params.longitude(0)
    val rightLongitude = params.longitude(1)
    
    val filteredDF = dfCast
      .filter(col("utc_timestamp").between(startTimeUnix, endTimeUnix))
      .filter(
        col("latitude").between(bottomLatitude, topLatitude)
      )
      .filter(
        col("longitude").between(leftLongitude, rightLongitude)
      )

    filteredDF
  }

  def timeFrame(
      spark: SparkSession,
      params: FilterParametersType
  ): (String, String) = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val startTimeUTC = LocalDateTime.parse(params.startTimestamp, formatter)
    .atOffset(ZoneOffset.UTC)
    .format(formatter)
    val endTimeUTC = LocalDateTime.parse(params.endTimestamp, formatter)
      .atOffset(ZoneOffset.UTC)
      .format(formatter)
    (startTimeUTC, endTimeUTC)
  }
}
