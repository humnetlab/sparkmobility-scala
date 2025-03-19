// src/main/scala/filter/dataProcessor.scala
package sparkjobs.filtering

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import sparkjobs.filtering.FilterParameters._
import scala.annotation.meta.param

object dataLoadFilter {
  def loadFilteredData(spark: SparkSession, df: DataFrame, params: FilterParametersType): DataFrame = {

    val (startTimeUnix, endTimeUnix) = unixTimeFrame(spark, params)
    val bottomLatitude = params.latitude(0)
    val topLatitude = params.latitude(1)
    val leftLongitude = params.longitude(0)
    val rightLongitude = params.longitude(1)

    val filteredDF = df
      .filter(col("utc_timestamp").between(startTimeUnix, endTimeUnix))
      .filter(
        col("latitude").between(bottomLatitude, topLatitude)
      )
      .filter(
        col("longitude").between(leftLongitude, rightLongitude)
      )

    // add local timestamp
    val timezone = "America/Los_Angeles"

    val dfConvertTime = filteredDF.withColumn(
      "local_time",
      to_utc_timestamp(
        from_unixtime(col("utc_timestamp")).cast("timestamp"),
        timezone
      )
    )

    val dfWithWeekday =
      dfConvertTime.withColumn("day", dayofweek(col("local_time")))

    val rowCount = filteredDF.count()

    dfWithWeekday
  }

  def unixTimeFrame(
      spark: SparkSession,
      params: FilterParametersType
  ): (Long, Long) = {
    // Construct start and end time strings
    val startTimeStr = params.startTimestamp
    val endTimeStr = params.endTimestamp
    // Create DataFrame to calculate Unix timestamps
    val df = spark.sqlContext
      .createDataFrame(
        Seq(
          (startTimeStr, endTimeStr)
        )
      )
      .toDF("start_time", "end_time")

    // Calculate Unix timestamps using Spark SQL functions
    val row = df
      .select(
        unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")
          .as("startTimeUnix"),
        unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss").as("endTimeUnix")
      )
      .first()

    (row.getLong(0), row.getLong(1))
  }
}
