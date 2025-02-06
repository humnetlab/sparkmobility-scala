// src/main/scala/filter/dataProcessor.scala
package sparkjobs.filtering

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import sparkjobs.filtering.FilterParameters._

object dataLoadFilter {
  def loadFilteredData(spark: SparkSession, df: DataFrame): DataFrame = {

    // Example of how to filter data
    // val start_time = "2000-10-01 00:00:00"
    // val end_time = "2024-10-02 23:59:59"

    // val startTimeUnix = unix_timestamp(lit(start_time), "yyyy-MM-dd HH:mm:ss")
    // val endTimeUnix = unix_timestamp(lit(end_time), "yyyy-MM-dd HH:mm:ss")
    val params = FilterParameters.fromJsonFile(
      "src/main/resources/config/DefaultParameters.json"
    )

    val (startTimeUnix, endTimeUnix) = unixTimeFrame(spark, params)
    val filteredDF = df
      .filter(col("utc_timestamp").between(startTimeUnix, endTimeUnix))
      .filter(
        col("latitude").between(params.bottomLatitude, params.topLatitude)
      )
      .filter(
        col("longitude").between(params.leftLongitude, params.rightLongitude)
      )
    // .filter(col("caid") === params.user_id)

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
      params: FilterParameters
  ): (Long, Long) = {
    // Construct start and end time strings
    val startTimeStr =
      f"${params.start_year}-${params.start_month}%02d-${params.start_day}%02d ${params.start_hour}%02d:${params.start_minute}%02d:${params.start_second}%02d"
    val endTimeStr =
      f"${params.end_year}-${params.end_month}%02d-${params.end_day}%02d ${params.end_hour}%02d:${params.end_minute}%02d:${params.end_second}%02d"
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
