package measures

import org.apache.spark.sql.functions.{col, regexp_extract, sec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object stayDurationDistribution {
  def duration(spark: SparkSession, data: DataFrame, outputPath: String): DataFrame ={
    val hoursDF = data
      .withColumn("stay_duration_in_seconds",
        unix_timestamp(col("stay_end_timestamp")) - unix_timestamp(col("stay_start_timestamp"))
      )
      .withColumn("interval_hours", 
        col("stay_duration_in_seconds") / 3600
      ) // Convert seconds to hours
      .select(
        col("caid"),
        col("interval_hours")
      )

    val binSize = 1.0 //hour

    val binnedData = hoursDF.withColumn("bin_index", floor(col("interval_hours") / lit(binSize)))

    val binnedWithRange = binnedData.withColumn("range", (col("bin_index") + 1) * lit(binSize))

    val resultDF = binnedWithRange.groupBy("range")
      .agg(count("*").alias("count"))
      .orderBy("range")
      .withColumn("probability", col("count") / hoursDF.count())

    resultDF
  }
}
