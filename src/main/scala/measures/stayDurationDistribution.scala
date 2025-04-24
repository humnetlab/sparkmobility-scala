package measures

import org.apache.spark.sql.functions.{col, regexp_extract, sec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object stayDurationDistribution {
  def duration(spark: SparkSession, data: DataFrame, outputPath:String): DataFrame ={
    val hoursDF = data.withColumn(
      "interval_hours", col("stay_duration") / 3600
    ).select("caid", "interval_hours")


    val binSize = 1.0 //hour

    val binnedData = hoursDF.withColumn("bin_index", floor(col("interval_hours") / lit(binSize)))

    val binnedWithRange = binnedData.withColumn("range", (col("bin_index") + 1) * lit(binSize))

    val resultDF = binnedWithRange.groupBy("range")
      .agg(count("*").alias("count"))
      .orderBy("range")
      .withColumn("probability", col("count") / hoursDF.count())

    resultDF.write
      .mode("overwrite") // Overwrites existing data at the output path
      .format("parquet")
      .save(outputPath)
    resultDF.show()
    resultDF
  }
}
