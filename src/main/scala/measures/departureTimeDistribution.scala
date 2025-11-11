package measures

import org.apache.spark.sql.functions.{col, regexp_extract, sec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object departureTimeDistribution {
  def departureTime(spark: SparkSession, data: DataFrame): DataFrame = {
    val resultDF = data
      .groupBy("hour_of_day")
      .agg(count("*").alias("count"))
      .orderBy("hour_of_day")
      .withColumn("probability", col("count") / data.count())
    resultDF
  }
}
