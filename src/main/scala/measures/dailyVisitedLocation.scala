package measures

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object dailyVisitedLocation {
  def visit(spark: SparkSession, data: DataFrame): DataFrame = {
    // Retrieve date from local time
    val dataWithDate = data.withColumn("date", to_date(col("local_time")))
    val visits = dataWithDate.groupBy("caid", "date")
      .agg(countDistinct("h3_index").alias("locations"))

    val countVisits = visits.groupBy("locations").count()

    val curDir = System.getProperty("user.dir")
    val relPath = "/data/intermediateResults/dailyVisitedLocation"
    val outputPath = curDir + relPath

    countVisits.write
      .mode("overwrite")
      .format("parquet")
      .save(outputPath)

    visits
  }
}