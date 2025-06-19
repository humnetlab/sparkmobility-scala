package measures
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object dailyVisitedLocation {
  def visit(spark: SparkSession, data: DataFrame, outputPath: String): DataFrame = {
    // Retrieve date from local time
    val dataWithDate = data.withColumn("date", to_date(col("local_time")))
    val visits = dataWithDate.groupBy("caid", "date")
      .agg(countDistinct("h3_index").alias("locations"))

    val countVisits = visits.groupBy("locations").count()
    countVisits
  }
}