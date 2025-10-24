package measures
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object locationDistribution {
  def locate(spark: SparkSession, data: DataFrame): DataFrame ={
    val location = data.groupBy("caid")
      .agg(
        first("home_h3_index").as("home_index"), // Replace with your columns
        first("work_h3_index").as("work_index"),
      )
    // Write the DataFrame in Parquet format
    location
  }
}
