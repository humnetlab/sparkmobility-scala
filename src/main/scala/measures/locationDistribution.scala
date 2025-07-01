package measures
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object locationDistribution {
  def locate(spark: SparkSession, data: DataFrame): DataFrame ={
    val location = data.groupBy("caid")
      .agg(
        first("home_h3_index").as("home_index"),
        first("work_h3_index").as("work_index"),
      )
    location
  }
}
