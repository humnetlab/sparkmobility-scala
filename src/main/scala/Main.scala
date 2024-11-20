import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import dataPreprocessing.dataLoadFilter
import dataPreprocessing.h3Indexer
import dataPreprocessing.locationType
import com.uber.h3core.H3Core
import measures.{dailyVisitedLocation, extractTrips, locationDistribution, stayDurationDistribution}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Filter")
      .master("local[*]")
      .config("spark.driver.memory", "8g")  // Increase based on system capacity
      .config("spark.executor.memory", "8g")
      .getOrCreate()

    val toHexString = udf((index: Long) => java.lang.Long.toHexString(index))
    val currentDir = System.getProperty("user.dir")
    val relativePath = "/data/12daysStayData"
    val folderPath = currentDir+relativePath
    val df = spark.read.parquet(folderPath)
    val indexDF = df.withColumnRenamed("stay_start_timestamp", "local_time")
      .withColumnRenamed("h3_id_region", "h3_index")
      .withColumn("h3_index_hex", toHexString(col("h3_index")))
      .drop(col("h3_index"))
      .withColumnRenamed("h3_index_hex", "h3_index")

    /**Data Preprocessing*/
    val filteredDF = dataLoadFilter.loadFilteredData(spark)
    val h3indexDF = h3Indexer.addIndex(spark, filteredDF, resolution = 10)
    val dfWithWeekday = df.withColumn("day", dayofweek(col("stay_start_timestamp")))

    /**Stay Detection*/
    val stayDF = dfWithWeekday.withColumnRenamed("h3_id_region", "h3_index")

    /**Location Type Extraction*/
    val homeDF = locationType.homeLocation(spark, indexDF)
    val workDF = locationType.workLocation(spark, homeDF)

    /**Write Metrics in Parquets*/
    locationDistribution.locate(spark, data=workDF)
    stayDurationDistribution.duration(spark, data=workDF)
    dailyVisitedLocation.visit(spark, indexDF)
    extractTrips.trip(spark, indexDF)

    spark.stop()
  }
}
