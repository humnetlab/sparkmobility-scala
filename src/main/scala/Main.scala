import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.uber.h3core.H3Core
import measures.{dailyVisitedLocation, extractTrips, locationDistribution, stayDurationDistribution}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Filter")
      .master("local[8]")
      .config("spark.driver.memory", "20g")  // Increase based on system capacity
      .config("spark.executor.memory", "18g")
      .getOrCreate()
    import spark.implicits._

    val toHexString = udf((index: Long) => java.lang.Long.toHexString(index))
    val currentDir = System.getProperty("user.dir")
    val relativePath = "/data/6-stays_h3_region.parquet"
    val folderPath = s"$currentDir$relativePath"
    
    println(s"Folder path: $folderPath")
    val df = spark.read
      .parquet(folderPath)
    
    val indexDF = df.withColumnRenamed("stay_start_timestamp", "local_time")
      .withColumnRenamed("h3_id_region", "h3_index")
      .withColumn("h3_index_hex", toHexString(col("h3_index")))
      .drop("h3_index")
      .withColumnRenamed("h3_index_hex", "h3_index")

    //indexDF.show()

    //Data Preprocessing
    //filtering
    //val filteredDF = dataLoadFilter.loadFilteredData(spark)//.limit(30)

    //h3 indexing
    //val indexDF = h3Indexer.addIndex(spark, filteredDF, resolution = 10)
    //indexDF.show()
    // Stay Detection
    /*
    val folderPath = curDir + "/data/stays-h3-region"
    val df = spark.read.parquet(folderPath)
    val dfWithWeekday = df.withColumn("day", dayofweek(col("stay_start_timestamp")))
    val stayDF = dfWithWeekday.withColumnRenamed("h3_id_region", "h3_index")
    //stayDF.show(50, truncate=false)

     */
    // Location Type Extraction
    //val homeDF = locationType.homeLocation(spark, indexDF)
    //val workDF = locationType.workLocation(spark, homeDF)
    //shuffle and show
    //workDF.orderBy(rand()).show(200, truncate = true)
    //Experiment Area
    //selected caid: 8a29a56c1577fff

    //unique locations?
    //Save:

    //val outputPath = curDir + relPath
    // Write the DataFrame in Parquet format
    /*
    workDF.write
      .mode("overwrite") // Overwrites existing data at the output path
      .format("parquet")
      .save(outputPath)

     */

    //Metrics
    //locationDistribution.locate(spark, data=workDF)
    //val duration = stayDurationDistribution.duration(spark, data=workDF)
    //val visitDF = dailyVisitedLocation.visit(spark, indexDF)

    val trips = extractTrips.trip(spark, indexDF)
    trips.show()

    //0000fb5bdd3906226293179557a7387fba6ca52ee26335f5ab489c4a1ada6924

    spark.stop()
  }
}
