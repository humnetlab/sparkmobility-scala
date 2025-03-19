import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.uber.h3core.H3Core
import measures.{dailyVisitedLocation, extractTrips, locationDistribution, stayDurationDistribution}
import utils.RunMode
import utils.RunMode.RunMode
import utils.TestUtils.runModeFromEnv
import org.apache.spark.internal.Logging
import utils.SparkFactory._
import pipelines.PipeExample
import utils.FileUtils
import java.io.File

object Main extends Logging{
  val runMode : RunMode = runModeFromEnv()

  // def repartitionParquet(spark: SparkSession, path: String): Unit = {
  //   val schema = StructType(Seq(
  //     StructField("id", StringType, nullable = false),       // _c0
  //     StructField("unknown", StringType, nullable = true),   // _c1
  //     StructField("lat", DoubleType, nullable = false),       // _c2
  //     StructField("lon", DoubleType, nullable = false),      // _c3
  //     StructField("altitude", DoubleType, nullable = true),      // _c4
  //     StructField("unixtime", LongType, nullable = false), // _c5
  //   ))
  //   var dataDF = FileUtils.readTextData(path, schema, spark)

  //   dataDF = dataDF
  //     .withColumn("timestamp_utc", from_unixtime(col("unixtime")/ 1000))
  //     .withColumn("date", to_date(col("timestamp_utc")))
  //   val outputPath = s"/Users/chris/Documents/quadrant/output"
  //   dataDF
  //     .select("id", "lat", "lon", "timestamp_utc", "date")
  //     .repartition(col("date"))
  //     .write
  //     .mode("overwrite")
  //     .parquet(outputPath)
  // }

  def main(args: Array[String]): Unit = {
    log.info("Creating spark session and running the job")
    var pipe = new PipeExample()

    // val basePath = "/Users/chris/Documents/quadrant/sample/"
    // val folders = new File(basePath).listFiles.filter(_.isDirectory)
    // val spark: SparkSession = createSparkSession(runMode, "FilterJobCSV")
    // folders.foreach { folder =>
    //   repartitionParquet(spark, folder.getAbsolutePath)
    // }
    // pipe.getStaysTest("/Users/chris/Documents/quadrant/output/filter_partioned")
    var input: String = "/Users/chris/Documents/quadrant/output/filter_partioned"
    var output: String = "/Users/chris/Documents/quadrant/output/stays_full.parquet"
    pipe.getStaysTest(input, output)

    // pipe.appendNeededColumns("/Users/chris/Documents/quadrant/output/stays.parquet")
    // pipe.getHomeWorkLocation("/data_1/quadrant/output/stays.parquet")
  }
}
