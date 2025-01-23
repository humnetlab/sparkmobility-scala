package pipelines
import org.apache.spark.sql.SparkSession

import org.apache.spark.internal.Logging
import sparkjobs.staydetection.StayDetection
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Row}
import org.apache.spark.sql.SaveMode

import utils.RunMode
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromEnv

class PipeExample extends Logging {
  // Class implementation goes here
  val runMode: RunMode = runModeFromEnv()

  val temporal_threshold_1: Long = 300 // second
  val spatial_threshold: Double  = 300 // meter
  val speed_threshold: Double =
    6.0 // km/h, if larger than speed_threshold --> passing
  val temporal_threshold_2: Int      = 3600 // second
  val resolution: Int                = 9
  val region_temporal_threshold: Int = 3600 // second
  val passing                        = true

  def getStaysTest(relativePath: String): Unit = {
    log.info("Creating spark session")

    val spark = SparkSession
      .builder()
      .appName("StayDetection")
      .master("local[*]")
      .config("spark.driver.memory", "64g")
      .config("spark.executor.memory", "64g")
      // .config("spark.executor.instances", "8")
      // .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()
    import spark.implicits._

    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet("./data/data_201901A") //   16.parquet
      .withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp")))
    // .limit(300000)

    log.info("Processing getStays")
    // 1 getStays
    val (getStays) = StayDetection.getStays(
      dataDF,
      spark,
      temporal_threshold_1,
      spatial_threshold
    )
    val getStaysCount = getStays.count()
    log.info("getStays Count: " + getStaysCount)
    log.info("Processing mapToH3")

    // 2 mapToH3
    val (passingResult, stays) = StayDetection.mapToH3(
      getStays,
      resolution,
      temporal_threshold_2,
      passing,
      speed_threshold
    )

    log.info("Processing getH3RegionMapping")
    // 3 getH3RegionMapping
    val h3RegionMapping = StayDetection.getH3RegionMapping(stays, spark)

    log.info("Processing h3RegionMapping")
    // h3RegionMapping
    val staysJoined = stays
      .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
      .orderBy("caid", "stay_index_h3")
    log.info("Processing mergeH3Region")

    // 4 mergeH3Region
    val staysH3Region =
      StayDetection.mergeH3Region(staysJoined, region_temporal_threshold)
    // staysH3Region.show(10)
    log.info("Writing document")
    staysH3Region.write
      .mode(SaveMode.Overwrite)
      .parquet("./tem_output/6-stays_h3_region.parquet")

  }

  def exampleFunction(param: String): String = {
    s"Hello, $param"
  }
  def exampleSpark(param: String): Unit = {
    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")

    log.debug("Reading csv from datasets in test")

    val csvDf = spark.read
      .option("header", "true")
      .csv("/Users/chris/Downloads/commute_bussines.csv")

    csvDf.show(10, false)
  }
}
