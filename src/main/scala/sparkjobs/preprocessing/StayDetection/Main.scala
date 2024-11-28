import org.apache.spark.sql.{DataFrame, SparkSession, functions => F, Row, Encoders}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, LongType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.uber.h3core.H3Core
import com.uber.h3core.util.LatLng
import org.apache.spark.SparkConf

import scala.annotation.varargs

import org.apache.log4j.{Level, Logger}
import scala.jdk.CollectionConverters._



object Main {

  val temporal_threshold_1: Long = 300 // second
  val spatial_threshold: Double = 300 // meter
  val speed_threshold: Double = 6.0 // km/h, if larger than speed_threshold --> passing
  val temporal_threshold_2: Int = 3600 // second
  val resolution: Int = 9
  val region_temporal_threshold: Int = 3600 // second

  val BATCH_SIZE = 6

  val minLocations = None //Some(1)
  val maxLocations = None // Some(3)
  val minStayDuration = None //Some(90000)
  val maxStayDuration = None //Some(86400) //Some(150000) //sec
  val minRowCount = None //Some(100)
  val maxRowCount = None //Some(300)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[H3Core]))

    val spark = SparkSession.builder()
      .appName("StayDetection")
      .master("local[12]") // [*] // [12] For 16-core machine
      .config("spark.driver.memory", "64g") //16 64
      .config("spark.executor.memory", "64g") //32 64
      // enable Garbage-First Garbage Collector (G1 GC)
      //.config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      //.config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      //.config("spark.sql.shuffle.partitions", "12") // [12] For 16-core machine
      //.config("spark.memory.offHeap.enabled", "true")
      //.config("spark.memory.offHeap.size", "16g")
      //.config("spark.executor.instances", "8")
      //.config("spark.sql.shuffle.partitions", "20")
      // Adjust if needed
      //.config("spark.memory.fraction", "0.6") // Default is 0.6
      //.config("spark.memory.storageFraction", "0.5") // Default is 0.5
      .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.security.action=ALL-UNNAMED")
      .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.security.action=ALL-UNNAMED")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      .getOrCreate()

    var dataDF = spark.read
      .option("inferSchema", "true")
      .parquet("./data/201901AB") //   16.parquet  data_201901A
      //.repartition(20) //5000
      .withColumn("utc_timestamp", F.to_timestamp(F.col("utc_timestamp")))
    //.limit(300000)

    //dataDF.show(10)
    //dataDF.write.mode(SaveMode.Overwrite).parquet("./tem_output/0-initial_data.parquet")


    // Batch processing of data grouped by the caid column
    println("Starting batch processing based on 'caid'")
    val caidDF = dataDF.select("caid").distinct()
    val windowSpec = Window.orderBy("caid")
    val caidDFWithIndex = caidDF.withColumn("row_num", row_number().over(windowSpec))

    val totalCAIDs = caidDFWithIndex.count()
    println("Total CAIDs: " + totalCAIDs)
    val batchSizePerGroup = math.ceil(totalCAIDs.toDouble / BATCH_SIZE).toInt
    println("Batch Size Per Group: " + batchSizePerGroup)

    val caidDFWithBatchID = caidDFWithIndex.withColumn("batch_id", ((col("row_num") - 1) / batchSizePerGroup).cast("int"))
    //val caidDFWithBatchID = caidDFWithIndex.withColumn("batch_id", (col("row_num") / BATCH_SIZE).cast("int"))
    val dataDFWithBatchID = dataDF.join(caidDFWithBatchID.select("caid", "batch_id"), Seq("caid"))
    val batchIDs = (0 until BATCH_SIZE).toArray
    //val batchIDs = dataDFWithBatchID.select("batch_id").distinct().rdd.map(r => r.getInt(0)).collect()

    //caidDFWithBatchID.groupBy("batch_id").count().show()
    //dataDFWithBatchID.groupBy("batch_id").count().show()




    for (batchID <- batchIDs) {
      println(s"Processing batch ID: $batchID")

      val h3 = H3Core.newInstance()
      val h3Broadcast = spark.sparkContext.broadcast(h3)

      val batchDF = dataDFWithBatchID.filter(col("batch_id") === batchID).drop("batch_id")
      //val batchDFCount = batchDF.count()
      //println("batchDF Count: " + batchDFCount)


      println("Processing getStays")
      // 1 getStays
      val (getStays) = stay_detection.getStays(batchDF, spark, temporal_threshold_1, spatial_threshold)
      //val (getStays) = stay_detection.getStays(dataDF, spark, temporal_threshold_1, spatial_threshold)
      //val getStaysCount = getStays.count()
      //println("getStays Count: " + getStaysCount)
      //// getStays.write.mode(SaveMode.Overwrite).parquet("./tem_output/1-stays.parquet").show(10)
      //getStays.write.mode(SaveMode.Overwrite).parquet("./tem_output/1-stays.parquet")

      // val rowCount = dataDF.count()
      // val colCount = dataDF.columns.length
      // println(s"DataFrame shape: ($rowCount, $colCount)")

      println("Processing mapToH3")
      // 2 mapToH3
      val (passing, stays) = stay_detection.mapToH3(getStays, spark, h3Broadcast, resolution, temporal_threshold_2, speed_threshold)
      // passing.show(10)
      // stays.show(10)
      //passing.write.mode(SaveMode.Overwrite).parquet("./tem_output/3-passing.parquet")
      //stays.write.mode(SaveMode.Overwrite).parquet("./tem_output/3-stays.parquet")

      println("Processing getH3RegionMapping")
      // 3 getH3RegionMapping
      val h3RegionMapping = stay_detection.getH3RegionMapping(stays, spark, h3Broadcast)
      // h3RegionMapping.show(10)
      //h3RegionMapping.write.mode(SaveMode.Overwrite).parquet("./tem_output/4-h3_region_mapping.parquet")

      println("Processing h3RegionMapping")
      // h3RegionMapping
      val staysJoined = stays
        .join(h3RegionMapping, Seq("caid", "h3_id"), "left")
        .orderBy("caid", "stay_index_h3")
      // staysJoined.show(10)
      //staysJoined.write.mode(SaveMode.Overwrite).parquet("./tem_output/5-stays_joined.parquet")

      println("Processing mergeH3Region")
      // 4 mergeH3Region
      val staysH3Region = stay_detection.mergeH3Region(staysJoined, region_temporal_threshold)
      // staysH3Region.show(10)

      // 5 final filtering
      val filteredRes = stay_detection.filterData(
        staysH3Region,
        spark,
        minLocations,
        maxLocations,
        minStayDuration,
        maxStayDuration,
        minRowCount,
        maxRowCount
      )

      println("Writing document")
      //staysH3Region.write.mode(SaveMode.Overwrite).parquet("./tem_output/6-stays_h3_region.parquet")
      filteredRes.write
        .mode(SaveMode.Append)
        //.option("compression", "snappy") // or "gzip", "lz4"
        .parquet("./tem_output/6-stays_h3_region.parquet")


      batchDF.unpersist(true)
      getStays.unpersist(true)
      passing.unpersist(true)
      stays.unpersist(true)
      h3RegionMapping.unpersist(true)
      staysJoined.unpersist(true)
      staysH3Region.unpersist(true)
      filteredRes.unpersist(true)

      h3Broadcast.destroy()

      println(s"Active threads: ${Thread.activeCount()}")
      //Thread.getAllStackTraces.keySet().forEach { thread =>
      //  println(s"Thread Name: ${thread.getName}, State: ${thread.getState}")
      //}
      //println(s"Broadcast ID: ${h3Broadcast.id}")

      spark.catalog.clearCache()
    }
    println("Batch processing completed.")
  }
}