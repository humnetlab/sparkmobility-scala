package pipelines
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

class PipeExample {
    // Class implementation goes here
    def examplePipe(): Unit = {
        val spark = SparkSession.builder.appName("PipeExample").getOrCreate()
        
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
    }
    def exampleFunction(param: String): String = {
        s"Hello, $param"
    }
}