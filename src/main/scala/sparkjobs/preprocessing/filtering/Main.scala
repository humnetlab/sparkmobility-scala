import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import dataPreprocessing.dataLoadFilter
import dataPreprocessing.h3Indexer
import com.uber.h3core.H3Core

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Filter")
      .master("local[*]")
      .getOrCreate()

    //filtering
    val filteredDF = dataLoadFilter.loadFilteredData(spark).limit(100)

    //h3 indexing
    val indexDF = h3Indexer.addIndex(spark, filteredDF, resolution = 10)

    indexDF.show(30, truncate = false)

    spark.stop()
  }
}