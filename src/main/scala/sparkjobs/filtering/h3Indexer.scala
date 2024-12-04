// src/main/scala/filter/dataProcessor.scala
package sparkjobs.filtering
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import com.uber.h3core.H3Core

object h3Indexer {
  def addIndex(spark: SparkSession, data: DataFrame, resolution: Int = 10): DataFrame = {

    val h3UDF = udf((latitude: Double, longitude: Double) => {
      val h3 = H3Core.newInstance()
      h3.latLngToCell(latitude, longitude, resolution).toHexString
    })

    val updatedData = data.withColumn("h3_index", h3UDF(col("latitude"), col("longitude")))
    updatedData
  }
}
