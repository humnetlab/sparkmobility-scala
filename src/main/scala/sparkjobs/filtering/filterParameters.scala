package sparkjobs.filtering

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read

case class FilterParametersType(
    deltaT: Int,
    spatialThreshold: Double,
    speedThreshold: Double,
    temporalThreshold: Int,
    hexResolution: Int,
    regionalTemporalThreshold: Int,
    passing: Boolean,
    startTimestamp: String,
    endTimestamp: String,
    longitude: Array[Float],
    latitude: Array[Float],
    homeToWork: Int,
    workToHome: Int,
    workDistanceLimit: Int,
    workFreqCountLimit: Int,
    timeZone: String
)

object FilterParameters {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def fromJsonFile(filePath: String): FilterParametersType = {
    val source = Source.fromFile(filePath)
    val jsonString =
      try source.mkString
      finally source.close()
    parse(jsonString).extract[FilterParametersType]
  }
}
