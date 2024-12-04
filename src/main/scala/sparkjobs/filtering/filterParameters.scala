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

case class FilterParameters(
    start_year: Int,
    start_month: Int,
    start_day: Int,
    start_hour: Int,
    start_minute: Int,
    start_second: Int,
    end_year: Int,
    end_month: Int,
    end_day: Int,
    end_hour: Int,
    end_minute: Int,
    end_second: Int,
    leftLongitude: Float,
    rightLongitude: Float,
    bottomLatitude: Float,
    topLatitude: Float,
    user_id: String,
    homeToWork: Int,
    workToHome: Int,
    workDistanceLimit: Int,
    workFreqCountLimit: Int,
    timeZone: String
)

object FilterParameters {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    def fromJsonFile(filePath: String): FilterParameters = {
        val source = Source.fromFile(filePath)
        val jsonString = try source.mkString finally source.close()
        parse(jsonString).extract[FilterParameters]
    }
}