/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package sparkjobs.filtering
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.io.Source

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
