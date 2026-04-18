/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pipelines

import org.json4s._
import org.json4s.jackson.JsonMethods._
import sparkjobs.filtering.FilterParameters

/** Stable, Python-friendly façade over [[Pipelines]].
  *
  * Python callers go through this object via py4j. All config is passed as JSON strings
  * (parsed in-memory) so there is no disk round-trip between the two languages, and
  * column maps are passed as JSON objects so the Python side does not need to hand-build
  * a Scala `Map` through `PythonUtils.toScalaMap`.
  */
object PyEntryPoint {

  private implicit val formats: Formats = DefaultFormats

  private lazy val pipe = new Pipelines()

  private def parseColumnMap(columnNamesJson: String): Map[String, String] =
    parse(columnNamesJson).extract[Map[String, String]]

  def getStays(
      inputPath: String,
      outputPath: String,
      timeFormat: String,
      inputFormat: String,
      delim: String,
      ifHeader: String,
      columnNamesJson: String,
      configJson: String
  ): Unit = {
    val columnNames = parseColumnMap(columnNamesJson)
    val params      = FilterParameters.fromJsonString(configJson)
    pipe.getStays(
      inputPath,
      outputPath,
      timeFormat,
      inputFormat,
      delim,
      ifHeader,
      columnNames,
      params
    )
  }

  def getHomeWorkLocation(
      inputPath: String,
      outputPath: String,
      configJson: String
  ): Unit = {
    val params = FilterParameters.fromJsonString(configJson)
    pipe.getHomeWorkLocation(inputPath, outputPath, params)
  }

  def getODMatrix(
      inputPath: String,
      outputPath: String,
      hexResolution: Int
  ): Unit =
    pipe.getODMatrix(inputPath, outputPath, hexResolution)

  def getFullODMatrix(
      inputPath: String,
      outputPath: String,
      hexResolution: Int
  ): Unit =
    pipe.getFullODMatrix(inputPath, outputPath, hexResolution)

  def getDailyVisitedLocation(inputPath: String, outputPath: String): Unit =
    pipe.getDailyVisitedLocation(inputPath, outputPath)

  def getLocationDistribution(inputPath: String, outputPath: String): Unit =
    pipe.getLocationDistribution(inputPath, outputPath)

  def getStayDurationDistribution(inputPath: String, outputPath: String): Unit =
    pipe.getStayDurationDistribution(inputPath, outputPath)

  def getDepartureTimeDistribution(inputPath: String, outputPath: String): Unit =
    pipe.getDepartureTimeDistribution(inputPath, outputPath)
}
