/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pipelines

import sparkjobs.filtering.FilterParameters

/** Contract tests for the Python ↔ Scala boundary. Python callers build a JSON
  * string with `json.dumps(params)` and hand it to `PyEntryPoint` via py4j; the
  * Scala side parses it into `FilterParametersType`. If either side's field
  * list drifts, these tests fail before the runtime does.
  */
class PyEntryPointSpec extends munit.FunSuite {

  private val sampleJson: String =
    """
      |{
      |  "deltaT": 600,
      |  "spatialThreshold": 200.0,
      |  "speedThreshold": 33.0,
      |  "temporalThreshold": 180,
      |  "hexResolution": 10,
      |  "regionalTemporalThreshold": 600,
      |  "passing": false,
      |  "startTimestamp": "2024-01-01 00:00:00",
      |  "endTimestamp": "2024-01-31 23:59:59",
      |  "longitude": [-122.6, -122.3],
      |  "latitude": [37.7, 37.9],
      |  "homeToWork": 7,
      |  "workToHome": 17,
      |  "workDistanceLimit": 500,
      |  "workFreqCountLimit": 3,
      |  "timeZone": "America/Los_Angeles"
      |}""".stripMargin

  test("FilterParameters.fromJsonString parses the expected schema") {
    val params = FilterParameters.fromJsonString(sampleJson)
    assertEquals(params.deltaT, 600)
    assertEqualsDouble(params.spatialThreshold, 200.0, 1e-9)
    assertEqualsDouble(params.speedThreshold, 33.0, 1e-9)
    assertEquals(params.temporalThreshold, 180)
    assertEquals(params.hexResolution, 10)
    assertEquals(params.regionalTemporalThreshold, 600)
    assertEquals(params.passing, false)
    assertEquals(params.startTimestamp, "2024-01-01 00:00:00")
    assertEquals(params.endTimestamp, "2024-01-31 23:59:59")
    assertEqualsDouble(params.longitude(0).toDouble, -122.6, 1e-3)
    assertEqualsDouble(params.longitude(1).toDouble, -122.3, 1e-3)
    assertEqualsDouble(params.latitude(0).toDouble, 37.7, 1e-3)
    assertEqualsDouble(params.latitude(1).toDouble, 37.9, 1e-3)
    assertEquals(params.homeToWork, 7)
    assertEquals(params.workToHome, 17)
    assertEquals(params.workDistanceLimit, 500)
    assertEquals(params.workFreqCountLimit, 3)
    assertEquals(params.timeZone, "America/Los_Angeles")
  }

  test("PyEntryPoint exposes the documented py4j surface") {
    val methods = PyEntryPoint.getClass.getDeclaredMethods.map(_.getName).toSet
    val expected = Set(
      "getStays",
      "getHomeWorkLocation",
      "getODMatrix",
      "getFullODMatrix",
      "getDailyVisitedLocation",
      "getLocationDistribution",
      "getStayDurationDistribution",
      "getDepartureTimeDistribution"
    )
    val missing = expected.diff(methods)
    assert(
      missing.isEmpty,
      s"PyEntryPoint is missing Python-facing methods: $missing"
    )
  }
}
