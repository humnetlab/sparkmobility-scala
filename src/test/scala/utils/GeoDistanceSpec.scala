/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

class GeoDistanceSpec extends munit.FunSuite {

  private val tolerance =
    1000.0 // meters — haversine on a sphere won't match geodesic tools exactly

  test("haversineMeters returns 0 for identical points") {
    assertEqualsDouble(
      GeoDistance.haversineMeters(37.7749, -122.4194, 37.7749, -122.4194),
      0.0,
      1e-6
    )
  }

  test("haversineMeters is symmetric") {
    val d1 = GeoDistance.haversineMeters(40.7128, -74.0060, 51.5074, -0.1278)
    val d2 = GeoDistance.haversineMeters(51.5074, -0.1278, 40.7128, -74.0060)
    assertEqualsDouble(d1, d2, 1e-6)
  }

  test("haversineMeters SF to NYC ~ 4130 km") {
    val meters =
      GeoDistance.haversineMeters(37.7749, -122.4194, 40.7128, -74.0060)
    assertEqualsDouble(meters, 4_130_000.0, tolerance * 50)
  }

  test("haversineMeters NYC to London ~ 5570 km") {
    val meters =
      GeoDistance.haversineMeters(40.7128, -74.0060, 51.5074, -0.1278)
    assertEqualsDouble(meters, 5_570_000.0, tolerance * 50)
  }

  test("h3DistanceMeters returns NaN for null inputs") {
    assert(GeoDistance.h3DistanceMeters(null, "8a2a1072b59ffff").isNaN)
    assert(GeoDistance.h3DistanceMeters("8a2a1072b59ffff", null).isNaN)
  }

  test("h3DistanceMeters between a cell and itself is 0") {
    val cell = "8a2a1072b59ffff"
    assertEqualsDouble(GeoDistance.h3DistanceMeters(cell, cell), 0.0, 1.0)
  }
}
