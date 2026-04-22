/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import com.uber.h3core.H3Core
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** Single source of truth for great-circle distance calculations.
  *
  * All public functions return meters. Callers that need kilometers divide
  * explicitly at the use site so the unit is obvious in the surrounding code.
  */
object GeoDistance extends Serializable {

  object H3CoreSingleton extends Serializable {
    @transient lazy val instance: H3Core = H3Core.newInstance()
  }

  private val earthRadiusMeters: Double = 6371000.0

  def haversineMeters(
      lat1: Double,
      lon1: Double,
      lat2: Double,
      lon2: Double
  ): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) *
      math.sin(dLon / 2) * math.sin(dLon / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    earthRadiusMeters * c
  }

  def h3DistanceMeters(h3Index: String, otherIndex: String): Double = {
    if (h3Index == null || otherIndex == null) Double.NaN
    else {
      val h3 = H3CoreSingleton.instance
      val a  = h3.cellToLatLng(h3Index)
      val b  = h3.cellToLatLng(otherIndex)
      haversineMeters(a.lat, a.lng, b.lat, b.lng)
    }
  }

  /** UDF variant of [[h3DistanceMeters]] that returns null when either input is
    * null (avoids propagating NaN into downstream aggregations).
    */
  val h3DistanceMetersUDF: UserDefinedFunction =
    udf[java.lang.Double, String, String] { (a: String, b: String) =>
      if (a == null || b == null) null
      else java.lang.Double.valueOf(h3DistanceMeters(a, b))
    }
}
