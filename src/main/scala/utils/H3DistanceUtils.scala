/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import com.uber.h3core.H3Core

/** Utility object for H3 distance calculations */
object H3DistanceUtils {

  /** Singleton instance of H3Core, ensuring it's lazily initialized */
  object H3CoreSingleton extends Serializable {
    @transient lazy val instance: H3Core = H3Core.newInstance()
  }

  /** Function to calculate the great-circle distance between two H3 cells */
  def distance(h3Index: String, homeIndex: String): Double = {
    if (h3Index == null || homeIndex == null) {
      Double.NaN
    } else {
      val h3        = H3CoreSingleton.instance // Access the singleton instance
      val geoCoord1 = h3.cellToLatLng(h3Index)
      val geoCoord2 = h3.cellToLatLng(homeIndex)
      val lat1      = geoCoord1.lat
      val lon1      = geoCoord1.lng
      val lat2      = geoCoord2.lat
      val lon2      = geoCoord2.lng

      // Calculate haversine distance
      val R    = 6371000 // Earth's radius in meters
      val dLat = Math.toRadians(lat2 - lat1)
      val dLon = Math.toRadians(lon2 - lon1)
      val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      R * c / 1000
    }
  }
}
