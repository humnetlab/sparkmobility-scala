/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import org.apache.spark.internal.Logging

object TestUtils extends Logging {

  // Truthy strings that flip the process into UNIT_TEST mode.
  private val TruthyUnitTest = Set("1", "true", "yes", "on")

  def runModeFromEnv(): RunMode.Value = {
    val raw = Option(System.getenv("UNIT_TEST"))
      .map(_.trim.toLowerCase)
      .getOrElse("")
    if (TruthyUnitTest.contains(raw)) {
      log.info("Running in UNIT_TEST mode")
      RunMode.UNIT_TEST
    } else {
      log.info("Running in PRODUCTION mode")
      RunMode.PRODUCTION
    }
  }
}
