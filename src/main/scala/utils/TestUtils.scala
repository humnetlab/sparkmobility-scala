package utils

import org.apache.spark.internal.Logging

object TestUtils extends Logging {

  def runModeFromEnv(): RunMode.Value = {

    val TESTSYSTEMS_RE = "false"

    if (System.getenv("UNIT_TEST") == TESTSYSTEMS_RE) {
      log.info("Running in UNIT_TEST mode")
      RunMode.UNIT_TEST
    } else {
      log.info("Running in PRODUCTION mode")
      RunMode.PRODUCTION
    }
  }
}
