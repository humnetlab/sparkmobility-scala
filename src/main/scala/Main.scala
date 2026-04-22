/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pipelines
import org.apache.spark.internal.Logging
import utils.RunMode.RunMode
import utils.TestUtils.runModeFromEnv

object Main extends Logging {
  val runMode: RunMode = runModeFromEnv()

  private val usage =
    "Usage: spark-submit sparkmobility.jar <input-parquet> <output-dir> <h3-resolution>"

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(usage)
      sys.exit(2)
    }
    val input      = args(0)
    val output     = args(1)
    val resolution = args(2).toInt

    log.info("Creating spark session and running the job")
    val pipe = new Pipelines()
    pipe.getFullODMatrix(input, output, resolution)
  }
}
