/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package measures

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}

object StayDurationDistribution {
  def duration(data: DataFrame): DataFrame = {
    val hoursDF = data
      .withColumn(
        "stay_duration_in_seconds",
        unix_timestamp(col("stay_end_timestamp")) - unix_timestamp(
          col("stay_start_timestamp")
        )
      )
      .withColumn(
        "interval_hours",
        col("stay_duration_in_seconds") / 3600
      ) // Convert seconds to hours
      .select(
        col("caid"),
        col("interval_hours")
      )

    val binSize = 1.0 // hour

    val binnedData = hoursDF.withColumn(
      "bin_index",
      floor(col("interval_hours") / lit(binSize))
    )

    val binnedWithRange =
      binnedData.withColumn("range", (col("bin_index") + 1) * lit(binSize))

    // Use a window sum over aggregated counts rather than a second `hoursDF.count()` action.
    val totalWindow = Window.partitionBy()
    binnedWithRange
      .groupBy("range")
      .agg(count("*").alias("count"))
      .orderBy("range")
      .withColumn("probability", col("count") / sum("count").over(totalWindow))
  }
}
