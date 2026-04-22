/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package measures

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}

object DepartureTimeDistribution {
  def departureTime(data: DataFrame): DataFrame = {
    // Compute total via a window sum instead of a second `data.count()` action — one job instead of two,
    // and avoids recomputing `data` if it's not cached.
    val totalWindow = Window.partitionBy()
    data
      .groupBy("hour_of_day")
      .agg(count("*").alias("count"))
      .orderBy("hour_of_day")
      .withColumn("probability", col("count") / sum("count").over(totalWindow))
  }
}
