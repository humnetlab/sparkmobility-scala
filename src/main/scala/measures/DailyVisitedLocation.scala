/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package measures
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DailyVisitedLocation {
  def visit(data: DataFrame): DataFrame = {
    // Retrieve date from local time
    val dataWithDate = data.withColumn("date", to_date(col("local_time")))
    val visits = dataWithDate
      .groupBy("caid", "date")
      .agg(countDistinct("h3_index").alias("locations"))

    // Total-sum via an unbounded window avoids a second action (previously `visits.count()`)
    // and keeps probability computation within a single Spark job.
    val totalWindow = Window.partitionBy()
    visits
      .groupBy("locations")
      .agg(count("*").alias("count"))
      .orderBy("locations")
      .withColumn("probability", col("count") / sum("count").over(totalWindow))
  }
}
