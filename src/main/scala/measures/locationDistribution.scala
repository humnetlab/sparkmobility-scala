/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package measures
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object locationDistribution {
  def locate(data: DataFrame): DataFrame = {
    val location = data
      .groupBy("caid")
      .agg(
        first("home_h3_index").as("home_index"), // Replace with your columns
        first("work_h3_index").as("work_index")
      )
    // Write the DataFrame in Parquet format
    location
  }
}
