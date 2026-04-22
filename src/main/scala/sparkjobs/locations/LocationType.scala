/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package sparkjobs.locations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sparkjobs.filtering._
import utils.GeoDistance

object LocationType extends Serializable {
  def homeLocation(data: DataFrame, params: FilterParametersType): DataFrame = {
    /*
     * Home locations of each user are labelled as type = 1.
     * Weekday night, when user is most likely to be at home, is defined as from 7pm of
     * Sunday to Thursday, to 8am of the following weekday.
     */
    val conditionHome = (
      (col("day_of_week").isin(1, 2, 3, 4, 5) && col("hour_of_day").between(
        params.workToHome,
        23
      )) ||
        (col("day_of_week").isin(2, 3, 4, 5, 6) && col("hour_of_day").between(
          0,
          params.homeToWork - 1
        )) ||
        (col("day_of_week").isin(6, 7))
    )

    val sleepDF = data.filter(conditionHome).cache()

    val h3_Frequency = sleepDF
      .groupBy("caid", "h3_index")
      .agg(count("h3_index").as("frequency"))

    val mostFrequentH3DF = h3_Frequency
      .withColumn(
        "rank",
        row_number().over(Window.partitionBy("caid").orderBy(desc("frequency")))
      )
      .filter(col("rank") === 1)
      .select(
        col("caid"),
        col("h3_index").alias("home_h3_index")
      )

    val resultDF = data
      .join(broadcast(mostFrequentH3DF), Seq("caid"), "left")
      .withColumn(
        "type",
        when(col("h3_index") === col("home_h3_index") && conditionHome, 1)
          .otherwise(0)
      )
    sleepDF.unpersist()
    resultDF
  }

  def workLocation(data: DataFrame, params: FilterParametersType): DataFrame = {
    /*
     * home is the dataframe containing home h3 hexagon of each user, inherited from
     * function homeLocation. Work location should satisfy the following criteria:
     * 1. timeframe: weekday working hour 8am to 7pm
     * 2. max (distance from home)*frequency
     * 3. n >= 3
     * 4. distance from home > 500m
     * */
    val conditionWork = col("day_of_week").isin(1, 2, 3, 4, 5) && col(
      "hour_of_day"
    ).between(params.homeToWork, params.workToHome)
    val workDF = data.filter(conditionWork)

    val workFrequency = workDF
      .groupBy("caid", "h3_index")
      .agg(count("h3_index").as("frequency"))

    // add home h3 address
    val workFreqDF = workFrequency.join(
      data.select("caid", "home_h3_index").distinct(),
      Seq("caid"),
      "left"
    )

    val workFreqWithDistance = workFreqDF
      .withColumn(
        "distance_from_home",
        GeoDistance.h3DistanceMetersUDF(col("h3_index"), col("home_h3_index"))
      )
      .withColumn(
        "distance_times_frequency",
        col("distance_from_home") * col("frequency")
      )

    // get the maximum freq*distance. Criteria #4 and #5 are incorporated in the filtering process.
    val windowSpec = Window
      .partitionBy("caid")
      .orderBy(desc("distance_times_frequency"))

    val rankedDF = workFreqWithDistance
      .withColumn("rank", row_number().over(windowSpec))

    val workPlaceDF = rankedDF
      .filter(
        col("rank") === 1 && col(
          "distance_from_home"
        ) > params.workDistanceLimit && col(
          "frequency"
        ) >= params.workFreqCountLimit
      )
      .select("caid", "h3_index")
      .withColumnRenamed("h3_index", "work_h3_index")

    val dataWithWork = data.join(workPlaceDF, Seq("caid"), "left")

    val updatedData = dataWithWork.withColumn(
      "type",
      when(col("type") === 1, 1)
        .when(col("h3_index") === col("work_h3_index") && conditionWork, 2)
        .otherwise(col("type"))
    )

    updatedData
  }

}
