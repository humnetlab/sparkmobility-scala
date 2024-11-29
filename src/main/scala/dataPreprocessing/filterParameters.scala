package dataPreprocessing

class filterParameters(
                      val start_year: Int = 2019,
                      val start_month: Int = 1,
                      val start_day: Int = 1,
                      val start_hour: Int = 10,
                      val start_minute: Int = 50,
                      val start_second: Int = 30,
                      val end_year: Int = 2020,
                      val end_month: Int = 1,
                      val end_day: Int = 2,
                      val end_hour: Int = 12,
                      val end_minute: Int = 20,
                      val end_second: Int = 45,
                      val leftLongitude: Float = -180,
                      val rightLongitude: Float = 180,
                      val bottomLatitude:Float = -90,
                      val topLatitude : Float = 90,
                      val user_id: String = "660f52298fdd4b6cd320ef18e44e04b9c8304c03952127865e218d994201e845",
                      val homeToWork : Int = 8,
                      val workToHome : Int = 19,
                      val workDistanceLimit : Int = 500,
                      val workFreqCountLimit : Int =  3
                      ) {
/*
  def getUnixTimestamps(spark: SparkSession): (Long, Long) = {
    // Construct start and end time strings
    val startTimeStr = f"$start_year-$start_month%02d-$start_day%02d $start_hour%02d:$start_minute%02d:$start_second%02d"
    val endTimeStr = f"$end_year-$end_month%02d-$end_day%02d $end_hour%02d:$end_minute%02d:$end_second%02d"

    // Create DataFrame to calculate Unix timestamps
    val df = spark.sqlContext.createDataFrame(Seq(
      (startTimeStr, endTimeStr)
    )).toDF("start_time", "end_time")

    // Calculate Unix timestamps using Spark SQL functions
    val row = df.select(
      unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss").as("startTimeUnix"),
      unix_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss").as("endTimeUnix")
    ).first()

    (row.getLong(0), row.getLong(1))
  }
  */
}