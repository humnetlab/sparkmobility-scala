from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example") \
    .config("spark.jars", "./timegeo010.jar") \
    .getOrCreate()


jvm = spark._jvm

pipe_example_instance = jvm.pipelines.PipeExample()

path = "/data/test/6-stays_h3_region.parquet"
result = pipe_example_instance.getHomeWorkLocation(path)

homes = result._1()
works = result._2()
homes.show()