from pyspark.sql import SparkSession

#BASIC SETUP
spark = SparkSession.builder \
    .appName("Example") \
    .config("spark.jars", "./timegeo010.jar") \
    .getOrCreate()

jvm = spark._jvm

pipe_example_instance = jvm.pipelines.PipeExample()

path = "/data/09.parquet"
result_2 = pipe_example_instance.getStaysTest(path)

print("Result:", result_2)
