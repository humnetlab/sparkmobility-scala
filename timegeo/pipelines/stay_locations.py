from pyspark.sql import SparkSession
import os
from pyspark.sql import SparkSession

TIMEGEO_JAR = os.getenv("TIMEGEO_JAR", "./timegeo010.jar")

spark = SparkSession.builder \
    .appName("Timegeo") \
    .config("spark.jars", TIMEGEO_JAR) \
    .getOrCreate()
jvm = spark._jvm

pipe_example_instance = jvm.pipelines.PipeExample()

path = "/data_1/quadrant/sample/"
result_2 = pipe_example_instance.getStaysTest(path)

print("Result:", result_2)
