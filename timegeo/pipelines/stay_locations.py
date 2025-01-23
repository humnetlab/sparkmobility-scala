from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example") \
    .config("spark.jars", "/Users/chris/Documents/humnetmobility/target/scala-2.13/timegeo_1_2.13-0.1.0-SNAPSHOT.jar") \
    .getOrCreate()

print("JARs loaded in Spark:", spark.sparkContext._conf.get("spark.jars"))

jvm = spark._jvm

# Check if the class exists
if hasattr(jvm.pipelines, "PipeExample"):
    print("Class 'PipeExample' is available in the JVM.")
else:
    print("Class 'PipeExample' is not found.")

# Access the correct Scala object
pipe_example_instance = jvm.pipelines.PipeExample()

path = "/data/6-stays_h3_region.parquet"
result_2 = pipe_example_instance.exampleFunction(path)

print("Result:", result_2)
