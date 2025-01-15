from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example") \
    .config("spark.jars", "/Users/chris/Documents/humnetmobility/timegeo_1_2.13-0.1.0-SNAPSHOT.jar") \
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
result = pipe_example_instance.exampleFunction("World")

resul_2 = pipe_example_instance.examplePipe()

print("Result:", result)
# List all Scala objects
# scala_objects = dir(jvm.timegeo)
# print("Scala objects:", scala_objects)
custom_operations = spark._jvm.example.CustomSparkOperations
custom_operations = jvm.example.CustomSparkOperations

# from py4j.java_gateway import JavaGateway

# gateway = JavaGateway()

# my_scala_functions = gateway.jvm.timegeo.PipeExample

# # Call Scala methods
# result = my_scala_functions.exampleFunction()
# print("Result:", result)

# from py4j.java_gateway import JavaGateway

# gateway = JavaGateway()

# my_scala_functions = gateway.jvm.timegeo.PipeExample

# # Call Scala methods
# result = my_scala_functions.exampleFunction()
# print("Result:", result)

