## HumNet Mobility Package
###
First run the following command to install the dependencies.

```sbt update```

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).


### TODO:

# Modules of the Spark Package:

## 1. Pre-processing
### a. Filtering
   1. Filter by region (lat long)
   2. Filter by users (user ids)  
      1. By number of records and the time span (active users)
   3. Filter by time (time window)

### b. Compression
   1. Compress raw records into h3 hexagons

### c. Stay detection
   1. Same algorithm as implemented in timegeo
