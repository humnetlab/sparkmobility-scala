// package sparkjobs.filtering

// import org.apache.spark.internal.Logging
// import org.apache.spark.sql.SparkSession
// import utils.RunMode
// import utils.RunMode.RunMode
// import utils.SparkFactory._
// import utils.TestUtils.runModeFromEnv
// import org.apache.spark.sql.DataFrame

// object SampleJob extends Logging {

//   val runMode : RunMode = runModeFromEnv()

//   def main(args: Array[String]): Unit = {

//     log.info("Creating spark session")
//     val spark: SparkSession = createSparkSession(runMode, "SampleJob")

//     log.debug("Reading csv from datasets in test")

//     val csvDf = spark.read.option("header","true").csv("/Users/chrisc/Downloads/conjunto_de_datos_enoe_2024_1t_csv/conjunto_de_datos_coe1_enoe_2024_1t/conjunto_de_datos/conjunto_de_datos_coe1_enoe_2024_1t.csv")

//     runMode match {
//       case RunMode.UNIT_TEST =>
//         csvDf.show(10, false)

//       case RunMode.PRODUCTION =>
//         log.info("Writing the data to table,")
//         // Insert operation
//     }

//   }

// }