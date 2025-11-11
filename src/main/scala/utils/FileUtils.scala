package utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._

import java.io.{File, FileWriter, InputStream}
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.reflect.io.Directory

object FileUtils extends Logging {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def readJsonAsMap(path: String): Map[String, String] = {
    val fileContent = readResourceFile(path)
    val jsonAsMap   = parse(fileContent).extract[Map[String, String]]
    jsonAsMap
  }

  def readResourceFile(path: String): String = {

    val stream: InputStream = getClass.getResourceAsStream(path)
    val lines               = scala.io.Source.fromInputStream(stream)
    try {
      lines.getLines().mkString(" ")
    } finally {
      lines.close()
    }
  }

  def validatePath(repoPath: String): Boolean = {
    if (Files.exists(Paths.get(repoPath))) true else false
  }

  def calculateSize(inputPath: String): Long = {
    val filePath = new File(inputPath)
    filePath.length()
  }

  def readFile(path: String): List[String] = {
    val text = Source.fromFile(path)
    text.getLines().toList
  }

  def saveFile(lines: List[String], path: String): Unit = {
    val fw = new FileWriter(path, false)
    try {
      lines.foreach(line => fw.write(line + "\n"))
    } finally {
      fw.close()
    }
  }

  def saveFile(
      lines: List[String],
      directory: String,
      fileName: String,
      writePermissions: Boolean = false
  ): Unit = {

    if (Directory(directory).exists) {
      log.info(s"Directory: $directory exists. Writing data to file")
    } else {
      createDir(directory)
      log.info(s"Directory: $directory doesnot exist. Created directory")
    }

    saveFile(lines, s"$directory/$fileName")
  }

  def saveFile(content: String, path: String): Unit = {
    val fw = new FileWriter(path, false)
    try {
      fw.write(content)
    } finally {
      fw.close()
    }
  }

  def createDir(dirPath: String, recursive: Boolean = false): Boolean = {
    val file = new File(dirPath)
    if (recursive) file.mkdirs()
    else file.mkdir()
  }

  def readParquetData(fullPath: String, spark: SparkSession): DataFrame = {
    log.info(s"Reading Parquet data from path: $fullPath")

    val path = new File(fullPath)
    val dataDF = if (path.isFile) {
      spark.read
        .option("inferSchema", "true")
        .parquet(fullPath)
    } else {
      spark.read
        .option("inferSchema", "true")
        .parquet(s"$fullPath/*")
    }

    dataDF
  }

  def readCSVData(
      fullPath: String,
      delim: String,
      ifHeader: String,
      spark: SparkSession
  ): DataFrame = {
    log.info(s"Reading Parquet data from path: $fullPath")

    val path = new File(fullPath)
    val dataDF = if (path.isFile) {
      spark.read
        .option("inferSchema", "true")
        .csv(fullPath)
    } else {
      spark.read
        .option("inferSchema", "true")
        .option("sep", delim)
        .option("header", ifHeader)
        .csv(s"$fullPath/*.gz")
    }

    dataDF
  }

  def readTextData(
      fullPath: String,
      schema: StructType,
      spark: SparkSession
  ): DataFrame = {
    log.info(s"Reading Parquet data from path: $fullPath")

    val path = new File(fullPath)
    val dataDF = if (path.isFile) {
      spark.read
        .schema(schema)
        .option("header", "false")
        .csv(fullPath)
    } else {
      spark.read
        .schema(schema)
        .option("header", "false")
        .csv(s"$fullPath/*")
    }

    dataDF
  }
}
