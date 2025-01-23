ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

ThisBuild / resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "JCenter" at "https://jcenter.bintray.com/"
)
 

lazy val root = (project in file("."))
  .settings(
    assemblyPackageScala / assembleArtifact := false,
    assemblyPackageDependency / assembleArtifact := false,
    assembly / mainClass := Some("pipelines.PipeExample"),
    name := "timegeo_1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "com.uber" % "h3" % "4.1.1", // 4.1.1, 3.6.3, 3.7.1, 3.7.3
      "org.plotly-scala" %% "plotly-render" % "0.8.2",
      "org.datasyslab" % "geotools-wrapper" % "1.6.0-28.2",
      "org.json4s" %% "json4s-native" % "3.7.0-M3",
      "ch.qos.logback" % "logback-classic" % "1.2.10"

      //"org.apache.hadoop" % "hadoop-client-api" % "3.3.4"
    ),
  )

