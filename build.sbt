ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.10"

ThisBuild / resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "JCenter" at "https://jcenter.bintray.com/"
)

lazy val root = (project in file("."))
  .settings(
    // assemblyPackageScala / assembleArtifact := false,
    // assemblyPackageDependency / assembleArtifact := false,
    assembly / mainClass := Some("pipelines.Pipelines"),
    name                 := "timegeo_1",
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-library" % scalaVersion.value,
      "org.apache.spark" %% "spark-core"    % "3.5.0",
      "org.apache.spark" %% "spark-sql"     % "3.5.0",
      "com.uber" % "h3" % "4.1.1", // 4.1.1, 3.6.3, 3.7.1, 3.7.3
      "org.plotly-scala" %% "plotly-render"    % "0.8.2",
      "org.datasyslab"    % "geotools-wrapper" % "1.6.0-28.2",
      "ch.qos.logback"    % "logback-classic"  % "1.2.10",
      "org.json4s"       %% "json4s-core"      % "3.6.11",
      "org.json4s"       %% "json4s-native"    % "3.6.11"

      // "org.apache.hadoop" % "hadoop-client-api" % "3.3.4"
    ),
  )

assembly / assemblyJarName := "timegeo010.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
