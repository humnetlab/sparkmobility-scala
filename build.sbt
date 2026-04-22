// Keep in lockstep with sparkmobility Python package version (pyproject.toml). The emitted JAR name
// encodes this version so Python's ensure_jar() can pull a matching artifact from the GitHub Release
// published by .github/workflows/release.yml on tag push.
ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.13.10"

ThisBuild / resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "JCenter" at "https://jcenter.bintray.com/"
)

headerLicense := Some(
  HeaderLicense.ALv2("2026", "humnetlab", HeaderLicenseStyle.SpdxSyntax)
)
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixConfig := Some(
  (ThisBuild / baseDirectory).value / ".scalafix.conf"
)

lazy val root = (project in file("."))
  .settings(
    assembly / mainClass := Some("pipelines.Main"),
    name                 := "sparkmobility",

    // Required by the OrganizeImports and RemoveUnused Scalafix rules on Scala 2.13.x.
    scalacOptions ++= Seq(
      "-Wunused:imports",
      "-Wunused"
    ),
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-library"    % scalaVersion.value,
      "org.apache.spark" %% "spark-core"       % "3.5.0",
      "org.apache.spark" %% "spark-sql"        % "3.5.0",
      "com.uber"          % "h3"               % "4.1.1",
      "org.plotly-scala" %% "plotly-render"    % "0.8.2",
      "org.datasyslab"    % "geotools-wrapper" % "1.6.0-28.2",
      "ch.qos.logback"    % "logback-classic"  % "1.2.10",
      "org.json4s"       %% "json4s-core"      % "3.6.11",
      "org.json4s"       %% "json4s-native"    % "3.6.11",
      "org.scalameta"    %% "munit"            % "1.0.0" % Test
    ),
    testFrameworks += new TestFramework("munit.Framework"),
    // Spark 3.5 touches sun.nio.ch.DirectBuffer reflectively; on JDK 17+ the module system
    // blocks that unless we explicitly open the relevant packages. Forking ensures the JVM
    // that actually runs the tests picks these up.
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )

assembly / assemblyJarName := s"sparkmobility-${version.value}.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
