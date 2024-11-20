name := "HumNetMobility"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.json4s" %% "json4s-native" % "3.7.0-M3"

)

resolvers += "Apache Releases" at "https://repository.apache.org/content/repositories/releases/"


libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "1.6.0-28.2"

libraryDependencies += "com.uber" % "h3" % "4.1.1"



resolvers += Resolver.mavenCentral
