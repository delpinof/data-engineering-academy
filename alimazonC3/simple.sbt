name := "Alimazon C3"

version := "1.0"

scalaVersion := "2.11.0"

mainClass := Some("AlimazonC3")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided"
