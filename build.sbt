name := "spark-salesforce-wave"
version := "1.0.0"
organization := "springml"
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.4.0",
  "org.apache.spark" % "spark-sql_2.11" % "1.4.0",
  "com.databricks" % "spark-csv_2.11" % "1.1.0"
)

// Spark Package Details (sbt-spark-package)
spName := "springml/spark-salesforce-wave"
sparkVersion := "1.4.0"
sparkComponents += "sql"
spDependencies += "databricks/spark-csv:1.0.3"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
