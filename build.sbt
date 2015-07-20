name := "spark-salesforce-wave"

version := "1.0.0"

organization := "springml"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.force.api" % "force-wsc" % "34.0.0",
  "com.force.api" % "force-partner-api" % "34.0.0"
)


resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

//unmanagedJars in Compile += file("lib/partner.jar")

// Spark Package Details (sbt-spark-package)
spName := "springml/spark-salesforce-wave"

sparkVersion := "1.4.0"

sparkComponents += "sql"

spDependencies += "databricks/spark-csv:1.0.3"

sparkComponents += "sql"


