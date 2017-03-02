name := "spark-salesforce"

version := "1.1"

organization := "com.springml"

scalaVersion := "2.11.8"

parallelExecution in Test := false

//resolvers += "Local IV2" at "file://"+Path.userHome.absolutePath+"/.ivy2/local"


libraryDependencies += "com.force.api" % "force-wsc" % "39.0.0"   % "provided"
libraryDependencies += "com.force.api" % "force-partner-api" % "39.0.0" % "provided"
libraryDependencies += "com.springml" % "salesforce-wave-api" % "1.0.8" % "provided" 

libraryDependencies += "org.mockito" % "mockito-core" % "2.7.1" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0" % "provided"

libraryDependencies += "com.madhukaraphatak" %% "java-sizeof" % "0.1" % "provided"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0" % "provided"

libraryDependencies += "com.force.api" % "force-metadata-api" % "39.0.0" % "provided"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.8.7"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.8.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"

excludeDependencies += "org.codehaus.woodstox" % "woodstox-core-asl"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


