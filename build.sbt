name := "spark-salesforce"

version := "1.1"

organization := "com.springml"

scalaVersion := "2.11.8"

parallelExecution in Test := false

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "local m2" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

libraryDependencies += "com.force.api" % "force-wsc" % "39.0.0"   % "provided"
libraryDependencies += "com.force.api" % "force-partner-api" % "39.0.0" % "provided"
libraryDependencies += "com.springml" % "salesforce-wave-api" % "1.0.9" % "provided" 

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

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.136"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.136"


//spName := "springml/spark-salesforce"

//sparkVersion := "2.1.0"

//spAppendScalaVersion := true

//sparkComponents += "sql"

//publishMavenStyle := true

//sp IncludeMaven := true

//spShortDescription := "Spark Salesforce Wave Connector"

//spDescription := """Spark Salesforce Wave Connector
//                    | - Creates Salesforce Wave Datasets using dataframe
//                    | - Constructs Salesforce Wave dataset's metadata using schema present in dataframe
//                    | - Can use custom metadata for constructing Salesforce Wave dataset's metadata""".stripMargin

//// licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

//publishTo := {
//  val nexus = "https://oss.sonatype.org/"
//  if (version.value.endsWith("SNAPSHOT"))
//    Some("snapshots" at nexus + "content/repositories/snapshots")
//  else
//    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
//}

//pomExtra := (
//  <url>https://github.com/springml/spark-salesforce</url>
//    <licenses>
//      <license>
//        <name>Apache License, Verision 2.0</name>
//        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
//        <distribution>repo</distribution>
//      </license>
//    </licenses>
//    <scm>
//      <connection>scm:git:github.com/springml/spark-salesforce</connection>
//      <developerConnection>scm:git:git@github.com:springml/spark-salesforce</developerConnection>
//      <url>github.com/springml/spark-salesforce</url>
//    </scm>
//    <developers>
//      <developer>
//       <id>springml</id>
//        <name>Springml</name>
//        <url>http://www.springml.com</url>
//      </developer>
//    </developers>)


