name := "spark-salesforce"

version := "1.1.1"

organization := "com.springml"

scalaVersion := "2.11.8"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "com.force.api" % "force-wsc" % "40.0.0",
  "com.force.api" % "force-partner-api" % "40.0.0",
  "com.springml" % "salesforce-wave-api" % "1.0.8",
  "org.mockito" % "mockito-core" % "2.0.31-beta"
)

parallelExecution in Test := false

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
libraryDependencies += "com.madhukaraphatak" %% "java-sizeof" % "0.1"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.4.4"
libraryDependencies += "org.codehaus.woodstox" % "woodstox-core-asl" % "4.4.0"

// Spark Package Details (sbt-spark-package)
spName := "springml/spark-salesforce"

spAppendScalaVersion := true

sparkVersion := "2.1.0"

sparkComponents += "sql"

publishMavenStyle := true

spIncludeMaven := true

spShortDescription := "Spark Salesforce Wave Connector"

retrieveManaged := true

spDescription := """Spark Salesforce Wave Connector
                    | - Creates Salesforce Wave Datasets using dataframe
                    | - Constructs Salesforce Wave dataset's metadata using schema present in dataframe
                    | - Can use custom metadata for constructing Salesforce Wave dataset's metadata""".stripMargin

// licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/springml/spark-salesforce</url>
    <licenses>
      <license>
        <name>Apache License, Verision 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/springml/spark-salesforce</connection>
      <developerConnection>scm:git:git@github.com:springml/spark-salesforce</developerConnection>
      <url>github.com/springml/spark-salesforce</url>
    </scm>
    <developers>
      <developer>
        <id>springml</id>
        <name>Springml</name>
        <url>http://www.springml.com</url>
      </developer>
    </developers>)


