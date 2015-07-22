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

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
libraryDependencies += "com.madhukaraphatak" %% "java-sizeof" % "0.1"


//unmanagedJars in Compile += file("lib/partner.jar")

// Spark Package Details (sbt-spark-package)
spName := "springml/spark-salesforce-wave"

sparkVersion := "1.4.0"

sparkComponents += "sql"

spDependencies += "databricks/spark-csv:1.0.3"

sparkComponents += "sql"

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

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


