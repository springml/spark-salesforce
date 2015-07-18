addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.2")
resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"
