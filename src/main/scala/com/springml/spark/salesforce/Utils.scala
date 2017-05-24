/*
 * Copyright 2015 springml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.springml.spark.salesforce

import scala.io.Source
import scala.util.parsing.json._
import com.sforce.soap.partner.{ SaveResult, Connector, PartnerConnection }
import com.sforce.ws.ConnectorConfig
import com.madhukaraphatak.sizeof.SizeEstimator
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import scala.collection.immutable.HashMap
import com.springml.spark.salesforce.metadata.MetadataConstructor
import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.fault.UnexpectedErrorFault
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI
import com.amazonaws.services.s3.{ AmazonS3URI, AmazonS3Client }
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import java.util.UUID
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.io.input.ReaderInputStream

/**
 * Utility to construct metadata and repartition RDD
 */
object Utils extends Serializable {

  @transient val logger = Logger.getLogger("Utils")

  def createConnection(username: String, password: String,
                       login: String, version: String): PartnerConnection = {
    val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)
    val endpoint = if (login.endsWith("/")) (login + "services/Soap/u/" + version) else (login + "/services/Soap/u/" + version);
    config.setAuthEndpoint(endpoint)
    config.setServiceEndpoint(endpoint)
    Connector.newConnection(config)
  }

  private var lastTempPathGenerated: String = null

  def logSaveResultError(result: SaveResult): Unit = {

    result.getErrors.map(error => {
      logger.error(error.getMessage)
      println(error.getMessage)
      error.getFields.map(logger.error(_))
      error.getFields.map { println }
    })
  }

  def repartition(rdd: RDD[Row]): RDD[Row] = {
    val totalDataSize = getTotalSize(rdd)
    val maxBundleSize = 1024 * 1024 * 10l;
    var partitions = 1
    if (totalDataSize > maxBundleSize) {
      partitions = Math.round(totalDataSize / maxBundleSize) + 1
    }

    val shuffle = rdd.partitions.length < partitions
    rdd.coalesce(partitions.toInt, shuffle)
  }

  def getTotalSize(rdd: RDD[Row]): Long = {
    // This can be fetched as optional parameter
    val NO_OF_SAMPLE_ROWS = 10;
    val totalRows = rdd.count();
    var totalSize = 0l

    if (totalRows > NO_OF_SAMPLE_ROWS) {
      val sampleObj = rdd.takeSample(false, NO_OF_SAMPLE_ROWS)
      val sampleRowSize = rowSize(sampleObj)
      totalSize = sampleRowSize * (totalRows / NO_OF_SAMPLE_ROWS)
    } else {

      totalSize = rddSize(rdd)
    }

    totalSize
  }

  def rddSize(rdd: RDD[Row]): Long = {
    rowSize(rdd.collect())
  }

  def rowSize(rows: Array[Row]): Long = {
    var sizeOfRows = 0l
    for (row <- rows) {
      // Converting to bytes
      val rowSize = SizeEstimator.estimate(row.toSeq.map { value => rowValue(value) }.mkString(","))
      sizeOfRows += rowSize
    }

    sizeOfRows
  }

  def rowValue(rowVal: Any): String = {
    if (rowVal == null) {
      ""
    } else {
      var value = rowVal.toString()
      if (value.contains("\"")) {
        value = value.replaceAll("\"", "\"\"")
      }
      if (value.contains("\"") || value.contains("\n") || value.contains(",")) {
        value = "\"" + value + "\""
      }
      value
    }
  }

  def metadataConfig(usersMetadataConfig: Option[String]) = {
    var systemMetadataConfig = readMetadataConfig();
    if (usersMetadataConfig != null && usersMetadataConfig.isDefined) {
      val usersMetadataConfigMap = readJSON(usersMetadataConfig.get)
      systemMetadataConfig = systemMetadataConfig ++ usersMetadataConfigMap
    }

    systemMetadataConfig
  }

  def csvHeadder(schema: StructType): String = {
    schema.fields.map(field => field.name).mkString(",")
  }

  def metadata(
    metadataFile: Option[String],
    usersMetadataConfig: Option[String],
    schema: StructType,
    datasetName: String): String = {

    if (metadataFile != null && metadataFile.isDefined) {
      logger.info("Using provided Metadata Configuration")
      val source = Source.fromFile(metadataFile.get)
      try source.mkString finally source.close()
    } else {
      logger.info("Constructing Metadata Configuration")
      val metadataConfig = Utils.metadataConfig(usersMetadataConfig)
      val metaDataJson = MetadataConstructor.generateMetaString(schema, datasetName, metadataConfig)
      metaDataJson
    }
  }

  def monitorJob(objId: String, username: String, password: String, login: String, version: String): Boolean = {
    var partnerConnection = Utils.createConnection(username, password, login, version)
    try {
      monitorJob(partnerConnection, objId, 500)
    } catch {
      case uefault: UnexpectedErrorFault => {
        val exMsg = uefault.getExceptionMessage
        logger.info("Error Message from Salesforce Wave " + exMsg)
        if (exMsg contains "Invalid Session") {
          logger.info("Session expired. Monitoring Job using new connection")
          return monitorJob(objId, username, password, login, version)
        } else {
          throw uefault
        }
      }
      case ex: Exception => {
        logger.info("Exception while checking the job in Salesforce Wave")
        throw ex
      }
    } finally {
      Try(partnerConnection.logout())
    }
  }

  private def monitorJob(connection: PartnerConnection,
                         objId: String, waitDuration: Long): Boolean = {
    val sobjects = connection.retrieve("Status", "InsightsExternalData", Array(objId))
    if (sobjects != null && sobjects.length > 0) {
      val status = sobjects(0).getField("Status")
      status match {
        case "Completed" => {
          logger.info("Upload Job completed successfully")
          true
        }
        case "CompletedWithWarnings" => {
          logger.warn("Upload Job completed with warnings. Check Monitor Job in Salesforce Wave for more details")
          true
        }
        case "Failed" => {
          logger.error("Upload Job failed in Salesforce Wave. Check Monitor Job in Salesforce Wave for more details")
          false
        }
        case "InProgress" => {
          logger.info("Upload Job is in progress")
          Thread.sleep(waitDuration)
          monitorJob(connection, objId, maxWaitSeconds(waitDuration))
        }
        case "New" => {
          logger.info("Upload Job not yet started in Salesforce Wave")
          Thread.sleep(waitDuration)
          monitorJob(connection, objId, maxWaitSeconds(waitDuration))
        }
        case "NotProcessed" => {
          logger.info("Upload Job not processed in Salesforce Wave. Check Monitor Job in Salesforce Wave for more details")
          true
        }
        case "Queued" => {
          logger.info("Upload Job Queued in Salesforce Wave")
          Thread.sleep(waitDuration)
          monitorJob(connection, objId, maxWaitSeconds(waitDuration))
        }
        case unknown => {
          logger.info("Upload Job status is not known " + unknown)
          true
        }
      }
    } else {
      logger.error("Upload Job details not found in Salesforce Wave")
      true
    }
  }

  private def maxWaitSeconds(waitDuration: Long): Long = {
    // 2 Minutes
    val maxWaitDuration = 120000
    if (waitDuration >= maxWaitDuration) maxWaitDuration else waitDuration * 2
  }

  private def readMetadataConfig(): Map[String, Map[String, String]] = {
    val source = Source.fromURL(getClass.getResource("/metadata_config.json"))
    val jsonContent = try source.mkString finally source.close()

    readJSON(jsonContent)
  }

  private def readJSON(jsonContent: String): Map[String, Map[String, String]] = {
    val result = JSON.parseFull(jsonContent)
    val resMap: Map[String, Map[String, String]] = result.get.asInstanceOf[Map[String, Map[String, String]]]
    resMap
  }

  /**
   * Given a URI, verify that the Hadoop FileSystem for that URI is not the S3 block FileSystem.
   * spark-salesforce cannot use this FileSystem because the files written to it will not be
   * readable by spark-salesforce (and vice versa).
   */
  def assertThatFileSystemIsNotS3BlockFileSystem(uri: URI, hadoopConfig: Configuration): Unit = {
    val fs = FileSystem.get(uri, hadoopConfig)
    // Note that we do not want to use isInstanceOf here, since we're only interested in detecting
    // exact matches. We compare the class names as strings in order to avoid introducing a binary
    // dependency on classes which belong to the `hadoop-aws` JAR, as that artifact is not present
    // in some environments (such as EMR). See #92 for details.
    if (fs.getClass.getCanonicalName == "org.apache.hadoop.fs.s3.S3FileSystem") {
      throw new IllegalArgumentException(
        "spark-salesforce does not support the S3 Block FileSystem. Please reconfigure `tempdir` to" +
          "use a s3n:// or s3a:// scheme.")
    }
  }

  /**
   * Redshift COPY and UNLOAD commands don't support s3n or s3a, but users may wish to use them
   * for data loads. This function converts the URL back to the s3:// format.
   */
  def fixS3Url(url: String): String = {
    url.replaceAll("s3[an]://", "s3://")
  }

  /**
   * Factory method to create new S3URI in order to handle various library incompatibilities with
   * older AWS Java Libraries
   */
  def createS3URI(url: String): AmazonS3URI = {
    try {
      // try to instantiate AmazonS3URI with url
      new AmazonS3URI(url)
    } catch {
      case e: IllegalArgumentException if e.getMessage.
        startsWith("Invalid S3 URI: hostname does not appear to be a valid S3 endpoint") => {
        new AmazonS3URI(addEndpointToUrl(url))
      }
    }
  }

  /**
   * Since older AWS Java Libraries do not handle S3 urls that have just the bucket name
   * as the host, add the endpoint to the host
   */
  def addEndpointToUrl(url: String, domain: String = "s3.amazonaws.com"): String = {
    val uri = new URI(url)
    val hostWithEndpoint = uri.getHost + "." + domain
    new URI(uri.getScheme,
      uri.getUserInfo,
      hostWithEndpoint,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment).toString
  }

  /**
   * Checks whether the S3 bucket for the given UI has an object lifecycle configuration to
   * ensure cleanup of temporary files. If no applicable configuration is found, this method logs
   * a helpful warning for the user.
   */
  def checkThatBucketHasObjectLifecycleConfiguration(
    tempDir: String,
    s3Client: AmazonS3Client): Unit = {
    try {
      val s3URI = createS3URI(Utils.fixS3Url(tempDir))
      val bucket = s3URI.getBucket
      assert(bucket != null, "Could not get bucket from S3 URI")
      val key = Option(s3URI.getKey).getOrElse("")
      val hasMatchingBucketLifecycleRule: Boolean = {
        val rules = Option(s3Client.getBucketLifecycleConfiguration(bucket))
          .map(_.getRules.asScala)
          .getOrElse(Seq.empty)
        rules.exists { rule =>
          // Note: this only checks that there is an active rule which matches the temp directory;
          // it does not actually check that the rule will delete the files. This check is still
          // better than nothing, though, and we can always improve it later.
          rule.getStatus == BucketLifecycleConfiguration.ENABLED && key.startsWith(rule.getPrefix)
        }
      }
      if (!hasMatchingBucketLifecycleRule) {
        logger.warn(s"The S3 bucket $bucket does not have an object lifecycle configuration to " +
          "ensure cleanup of temporary files. Consider configuring `tempdir` to point to a " +
          "bucket with an object lifecycle policy that automatically deletes files after an " +
          "expiration period. For more information, see " +
          "https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html")
      }
    } catch {
      case NonFatal(e) =>
        logger.warn("An error occurred while trying to read the S3 bucket lifecycle configuration", e)
    }
  }

  def saveS3File(fileName: String, s3Client: AmazonS3Client, input: java.io.InputStream): Unit = {
    val s3URI = createS3URI(Utils.fixS3Url(fileName))
    val bucket = s3URI.getBucket
    val filename = s3URI.getKey
    val baos = new java.io.ByteArrayOutputStream();
    logger.trace(s"Saving $filename")
    s3Client.putObject(bucket, filename, input, new ObjectMetadata())
  }

  /**
   * Creates a randomly named temp directory path for intermediate data
   */
  def makeTempPath(tempRoot: String): String = {
    lastTempPathGenerated = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)
    lastTempPathGenerated
  }

  /**
   * Joins prefix URL a to path suffix b, and appends a trailing /, in order to create
   * a temp directory path for S3.
   */
  def joinUrls(a: String, b: String): String = {
    a.stripSuffix("/") + "/" + b.stripPrefix("/").stripSuffix("/") + "/"
  }

  /**
   * Returns a copy of the given URI with the user credentials removed.
   */
  def removeCredentialsFromURI(uri: URI): URI = {
    new URI(
      uri.getScheme,
      null, // no user info
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
  }

  import java.io.BufferedReader
  import java.io.InputStream
  import com.springml.salesforce.wave.model.BatchResult
  import java.io.InputStreamReader

  def sampleResult(stream: InputStream): (InputStream,BatchResult) = {
    val result = new BatchResult()
    val bufferSize = 40000
    val reader = new BufferedReader(new InputStreamReader(stream));
    try {
      import scala.collection.JavaConverters._
      val line = reader.readLine();
      logger.trace("HEADER:" + line);
      if (line != null) {
        val columns = line.split(",");
        result.setHeader(columns.toList.asJava)
      }
      reader.mark(bufferSize)
      var n = 0
      var charsRead = 0
      var avgLineLength = 0
      var row: String = ""
      while (row != null && (charsRead + 3 * avgLineLength < bufferSize / 2) && n < 200) {
        row = reader.readLine();
       // println(row);
        n += 1
        if (row != null) {
          val size = row.length();
          charsRead += size
          if (avgLineLength == 0) avgLineLength = size
          else { avgLineLength = (avgLineLength * n + size) / n }
          val columns = row.split(",");
          result.addRecord(columns.toList.asJava)
        }
      }
    } finally {
      reader.reset()
    }
    (new ReaderInputStream(reader), result)
  }

}
