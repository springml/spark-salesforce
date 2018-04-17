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
import com.sforce.soap.partner.{Connector, PartnerConnection, SaveResult}
import com.sforce.ws.ConnectorConfig
import com.madhukaraphatak.sizeof.SizeEstimator
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

import scala.collection.immutable.HashMap
import com.springml.spark.salesforce.metadata.MetadataConstructor
import com.sforce.soap.partner.sobject.SObject
import scala.concurrent.duration._
import com.sforce.soap.partner.fault.UnexpectedErrorFault

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/**
 * Utility to construct metadata and repartition RDD
 */
object Utils extends Serializable {
  @transient val logger = Logger.getLogger("Utils")

  def createConnection(username: String, password: String,
      login: String, version: String):PartnerConnection = {
    val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)
    val endpoint = if (login.endsWith("/")) (login + "services/Soap/u/" + version) else (login + "/services/Soap/u/" + version)
    config.setAuthEndpoint(endpoint)
    config.setServiceEndpoint(endpoint)
    Connector.newConnection(config)
  }

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
    val maxBundleSize = 1024 * 1024 * 10l
    var partitions = 1
    if (totalDataSize > maxBundleSize) {
      partitions = Math.round(totalDataSize / maxBundleSize) + 1
    }

    val shuffle = rdd.partitions.length < partitions
    rdd.coalesce(partitions.toInt, shuffle)
  }

  def getTotalSize(rdd: RDD[Row]): Long = {
    // This can be fetched as optional parameter
    val NO_OF_SAMPLE_ROWS = 10
    val totalRows = rdd.count()
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

  def rddSize(rdd: RDD[Row]) : Long = {
    rowSize(rdd.collect())
  }

  def rowSize(rows: Array[Row]) : Long = {
      var sizeOfRows = 0l
      for (row <- rows) {
        // Converting to bytes
        val rowSize = SizeEstimator.estimate(row.toSeq.map { value => rowValue(value) }.mkString(","))
        sizeOfRows += rowSize
      }

      sizeOfRows
  }

  def rowValue(rowVal: Any) : String = {
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
    var systemMetadataConfig = readMetadataConfig()
    if (usersMetadataConfig != null && usersMetadataConfig.isDefined) {
      val usersMetadataConfigMap = readJSON(usersMetadataConfig.get)
      systemMetadataConfig = systemMetadataConfig ++ usersMetadataConfigMap
    }

    systemMetadataConfig
  }

  def csvHeadder(schema: StructType) : String = {
    schema.fields.map(field => field.name).mkString(",")
  }

  def metadata(
      metadataFile: Option[String],
      usersMetadataConfig: Option[String],
      schema: StructType,
      datasetName: String) : String = {

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

  def monitorJob(objId: String, username: String, password:
      String, login: String, version: String) : Boolean = {
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

  def retryWithExponentialBackoff(
      func:() => Boolean,
      timeoutDuration: FiniteDuration,
      initSleepInterval: FiniteDuration,
      maxSleepInterval: FiniteDuration): Boolean = {

    val timeout = timeoutDuration.toMillis
    var waited = 0L
    var sleepInterval = initSleepInterval.toMillis
    var done = false

    do {
      done = func()
      if (!done) {
        sleepInterval = math.min(sleepInterval * 2, maxSleepInterval.toMillis)

        var sleepTime = math.min(sleepInterval, timeout - waited)
        if (sleepTime < 1L) {
          sleepTime = 1
        }
        Thread.sleep(sleepTime)
        waited += sleepTime
      }
    } while (!done && waited < timeout)

    done
  }

  private def monitorJob(connection: PartnerConnection,
      objId: String, waitDuration: Long) : Boolean = {
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

  private def maxWaitSeconds(waitDuration: Long) : Long = {
    // 2 Minutes
    val maxWaitDuration = 120000
    if (waitDuration >= maxWaitDuration) maxWaitDuration else waitDuration * 2
  }

  private def readMetadataConfig() : Map[String, Map[String, String]] = {
    val source = Source.fromURL(getClass.getResource("/metadata_config.json"))
    val jsonContent = try source.mkString finally source.close()

    readJSON(jsonContent)
  }

  private def readJSON(jsonContent : String) : Map[String, Map[String, String]]= {
    val result = JSON.parseFull(jsonContent)
    val resMap: Map[String, Map[String, String]] = result.get.asInstanceOf[Map[String, Map[String, String]]]
    resMap
  }

}
