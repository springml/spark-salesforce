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

import java.text.SimpleDateFormat

import com.springml.salesforce.wave.api.{APIFactory, BulkAPI, ForceAPI, WaveAPI}
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Default source for Salesforce wave data source.
 * It can write given DF to Salesforce wave repository
 * It can read Salesforce wave data source using provided SAQL and construct dataframe
 * It can read Salesforce objects using provided SOQL and construct dataframe
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])
  private def createReturnRelation(data: DataFrame) = {

    new BaseRelation {
      override def sqlContext: SQLContext = data.sqlContext
      override def schema: StructType = data.schema
    }
  }

  /**
   * Execute the SAQL against Salesforce Wave and construct dataframe with the result
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   *
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val username = optionalParam(parameters, "SF_USERNAME", "username")
    val authToken = optionalParam(parameters, "SF_AUTHTOKEN", "authToken")
    validateMutualExclusive(username, authToken, "username", "authToken")
    var password = ""
    var serverUrl = ""
    if (username.isDefined) {
      password = param(parameters, "SF_PASSWORD", "password")
      serverUrl = parameters.getOrElse("login", "https://login.salesforce.com")
    } else {
      serverUrl = parameters.getOrElse("instanceUrl", "https://login.salesforce.com")
    }


    val version = parameters.getOrElse("version", "36.0")
    val saql = parameters.get("saql")
    val soql = parameters.get("soql")
    val resultVariable = parameters.get("resultVariable")
    val pageSize = parameters.getOrElse("pageSize", "1000")
    val sampleSize = parameters.getOrElse("sampleSize", "1000")
    val maxRetry = parameters.getOrElse("maxRetry", "5")
    val inferSchema = parameters.getOrElse("inferSchema", "false")
    val dateFormat = parameters.getOrElse("dateFormat", null)
    // This is only needed for Spark version 1.5.2 or lower
    // Special characters in older version of spark is not handled properly
    val encodeFields = parameters.get("encodeFields")
    val replaceDatasetNameWithId = parameters.getOrElse("replaceDatasetNameWithId", "false")

    validateMutualExclusive(saql, soql, "saql", "soql")
    val inferSchemaFlag = flag(inferSchema, "inferSchema")

    if (saql.isDefined) {

      var waveAPI:WaveAPI = null

      if (username.isDefined) {
        waveAPI = APIFactory.getInstance.waveAPI(username.get, password, serverUrl, version)
      } else {
        waveAPI = APIFactory.getInstance.waveAPIwAuthToken(authToken.get, serverUrl, version)
      }

      DatasetRelation(waveAPI, null, saql.get, schema, sqlContext,
          resultVariable, pageSize.toInt, sampleSize.toInt,
          encodeFields, inferSchemaFlag, replaceDatasetNameWithId.toBoolean, sdf(dateFormat))
    } else {
      if (replaceDatasetNameWithId.toBoolean) {
        logger.warn("Ignoring 'replaceDatasetNameWithId' option as it is not applicable to soql")
      }
      var forceAPI: ForceAPI = null
      if (username.isDefined) {
        forceAPI = APIFactory.getInstance.forceAPI(username.get, password, serverUrl,
          version, Integer.getInteger(pageSize), Integer.getInteger(maxRetry))
      } else {
        forceAPI = APIFactory.getInstance.forceAPIwAuthToken(authToken.get, serverUrl,
          version, Integer.getInteger(pageSize), Integer.getInteger(maxRetry))
      }
      DatasetRelation(null, forceAPI, soql.get, schema, sqlContext,
          null, 0, sampleSize.toInt, encodeFields, inferSchemaFlag,
          replaceDatasetNameWithId.toBoolean, sdf(dateFormat))
    }

  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = optionalParam(parameters, "SF_USERNAME", "username")
    val authToken = optionalParam(parameters, "SF_AUTHTOKEN", "authToken")

    validateMutualExclusive(username, authToken, "username", "authToken")

    var password = ""
    var serverUrl = ""
    if (username.isDefined) {
      password = param(parameters, "SF_PASSWORD", "password")
      serverUrl = parameters.getOrElse("login", "https://login.salesforce.com")
    } else {
      serverUrl = parameters.getOrElse("instanceUrl", "https://login.salesforce.com")
    }

    val datasetName = parameters.get("datasetName")
    val sfObject = parameters.get("sfObject")
    val appName = parameters.getOrElse("appName", null)
    val version = parameters.getOrElse("version", "36.0")
    val usersMetadataConfig = parameters.get("metadataConfig")
    val upsert = parameters.getOrElse("upsert", "false")
    val metadataFile = parameters.get("metadataFile")
    val encodeFields = parameters.get("encodeFields")
    val monitorJob = parameters.getOrElse("monitorJob", "false")

    validateMutualExclusive(datasetName, sfObject, "datasetName", "sfObject")

    if (datasetName.isDefined) {
      val upsertFlag = flag(upsert, "upsert")
      if (upsertFlag) {
        if (metadataFile == null || !metadataFile.isDefined) {
          sys.error("metadataFile has to be provided for upsert" )
        }
      }

      logger.info("Writing dataframe into Salesforce Wave")
      writeInSalesforceWave(username, password, authToken, serverUrl, version,
          datasetName.get, appName, usersMetadataConfig, mode,
          flag(upsert, "upsert"), flag(monitorJob, "monitorJob"), data, metadataFile)
    } else {
      logger.info("Updating Salesforce Object")
      updateSalesforceObject(username, password, authToken, serverUrl, version, sfObject.get, mode, data)
    }

    return createReturnRelation(data)
  }

  private def updateSalesforceObject(
      username: Option[String],
      password: String,
      authToken: Option[String],
      serverUrl: String,
      version: String,
      sfObject: String,
      mode: SaveMode,
      data: DataFrame) {

    val csvHeader = Utils.csvHeadder(data.schema)
    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("Repartitioning rdd for 10mb partitions")
    val repartitionedRDD = Utils.repartition(data.rdd)
    logger.info("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    var bulkAPI:BulkAPI = null
    if (username.isDefined) {
      bulkAPI = APIFactory.getInstance.bulkAPI(username.get, password, serverUrl, version)
    } else {
      bulkAPI = APIFactory.getInstance.bulkAPIwAuthToken(authToken.get, serverUrl, version)
    }
    val writer = new SFObjectWriter(username, password, authToken, serverUrl, version, sfObject, mode, csvHeader)
    logger.info("Writing data")
    val successfulWrite = writer.writeData(repartitionedRDD)
    logger.info(s"Writing data was successful was $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to update salesforce object")
    }

  }

  private def writeInSalesforceWave(
      username: Option[String],
      password: String,
      authToken: Option[String],
      serverUrl: String,
      version: String,
      datasetName: String,
      appName: String,
      usersMetadataConfig: Option[String],
      mode: SaveMode,
      upsert: Boolean,
      monitorJob: Boolean,
      data: DataFrame,
      metadata: Option[String]) {
    val dataWriter = new DataWriter(username, password, authToken, serverUrl, version, datasetName, appName)

    val metaDataJson = Utils.metadata(metadata, usersMetadataConfig, data.schema, datasetName)

    logger.info(s"Metadata for dataset $datasetName is $metaDataJson")
    logger.info(s"Uploading metadata for dataset $datasetName")
    val writtenId = dataWriter.writeMetadata(metaDataJson, mode, upsert)
    if (!writtenId.isDefined) {
      sys.error("Unable to write metadata for dataset " + datasetName)
    }
    logger.info(s"Able to write the metadata is $writtenId")

    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("Repartitioning rdd for 10mb partitions")
    val repartitionedRDD = Utils.repartition(data.rdd)
    logger.debug("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    logger.info("Writing data")
    val successfulWrite = dataWriter.writeData(repartitionedRDD, writtenId.get)
    logger.info(s"Written data successfully? $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to write data for " + datasetName)
    }

    logger.info("Committing...")
    val committed = dataWriter.commit(writtenId.get)
    logger.info(s"committing data was successful was $committed")

    if (!committed) {
      sys.error("Unable to commit data for " + datasetName)
    }

    logger.info(s"Successfully written data for dataset $datasetName ")
    println(s"Successfully written data for dataset $datasetName ")

    if (monitorJob) {
      logger.info("Monitoring Job status in Salesforce wave")
      if (Utils.monitorJob(writtenId.get, username, password, authToken, serverUrl, version)) {
        logger.info(s"Successfully dataset $datasetName has been processed in Salesforce Wave")
      } else {
        sys.error(s"Upload Job for dataset $datasetName failed in Salesforce Wave. Check Monitor Job in Salesforce Wave for more details")
      }
    }

  }


  private def validateMutualExclusive(opt1: Option[String], opt2: Option[String],
      opt1Name: String, opt2Name: String) {
    if ((opt1.isDefined && opt2.isDefined)) {
      sys.error(s"""Anyone '$opt1Name' or '$opt2Name' have to be specified for creating dataframe""")
    }

    if ((!opt1.isDefined && !opt2.isDefined)) {
      sys.error(s"""Either '$opt1Name' or '$opt2Name' have to be specified for creating dataframe""")
    }
  }


  private def optionalParam(parameters: Map[String, String], envName: String, paramName: String) : Option[String] = {
    val envProp = sys.env.get(envName)
    if (envProp != null && envProp.isDefined) {
      return Option(envProp.get)
    }

    val param = parameters.getOrElse(paramName, null)
    return Option(param)
  }

  private def param(parameters: Map[String, String], envName: String, paramName: String) : String = {
    val envProp = sys.env.get(envName)
    if (envProp != null && envProp.isDefined) {
      return envProp.get
    }

    parameters.getOrElse(paramName,
        sys.error(s"""Either '$envName' has to be added in environment or '$paramName' must be specified for salesforce package."""))
  }

  private def sdf(dateFormat: String) : SimpleDateFormat = {
    var simpleDateFormat : SimpleDateFormat = null
    if (dateFormat != null && !dateFormat.isEmpty) {
      simpleDateFormat = new SimpleDateFormat(dateFormat)
    }

    simpleDateFormat
  }

  private def flag(paramValue: String, paramName: String) : Boolean = {
    if (paramValue == "false") {
      false
    } else if (paramValue == "true") {
      true
    } else {
      sys.error(s"""'$paramName' flag can only be true or false""")
    }
  }
}
