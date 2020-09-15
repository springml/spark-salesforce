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

import com.springml.salesforce.wave.api.APIFactory
import org.apache.commons.io.FileUtils.byteCountToDisplaySize
import org.apache.http.Header
import org.apache.http.message.BasicHeader
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.util.Try

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
    val username = param(parameters, "SF_USERNAME", "username")
    val password = param(parameters, "SF_PASSWORD", "password")
    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    val version = parameters.getOrElse("version", "36.0")
    val saql = parameters.get("saql")
    val soql = parameters.get("soql")
    val resultVariable = parameters.get("resultVariable")
    val pageSize = getIntParam(parameters, "pageSize").getOrElse(1000)
    val sampleSize = getIntParam(parameters, "sampleSize").getOrElse(1000)
    val maxRetry = getIntParam(parameters, "maxRetry").getOrElse(5)
    val inferSchemaFlag = getBooleanParam(parameters, "inferSchema").getOrElse(false)
    val dateFormat = parameters.getOrElse("dateFormat", null)
    // This is only needed for Spark version 1.5.2 or lower
    // Special characters in older version of spark is not handled properly
    val encodeFields = parameters.get("encodeFields")
    val replaceDatasetNameWithId = parameters.getOrElse("replaceDatasetNameWithId", "false")

    val bulkFlag = getBooleanParam(parameters, "bulk").getOrElse(false)
    val queryAllFlag = getBooleanParam(parameters, "queryAll").getOrElse(false)

    validateMutualExclusive(saql, soql, "saql", "soql")

    if (saql.isDefined) {
      val waveAPI = APIFactory.getInstance.waveAPI(username, password, login, version)
      DatasetRelation(waveAPI, null, saql.get, schema, sqlContext,
          resultVariable, pageSize, sampleSize,
          encodeFields, inferSchemaFlag, replaceDatasetNameWithId.toBoolean, sdf(dateFormat),
          queryAllFlag)
    } else {
      if (replaceDatasetNameWithId.toBoolean) {
        logger.warn("Ignoring 'replaceDatasetNameWithId' option as it is not applicable to soql")
      }

      if (soql.isEmpty) {
        throw new Exception("soql must not be empty")
      }

      if (bulkFlag) {
        createBulkRelation(sqlContext, username, password, login, version, inferSchemaFlag, parameters, schema)
      } else {
        val forceAPI = APIFactory.getInstance.forceAPI(username, password, login, version, pageSize, maxRetry)
        DatasetRelation(null, forceAPI, soql.get, schema, sqlContext,
          null, 0, sampleSize.toInt, encodeFields, inferSchemaFlag,
          replaceDatasetNameWithId.toBoolean, sdf(dateFormat), queryAllFlag)
      }
    }

  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = param(parameters, "SF_USERNAME", "username")
    val password = param(parameters, "SF_PASSWORD", "password")
    val datasetName = parameters.get("datasetName")
    val sfObject = parameters.get("sfObject")
    val appName = parameters.getOrElse("appName", null)
    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    val version = parameters.getOrElse("version", "36.0")
    val usersMetadataConfig = parameters.get("metadataConfig")
    val upsertFlag = getBooleanParam(parameters, "upsert").getOrElse(false)
    val batchSize = getLongParam(parameters, "batchSize").getOrElse(1024 * 1024 * 10L)
    val batchRecords = getIntParam(parameters, "batchRecords")
    val metadataFile = parameters.get("metadataFile")
    val monitorJobFlag = getBooleanParam(parameters, "monitorJob").getOrElse(false)
    val externalIdFieldName = parameters.getOrElse("externalIdFieldName", "Id")

    validateMutualExclusive(datasetName, sfObject, "datasetName", "sfObject")

    if (datasetName.isDefined) {
      if (upsertFlag) {
        if (metadataFile == null || !metadataFile.isDefined) {
          sys.error("metadataFile has to be provided for upsert" )
        }
      }

      logger.info("Writing dataframe into Salesforce Wave")
      writeInSalesforceWave(username, password, login, version,
          datasetName.get, appName, usersMetadataConfig, mode,
          upsertFlag, monitorJobFlag, batchSize, batchRecords, data, metadataFile)
    } else {
      logger.info("Updating Salesforce Object")
      updateSalesforceObject(username, password, login, version, sfObject.get, mode,
          upsertFlag, externalIdFieldName, batchSize, batchRecords, data)
    }

    createReturnRelation(data)
  }

  private def updateSalesforceObject(
      username: String,
      password: String,
      login: String,
      version: String,
      sfObject: String,
      mode: SaveMode,
      upsert: Boolean,
      externalIdFieldName: String,
      batchSize: Long,
      batchRecords: Option[Int],
      data: DataFrame) {

    val csvHeader = Utils.csvHeadder(data.schema)
    logger.info("Number of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info(s"Repartitioning rdd for ${byteCountToDisplaySize(batchSize)} partitions${batchRecords.fold("")(n => s" with $n records")}")
    val repartitionedRDD = Utils.repartition(data.rdd, batchSize, batchRecords)
    logger.info("Number of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    val writer = new SFObjectWriter(username, password, login, version, sfObject, mode, upsert, externalIdFieldName, csvHeader)
    logger.info("Writing data")
    val successfulWrite = writer.writeData(repartitionedRDD)
    logger.info(s"Writing data was successful was $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to update salesforce object")
    }

  }

  private def createBulkRelation(
      sqlContext: SQLContext,
      username: String,
      password: String,
      login: String,
      version: String,
      inferSchemaFlag: Boolean,
      parameters: Map[String, String],
      schema: StructType): BulkRelation = {
    val soql = parameters.get("soql")

    val sfObject = parameters.get("sfObject")
    if (sfObject.isEmpty) {
      throw new Exception("sfObject must not be empty when performing bulk query")
    }

    val timeout = getLongParam(parameters, "timeout").getOrElse(600000L)

    var customHeaders = ListBuffer[Header]()
    val pkChunking = getBooleanParam(parameters, "pkChunking").getOrElse(false)

    if (pkChunking) {
      val pkChunkingValue = getIntParam(parameters, "chunkSize").fold("true")(size => s"chunkSize=$size")
      customHeaders += new BasicHeader("Sforce-Enable-PKChunking", pkChunkingValue)
    }

    BulkRelation(
      username,
      password,
      login,
      version,
      soql.get,
      sfObject.get,
      customHeaders.toList,
      schema,
      sqlContext,
      inferSchemaFlag,
      timeout
    )
  }

  private def writeInSalesforceWave(
      username: String,
      password: String,
      login: String,
      version: String,
      datasetName: String,
      appName: String,
      usersMetadataConfig: Option[String],
      mode: SaveMode,
      upsert: Boolean,
      monitorJob: Boolean,
      batchSize: Long,
      batchRecords: Option[Int],
      data: DataFrame,
      metadata: Option[String]) {
    val dataWriter = new DataWriter(username, password, login, version, datasetName, appName)

    val metaDataJson = Utils.metadata(metadata, usersMetadataConfig, data.schema, datasetName)

    logger.info(s"Metadata for dataset $datasetName is $metaDataJson")
    logger.info(s"Uploading metadata for dataset $datasetName")
    val writtenId = dataWriter.writeMetadata(metaDataJson, mode, upsert)
    if (!writtenId.isDefined) {
      sys.error("Unable to write metadata for dataset " + datasetName)
    }
    logger.info(s"Able to write the metadata is $writtenId")

    logger.info("Number of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info(s"Repartitioning rdd for ${byteCountToDisplaySize(batchSize)} partitions${batchRecords.fold("")(n => s" with $n records")}")
    val repartitionedRDD = Utils.repartition(data.rdd, batchSize, batchRecords)
    logger.debug("Number of partitions after repartitioning is " + repartitionedRDD.partitions.length)

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
      if (Utils.monitorJob(writtenId.get, username, password, login, version)) {
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

  private def getBooleanParam(params: Map[String, String], paramName: String) = params.get(paramName).map {
    case "false" => false
    case "true" => true
    case _ => sys.error(s"'$paramName' flag can only be true or false")
  }

  private def getLongParam(params: Map[String, String], paramName: String) = params.get(paramName)
    .map(v => Try(v.toLong).getOrElse(sys.error(s"'$paramName' must be of type Long")))

  private def getIntParam(params: Map[String, String], paramName: String) = params.get(paramName)
    .map(v => Try(v.toInt).getOrElse(sys.error(s"'$paramName' must be of type Int")))
}
