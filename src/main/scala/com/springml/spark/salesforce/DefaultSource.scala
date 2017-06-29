/*
 * Copyright 2015 - 2017, springml, oolong  
 * Contributors  :
 * 	  Samual Alexander, springml  
 * 		Kagan Turgut, Oolong Inc.
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

import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.{ Connector, PartnerConnection }
import com.sforce.ws.ConnectorConfig
import com.springml.spark.salesforce.Utils._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{ BaseRelation, CreatableRelationProvider }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import com.springml.spark.salesforce.metadata.MetadataConstructor
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import com.springml.salesforce.wave.api.APIFactory
import org.apache.spark.sql.DataFrame
import scala.io.Source
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.AWSCredentialsProvider

/**
 * Default source for Salesforce wave data source.
 * It can write given DF to Salesforce wave repository
 * It can read Salesforce wave data source using provided SAQL and construct dataframe
 * It can read Salesforce objects using provided SOQL and construct dataframe
 */
class DefaultSource (s3ClientFactory: AWSCredentialsProvider => AmazonS3Client) extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])
  
  /**
   * Default constructor required by Data Source API
   */
  def this() = this(awsCredentials => new AmazonS3Client(awsCredentials))

  
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

    val params = Parameters.mergeParameters(parameters,false)

    if (params.saql.isDefined) {
      val waveAPI = APIFactory.getInstance.waveAPI(params.user, params.password, params.login, params.version)
      DatasetRelation(waveAPI, null, params.saql.get, schema, sqlContext,
        params.resultVariable, params.pageSize, params.sampleSize,
        params.encodeFields, params.inferSchema, params.replaceDatasetNameWithId)
    } else {
      if (params.replaceDatasetNameWithId) {
        logger.warn("Ignoring 'replaceDatasetNameWithId' option as it is not applicable to soql")
      }

      /**
       * SOQL running in bulk mode
       */
      if (params.soql.isDefined && params.bulk) {
        BulkRelation(params, s3ClientFactory, None)(sqlContext)
      } else {
        val forceAPI = APIFactory.getInstance.forceAPI(params.user, params.password, params.login,
          params.version, params.pageSize, params.maxRetry)
        DatasetRelation(null, forceAPI, params.soql.get, schema, sqlContext,
          null, 0, params.sampleSize, params.encodeFields, params.inferSchema, params.replaceDatasetNameWithId)
      }
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val params = Parameters.mergeParameters(parameters, true)

    if (params.datasetName.isDefined) {
      logger.info("Writing dataframe into Salesforce Wave")
      writeInSalesforceWave(params.user, params.password, params.login, params.version,
        params.datasetName.get, params.appName.getOrElse(null), params.userMetaConfig, mode,
        params.upsert, params.monitorJob, data, params.metadataFile)
    } else {
      logger.info("Updating Salesforce Object")
      updateSalesforceObject(params.user, params.password, params.login, params.version, params.sfObject.get, mode, data)
    }

    return createReturnRelation(data)
  }
  
  private def updateSalesforceObject(
    username: String,
    password: String,
    login: String,
    version: String,
    sfObject: String,
    mode: SaveMode,
    data: DataFrame) {

    val csvHeader = Utils.csvHeadder(data.schema);
    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("Repartitioning rdd for 10mb partitions")
    val repartitionedRDD = Utils.repartition(data.rdd)
    logger.info("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    val bulkAPI = APIFactory.getInstance.bulkAPI(username, password, login, version)
    val writer = new SFObjectWriter(username, password, login, version, sfObject, mode, csvHeader)
    logger.info("Writing data")
    val successfulWrite = writer.writeData(repartitionedRDD)
    logger.info(s"Writing data was successful was $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to update salesforce object")
    }

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
      if (Utils.monitorJob(writtenId.get, username, password, login, version)) {
        logger.info(s"Successfully dataset $datasetName has been processed in Salesforce Wave")
      } else {
        sys.error(s"Upload Job for dataset $datasetName failed in Salesforce Wave. Check Monitor Job in Salesforce Wave for more details")
      }
    }

  }

}
