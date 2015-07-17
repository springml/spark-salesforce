package com.springml.spark.salesforce

import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.{Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import com.springml.spark.salesforce.Utils._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

/**
 * Default source for SalesForce wave data source. It writes any
 * given DF to Salesforce wave repository*
 *
 */
class DefaultSource extends CreatableRelationProvider{
  @transient val logger = Logger.getLogger(classOf[DefaultSource])
  private def createReturnRelation(data: DataFrame) = {
    new BaseRelation {

      override def sqlContext: SQLContext = data.sqlContext

      override def schema: StructType = data.schema
    }

  }


  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = parameters.getOrElse("username", sys.error("'username' must be specified for sales force."))
    val password = parameters.getOrElse("password", sys.error("'password' must be specified for sales force."))
    val datasetName = parameters.getOrElse("datasetName", sys.error("'datasetName' must be specified for sales force."))


    val dataWriter = new DataWriter(username,password,datasetName)

    val metaDataJson = Utils.generateMetaString(data.schema, datasetName)
    logger.info(s"metadata for dataset $datasetName is $metaDataJson")

    logger.info("uploading metadata for dataset " + datasetName)

    val writtenId = dataWriter.writeMetadata(metaDataJson)

    if (!writtenId.isDefined) {
      sys.error("unable to write metadata for data set" + datasetName)
    }

    logger.info(s"able to write the metadata is $writtenId")


    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("repartitioning rdd for 10mb partitions")

    val repartitionedRDD = Utils.repartition(data.rdd)

    logger.info("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    logger.info("writing data")

    val successfulWrite = dataWriter.writeData(repartitionedRDD, writtenId.get)

    logger.info(s"writing data was successful was$successfulWrite")

    if (!successfulWrite) {
      sys.error("unable to write data for " + datasetName)
    }

    logger.info("committing")

    val committed = dataWriter.commit(writtenId.get)

    logger.info(s"committing data was successful was $committed")

    if (!committed) {
      sys.error("unable to commit data for " + datasetName)
    }

    logger.info(s"successfully written data for dataset $datasetName ")

    return createReturnRelation(data)
  }


}



