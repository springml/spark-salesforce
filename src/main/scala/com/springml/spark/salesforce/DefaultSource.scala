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
 * Entry point to the save part
 */


class DefaultSource extends CreatableRelationProvider {
   @transient val logger = Logger.getLogger(classOf[DefaultSource])



  private def writeMetadata(metaDataJson: String, datasetName: String,partnerConnection:PartnerConnection): Option[String] = {

    val sobj = new SObject()
    sobj.setType("InsightsExternalData")
    sobj.setField("Format", "Csv")
    sobj.setField("EdgemartAlias", datasetName)
    sobj.setField("MetadataJson", metaDataJson.getBytes)
    sobj.setField("Operation", "Overwrite")
    sobj.setField("Action", "None")
    val results = partnerConnection.create(Array(sobj))
    results.map(saveResult => {
      if (saveResult.isSuccess) {
        logger.info("successfully wrote metadata")
        Some(saveResult.getId)
      } else {
        logger.error("failed to write metadata")
        logSaveResultError(saveResult)
        None
      }
    }).head

  }

  private def repartition(rdd:RDD[Row]):RDD[Row] = {

    val NO_OF_ROWS_PARTITION=500
    val totalRows = rdd.count()
    val partititons = totalRows / NO_OF_ROWS_PARTITION
    val noPartitions = Math.max(rdd.partitions.length,partititons)
    val shuffle = rdd.partitions.length < partititons
    rdd.coalesce(noPartitions.toInt,shuffle)
  }


  private def writeRawData(rdd: RDD[Row], datasetName: String,userName:String,password:String):Boolean = {

    val csvRDD = rdd.map(row => row.toSeq.map(value => value.toString).mkString(","))
    csvRDD.mapPartitionsWithIndex {
      case (index, iterator) => {
        @transient val logger = Logger.getLogger(classOf[DefaultSource])
        val partNumber = index + 1
        val data = iterator.toArray.mkString("\n")
        val sobj = new SObject()
        sobj.setType("InsightsExternalDataPart")
        sobj.setField("DataFile", data.getBytes)
        sobj.setField("InsightsExternalDataId", datasetName)
        sobj.setField("PartNumber", partNumber)
        val partnerConnection = Utils.createConnection(userName,password)
        val results = partnerConnection.create(Array(sobj))

        val resultSuccess= results.map(saveResult => {
          if (saveResult.isSuccess) {
            logger.info(s"successfully written for $datasetName for part $partNumber")
            true
          } else {
            logger.info(s"error writing for $datasetName for part $partNumber")
            logSaveResultError(saveResult)
            false
          }
        }).head
        List(resultSuccess).iterator
      }

    }.reduce((a,b) => a && b)
  }

  private def commit(id: String,partnerConnection: PartnerConnection):Boolean = {

    val sobj = new SObject()

    sobj.setType("InsightsExternalData")

    sobj.setField("Action", "Process")

    sobj.setId(id)

    val results = partnerConnection.update(Array(sobj))

    // This is the rowID from the previous example.
    val saved = results.map(saveResult => {
      if (saveResult.isSuccess) {
        logger.info("committed complete")
        true
      } else {
        println(saveResult.getErrors.toList)
        false
      }
    }).reduce((a, b) => a && b)
    saved
  }




  private def createConnection(username:String,password:String):PartnerConnection = {
    val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)
    Connector.newConnection(config)


  }


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


    logger.info("connecting to sales force")
    val connection = createConnection(username,password)
    logger.info("connected to sales force")

    val metaDataJson = Utils.generateMetaString(data.schema, "test")
    logger.info(s"metadata for dataset $datasetName is $metaDataJson")

    logger.info("uploading metadata for dataset " + datasetName)

    val writtenId = writeMetadata(metaDataJson, datasetName,connection)

    if(!writtenId.isDefined) {
      sys.error("unable to write metadata for data set" +datasetName)
    }

    logger.info(s"able to write the metadata is $writtenId")


    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("repartitioning rdd for 10mb partitions")

    val repartitionedRDD = repartition(data.rdd)

    logger.info("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    logger.info("writing data")

    val successfulWrite = writeRawData(repartitionedRDD, writtenId.get,username,password)

    logger.info(s"writing data was successful was$successfulWrite")

    if(!successfulWrite) {
      sys.error("unable to write data for " +datasetName)
    }

    logger.info("committing")

    val committed = commit(writtenId.get,connection)

    logger.info(s"committing data was successful was $committed")

    if(!committed) {
      sys.error("unable to commit data for " +datasetName)
    }


    return createReturnRelation(data)
  }




}



