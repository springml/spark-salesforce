package com.springml.spark.salesforce

import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.{Connector, PartnerConnection, SaveResult}
import com.sforce.ws.ConnectorConfig
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import Utils._

/**
 * Entry point to the Data source API
 */
class DefaultSource extends CreatableRelationProvider{

  val logger = Logger.getLogger(classOf[DefaultSource])


  private def repartition(rdd:RDD[Row]):RDD[Row] = rdd

  private def writeRawData(rdd: RDD[Row], datasetName: String,userName:String,password:String) = {

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

        results.map(saveResult => {
          if (saveResult.isSuccess) {
            logger.info(s"successfully written for $datasetName for part $partNumber")
          } else {
            logSaveResultError(saveResult)
          }
        })

        List(true).iterator
      }

    }.collect()
  }

  private def commit(id: String,partnerConnection: PartnerConnection) = {

    val sobj = new SObject()

    sobj.setType("InsightsExternalData")

    sobj.setField("Action", "Process")

    sobj.setId(id)

    val results = partnerConnection.update(Array(sobj))

    println(sobj.getField("Action"))

    // This is the rowID from the previous example.
    val saved = results.map(saveResult => {
      if (saveResult.isSuccess) {
        println("committed" + saveResult.getSuccess)
        true
      } else {
        println(saveResult.getErrors.toList)

        false
      }
    }).reduce((a, b) => a || b)
  }


  private def writeMetadata(metaDataJson:String,datasetName:String,partnerConnection: PartnerConnection): Option[String] ={

    val sobj = new SObject()
    sobj.setType("InsightsExternalData")
    sobj.setField("Format","Csv")
    sobj.setField("EdgemartAlias", datasetName)
    sobj.setField("MetadataJson",metaDataJson.getBytes)
    sobj.setField("Operation","Overwrite")
    sobj.setField("Action","None")
    val results = partnerConnection.create(Array(sobj))
    results.map(saveResult => {
      if(saveResult.isSuccess) {
        logger.info("successfully wrote metadata")
        Some(saveResult.getId)
      } else {
        logger.error("failed to write metadata")
        logSaveResultError(saveResult)
        None
      }
    }).filter(_.isDefined).head
  }

  private def createConnection(username:String,password:String):PartnerConnection = {
    val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)
    Connector.newConnection(config)

  }





  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val username = parameters.getOrElse("username", sys.error("'username' must be specified for sales force."))
    val password = parameters.getOrElse("password", sys.error("'password' must be specified for sales force."))
    val datasetName = parameters.getOrElse("datasetName", "testdataset")

    logger.info("connecting to sales force")
    val connection = createConnection(username,password)
    logger.info("connected to sales force")

    val metaDataJson = Utils.generateMetaString(data.schema, "test")
    logger.info(s"metadata for dataset $datasetName is $metaDataJson")

    logger.info("uploading metadata for dataset " + datasetName)

    val writtenId = writeMetadata(metaDataJson, datasetName,connection)
    logger.info(s"able to write the metadata is $writtenId")

    logger.info("repartitioning rdd for 10mb partitions")

    val repartitionedRDD = repartition(data.rdd)

    logger.info("writing raw data")

    val rawDataId = writeRawData(repartitionedRDD, writtenId.get,username,password)

    logger.info(s"able to write the raw data is $rawDataId")

    logger.info("finalizing")

    commit(writtenId.get,connection)


    return  null // need to return an relation
  }
}


