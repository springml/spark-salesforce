package com.springml.spark.salesforce

import java.nio.charset.Charset

import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.{SaveResult, Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import com.springml.spark.salesforce.Utils
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}

import scala.util.Try

/**
 * Entry point to the save part
 */


class DefaultSource extends CreatableRelationProvider{
  val logger = Logger.getLogger(classOf[DefaultSource])
  def logSaveResultError(result:SaveResult): Unit = {
    result.getErrors.map(error => {
      logger.error(error.getMessage)
      error.getFields.map(logger.error(_))
    })
  }
  private def writeMetadata(metaDataJson:String,datasetName:String)(implicit partnerConnection: PartnerConnection): Option[String] ={

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


  private def writeRawData(rdd:RDD[Row],datsetName:String)(implicit partnerConnection: PartnerConnection) = {

    val data = rdd.map(_.toString().mkString(",")).collect().mkString("\n")
    val sobj = new SObject()
    sobj.setType("InsightsExternalDataPart")
    sobj.setField("DataFile", data.getBytes)
    //sobj.setField("EdgemartAlias", datsetName)
    sobj.setField("InsightsExternalDataId", datsetName)
    sobj.setField("PartNumber", 1)
    val results = partnerConnection.create(Array(sobj))

    var parentId = ""
    val saved  =results.map(saveResult => {
      if(saveResult.isSuccess) {
        parentId = saveResult.getId
        true
      } else {
        println(saveResult.getErrors.toList)

        false
      }
    }).reduce((a,b) => a || b)

    parentId
  }

  private def commit(id:String)(implicit partnerConnection: PartnerConnection) = {

    val sobj = new SObject()

    sobj.setType("InsightsExternalData")

    sobj.setField("Action","Process")

    sobj.setId(id)

    val results = partnerConnection.update(Array(sobj))

    println(sobj.getField("Action"))

     // This is the rowID from the previous example.
    val saved  =results.map(saveResult => {
        if(saveResult.isSuccess) {
          println("commited"+saveResult.getSuccess)
          true
        } else {
          println(saveResult.getErrors.toList)

          false
        }
      }).reduce((a,b) => a || b)



  }



  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = parameters.getOrElse("username", sys.error("'username' must be specified for sales force."))
    val password = parameters.getOrElse("password", sys.error("'password' must be specified for sales force."))
    val datasetName = parameters.getOrElse("datasetName", "testdataset")

    val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)

    implicit val connection = Connector.newConnection(config)

    logger.info("able to connect to the sales force webservice")
    logger.info("end points are" +config.getAuthEndpoint)

    val metaDataJson = Utils.generateMetaString(data.schema,"test")
    logger.info(s"metadata for dataset $datasetName is $metaDataJson")

    //logger.info("uploading metadata for dataset " + datasetName)

    val writtenId = writeMetadata(metaDataJson,datasetName)
    logger.info(s"able to write the metadata is $writtenId")

    val rawDataId = writeRawData(data.rdd,writtenId.get)

    logger.info(s"able to write the raw data is $rawDataId")

    commit(writtenId.get)

    return  null
  }


}

