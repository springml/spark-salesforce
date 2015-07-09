package com.springml.spark.salesforce

import com.databricks.spark.csv.CsvRelation
import com.sforce.soap.partner.{Connector, SaveResult, PartnerConnection}
import com.sforce.soap.partner.sobject.SObject
import com.sforce.ws.ConnectorConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}

/**
 * Entry point to the Data source API
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
    implicit val connection = createConnection(username,password)
    logger.info("connected to sales force")

    val metaDataJson = Utils.generateMetaString(data.schema,"test")
    logger.info(s"metadata for dataset $datasetName is $metaDataJson")

    val writtenId = writeMetadata(metaDataJson,datasetName)
    logger.info(s"able to write the metadata is $writtenId")




    return  null // need to return an relation
  }
}


