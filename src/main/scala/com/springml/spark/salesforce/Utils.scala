package com.springml.spark.salesforce

import com.sforce.soap.partner.{SaveResult, Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

/**
 * Created by madhu on 9/7/15.
 */
object Utils extends Serializable{


  private def fieldJson(fieldName:String,datasetName:String) = {
    val qualifiedName = datasetName+"."+fieldName
    //if(fieldName.equalsIgnoreCase("transactionId"))
    s"""{
     "description": "",
      "fullyQualifiedName": "$qualifiedName",
      "label": "$fieldName",
      "name": "$fieldName",
      "isSystemField": false,
      "isUniqueId": false,
      "isMultiValue": false,
      "type": "Text"
    } """
    /*else
      s"""{
     "description": "",
      "fullyQualifiedName": "$qualifiedName",
      "label": "$fieldName",
      "name": "$fieldName",
      "isSystemField": false,
      "isUniqueId": false,
      "isMultiValue": false,
      "type": "Numeric",
      "defaultValue": "0",
      "precision": 10
      } """*/

  }

  def generateMetaString(schema:StructType,datasetName:String):String = {
    val beginJsonString =
      s"""
        |{
        |"fileFormat": {
        |"charsetName": "UTF-8",
        |"fieldsDelimitedBy": ",",
        |"numberOfLinesToIgnore": 1
        |},
        |"objects": [
        |{
        |"connector": "AcmeCSVConnector",
        |"description": "",
        |"fullyQualifiedName": "$datasetName",
        |"label": "$datasetName",
        |"name": "$datasetName",
      """.stripMargin

    val fieldsJson = schema.fieldNames.map(field => fieldJson(field,datasetName)).mkString(",")

    val finalJson = beginJsonString+"""  "fields":[  """+ fieldsJson+"]"+"}]}"

    finalJson
  }

  def createConnection(username:String,password:String):PartnerConnection = {
    val config = new ConnectorConfig()
    config.setUsername(username)
    config.setPassword(password)
    Connector.newConnection(config)

  }

  def logSaveResultError(result: SaveResult): Unit = {
    @transient val logger = Logger.getLogger(classOf[DefaultSource])
    result.getErrors.map(error => {
      logger.error(error.getMessage)
      error.getFields.map(logger.error(_))
    })
  }





}
