package com.springml.spark.salesforce

import com.sforce.soap.partner.{SaveResult, Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

/**
 * Created by madhu on 9/7/15.
 */
object Utils extends Serializable{


  private def fieldJson(fieldName:String,datasetName:String) = {
    val qualifiedName = datasetName+"."+fieldName
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
  }

  def generateMetaString(schema:StructType, datasetName:String):String = {
    val beginJsonString =
      s"""
        |{
        |"fileFormat": {
        |"charsetName": "UTF-8",
        |"fieldsDelimitedBy": ",",
        |"numberOfLinesToIgnore": 0
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

   def repartition(rdd: RDD[Row]): RDD[Row] = {

    val NO_OF_ROWS_PARTITION = 500
    val totalRows = rdd.count()
    val partitions = Math.round(totalRows / NO_OF_ROWS_PARTITION) + 1
    //val noPartitions = Math.max(rdd.partitions.length, partititons)
    val shuffle = rdd.partitions.length < partitions
    rdd.coalesce(partitions.toInt, shuffle)
  }





}
