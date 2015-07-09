package com.springml.spark.salesforce

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

/**
 * Created by madhu on 9/7/15.
 */
object Utils {

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





}
