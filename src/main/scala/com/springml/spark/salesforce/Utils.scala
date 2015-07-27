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

import com.sforce.soap.partner.{SaveResult, Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import com.madhukaraphatak.sizeof.SizeEstimator

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

/**
 * Utility to construct metadata and repartition RDD
 */
object Utils extends Serializable {

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
    config.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/34.0")
    config.setServiceEndpoint("https://login.salesforce.com/services/Soap/u/34.0")
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
    val totalDataSize = getTotalSize(rdd)
    val maxBundleSize = 1024 * 1024 * 10l;
    var partitions = 1
    if (totalDataSize > maxBundleSize) {
      partitions = Math.round(totalDataSize / maxBundleSize) + 1
    }

    val shuffle = rdd.partitions.length < partitions
    rdd.coalesce(partitions.toInt, shuffle)
  }

  def getTotalSize(rdd: RDD[Row]): Long = {
    // This can be fetched as optional parameter
    val NO_OF_SAMPLE_ROWS = 10;
    val totalRows = rdd.count();
    var totalSize = 0l
    
    if (totalRows > NO_OF_SAMPLE_ROWS) {
      val sampleObj = rdd.takeSample(false, NO_OF_SAMPLE_ROWS)
      val sampleRowSize = rowSize(sampleObj)
      totalSize = sampleRowSize * (totalRows / NO_OF_SAMPLE_ROWS)
    } else {
    
      totalSize = rddSize(rdd)
    }
    
    totalSize
  }

  def rddSize(rdd: RDD[Row]) : Long = {
    rowSize(rdd.collect())
  }
  
  def rowSize(rows: Array[Row]) : Long = {
      var sizeOfRows = 0l
      for (row <- rows) {
      //for (i <- 0 until rows.length) {
        val rowSize = SizeEstimator.estimate(row.toSeq.map { value => value.toString() }.mkString(","))
        // Converting to bytes
        sizeOfRows += rowSize
      }
    
      sizeOfRows
  }
  
}
