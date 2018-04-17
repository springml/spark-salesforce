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

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

class TestUtils extends FunSuite with BeforeAndAfterEach {
  var ss: SparkSession = _

  override def beforeEach() {
    ss = SparkSession.builder().master("local").appName("Test Utils").getOrCreate()
  }

  override def afterEach() {
    ss.stop()
  }

  test("Test Metadata Configuration") {
    val metadataConfig = Utils.metadataConfig(null)

    assert(metadataConfig.size == 8)
    val integerConfig = metadataConfig.get("integer")
    assert(integerConfig.isDefined == true)
    assert(integerConfig.get.get("precision").isDefined == true)
    val timestampConfig = metadataConfig.get("timestamp")
    assert(timestampConfig.isDefined == true)
    assert(timestampConfig.get.get("format").isDefined == true)
    val doubleConfig = metadataConfig.get("double")
    assert(doubleConfig.isDefined == true)
    assert(doubleConfig.get.get("precision").isDefined == true)
  }

  test("Test Custom Metadata Configuration") {
    val customTimestampConfig = """{"timestamp":{"wave_type":"Date","format":"yyyy/MM/dd'T'HH:mm:ss"}}"""
    val metadataConfig = Utils.metadataConfig(Some(customTimestampConfig))

    assert(metadataConfig.size == 8)
    val timestampConfig = metadataConfig.get("timestamp")
    assert(timestampConfig.isDefined == true)
    assert(timestampConfig.get.get("format").isDefined == true)
    assert(timestampConfig.get.get("format").get == "yyyy/MM/dd'T'HH:mm:ss")
  }

  test("Test Custom Metadata Configuration with new datatype") {
    val customTimestampConfig = """{"mydataType":{"wave_type":"Date","format":"yy-MM-dd"}}"""
    val metadataConfig = Utils.metadataConfig(Some(customTimestampConfig))

    assert(metadataConfig.size == 9)
    val myDataTypeConfig = metadataConfig.get("mydataType")
    assert(myDataTypeConfig.isDefined == true)
    assert(myDataTypeConfig.get.get("format").isDefined == true)
    assert(myDataTypeConfig.get.get("format").get == "yy-MM-dd")
  }

  test("Test repartition for in memory RDD") {
    val inMemoryData = (0 to 2000).map(value => {
      val rowValues = Array(value, value + 1, value + 2, value + 3).map(value => value.toString)
      Row.fromSeq(rowValues)
    })

    val inMemoryRDD = ss.sparkContext.makeRDD(inMemoryData)
    val columnNames = List("c1", "c2", "c3", "c4")
    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))
    val schema = StructType(columnStruct)
    val inMemoryDF = ss.sqlContext.createDataFrame(inMemoryRDD, schema)

    val repartitionDF = Utils.repartition(inMemoryRDD)
    assert(repartitionDF.partitions.length == 1)
  }

  test("Test repartition for local CSV file with size less than 10 MB") {
    val csvURL= getClass.getResource("/ad-server-data-formatted.csv")
    val csvFilePath = csvURL.getPath
    val csvDF = ss.read.option("header", "true").csv(csvFilePath)

    val repartitionDF = Utils.repartition(csvDF.rdd)
    assert(repartitionDF.partitions.length == 1)
  }

  test("Test repartition for local CSV file with size > 10 MB and < 20 MB") {
    val csvURL= getClass.getResource("/minified_GDS_90.csv")
    val csvFilePath = csvURL.getPath
    val csvDF = ss.read.option("header", "true").csv(csvFilePath)

    val repartitionDF = Utils.repartition(csvDF.rdd)
    assert(repartitionDF.partitions.length == 2)
  }

  test("Check whether CSV Header constructed properly") {
    val intField = StructField("c1", IntegerType, true)
    val longField = StructField("c2", LongType, true)
    val floatField = StructField("c3", FloatType, true)
    val dateField = StructField("c4", DateType, true)
    val stringField = StructField("c5", StringType, true)

    val columnStruct = Array[StructField] (intField, longField, floatField, dateField, stringField)
    val schema = StructType(columnStruct)

    val expected = "c1,c2,c3,c4,c5"
    val actual = Utils.csvHeadder(schema)
    assert(expected.equals(actual))
  }

  test("retry with expoential backoff") {
    val timeoutDuration = FiniteDuration(5000L, MILLISECONDS)
    val initSleepIntervalDuration = FiniteDuration(100L, MILLISECONDS)
    val maxSleepIntervalDuration = FiniteDuration(500L, MILLISECONDS)
    var completed = false
    var attempts = 0
    var expectedNumberAttempts = 2
    Utils.retryWithExponentialBackoff(() => {
      attempts += 1
      completed = attempts == expectedNumberAttempts
      completed
    }, timeoutDuration, initSleepIntervalDuration, maxSleepIntervalDuration)

    assert(attempts == expectedNumberAttempts)
  }

}
