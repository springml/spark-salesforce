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
package com.springml.spark.salesforce.examples

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Writing in memory csv data to salesforce
 */
object InMemoryWrite {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("csv write")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val dataSetName = args(1)

    val inMemoryData = (0 to 500).map(value => {
      val rowValues = Array(value, value + 1, value + 2, value + 3).map(value => value.toString)
      Row.fromSeq(rowValues)
    })

    val inMemoryRDD = sc.makeRDD(inMemoryData)
    val columnNames = List("c1", "c2", "c3", "c4")
    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))
    val schema = StructType(columnStruct)

    val inMemoryDF = sqlContext.createDataFrame(inMemoryRDD, schema)
    inMemoryDF.printSchema()

    inMemoryDF.write.format("com.springml.spark.salesforce").option("username", args(2)).
      option("password", args(3)).option("datasetName", dataSetName).save()

  }

}
