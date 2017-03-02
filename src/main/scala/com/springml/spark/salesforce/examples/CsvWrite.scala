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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Example to read a CSV file and write it into Salesforce Wave
 */
object CsvWrite {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("csv write")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    val dataSetName = args(2)

    df.printSchema()

    df.write.format("com.springml.spark.salesforce").option("username", args(3)).
      option("password", args(4)).option("datasetName", dataSetName).save()


  }

}
