package com.springml.spark.salesforce.examples

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Writing in memory csv data to salesforce
 */
object InMemoryWrite {

  def main(args: Array[String]) {

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

    inMemoryDF.write.format("com.springml.spark.salesforce").option("username", "sparktest@springml.com").
      option("password", "Fire2015!ZlvTTJsBJopFBMXJWt0xBUjg0").option("datasetName", dataSetName).save()


  }

}
