package com.springml.spark.salesforce

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.scalatest.FunSuite
class TestUtils extends FunSuite {

  test("Test Metadata generation") {

    val columnNames = List("c1", "c2", "c3", "c4")
    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))
    val schema = StructType(columnStruct)

    val schemaString = Utils.generateMetaString(schema,"sampleDataSet")

    assert(schemaString.length > 0)

    assert(schemaString.contains("sampleDataSet"))

  }

  test("Test repartition") {
    val sparkConf = new SparkConf().setMaster("local").setAppName("data repartition")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    val inMemoryData = (0 to 2000).map(value => {
      val rowValues = Array(value, value + 1, value + 2, value + 3).map(value => value.toString)
      Row.fromSeq(rowValues)
    })

    val inMemoryRDD = sc.makeRDD(inMemoryData)

    val columnNames = List("c1", "c2", "c3", "c4")

    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))

    val schema = StructType(columnStruct)

    val inMemoryDF = sqlContext.createDataFrame(inMemoryRDD, schema)

    val repartitionDF = Utils.repartition(inMemoryRDD)

    assert(repartitionDF.partitions.length >= 29)


  }




}
