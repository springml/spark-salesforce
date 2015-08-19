package com.springml.spark.salesforce

import java.util.ArrayList
import java.util.HashMap

import scala.collection.JavaConversions._

import org.scalatest.{ FunSuite, BeforeAndAfterEach}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import com.springml.salesforce.wave.api.WaveAPI
import com.springml.salesforce.wave.model.{ QueryResult, Results}

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructField, IntegerType, StructType, StringType}

/**
 * Test DatasetRelation with schema and without schema
 */
class TestDatasetRelation extends FunSuite with BeforeAndAfterEach with MockitoSugar {
  val waveAPI = mock[WaveAPI]
  val saql = "q = load \"0FbB000000007qmKAA/0FcB00000000LgTKAU\"; q = group q by ('event', 'device_type'); q = foreach q generate 'event' as 'event',  'device_type' as 'device_type', count() as 'count'; q = limit q 2000;";
  val qr = testQR()

  private def testQR() : QueryResult = {
    val results = new Results
    val records: java.util.List[java.util.Map[String, String]] = new ArrayList[java.util.Map[String, String]]
    val record: java.util.Map[String, String] = new HashMap[String, String]
    record.put("count", "12")
    record.put("device_type", "Android")
    records.add(record)
    results.setRecords(records)

    val qr = new QueryResult
    qr.setResults(results)
    qr.setQuery(saql);

    qr
  }

  override def beforeEach() {
    when(waveAPI.query(saql)).thenReturn(qr)
  }

  private def validate(rdd: RDD[Row]) {
    assert(rdd != null)
    assert(rdd.count() == 1l)
    val arr = rdd.collect()
    val actualRecord = arr.apply(0)
    assert(actualRecord != null)
    print(actualRecord.mkString)
    assert(actualRecord.mkString.contains("12Android"))
  }

  test ("test read without schema") {
    val dr = DatasetRelation(waveAPI, saql, null, TestSQLContext)
    val rdd = dr.buildScan()
    validate(rdd)
  }

  test ("test read with schema") {
    val countField = StructField("count", IntegerType, true)
    val deviceTypeField = StructField("device_type", StringType, true)

    val fields = Array[StructField] (countField, deviceTypeField)
    val schema = StructType(fields)

    val dr = DatasetRelation(waveAPI, saql, schema, TestSQLContext)
    val rdd = dr.buildScan()
    validate(rdd)
  }
}