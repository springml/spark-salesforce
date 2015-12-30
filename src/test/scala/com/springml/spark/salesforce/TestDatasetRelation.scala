package com.springml.spark.salesforce

import java.util.ArrayList
import java.util.HashMap
import scala.collection.JavaConversions._
import org.scalatest.{ FunSuite, BeforeAndAfterEach}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.springml.salesforce.wave.api.WaveAPI
import com.springml.salesforce.wave.model.{ QueryResult, Results}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ StructField, IntegerType, StructType, StringType}
import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.spark.sql.{ SQLContext, Row}
import com.springml.salesforce.wave.api.ForceAPI
import com.springml.salesforce.wave.model.SOQLResult

/**
 * Test DatasetRelation with schema and without schema
 */
class TestDatasetRelation extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val waveAPI = mock[WaveAPI]
  val forceAPI = mock[ForceAPI]
  val saql = "q = load \"0FbB000000007qmKAA/0FcB00000000LgTKAU\"; q = group q by ('event', 'device_type'); q = foreach q generate 'event' as 'event',  'device_type' as 'device_type', count() as 'count'; q = limit q 2000;";
  val soql = "SELECT AccountId, Id, ProposalID__c FROM Opportunity where ProposalID__c != null";
  val qr = testQR()
  val soqlQR = testSOQLQR()
  val paginatedQR = testPaginatedQR()

  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  override def beforeEach() {
    when(waveAPI.query(saql)).thenReturn(qr)
    when(waveAPI.queryWithPagination(saql, "q", 2)).thenReturn(paginatedQR)
    when(waveAPI.queryMore(any())).thenReturn(qr)
    when(forceAPI.query(soql)).thenReturn(soqlQR);
    sparkConf = new SparkConf().setMaster("local").setAppName("Test Dataset Relation")
    sc = new SparkContext(sparkConf)
  }

  override def afterEach() {
    sc.stop()
  }

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
    qr.setDone(true)

    qr
  }

  private def testPaginatedQR() : QueryResult = {
    val results = new Results
    val records: java.util.List[java.util.Map[String, String]] = new ArrayList[java.util.Map[String, String]]
    val record: java.util.Map[String, String] = new HashMap[String, String]
    record.put("count", "12")
    record.put("device_type", "Android")
    records.add(record)

    val record1: java.util.Map[String, String] = new HashMap[String, String]
    record1.put("count", "15")
    record1.put("device_type", "iOS")
    records.add(record1)
    results.setRecords(records)

    val qr = new QueryResult
    qr.setResults(results)
    qr.setQuery(saql)
    qr.setDone(false)
    qr.setOffset(2)
    qr.setLimit(2)
    qr.setResultVariable("q")

    qr
  }

  private def testSOQLQR() : SOQLResult = {
    val results = new SOQLResult
    results.setDone(true)
    val records: java.util.List[java.util.Map[String, Object]] = new ArrayList[java.util.Map[String, Object]]
    val record: java.util.Map[String, Object] = new HashMap[String, Object]
    record.put("count", "12")
    record.put("device_type", "Android")
    records.add(record)
    results.setRecords(records)

    results
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
    val sqlContext = new SQLContext(sc)
    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, null, 0)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test read with schema") {
    val sqlContext = new SQLContext(sc)
    val countField = StructField("count", IntegerType, true)
    val deviceTypeField = StructField("device_type", StringType, true)

    val fields = Array[StructField] (countField, deviceTypeField)
    val schema = StructType(fields)

    val dr = DatasetRelation(waveAPI, null, saql, schema, sqlContext, null, 0)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test queryMore") {
    val sqlContext = new SQLContext(sc)
    var resultVariable = None: Option[String]
    resultVariable = Some("q")
    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, resultVariable, 2)
    val rdd = dr.buildScan()
    assert(rdd != null)
    // 2 - During initial read
    // 1 - During query more
    assert(rdd.count() == 3l)
    sc.stop()
  }

  test ("test read using soql without schema") {
    val sqlContext = new SQLContext(sc)
    val dr = DatasetRelation(null, forceAPI, soql, null, sqlContext, null, 0)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test read using soql with schema") {
    val sqlContext = new SQLContext(sc)
    val countField = StructField("count", IntegerType, true)
    val deviceTypeField = StructField("device_type", StringType, true)

    val fields = Array[StructField] (countField, deviceTypeField)
    val schema = StructType(fields)

    val dr = DatasetRelation(null, forceAPI, soql, schema, sqlContext, null, 0)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }
}