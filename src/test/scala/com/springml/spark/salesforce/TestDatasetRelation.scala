package com.springml.spark.salesforce

import java.text.SimpleDateFormat
import java.util.{ArrayList, HashMap}

import com.springml.salesforce.wave.api.{ForceAPI, WaveAPI}
import com.springml.salesforce.wave.model.{QueryResult, Results, SOQLResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}

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
  val accountDatasetName = "Account"
  val productsDatasetName = "Products"
  val accountDatasetId = "0FbB00000000KybKAE/0FcB0000000DGKeKAO"
  val productsDatasetId = "0FbB00000000Uf3KAE/0FcB0000000FHWdKAO"

  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  override def beforeEach() {
    when(waveAPI.query(saql)).thenReturn(qr)
    when(waveAPI.queryWithPagination(saql, "q", 2)).thenReturn(paginatedQR)
    when(waveAPI.queryMore(any())).thenReturn(qr)
    when(waveAPI.getDatasetId(accountDatasetName)).thenReturn(accountDatasetId)
    when(waveAPI.getDatasetId(productsDatasetName)).thenReturn(productsDatasetId)

    when(forceAPI.query(soql)).thenReturn(soqlQR);
    when(forceAPI.query(soql, false)).thenReturn(soqlQR);
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
    record.put("created_date", "2017-08-04T16:45:30")
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
    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, null, 0, 100, null, false,
      false, null, false)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test read with schema") {
    val sqlContext = new SQLContext(sc)
    val countField = StructField("count", IntegerType, true)
    val deviceTypeField = StructField("device_type", StringType, true)
    val createdTypeField = StructField("created_date", TimestampType, true)

    val fields = Array[StructField] (countField, deviceTypeField, createdTypeField)
    val schema = StructType(fields)

    val dr = DatasetRelation(waveAPI, null, saql, schema, sqlContext, null, 0, 100, null, false,
      false, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"), false)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test queryMore") {
    val sqlContext = new SQLContext(sc)
    var resultVariable = None: Option[String]
    resultVariable = Some("q")
    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, resultVariable, 2, 100, null,
      false, false, null, false)
    val rdd = dr.buildScan()
    assert(rdd != null)
    // 2 - During initial read
    // 1 - During query more
    assert(rdd.count() == 3l)
    sc.stop()
  }

  test ("test read using soql without schema") {
    val sqlContext = new SQLContext(sc)
    val dr = DatasetRelation(null, forceAPI, soql, null, sqlContext, null, 0, 100, null, false,
      false, null, false)
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

    val dr = DatasetRelation(null, forceAPI, soql, schema, sqlContext, null, 0, 100, null, false,
      false, null, false)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test infer schema") {
    val sqlContext = new SQLContext(sc)
    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, null, 0, 100, null, true,
      false, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"), false)

    val inferedSchema = dr.schema
    print("inferedSchema  : " + inferedSchema)
    val countField = inferedSchema.apply("count")
    print("Count field Data Type " + countField.dataType)
    assert(IntegerType == countField.dataType)

    val deviceTypeField = inferedSchema.apply("device_type")
    print("Device Type field Data Type " + deviceTypeField.dataType)
    assert(StringType == deviceTypeField.dataType)

    val createdDateField = inferedSchema.apply("created_date")
    print("Created Date field Data Type " + createdDateField.dataType)
    assert(TimestampType == createdDateField.dataType)

    sc.stop()
  }

  test ("test replace dataset name") {
    when(waveAPI.query(any())).thenReturn(qr)

    val saql = """q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val expectedSaql = """q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val dr = DatasetRelation(waveAPI, null, saql, null, null, null, 0, 100, null, true, true, null, false)
    val modSaql = dr.replaceDatasetNameWithId(saql, 0)

    assert(expectedSaql.equals(modSaql))
  }

  test ("test replace dataset name for multiple load stmts") {
    when(waveAPI.query(any())).thenReturn(qr)

    val saql = """q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "Products";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val expectedSaql = """q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "0FbB00000000Uf3KAE/0FcB0000000FHWdKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val dr = DatasetRelation(waveAPI, null, saql, null, null, null, 0, 100, null, true, true, null, false)
    val modSaql = dr.replaceDatasetNameWithId(saql, 0)

    assert(expectedSaql.equals(modSaql))
  }

  test ("test replace dataset name for multiple load stmts of same dataset") {
    when(waveAPI.query(any())).thenReturn(qr)

    val saql = """q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val expectedSaql = """q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val dr = DatasetRelation(waveAPI, null, saql, null, null, null, 0, 100, null, true, true, null, false)
    val modSaql = dr.replaceDatasetNameWithId(saql, 0)

    assert(expectedSaql.equals(modSaql))
  }

  test ("test replace dataset name for multiple load stmts of different dataset") {
    when(waveAPI.query(any())).thenReturn(qr)

    val saql = """q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "Products";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val expectedSaql = """q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;
                  q = load "0FbB00000000Uf3KAE/0FcB0000000FHWdKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""

    val dr = DatasetRelation(waveAPI, null, saql, null, null, null, 0, 100, null, true, true, null, false)
    val modSaql = dr.replaceDatasetNameWithId(saql, 0)

    assert(expectedSaql.equals(modSaql))
  }

  test ("test should not replace dataset name") {
    val sqlContext = new SQLContext(sc)
    val saql = """q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""
    when(waveAPI.query(saql)).thenReturn(qr)

    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, null, 0, 100, null, false,
      false, null, false)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test should replace dataset name") {
    val sqlContext = new SQLContext(sc)
    val saql = """q = load "Account";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""
    val expectedSaql = """q = load "0FbB00000000KybKAE/0FcB0000000DGKeKAO";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""
    when(waveAPI.query(expectedSaql)).thenReturn(qr)

    val dr = DatasetRelation(waveAPI, null, saql, null, sqlContext, null, 0, 100, null, false,
      true, null, false)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()
  }

  test ("test replace dataset name with invalid datasetName") {
    val saql = """q = load "xyz";
                  q = group q by all;
                  q = foreach q generate count() as 'count';
                  q = limit q 2000;"""
    when(waveAPI.query(saql)).thenReturn(qr)

    val dr = DatasetRelation(waveAPI, null, saql, null, null, null, 0, 100, null, true, true, null, false)
    val modSaql = dr.replaceDatasetNameWithId(saql, 0)

    assert(saql.equals(modSaql))

  }

  test("queryAll false") {
    val sqlContext = new SQLContext(sc)

    reset(forceAPI)
    when(forceAPI.query(soql, false)).thenReturn(soqlQR);

    val dr = DatasetRelation(null, forceAPI, soql, null, sqlContext, null, 0, 100, null, false,
      false, null, false)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()

    verify(forceAPI).query(soql, false)
  }

  test("queryAll true") {
    val sqlContext = new SQLContext(sc)

    reset(forceAPI)
    when(forceAPI.query(soql, true)).thenReturn(soqlQR);

    val dr = DatasetRelation(null, forceAPI, soql, null, sqlContext, null, 0, 100, null, false,
      false, null, true)
    val rdd = dr.buildScan()
    validate(rdd)
    sc.stop()

    verify(forceAPI).query(soql, true)
  }
}
