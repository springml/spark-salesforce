package com.springml.spark.salesforce

import java.util

import com.springml.salesforce.wave.api.BulkAPI
import com.springml.salesforce.wave.model.{BatchInfo, BatchInfoList, JobInfo}
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.mock.MockitoSugar
import java.util.ArrayList

import org.apache.http.Header
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType};

class TestBulkRelation extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val bulkAPI = mock[BulkAPI]

  val jobInfo = mock[JobInfo]

  val batchInfo1 = mock[BatchInfo]
  val batchInfo2 = mock[BatchInfo]
  val batchInfo3 = mock[BatchInfo]

  val batchInfoList = mock[BatchInfoList]
  val batchInfos = new ArrayList[BatchInfo](util.Arrays.asList(batchInfo1, batchInfo2, batchInfo3))

  val jobId = "12345678"
  val batchInfoId1 = "87654321"
  val batchInfoId2 = "97654321"
  val batchInfoId3 = "97654320"

  val batchInfoStatus1 = "Completed"
  val batchInfoStatus2 = "Completed"
  val batchInfoStatus3 = "Not Processed"

  val batchResultIds1 = new ArrayList[String](util.Arrays.asList("1", "4"))
  val batchResultIds2 = new ArrayList[String](util.Arrays.asList("2"))

  val batchResult1 = "Id,Name\n123,Name1\n124,Name2\n"
  val batchResult2 = "Id,Name\n125,Name3\n"

  val sfObject = "TestObject"
  val soql = "SELECT Id, Name FROM TestObject"
  val customHeaders = List(new BasicHeader("Sforce-Enable-PKChunking", "true"))


  var sparkConf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override def beforeEach() {
    when(jobInfo.getId()).thenReturn(jobId)


    when(bulkAPI.createJob(any(), any())).thenReturn(jobInfo)
    when(bulkAPI.addBatch(jobId, soql)).thenReturn(batchInfo3)
    when(bulkAPI.getBatchInfoList(jobId)).thenReturn(batchInfoList)
    when(batchInfoList.getBatchInfo).thenReturn(batchInfos)

    when(batchInfo1.getId()).thenReturn(batchInfoId1)
    when(batchInfo1.getState()).thenReturn(batchInfoStatus1)

    when(batchInfo2.getId()).thenReturn(batchInfoId2)
    when(batchInfo2.getState()).thenReturn(batchInfoStatus2)

    when(batchInfo3.getId()).thenReturn(batchInfoId3)
    when(batchInfo3.getState()).thenReturn(batchInfoStatus3)

    when(bulkAPI.isCompleted(jobId)).thenReturn(true)

    when(bulkAPI.getBatchResultIds(jobId, batchInfoId1)).thenReturn(batchResultIds1)
    when(bulkAPI.getBatchResultIds(jobId, batchInfoId2)).thenReturn(batchResultIds2)

    when(bulkAPI.getBatchResult(jobId, batchInfoId1, batchResultIds1.get(batchResultIds1.size() - 1))).thenReturn(batchResult1)
    when(bulkAPI.getBatchResult(jobId, batchInfoId2, batchResultIds2.get(batchResultIds2.size() - 1))).thenReturn(batchResult2)

    sparkConf = new SparkConf().setMaster("local").setAppName("Test Bulk Relation")
    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
  }

  override def afterEach() {
    sc.stop()
  }

  test("test read using soql") {
    val bulkRelation = new BulkRelation(
      soql,
      sfObject,
      bulkAPI,
      customHeaders,
      null,
      sqlContext,
      true,
      10000
    )

    val records = bulkRelation.buildScan().collect()

    assert(records.length == 3)
    assert(records.contains(Row(123, "Name1")))
    assert(records.contains(Row(124, "Name2")))
    assert(records.contains(Row(125, "Name3")))
  }

  test("test infer schema") {
    val bulkRelation = new BulkRelation(
      soql,
      sfObject,
      bulkAPI,
      customHeaders,
      null,
      sqlContext,
      true,
      10000
    )

    val inferedSchema = bulkRelation.schema
    val idField = inferedSchema.apply("Id")
    assert(IntegerType == idField.dataType)

    val nameField = inferedSchema.apply("Name")
    assert(StringType == nameField.dataType)
  }
}
