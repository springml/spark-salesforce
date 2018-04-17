package com.springml.spark.salesforce

import org.mockito.Mockito.{when, _}
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import com.springml.salesforce.wave.api.BulkAPI
import org.apache.spark.{SparkConf, SparkContext}
import com.springml.salesforce.wave.model.{BatchInfo, JobInfo}
import com.springml.salesforce.wave.util.WaveAPIConstants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.ArgumentCaptor
import org.mockito.mock.SerializableMode

class TestSFObjectWriter extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val contact = "Contact";
  val jobId = "750B0000000WlhtIAC";
  val batchId = "751B0000000scSHIAY";
  val data = "id,desc\n003B00000067Rnx,Desc1\n001B00000067Rnx,Desc2";

  var bulkAPI: BulkAPI = _

  val username = "username"
  val password = "password"
  val login = "login"
  val apiVersion = "apiVersion"
  val externalId = "externalId"

  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  override def beforeEach() {
    bulkAPI = mock[BulkAPI](withSettings().serializable(SerializableMode.ACROSS_CLASSLOADERS))

    val jobInfo = new JobInfo
    jobInfo.setId(jobId)
    when(bulkAPI.createJob(any[JobInfo])).thenReturn(jobInfo)

    val batchInfo = new BatchInfo
    batchInfo.setId(batchId)
    batchInfo.setJobId(jobId)
    when(bulkAPI.addBatch(jobId, data)).thenReturn(batchInfo)

    when(bulkAPI.closeJob(jobId)).thenReturn(jobInfo)
    when(bulkAPI.isCompleted(jobId)).thenReturn(true)

    sparkConf = new SparkConf().setMaster("local").setAppName("Test SF Object Update")
    sc = new SparkContext(sparkConf)
  }

  override def afterEach(): Unit = {
    sc.stop()
  }

  private def sampleDF() : DataFrame = {
    val dataArray = data.split("\n").map(_.split(","))
    val rowArray = new Array[Row](2)

    rowArray(0) = Row.fromSeq(dataArray(1))
    rowArray(1) = Row.fromSeq(dataArray(2))

    val rdd = sc.parallelize(rowArray)
    val schema = StructType(
      StructField(dataArray(0)(0), StringType, true) ::
      StructField(dataArray(0)(1), StringType, true) :: Nil)

    val sqlContext = new SQLContext(sc)
    sqlContext.createDataFrame(rdd, schema)
  }

  private def testBulkAPIJobCreation(writer: SFObjectWriter, rdd: RDD[Row]): JobInfo = {
    val result = writer.writeData(rdd)

    val createJobInfoCaptor = ArgumentCaptor.forClass(classOf[JobInfo])
    verify(bulkAPI).createJob(createJobInfoCaptor.capture())
    val jobInfoCaptorValue = createJobInfoCaptor.getValue()

    assert(jobInfoCaptorValue.getExternalIdFieldName.equals(externalId))
    assert(jobInfoCaptorValue.getContentType.equals(WaveAPIConstants.STR_CSV))
    assert(jobInfoCaptorValue.getObject.equals(contact))
    assert(result)

    jobInfoCaptorValue
  }

  test ("Write to Salesforce with Append mode") {
    val df = sampleDF()
    val csvHeader = Utils.csvHeadder(df.schema)
    val writer = new SFObjectWriter(
      bulkAPI,
      contact,
      SaveMode.Append,
      false,
      externalId,
      csvHeader
    )
    val jobInfoCaptorValue = testBulkAPIJobCreation(writer, df.rdd)

    assert(jobInfoCaptorValue.getOperation.equals("insert"))
  }

  test ("Write to Salesforce with Overwrite mode") {
    val df = sampleDF()
    val csvHeader = Utils.csvHeadder(df.schema)
    val writer = new SFObjectWriter(
      bulkAPI,
      contact,
      SaveMode.Overwrite,
      false,
      externalId,
      csvHeader
    )
    val jobInfoCaptorValue = testBulkAPIJobCreation(writer, df.rdd)

    assert(jobInfoCaptorValue.getOperation.equals("update"))
  }

  test ("Write to Salesforce with ErrorIfExists mode") {
    val df = sampleDF()
    val csvHeader = Utils.csvHeadder(df.schema)
    val writer = new SFObjectWriter(
      bulkAPI,
      contact,
      SaveMode.ErrorIfExists,
      false,
      externalId,
      csvHeader
    )
    val jobInfoCaptorValue = testBulkAPIJobCreation(writer, df.rdd)

    assert(jobInfoCaptorValue.getOperation.equals("insert"))
  }

  test ("Write to Salesforce with Ignore mode") {
    val df = sampleDF()
    val csvHeader = Utils.csvHeadder(df.schema)
    val writer = new SFObjectWriter(
      bulkAPI,
      contact,
      SaveMode.Ignore,
      false,
      externalId,
      csvHeader
    )
    val jobInfoCaptorValue = testBulkAPIJobCreation(writer, df.rdd)

    assert(jobInfoCaptorValue.getOperation.equals("insert"))
  }

  test ("Write to Salesforce with upsert flag and Append mode") {
    val df = sampleDF()
    val csvHeader = Utils.csvHeadder(df.schema)
    val writer = new SFObjectWriter(
      bulkAPI,
      contact,
      SaveMode.Append,
      true,
      externalId,
      csvHeader
    )
    val jobInfoCaptorValue = testBulkAPIJobCreation(writer, df.rdd)

    assert(jobInfoCaptorValue.getOperation.equals("upsert"))
  }

  test ("Write to Salesforce with upsert flag and Overwrite mode") {
    val df = sampleDF()
    val csvHeader = Utils.csvHeadder(df.schema)
    val writer = new SFObjectWriter(
      bulkAPI,
      contact,
      SaveMode.Overwrite,
      true,
      externalId,
      csvHeader
    )
    val jobInfoCaptorValue = testBulkAPIJobCreation(writer, df.rdd)

    assert(jobInfoCaptorValue.getOperation.equals("upsert"))
  }
}