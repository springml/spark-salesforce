package com.springml.spark.salesforce

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, BeforeAndAfterEach}
import com.springml.salesforce.wave.api.BulkAPI
import org.apache.spark.{ SparkConf, SparkContext}
import com.springml.salesforce.wave.model.{ JobInfo, BatchInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, DataFrame, SQLContext}
import org.apache.spark.sql.types.{ StructType, StringType, StructField}

class TestSFObjectWriter extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val contact = "Contact";
  val jobId = "750B0000000WlhtIAC";
  val batchId = "751B0000000scSHIAY";
  val data = "Id,Description\n003B00000067Rnx,123456\n003B00000067Rnw,7890";

  val bulkAPI = mock[BulkAPI](withSettings().serializable())
  val writer = mock[SFObjectWriter]

  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  override def beforeEach() {
    val jobInfo = new JobInfo
    jobInfo.setId(jobId)
    when(bulkAPI.createJob(contact)).thenReturn(jobInfo)

    val batchInfo = new BatchInfo
    batchInfo.setId(batchId)
    batchInfo.setJobId(jobId)
    when(bulkAPI.addBatch(jobId, data)).thenReturn(batchInfo)

    when(bulkAPI.closeJob(jobId)).thenReturn(jobInfo)
    when(bulkAPI.isCompleted(jobId)).thenReturn(true)
    when(writer.bulkAPI()).thenReturn(bulkAPI)

    sparkConf = new SparkConf().setMaster("local").setAppName("Test SF Object Update")
    sc = new SparkContext(sparkConf)
  }

  private def sampleDF() : DataFrame = {
    val rowArray = new Array[Row](2)
    val fieldArray = new Array[String](2)

    fieldArray(0) = "003B00000067Rnx"
    fieldArray(1) = "Desc1"
    rowArray(0) = Row.fromSeq(fieldArray)

    val fieldArray1 = new Array[String](2)
    fieldArray1(0) = "001B00000067Rnx"
    fieldArray1(1) = "Desc2"
    rowArray(1) = Row.fromSeq(fieldArray1)

    val rdd = sc.parallelize(rowArray)
    val schema = StructType(
      StructField("id", StringType, true) ::
      StructField("desc", StringType, true) :: Nil)

    val sqlContext = new SQLContext(sc)
    sqlContext.createDataFrame(rdd, schema)
  }

  test ("Write Object to Salesforce") {
    val df = sampleDF();
    val csvHeader = Utils.csvHeadder(df.schema)
    writer.writeData(df.rdd)
    sc.stop()
  }
}