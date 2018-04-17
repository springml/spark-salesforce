package com.springml.spark.salesforce

import java.text.SimpleDateFormat

import com.springml.salesforce.wave.api.BulkAPI
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.springml.salesforce.wave.model.{BatchInfo, BatchInfoList, JobInfo}
import org.apache.http.Header
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/**
  * Relation class for reading data from Salesforce and construct RDD
  */
case class BulkRelation(
    query: String,
    sfObject: String,
    bulkAPI: BulkAPI,
    customHeaders: List[Header],
    userSchema: StructType,
    sqlContext: SQLContext,
    inferSchema: Boolean,
    timeout: Long) extends BaseRelation with TableScan {

  import sqlContext.sparkSession.implicits._

  private val logger = Logger.getLogger(classOf[BulkRelation])

  def buildScan() = records.rdd

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      records.schema
    }

  }

  lazy val records: DataFrame = {
    val inputJobInfo = new JobInfo("CSV", sfObject, "query")
    val jobInfo = bulkAPI.createJob(inputJobInfo, customHeaders.asJava)
    val jobId = jobInfo.getId

    val batchInfo = bulkAPI.addBatch(jobId, query)

    if (awaitJobCompleted(jobId)) {
      val batchInfoList = bulkAPI.getBatchInfoList(jobId)
      val batchInfos = batchInfoList.getBatchInfo().asScala.toList
      val completedBatchInfos = batchInfos.filter(batchInfo => batchInfo.getState().equals("Completed"))

      val csvData = sqlContext.sparkContext.parallelize(
        completedBatchInfos.flatMap(batchInfo => {
          val resultIds = bulkAPI.getBatchResultIds(jobId, batchInfo.getId)

          val result = bulkAPI.getBatchResult(jobId, batchInfo.getId, resultIds.get(resultIds.size() - 1))

          result.lines.toList
        })
      ).toDS()

      sqlContext.sparkSession.read.option("header", true).option("inferSchema", inferSchema).csv(csvData)
    } else {
      throw new Exception("Job completion timeout")
    }
  }

  private def awaitJobCompleted(jobId: String): Boolean = {
    val timeoutDuration = FiniteDuration(timeout, MILLISECONDS)
    val initSleepIntervalDuration = FiniteDuration(200L, MILLISECONDS)
    val maxSleepIntervalDuration = FiniteDuration(10000L, MILLISECONDS)
    var completed = false
    Utils.retryWithExponentialBackoff(() => {
      completed = bulkAPI.isCompleted(jobId)
      completed
    }, timeoutDuration, initSleepIntervalDuration, maxSleepIntervalDuration)

    return completed
  }
}