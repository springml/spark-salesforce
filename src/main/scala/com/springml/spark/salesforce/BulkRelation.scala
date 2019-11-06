package com.springml.spark.salesforce

import java.text.SimpleDateFormat

import com.springml.salesforce.wave.api.{APIFactory, BulkAPI}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.springml.salesforce.wave.model.{BatchInfo, BatchInfoList, JobInfo}
import org.apache.http.Header
import org.apache.spark.rdd.RDD
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvWriterSettings
import com.univocity.parsers.csv.CsvWriter
import java.io.StringReader
import java.io.StringWriter

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/**
  * Relation class for reading data from Salesforce and construct RDD
  */
case class BulkRelation(
    username: String,
    password: String,
    login: String,
    version: String,
    query: String,
    sfObject: String,
    customHeaders: List[Header],
    userSchema: StructType,
    sqlContext: SQLContext,
    inferSchema: Boolean,
    timeout: Long,
    maxCharsPerColumn: Int) extends BaseRelation with TableScan {

  import sqlContext.sparkSession.implicits._

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
      bulkAPI.closeJob(jobId)

      val batchInfoList = bulkAPI.getBatchInfoList(jobId)
      val batchInfos = batchInfoList.getBatchInfo().asScala.toList
      val completedBatchInfos = batchInfos.filter(batchInfo => batchInfo.getState().equals("Completed"))
      val completedBatchInfoIds = completedBatchInfos.map(batchInfo => batchInfo.getId)

      val fetchBatchInfo = (batchInfoId: String) => {
        val resultIds = bulkAPI.getBatchResultIds(jobId, batchInfoId)

        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultIds.get(resultIds.size() - 1))

        // Use Csv parser to split CSV by rows to cover edge cases (ex. escaped characters, new line within string, etc)
        def splitCsvByRows(csvString: String): Seq[String] = {
          // The CsvParser interface only interacts with IO, so StringReader and StringWriter
          val inputReader = new StringReader(csvString)

          val parserSettings = new CsvParserSettings()
          parserSettings.setLineSeparatorDetectionEnabled(true)
          parserSettings.getFormat.setNormalizedNewline(' ')
          parserSettings.setMaxCharsPerColumn(maxCharsPerColumn)

          val readerParser = new CsvParser(parserSettings)
          val parsedInput = readerParser.parseAll(inputReader).asScala

          val outputWriter = new StringWriter()

          val writerSettings = new CsvWriterSettings()
          writerSettings.setQuoteAllFields(true)
          writerSettings.setQuoteEscapingEnabled(true)

          val writer = new CsvWriter(outputWriter, writerSettings)
          parsedInput.foreach { writer.writeRow(_) }

          outputWriter.toString.lines.toList
        }

        splitCsvByRows(result)
      }

      val csvData = sqlContext
        .sparkContext
        .parallelize(completedBatchInfoIds)
        .flatMap(fetchBatchInfo).toDS()

      sqlContext
        .sparkSession
        .read
        .option("header", true)
        .option("inferSchema", inferSchema)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", true)
        .csv(csvData)
    } else {
      bulkAPI.closeJob(jobId)
      throw new Exception("Job completion timeout")
    }
  }

  // Create new instance of BulkAPI every time because Spark workers cannot serialize the object
  private def bulkAPI(): BulkAPI = {
    APIFactory.getInstance().bulkAPI(username, password, login, version)
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