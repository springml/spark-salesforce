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
  import scala.collection.JavaConversions._

  @transient lazy val logger: Logger = Logger.getLogger(classOf[BulkRelation])

  def buildScan() = records.rdd

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      records.schema
    }

  }

  lazy val records: DataFrame = {
    val inputJobInfo = new JobInfo("CSV", sfObject, "queryAll")
    val jobInfo = bulkAPI.createJob(inputJobInfo, customHeaders.asJava)
    val jobId = jobInfo.getId
    logger.error(">>> Obtained jobId: " + jobId)

    val batchInfo = bulkAPI.addBatch(jobId, query)
    logger.error(">>> Obtained batchInfo: " + batchInfo)

    if (awaitJobCompleted(jobId)) {
      bulkAPI.closeJob(jobId)

      val batchInfoList = bulkAPI.getBatchInfoList(jobId)
      val batchInfos = batchInfoList.getBatchInfo().asScala.toList

      logger.error(">>> Obtained batchInfos: " + batchInfos)
      logger.error(">>>>>> Obtained batchInfos.size: " + batchInfos.size)

      val completedBatchInfos = batchInfos.filter(batchInfo => batchInfo.getState().equals("Completed"))
      val completedBatchInfoIds = completedBatchInfos.map(batchInfo => batchInfo.getId)

      logger.error(">>> Obtained completedBatchInfoIds: " + completedBatchInfoIds)
      logger.error(">>> Obtained completedBatchInfoIds.size: " + completedBatchInfoIds.size)

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
        parsedInput.foreach {
          writer.writeRow(_)
        }

        outputWriter.toString.lines.toList
      }

      val fetchAllResults = (resultId: String, batchInfoId: String) => {
        logger.error("Getting Result for ResultId: " + resultId)
        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultId)

        val splitRows = splitCsvByRows(result)

        logger.error("Result Rows size: " + splitRows.size)
        logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))

        splitRows
      }

      val fetchBatchInfo = (batchInfoId: String) => {
        logger.error(">>> About to fetch Results in batchInfoId: " + batchInfoId)

        val resultIds = bulkAPI.getBatchResultIds(jobId, batchInfoId)
        logger.error(">>> Got ResultsIds in batchInfoId: " + resultIds)
        logger.error(">>> Got ResultsIds in batchInfoId.size: " + resultIds.size)
        logger.error(">>> Got ResultsIds in Last Result Id: " + resultIds.get(resultIds.size() - 1))

//        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultIds.get(resultIds.size() - 1))

//        logger.error(">>> Got Results - Results (string) length: " + result.length)

        // Use Csv parser to split CSV by rows to cover edge cases (ex. escaped characters, new line within string, etc)
//        def splitCsvByRows(csvString: String): Seq[String] = {
//          // The CsvParser interface only interacts with IO, so StringReader and StringWriter
//          val inputReader = new StringReader(csvString)
//
//          val parserSettings = new CsvParserSettings()
//          parserSettings.setLineSeparatorDetectionEnabled(true)
//          parserSettings.getFormat.setNormalizedNewline(' ')
//          parserSettings.setMaxCharsPerColumn(maxCharsPerColumn)
//
//          val readerParser = new CsvParser(parserSettings)
//          val parsedInput = readerParser.parseAll(inputReader).asScala
//
//          val outputWriter = new StringWriter()
//
//          val writerSettings = new CsvWriterSettings()
//          writerSettings.setQuoteAllFields(true)
//          writerSettings.setQuoteEscapingEnabled(true)
//
//          val writer = new CsvWriter(outputWriter, writerSettings)
//          parsedInput.foreach { writer.writeRow(_) }
//
//          outputWriter.toString.lines.toList
//        }

        val resultIdsBatchInfoIdPairs: List[(String, String)] = resultIds.toList.map { resultId: String => {
          (resultId, batchInfoId)
        }}

        // AS addition - START
//        val allRows: Seq[String] = resultIds.toList.flatMap { resultId: String => {
//          logger.error("Getting Result for ResultId: " + resultId)
//          val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultId)
//
//          val splitRows = splitCsvByRows(result)
//
//          logger.error("Result Rows size: " + splitRows.size)
//          logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))
//
//          splitRows
//        }}

        val allRows: Seq[String] = resultIdsBatchInfoIdPairs.flatMap { case(resultId, batchInfoId) =>
            fetchAllResults(resultId, batchInfoId)
        }

        allRows
        // AS Addition - END

//        val splitRows = splitCsvByRows(result)
//        logger.error("Result Rows size: " + splitRows.size)
//        logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))
//        splitRows

      }

      // AS addition - START
      val csvData: Dataset[String] = if (completedBatchInfoIds.size == 1) {
        val resultIds = bulkAPI.getBatchResultIds(jobId, completedBatchInfoIds.head)

        val resultIdsCompletedBatchInfoIdPairs: List[(String, String)] = resultIds.toList.map { resultId: String => {
          (resultId, completedBatchInfoIds.head)
        }}

        logger.error(">>>> Will Parallelize Result IDs, CBatchInfoId: " + resultIdsCompletedBatchInfoIdPairs)

        sqlContext
          .sparkContext
          .parallelize(resultIdsCompletedBatchInfoIdPairs)
          .flatMap { case (resultId, batchInfoId) =>
            fetchAllResults(resultId, batchInfoId)
          }.toDS()
      } else {
        logger.error(">>>> Will Parallelize CompletedBatchInfoIds: " + completedBatchInfoIds)

        sqlContext
          .sparkContext
          .parallelize(completedBatchInfoIds)
          .flatMap(fetchBatchInfo).toDS()
      }
      // AS addition - END

//      val csvData = sqlContext
//        .sparkContext
//        .parallelize(completedBatchInfoIds)
//        .flatMap(fetchBatchInfo).toDS()

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