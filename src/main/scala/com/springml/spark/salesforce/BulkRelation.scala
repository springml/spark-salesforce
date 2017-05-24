package com.springml.spark.salesforce

import java.math.BigDecimal
import scala.collection.mutable.Queue
import java.sql.Date
import java.sql.Timestamp
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import com.springml.salesforce.wave.api.BulkAPI
import com.springml.salesforce.wave.model.BatchInfo
import com.springml.salesforce.wave.model.JobInfo
import org.apache.spark.sql.catalyst.ScalaReflection
import java.net.URLEncoder
import java.util.Random
import com.springml.salesforce.wave.api.APIFactory
import java.net.URI
import com.springml.spark.salesforce.Parameters.MergedParameters
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.AWSCredentialsProvider
import java.io.InputStreamReader
import java.net.URI
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import collection.JavaConverters._
import scala.collection.JavaConversions
import com.springml.salesforce.wave.model.BatchResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SQLContext }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import java.io.BufferedReader
import java.io.InputStream
import scala.concurrent.Future
import scala.concurrent.forkjoin._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.annotation.tailrec
import scala.util.{ Success, Failure }
import scala.concurrent._
import scala.concurrent.duration.Duration
import ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.concurrent.duration._
import com.springml.salesforce.wave.model.BatchInfo
import com.springml.salesforce.wave.model.BatchResult
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import scala.Left
import scala.Right
import java.io.InputStream
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit;

private[salesforce] case class BulkRelation(
    params: MergedParameters,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client,
    userSchema: Option[StructType])(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  /**
   * Logger
   */
  private val logger = Logger.getLogger(classOf[BulkRelation])

  /**
   * Bulk API
   */
  val bulkAPI = APIFactory.getInstance.bulkAPI(params.user, params.password, params.login, params.version)
  val tempDir = params.createPerQueryTempDir()
  private val name = params.objectName
  override def toString: String = s"BulkRelation($name)"
  var job: JobInfo = _
  val batchResultPromise = Promise[BatchResult]()
  val promiseFuture = batchResultPromise.future
  var sampleBatchResult: Option[BatchResult] = None

  init()

  def init() = {
    if (sqlContext != null) {
      Utils.assertThatFileSystemIsNotS3BlockFileSystem(
        new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
    }
    job = bulkAPI.createQueryJob(name)
    val batch = bulkAPI.addBatch(job.getId, params.soql.get)
    if (batch.isFailed())
      throw new IllegalArgumentException(s"Bulk API Batch failed: ${batch.getStateMessage}")
    promiseFuture onSuccess {
      case result =>
        { logger.trace("Computed Sample RDD from first batch."); sampleBatchResult = Some(result); sampleBatchResult; }
    }

  }

  /**
   * Sample
   */
  lazy val sampleRDD: RDD[Array[String]] = computeSampleRDD

  private def computeSampleRDD: RDD[Array[String]] = {
    runBulkOperation()
    for (br: BatchResult <- sampleBatchResult) {
      val records = br.getRecords.map { row => JavaConversions.asScalaBuffer(row).toArray }.toSeq
      return sqlContext.sparkContext.parallelize(records)
    }
    throw new IllegalArgumentException("This should not have happened");
  }

  ////////////////////  BATCH PROCESSING ///////////////////////////////
  /**
   * Batch Processing
   */

  private def batchList: List[BatchInfo] = {
    import collection.JavaConverters._
    logger.trace("Waiting 15 seconds for batches to be ready!")
    Thread.sleep(15000);
    bulkAPI.getBatchInfoList(job.getId).getBatchInfo.asScala.toList
  }

  lazy val batchProcessors: Seq[Future[BatchResult]] = {
    val batches = batchList;
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3Client)
    batches.foreach { bi: BatchInfo => logger.trace(s"batch Id: ${bi.getId} status: ${bi.getState} message: ${bi.getStateMessage} ") }
    val bs = batches.filter(_.hasDataToLoad).toSeq
    bs.map(bi => Future { retry(3, 20 second) { processBatch(20, bi) } })
  }

  @annotation.tailrec
  final def processBatch(n: Int, bi: BatchInfo): BatchResult = {
    if (n < 0)
      throw new IllegalStateException(s"Batch $bi is in invalid state: ${bi.getStateMessage}. Will stop processing it!")
    if (bi.needsTime())
      Thread.sleep(30000)
    if (bi.isCompleted()) {
      var stream: InputStream = null
      try {
        stream = bulkAPI.queryBatchStream(bi)
        if (!sampleBatchResult.isDefined) {
          val r = Utils.sampleResult(stream)
          sampleBatchResult = Some(r._2)
          stream = r._1
        }
        val fn = s"$tempDir${job.getId}/${bi.getId}"
        Utils.saveS3File(fn, s3Client, stream)
        new BatchResult(bi.getJobId, bi.getId, fn)
      } finally {
        if (stream != null) stream.close
      }
    } else {
      logger.trace(s"$bi is not ready yet! Will wait 30 secs! counter:$n")
      processBatch(n - 1, bulkAPI.getBatchInfo(bi.getJobId, bi.getId))
    }
  }

  /**
   * Retry the function call n number of times, with exponentially increasing pauses in between non-fatal failures
   */
  @annotation.tailrec
  private def retry[T](n: Int, pause: Duration)(fn: => T): T = {
    scala.util.Try { fn } match {
      case Success(x) => x
      case Failure(e) if n > 1 && NonFatal(e) => {
        logger.trace(s"${e.getMessage} - Pausing ${pause.toMillis / 1000} seconds to retry")
        Thread.sleep(pause.toMillis)
        retry(n - 1, pause + pause)(fn)
      }
      case Failure(e) => { logger.trace(s"${e.getMessage}. Will NOT retry!"); throw e }
    }
  }

  /**
   * Completes upon all Futures complete or upon first failure.
   * Sets the sample result from the first success
   */
  @annotation.tailrec
  private def awaitSuccess[BatchResult](
    futureSeq: Seq[Future[BatchResult]],
    done: Seq[BatchResult] = Seq()): Either[Throwable, Seq[BatchResult]] = {
    val first = Future.firstCompletedOf(futureSeq)

    Await.ready(first, Duration.Inf).value match {
      case None             => awaitSuccess(futureSeq, done) // Shouldn't happen!
      case Some(Failure(e)) => Left(e)
      case Some(Success(_)) =>
        logger.trace(sampleBatchResult)
        if (!sampleBatchResult.isDefined) {
          logger.trace(s"FirstSuccess:${first.value}")
          batchResultPromise completeWith first.asInstanceOf[scala.concurrent.Future[com.springml.salesforce.wave.model.BatchResult]]
        }
        val (complete, running) = futureSeq.partition(_.isCompleted)
        val answers = complete.flatMap(_.value)
        answers.find(_.isFailure) match {
          case Some(Failure(e)) => Left(e)
          case _ =>
            if (running.length > 0) awaitSuccess(running, answers.map(_.get) ++: done)
            else Right(answers.map(_.get) ++: done)
        }
    }
  }

  private def header: Array[String] = {
    for (br <- sampleBatchResult) {
      return br.getHeader.map(_.toString).toArray
    }
    Array()
  }

  override def schema: StructType = {
    // ScalaReflection.schemaFor[Batch].dataType.asInstanceOf[StructType]
    if (userSchema.isDefined) userSchema.get
    else {
      val sample = sampleRDD
      if (sample != null && params.inferSchema) {
        val rawcolumns = header;
        val blacklist: Set[Char] = Set(',', '`', '"', '\'')
        val columns = rawcolumns.map(line => line.filterNot(c => blacklist.contains(c)))
        InferSchema(sample, columns)
      } else {
        new StructType(); // should not happen
      }
    }
  }

  def s3Client = {
    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration);
    s3ClientFactory(creds)
  }

  private val LOCK = new ReentrantLock

  /**
   * Reentrant operation to process the batches
   */
  def runBulkOperation() {
    try {
      val gotLock = LOCK.tryLock(10, TimeUnit.MILLISECONDS)
      if (gotLock && !this.sampleBatchResult.isDefined) {
        try {
          logger.trace(s"Starting to run batch processors")
          awaitSuccess(batchProcessors)
          logger.trace(s"Finished running batch processors")
        } finally {
          if (gotLock) {
            LOCK.unlock()
            logger.trace(s"Unlocked lock")
          }
        }
      }
    } catch {
      case e: InterruptedException => {
        logger.error(s"Bulk processing of query interrupted: ${params.soql.get}", e)
      }
      case e: Exception => {
        logger.error(s"Error in processing bulk query: ${params.soql.get}", e)
        throw e
      }

    } finally {
      try {
     //   bulkAPI.closeJob(job.getId)
      } catch {
        case e: Exception => {
          logger.error(s"Error in closing bulk job for: ${params.soql.get}", e)
          throw e
        }
      }
    }

  }

  override def buildScan(): RDD[Row] = {
    logger.trace("In BuildScan. Before running Bulk Operation.");

    runBulkOperation()

    val filesToRead: Seq[String] = {
      val cleanedTempDirUri = Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
      val s3URI = Utils.createS3URI(cleanedTempDirUri)
      val folderContents = s3Client.listObjects(s3URI.getBucket, s3URI.getKey).getObjectSummaries
      folderContents.map(file => s"s3n://${file.getBucketName}/${file.getKey}")
    }

    filesToRead.foreach(logger.trace(_));

    logger.trace("Finished processing batches. Starting to load");

    try {
      sqlContext.read
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .schema(schema)
        .option("header", "false")
        .load(filesToRead: _*).rdd
    } catch {
      case e: Exception => e.printStackTrace(); throw e;
    }
  }

}
