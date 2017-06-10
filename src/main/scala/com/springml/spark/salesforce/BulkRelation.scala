package com.springml.spark.salesforce

import java.sql.{ Date, Timestamp }
import scala.collection.JavaConversions.{ asScalaBuffer, mapAsScalaMap }
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.{ StructType, TimestampType }
import com.springml.salesforce.wave.api.{ BulkAPI, APIFactory }
import com.springml.salesforce.wave.model.{ BatchInfo, JobInfo, BatchResult }
import com.springml.spark.salesforce.Parameters.MergedParameters
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.AWSCredentialsProvider
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import collection.JavaConverters._
import scala.collection.JavaConversions
import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SQLContext }
import java.io.BufferedReader
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit;
import scala.annotation.tailrec
import scala.util.{ Success, Failure }
import ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.Left
import scala.Right
import java.io.InputStream
import java.net.URI
import java.lang.management.ManagementFactory

private[salesforce] case class BulkRelation(
    params: MergedParameters,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client,
    userSchema: Option[StructType])(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  /**
   * Logger
   */
  private val logger = Logger.getLogger(classOf[BulkRelation])

  private class IllegalStateDoNotRetry(message: String = null, cause: Throwable = null)
    extends IllegalStateException(message, cause)
  private class OutofMemoryWithRetry(message: String = null, cause: Throwable = null)
    extends IllegalStateException(message, cause)

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
  var s3Client: AmazonS3Client = _

  init()

  def init() = {
    if (sqlContext != null) {
      Utils.assertThatFileSystemIsNotS3BlockFileSystem(
        new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
    }
    val pk = params.pkChunking
    pk match {
      case Some((k, v)) => job = bulkAPI.createQueryJob(name, k, v, params.queryAll)
      case None         => job = bulkAPI.createQueryJob(name, false)
    }

    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration);
    s3Client = s3ClientFactory(creds)

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
    throw new IllegalArgumentException("This should not have happened. For large tables, try using PKChunking, or if not, try increasing the number of trials and the wait limit in between trials. ");
  }

  ////////////////////  BATCH PROCESSING ///////////////////////////////
  /**
   * Batch Processing
   */

  private def batchList: List[BatchInfo] = {
    import collection.JavaConverters._
    val batches = bulkAPI.getBatchInfoList(job.getId).getBatchInfo.asScala.toList
    logger.trace(s"Waiting for bulk query batches to be ready! Will recheck every 10 seconds")
    patientBatchList(params.maxBatchRetry * 4, batches)
  }

  @annotation.tailrec
  private def patientBatchList(n: Int, batches: List[BatchInfo]): List[BatchInfo] = {
    if (n < 0) return batches
    batches.find(bi => bi.isCompleted()) match {
      case Some(_) => {println("!");return batches}
      case None => {
        print(".")
        Thread.sleep(10000);
        import collection.JavaConverters._
        val bs = bulkAPI.getBatchInfoList(job.getId).getBatchInfo.asScala.toList
        patientBatchList(n - 1, bs)
      }
    }
  }

  lazy val batchProcessors: Seq[Future[BatchResult]] = {
    val batches = batchList;
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3Client)
    val filtered = batches.filter(_.hasDataToLoad).toSeq
    logger.info(s"The following batches will be processed:")
    filtered.foreach(bi => logger.info(s"   Batch:$bi"))
    filtered.map(bi => Future { retry(params.maxFutureRetry, 20 second) { processBatch(params.maxBatchRetry, bi) } })
  }

  @annotation.tailrec
  final def processBatch(n: Int, bi: BatchInfo): BatchResult = {
    if (n < 0)
      throw new IllegalStateException(s"Batch $bi is in invalid state: ${bi.getStateMessage}. Will stop processing it!")
    else if (!bi.hasDataToLoad())
      throw new IllegalStateDoNotRetry(s"Batch $bi is in not to be processed: ${bi.getStateMessage}!")
    if (bi.needsTime())
      Thread.sleep(30000)
    if (bi.isCompleted()) {
      var stream: InputStream = null
      try {
        memUsage(s" >> before $bi")
        stream = bulkAPI.queryBatchStream(bi)
        if (!sampleBatchResult.isDefined) {
          val r = Utils.sampleResult(stream)
          if (!r._2.isEmpty) {
            sampleBatchResult = Some(r._2)
            stream = r._1
          }
        }
        val fn = s"$tempDir${job.getId}/${bi.getId}"
        Utils.saveS3File(fn, s3Client, stream, memUsage());
        new BatchResult(bi.getJobId, bi.getId, fn)
      } catch {
        case e: Exception => { logger.error(s"Error in processing batch: $bi"); throw e }
        case e: OutOfMemoryError =>
          { outOfMemory(e, bi) }
      } finally {
        if (stream != null) stream.close
        memUsage(s" << after $bi")
      }
    } else {
      logger.trace(s"$bi is not ready yet! Will wait 30 secs! counter:$n")
      processBatch(n - 1, bulkAPI.getBatchInfo(bi.getJobId, bi.getId))
    }
  }

  private def memUsage(msg: String = "") = {
    val memoryBean = ManagementFactory.getMemoryMXBean()
    val heapUsage = memoryBean.getHeapMemoryUsage();
    val maxMemory = heapUsage.getMax() / (1024 * 1024);
    val usedMemory = heapUsage.getUsed() / (1024 * 1024);
    val message = s"$msg  :: memory use : ${usedMemory}M / ${maxMemory}M"
    if (msg != null && msg.size > 0) logger.trace(message)
    (maxMemory, usedMemory)
  }

  @throws(classOf[OutofMemoryWithRetry])
  private def outOfMemory(e: OutOfMemoryError, batch: BatchInfo) = {
    val mem = memUsage()
    val maxMemory = mem._1
    val usedMemory = mem._2
    val msg = s"Out of memory while processing $batch message"
    logger.error(msg, e)
    throw new OutofMemoryWithRetry(msg)
  }

  /**
   * Retry the function call n number of times, with exponentially increasing pauses in between non-fatal failures
   */
  @annotation.tailrec
  private def retry[T](n: Int, pause: Duration)(fn: => T): T = {
    scala.util.Try { fn } match {
      case Success(x) => x
      case Failure(e) if n > 1 && NonFatal(e) && !e.isInstanceOf[IllegalStateDoNotRetry] => {
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
      case Some(Success(r)) =>
        val rr: BatchResult = r.asInstanceOf[BatchResult]
        logger.trace(s"Processed batch: ${rr}")
        if (!sampleBatchResult.isDefined) {
          logger.trace(s"First success candidate:${first.value}")
          batchResultPromise completeWith first.asInstanceOf[scala.concurrent.Future[com.springml.salesforce.wave.model.BatchResult]]
        }
        val (complete, running) = futureSeq.partition(_.isCompleted)
        memUsage(s" Completed: ${complete.size} Running: ${running.size}")
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

  //  def s3Client: AmazonS3Client = {
  //    logger.trace("Creating S3 Client")
  //    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration);
  //    s3ClientFactory(creds)
  //  }

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
            try {
              logger.trace(s"Closing salesforce job: $job")
              bulkAPI.closeJob(job.getId)
            } catch {
              case e: Exception => {
                logger.error(s"Error in closing bulk job for: ${params.soql.get}", e)
              }
            }
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

    try {
      val dfr = sqlContext.read
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .schema(schema)
        .option("header", "true")
        .option("wholeFile",params.wholeFile)
       params.quote match  { case Some(q) => dfr.option("quote",q)  case _ => {} }
       params.escape match { case Some(e) => dfr.option("escape",e) case _ => {} }
       dfr.load(filesToRead: _*).rdd
    } catch {
      case e: Exception => e.printStackTrace(); throw e;
    }
  }

}
