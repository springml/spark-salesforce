//package com.springml.spark.salesforce
//
//import java.math.BigDecimal
//import scala.collection.mutable.Queue
//import java.sql.Date
//import java.sql.Timestamp
//import scala.collection.JavaConversions.asScalaBuffer
//import scala.collection.JavaConversions.mapAsScalaMap
//import org.apache.log4j.Logger
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.sources.BaseRelation
//import org.apache.spark.sql.sources.TableScan
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.TimestampType
//import com.springml.salesforce.wave.api.BulkAPI
//import com.springml.salesforce.wave.model.BatchInfo
//import org.apache.spark.sql.catalyst.ScalaReflection
//import java.net.URLEncoder
//import java.util.Random
//import com.springml.salesforce.wave.api.APIFactory
//import java.net.URI
//import com.springml.spark.salesforce.Parameters.MergedParameters
//import com.amazonaws.services.s3.AmazonS3Client
//import com.amazonaws.auth.AWSCredentialsProvider
//import java.io.InputStreamReader
//import java.net.URI
//import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import collection.JavaConverters._
//import scala.collection.JavaConversions
//import com.springml.salesforce.wave.model.BulkResult
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.sources._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SQLContext }
//import org.apache.spark.sql.catalyst.encoders.RowEncoder
//import java.io.BufferedReader
//import java.io.InputStream
//
//// remove the Executors and ExecutionContext imports at spark version 2.12
//import java.util.concurrent.Executors
//import concurrent.ExecutionContext
//
//private[salesforce] case class BulkRelation2(
//    params: MergedParameters,
//    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client,
//    userSchema: Option[StructType])(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {
//
//  private val logger = Logger.getLogger(classOf[DatasetRelation])
//
//  val bulkAPI = APIFactory.getInstance.bulkAPI(params.user, params.password, params.login, params.version)
//
//  val executorService = Executors.newFixedThreadPool(4)
//  implicit val ec = ExecutionContext.fromExecutorService(executorService)
//
//  if (sqlContext != null) {
//    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
//      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
//  }
//
//  private val name = params.objectName
//  val job = bulkAPI.createQueryJob(name)
//  val batch = bulkAPI.addBatch(job.getId, params.soql.get)
//  if (batch.isFailed()) throw new IllegalArgumentException(s"Bulk API Batch failed: ${batch.getStateMessage}")
//
//  val batches = { val batches = batchList(); process(batches); batches }
//
//  override def toString: String = s"BulkRelation($name)"
//
//  def batchList(): List[BatchInfo] = {
//    import collection.JavaConverters._
//    logger.trace("Waiting for batches to be ready!")
//    Thread.sleep(30000);
//    bulkAPI.getBatchInfoList(job.getId).getBatchInfo.asScala.toList
//  }
//
//  var sampleBatchResult: BatchResult = _
//
//  private def header: Array[String] = {
//    if (sampleBatchResult != null) {
//      sampleBatchResult.getHeader.map(_.toString).toArray
//    } else {
//      Array()
//    }
//  }
//
//  override def schema: StructType = {
//    // ScalaReflection.schemaFor[Batch].dataType.asInstanceOf[StructType]
//    if (userSchema.isDefined) userSchema.get
//    else {
//      val sample = sampleRDD
//      if (sample != null && params.inferSchema) {
//        InferSchema(sample, header)
//      } else {
//        new StructType(); // should not happen
//      }
//    }
//  }
//
//  //  val mapper = new ObjectMapper() with ScalaObjectMapper
//  //  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
//  //    mapper.readValue[T](json)
//  //  }
//
//  private def sampleRDD: RDD[Array[String]] = {
//    if (sampleBatchResult == null) {
//       null
//    } else {
//      val records = sampleBatchResult.getRecords.map { row => JavaConversions.asScalaBuffer(row).toArray }.toSeq
//      sqlContext.sparkContext.parallelize(records)
//    }
//  }
//
//  def s3Client = {
//    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration);
//    s3ClientFactory(creds)
//  }
//  lazy val tempDir = params.createPerQueryTempDir()
//
//  def process(batches: List[BatchInfo]): List[String] = {
//    var files: List[String] = Nil
//    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3Client)
//
//    batches.filter(bi => true).map {
//      bi: BatchInfo => println(s"batch Id: ${bi.getId} status: ${bi.getState} message: ${bi.getStateMessage} ")
//    }
//
//    val batchQueue = new Queue[BatchInfo]
//    batchList.filter(bi => bi.hasDataToLoad()).foreach(batchQueue.enqueue(_))
//
//    while (!batchQueue.isEmpty) {
//      val bi = batchQueue.dequeue()
//      val stream: InputStream = null
//      if (bi.isCompleted()) {
//        try {
//          val stream = bulkAPI.queryBatchStream(bi)
//          if (sampleBatchResult == null) setSampleResult(stream)
//          val fn = s"$tempDir${job.getId}/${bi.getId}"
//          Utils.saveS3File(fn, s3Client, stream)
//          files = fn :: files
//        } finally {
//          if (stream != null) stream.close
//        }
//      } else {
//        if (bi.needsTime()) {
//          def bim: BatchInfo = { println(s"Waiting for batch[${bi.getId}] to get ready"); Thread.sleep(30000); bi }
//          batchQueue.enqueue(bulkAPI.getBatchInfo(bim.getJobId, bim.getId))
//        }
//      }
//    }
//    files
//  }
//
//  override def buildScan(): RDD[Row] = {
//
//    val spark = sqlContext.sparkSession
//
//    val files = process(batchList)
//
//    //     Read the MANIFEST file to get the list of S3 part files that were written by SalesForce Batch Processor.
//    //     We need to use a manifest in order to guard against S3's eventually-consistent listings.
//    val filesToRead: Seq[String] = {
//      val cleanedTempDirUri = Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
//      val s3URI = Utils.createS3URI(cleanedTempDirUri)
//      val folderContents = s3Client.listObjects(s3URI.getBucket, s3URI.getKey).getObjectSummaries
//      folderContents.map(file => s"s3n://${file.getBucketName}/${file.getKey} ")
//    }
//    
//    filesToRead.foreach(println(_));
//    files.foreach(println(_));
//    
//    try {
//      sqlContext.read
//        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
//        .schema(schema)
//        .option("header", "false")
//        .load(filesToRead: _*).rdd
//
//    } catch {
//      case e: Exception => e.printStackTrace(); throw e;
//    }
//  }
//
//  private def setSampleResult(stream: InputStream) = {
//    import scala.collection.JavaConverters._
//    val reader = new BufferedReader(new InputStreamReader(stream));
//    val result = new BatchResult()
//    val line = reader.readLine();
//    println("HEADER:" + line);
//    if (line != null) {
//      val columns = line.split(",");
//      result.setHeader(columns.toList.asJava)
//    }
//    if (sampleBatchResult == null) sampleBatchResult = result
//
//    val bufferSize = 40000
//    try {
//      reader.mark(bufferSize);
//      // hack to cleanup
//      var n = 1
//      var charsRead = 0
//      var avgLineLength = 0
//      var row: String = ""
//      while (row != null && (charsRead + 3 * avgLineLength < bufferSize / 2) && n < 200) {
//        row = reader.readLine();
//        n += 1
//        if (row != null) {
//          val size = row.length();
//          charsRead += size
//          if (avgLineLength == 0) avgLineLength = size
//          else { avgLineLength = (avgLineLength * n + size) / n }
//          val columns = row.split(",");
//          result.addRecord(columns.toList.asJava)
//        }
//      }
//    } finally {
//      reader.reset()
//    }
//  }
//
//}
