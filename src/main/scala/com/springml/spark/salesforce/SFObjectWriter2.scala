package com.springml.spark.salesforce

import java.io.BufferedReader

import com.springml.salesforce.wave.api.{APIFactory, BulkAPI}
import com.springml.salesforce.wave.model.JobInfo
import com.springml.salesforce.wave.util.WaveAPIConstants
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import endolabs.salesforce.bulkv2.{AccessToken, Bulk2Client, Bulk2ClientBuilder}
import com.force.api._
import endolabs.salesforce.bulkv2.`type`.OperationEnum
import endolabs.salesforce.bulkv2.response.GetJobInfoResponse

import scala.util.Try
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

class SFObjectWriter2(val username: String,
                      val password: String,
                      val login: String,
                      val version: String,
                      val sfObject: String,
                      val mode: SaveMode,
                      val upsert: Boolean,
                      val externalIdFieldName: String,
                      val csvHeader: String,
                      val batchSize: Integer) extends Serializable {

  @transient val logger = Logger.getLogger(classOf[SFObjectWriter])

  def writeData(rdd: RDD[Row]): Boolean = {

    val csvRDD = rdd.map { row =>
      val schema = row.schema.fields
      row.toSeq.indices.map(
        index => Utils.cast(row, schema(index).dataType, index)
      ).mkString(",")
    }

//    val partitionCnt = (1 + csvRDD.count() / batchSize).toInt
//    val partitionedRDD = csvRDD.repartition(partitionCnt)

//    val jobInfo = new JobInfo(WaveAPIConstants.STR_CSV, sfObject, operation(mode, upsert))
//    jobInfo.setExternalIdFieldName(externalIdFieldName)
    //    jobInfo.setConcurrencyMode("Serial")

    //    jobInfo.setNumberRetries("10")
    val OperationEnum = operation(mode, upsert)
//    val jobId = bulkAPI.createJob(jobInfo).getId
    var bulkJobIDs = csvRDD.mapPartitionsWithIndex {
      case (index, iterator) => {

        val records = iterator.toArray.mkString("\n")
        var batchInfoId: String = null
        var id = ""
        if (records != null && !records.isEmpty()) {
          val createResponse = client.createJob(sfObject, OperationEnum)
          id = createResponse.getId
          val data = csvHeader + "\n" + records
//          val batchInfo = bulkAPI.addBatch(jobId, data)
//          batchInfoId = batchInfo.getId
          client.uploadJobData(id, data)
          client.closeJob(createResponse.getId)
//          id = createResponse.getId
        }

//        val success = (batchInfoId != null)
        // Job status will be checked after completing all batches
        List(id).iterator
      }
    }.collect().filter(_ != null)
    val allIds = bulkJobIDs

    var i = 1
    var failedRecords = 0
    val TIMEOUT_MAX = 7000
    val BREAK_LOOP = 9000
    var isEmpty = false
    while (i < TIMEOUT_MAX) {
      var data: Array[GetJobInfoResponse] = Array()
      try {
       data = bulkJobIDs.map{
        id =>
          client.getJobInfo(id)
      }
      failedRecords += data.map(x => x.getNumberRecordsFailed.toInt).sum

//      val printFailedResults = new BufferedReader(client.getJobFailedRecordResults(data.head))
      data.foreach{
        x => if (x.getNumberRecordsFailed != 0) {
          logger.info(s"${x.getRetries} number of retries")
          val results = new BufferedReader(client.getJobFailedRecordResults(x.getId))
          results.lines().iterator().asScala.foreach{
            line =>
              println(line)
              logger.info(line)
          }
        }
      }
      println(s"${data.count(x=> x.isFinished)} finished jobs ${data.length} remanining jobs")
      println(s"${data.take(10).map(x => (x.getId, x.getState.toJsonValue)).mkString(",")} sample states")
      } catch {
        case e: Exception => println(e)
          bulkJobIDs = allIds
      }
      if (data.count(x=> x.isFinished) == data.length) {
        i = BREAK_LOOP
        isEmpty = data.isEmpty
      } else {
        bulkJobIDs = data.filter(x => !x.isFinished).map(x => x.getId)
        logger.info("Job not completed, waiting...")
        Thread.sleep(10000)
        i = i + 1
      }
    }
    if(isEmpty) {
      return true
    }

    if (i == BREAK_LOOP && failedRecords == 0){
      return true
    } else if(failedRecords != 0) {
      logger.info("Job failed. Timeout...")
      return true
    }
    print("Returning false...")
    logger.info("Job not completed. Timeout...")
    false

  }

//  // Create new instance of BulkAPI every time because Spark workers cannot serialize the object
//  private def bulkAPI(): BulkAPI = {
//    APIFactory.getInstance().bulkAPI(username, password, login, version)
//  }
//  val bulkAPIClient = new ForceApi(new ApiConfig()
//  .setUsername(username)
//  .setPassword(password)
//  .setLoginEndpoint(login)
//  .setApiVersion(ApiVersion.V48)
//)
  private def bulkAPIClient(): ForceApi = {
  new ForceApi(new ApiConfig()
    .setUsername(username)
    .setPassword(password)
    .setLoginEndpoint(login)
    .setApiVersion(ApiVersion.V48))
}
  private  def client() = {

    new Bulk2ClientBuilder().withSessionId(bulkAPIClient.getSession.getAccessToken, bulkAPIClient.getSession.getApiEndpoint)
      .build()
  }

  private def operation(mode: SaveMode, upsert: Boolean): OperationEnum = {
    if (upsert) {
      OperationEnum.UPSERT
    } else if (mode != null && SaveMode.Overwrite.name().equalsIgnoreCase(mode.name())) {
//      WaveAPIConstants.STR_UPDATE
      OperationEnum.UPDATE
    } else if (mode != null && SaveMode.Append.name().equalsIgnoreCase(mode.name())) {
//      WaveAPIConstants.STR_INSERT
      OperationEnum.INSERT
    } else {
      logger.warn("SaveMode " + mode + " Not supported. Using 'insert' operation")
      OperationEnum.INSERT
    }
  }


}
