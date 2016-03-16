package com.springml.spark.salesforce

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.springml.salesforce.wave.api.APIFactory
import com.springml.salesforce.wave.api.BulkAPI

/**
 * Write class responsible for update Salesforce object using data provided in dataframe
 * First column of dataframe contains Salesforce Object
 * Next subsequent columns are fields to be updated
 */
class SFObjectWriter (
    val username: String,
    val password: String,
    val login: String,
    val apiVersion: String,
    val sfObject: String,
    val csvHeader: String
    ) extends Serializable {

  @transient val logger = Logger.getLogger(classOf[SFObjectWriter])

  def writeData(rdd: RDD[Row]): Boolean = {
    val csvRDD = rdd.map(row => row.toSeq.map(value => value.toString()).mkString(","))
    val jobId = bulkAPI.createJob(sfObject).getId

    csvRDD.mapPartitionsWithIndex {
      case (index, iterator) => {
        val data = csvHeader + "\n" + iterator.toArray.mkString("\n")
        val batchInfo = bulkAPI.addBatch(jobId, data)
        val success = (batchInfo.getId != null)
        // Job status will be checked after completing all batches
        List(success).iterator
      }
    }.reduce((a, b) => a & b)

    bulkAPI.closeJob(jobId)
    var i = 1
    while (i < 999999) {
      if (bulkAPI.isCompleted(jobId)) {
        logger.info("Job completed")
        return true
      }

      logger.info("Job not completed, waiting...")
      Thread.sleep(200)
      i = i + 1
    }

    print("Returning false...")
    logger.info("Job not completed. Timeout..." )
    false
  }

  def bulkAPI() : BulkAPI = {
    APIFactory.getInstance.bulkAPI(username, password, login, apiVersion)
  }

}