package com.springml.spark.salesforce.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext



/**
 * Created by madhu on 9/7/15.
 */
object CsvRead {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("csv write")
    .set("mapred.max.split.size","8000000").set("mapred.min.split.size","8000000")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.max.split.size","8000000")
    sc.hadoopConfiguration.set("mapred.min.split.size","8000000")
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    val dataSetName = args(2)

    df.printSchema()

    df.write.format("com.springml.spark.salesforce").option("username","sparktest@springml.com").
    option("password","Fire2015!ZlvTTJsBJopFBMXJWt0xBUjg0").option("datasetName",dataSetName).save()


  }

}
