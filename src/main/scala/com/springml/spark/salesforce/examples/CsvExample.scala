package com.springml.spark.salesforce.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by madhu on 9/7/15.
 */
object CsvExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"csv read")
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    df.printSchema()

    df.write.format("com.springml.spark.salesforce").option("username","sparktest@springml.com").
    option("password","Fire2015!ZlvTTJsBJopFBMXJWt0xBUjg0").save()


  }

}
