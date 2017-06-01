package com.springml.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

package object salesforce {

  /**
   * Wrapper of SQLContext that provide `batchFile` method.
   */
  implicit class SalesForceContext(sqlContext: SQLContext) {

  }
}