package com.springml.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

package object salesforce {

  /**
   * Wrapper of SQLContext that provide `batchFile` method.
   */
  implicit class SalesForceContext(sqlContext: SQLContext) {

//    /**
//     * Read a file unloaded from Salesforce Batch API into a DataFrame.
//     * @param path input path
//     * @return a DataFrame with all string columns
//     */
//    def batchFile(path: String, columns: Seq[String]): DataFrame = {
//      val sc = sqlContext.sparkContext
//      val rdd = sc.newAPIHadoopFile(path, classOf[SalesForceBatchInputFormat],
//        classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
//      // TODO: allow setting NULL string.
//      val nullable = rdd.values.map(_.map(f => if (f.isEmpty) null else f)).map(x => Row(x: _*))
//      val schema = StructType(columns.map(c => StructField(c, StringType, nullable = true)))
//      sqlContext.createDataFrame(nullable, schema)
//    }
//
//    /**
//     * Reads a table unload from Salesforce Batch API with its schema.
//     */
//    def batchFile(path: String, schema: StructType): DataFrame = {
//      val casts = schema.fields.map { field =>
//        col(field.name).cast(field.dataType).as(field.name)
//      }
//      batchFile(path, schema.fieldNames).select(casts: _*)
//    }
  }
}