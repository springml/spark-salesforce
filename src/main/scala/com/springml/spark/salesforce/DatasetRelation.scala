package com.springml.spark.salesforce

import java.math.BigDecimal
import java.sql.{ Timestamp, Date }
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.{ BaseRelation, TableScan}
import org.apache.spark.sql.{ Row, SQLContext}
import com.springml.salesforce.wave.api.APIFactory
import org.apache.spark.sql.types.{ StructField, StringType, ByteType, ShortType, IntegerType }
import org.apache.spark.sql.types.{ LongType, DataType, FloatType, DoubleType, BooleanType }
import org.apache.spark.sql.types.{ DecimalType, TimestampType, DateType, StructType}
import com.springml.salesforce.wave.api.WaveAPI

/**
 * Relation class for reading data from Salesforce and construct RDD
 */
case class DatasetRelation(
    waveAPI: WaveAPI,
    query: String,
    userSchema: StructType,
    sqlContext: SQLContext) extends BaseRelation with TableScan {

  private val logger = Logger.getLogger(classOf[DatasetRelation])

  val records = read()

  def read(): java.util.List[java.util.Map[String, String]] = {
    // Query getting executed here
    val resultSet = waveAPI.query(query)
    resultSet.getResults.getRecords
  }

  private def cast(fieldValue: String, toType: DataType, nullable: Boolean = true): Any = {
    if (fieldValue == "" && nullable && !toType.isInstanceOf[StringType]) {
      null
    } else {
      toType match {
        case _: ByteType => fieldValue.toByte
        case _: ShortType => fieldValue.toShort
        case _: IntegerType => fieldValue.toInt
        case _: LongType => fieldValue.toLong
        case _: FloatType => fieldValue.toFloat
        case _: DoubleType => fieldValue.toDouble
        case _: BooleanType => fieldValue.toBoolean
        case _: DecimalType => new BigDecimal(fieldValue.replaceAll(",", ""))
        case _: TimestampType => Timestamp.valueOf(fieldValue)
        case _: DateType => Date.valueOf(fieldValue)
        case _: StringType => fieldValue
        case _ => throw new RuntimeException(s"Unsupported data type: ${toType.typeName}")
      }
    }
  }

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      // Construct the schema with all fields as String
      val firstRow = records.iterator().next()
      val structFields = new Array[StructField](firstRow.size())
      var index: Int = 0
      for (fieldEntry <- firstRow) {
        structFields(index) = StructField(fieldEntry._1, StringType, nullable = true)
        index = index + 1
      }

      StructType(structFields)
    }
  }

  override def buildScan(): RDD[Row] = {
    val schemaFields = schema.fields
    logger.info("Total records size : " + records.size())
    val rowArray = new Array[Row](records.size())
    var rowIndex: Int = 0
    for (row <- records) {
      val fieldArray = new Array[Any](schemaFields.length)
      logger.debug("Total Fields length : " + schemaFields.length)
      var fieldIndex: Int = 0
      for (fields <- schemaFields) {
        val fieldValue = row(fields.name)
        logger.debug("fieldValue " + fieldValue)
        fieldArray(fieldIndex) = cast(fieldValue, fields.dataType, fields.nullable)
        fieldIndex = fieldIndex + 1
      }

      logger.debug("rowIndex : " + rowIndex)
      rowArray(rowIndex) = Row.fromSeq(fieldArray)
      rowIndex = rowIndex + 1
    }
    sqlContext.sparkContext.parallelize(rowArray)
  }

}