package com.springml.spark.salesforce

import java.math.BigDecimal
import java.net.URLEncoder
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Random

import com.springml.salesforce.wave.api.{ForceAPI, WaveAPI}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, ShortType}
import org.apache.spark.sql.types.{StructField, StructType, StringType, TimestampType}

import scala.collection.JavaConversions.{asScalaBuffer, mapAsScalaMap}

/**
 * Relation class for reading data from Salesforce and construct RDD
 */
case class DatasetRelation(
    waveAPI: WaveAPI,
    forceAPI: ForceAPI,
    query: String,
    userSchema: StructType,
    sqlContext: SQLContext,
    resultVariable: Option[String],
    pageSize: Int,
    sampleSize: Int,
    encodeFields: Option[String],
    inferSchema: Boolean,
    replaceDatasetNameWithId: Boolean,
    sdf: SimpleDateFormat,
    queryAll: Boolean) extends BaseRelation with TableScan {

  private val logger = Logger.getLogger(classOf[DatasetRelation])

  val records = read()

  def read(): java.util.List[java.util.Map[String, String]] = {
    var records: java.util.List[java.util.Map[String, String]]= null
    // Query getting executed here
    if (waveAPI != null) {
      records = queryWave()
    } else if (forceAPI != null) {
      records = querySF()
    }

    records
  }

  private def queryWave(): java.util.List[java.util.Map[String, String]] = {
    var records: java.util.List[java.util.Map[String, String]]= null

    var saql = query
    if (replaceDatasetNameWithId) {
      logger.debug("Original Query " + query)
      saql = replaceDatasetNameWithId(query, 0)
      logger.debug("Modified Query " + saql)
    }

    if (resultVariable == null || !resultVariable.isDefined) {
      val resultSet = waveAPI.query(saql)
      records = resultSet.getResults.getRecords
    } else {
      var resultSet = waveAPI.queryWithPagination(saql, resultVariable.get, pageSize)
      records = resultSet.getResults.getRecords

      while (!resultSet.isDone()) {
        resultSet = waveAPI.queryMore(resultSet)
        records.addAll(resultSet.getResults.getRecords)
      }
    }

    records
  }

  def replaceDatasetNameWithId(query : String, startIndex : Integer) : String = {
    var modQuery = query

    logger.debug("start Index : " + startIndex)
    logger.debug("query : " + query)
    val loadIndex = query.indexOf("load", startIndex)
    logger.debug("loadIndex : " + loadIndex + "\n")
    if (loadIndex != -1) {
      val startDatasetIndex = query.indexOf('\"', loadIndex + 1)
      val endDatasetIndex = query.indexOf('\"', startDatasetIndex + 1)
      val datasetName = query.substring(startDatasetIndex + 1, endDatasetIndex)

      val datasetId = waveAPI.getDatasetId(datasetName)
      if (datasetId != null) {
        modQuery = query.replaceAll(datasetName, datasetId)
      }

      modQuery = replaceDatasetNameWithId(modQuery, endDatasetIndex + 1)
    }

    modQuery
  }

  private def querySF(): java.util.List[java.util.Map[String, String]] = {
      var records: java.util.List[java.util.Map[String, String]]= null

      var resultSet = forceAPI.query(query, queryAll)
      records = resultSet.filterRecords()

      while (!resultSet.isDone()) {
        resultSet = forceAPI.queryMore(resultSet)
        records.addAll(resultSet.filterRecords())
      }

      return records
  }

  private def cast(fieldValue: String, toType: DataType,
      nullable: Boolean = true, fieldName: String): Any = {
    if ((fieldValue == "" || fieldValue.stripMargin.toLowerCase() == "null") && nullable && !toType.isInstanceOf[StringType]) {
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
        case _: TimestampType => {
          if (sdf != null) {
            new Timestamp(sdf.parse(fieldValue).getTime)
          } else {
            Timestamp.valueOf(fieldValue)
          }
        }
        case _: DateType => Date.valueOf(fieldValue)
        case _: StringType => encode(fieldValue, fieldName)
        case _ => throw new RuntimeException(s"Unsupported data type: ${toType.typeName}")
      }
    }
  }

  private def encode(value: String, fieldName: String): String = {
    if (shouldEncode(fieldName)) {
      URLEncoder.encode(value, "UTF-8")
    } else {
      value
    }
  }

  private def shouldEncode(fieldName: String) : Boolean = {
    if (encodeFields != null && encodeFields.isDefined) {
      val toBeEncodedField = encodeFields.get.split(",")
      return toBeEncodedField.contains(fieldName)
    }

    false
  }

  private def sampleRDD: RDD[Array[String]] = {
    logger.debug("Sample Size : " + getSampleSize)
    // Constructing RDD from records
    val sampleRowArray = new Array[Array[String]](getSampleSize)
    for (i <- 0 to getSampleSize - 1) {
      val row = records(i)
      logger.debug("rows size : " + row.size())
      val fieldArray = new Array[String](row.size())

      var fieldIndex: Int = 0
      for (column <- row) {
        fieldArray(fieldIndex) = column._2
        fieldIndex = fieldIndex + 1
      }

      sampleRowArray(i) = fieldArray
    }

    // Converting the Array into RDD
    sqlContext.sparkContext.parallelize(sampleRowArray)
  }

  private def getSampleSize : Integer = {
    // If the record is less than sampleSize, then the whole data is used as sample
    val totalRecordsSize = records.size()
    logger.debug("Total Record Size: " + totalRecordsSize)
    if (totalRecordsSize < sampleSize) {
      logger.debug("Total Record Size " + totalRecordsSize
          + " is Smaller than Sample Size "
          + sampleSize + ". So total records are used for sampling")
      totalRecordsSize
    } else {
      sampleSize
    }
  }

  private def header: Array[String] = {
    val sampleList = sample

    var header : Array[String] = null
    for (currentRecord <- sampleList) {
      logger.debug("record size " + currentRecord.size())
      val recordHeader = new Array[String](currentRecord.size())
      var index: Int = 0
      for ((k, _) <- currentRecord) {
        logger.debug("Key " + k)
        recordHeader(index) = k
        index = index + 1
      }

      if (header == null || header.length < recordHeader.length) {
        header = recordHeader
      }
    }

    header
  }

  private def sample: java.util.List[java.util.Map[String, String]] = {
    val sampleRecords = new java.util.ArrayList[java.util.Map[String, String]]()
    val random = new Random()
    val totalSize = records.size()
    for (i <- 0 to getSampleSize) {
      sampleRecords += records.get(random.nextInt(totalSize))
    }

    sampleRecords
  }

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else if (records == null || records.size() == 0) {
      new StructType()
    } else if (inferSchema) {
      InferSchema(sampleRDD, header, sdf)
    } else {
      val schemaHeader = header
      val structFields = new Array[StructField](schemaHeader.length)
      var index: Int = 0
      logger.debug("header size " + schemaHeader.length)
      for (fieldEntry <- schemaHeader) {
        logger.debug("header (" + index + ") = " + fieldEntry)
        structFields(index) = StructField(fieldEntry, StringType, nullable = true)
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
        val value = fieldValue(row, fields.name)
        logger.debug("fieldValue " + value)
        fieldArray(fieldIndex) = cast(value, fields.dataType, fields.nullable, fields.name)
        fieldIndex = fieldIndex + 1
      }

      logger.debug("rowIndex : " + rowIndex)
      rowArray(rowIndex) = Row.fromSeq(fieldArray)
      rowIndex = rowIndex + 1
    }
    sqlContext.sparkContext.parallelize(rowArray)
  }

  private def fieldValue(row: java.util.Map[String, String], name: String) : String = {
    if (row.contains(name)) {
      row(name)
    } else {
      logger.debug("Value not found for " + name)
      ""
    }
  }
}
