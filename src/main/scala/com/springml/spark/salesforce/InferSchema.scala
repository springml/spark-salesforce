package com.springml.spark.salesforce

import scala.util.control.Exception._

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType}
import org.apache.spark.sql.types.{IntegerType, LongType, NullType, ShortType, StringType}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

/**
 * Utility to InferSchema from the provided Sample
 */
object InferSchema {
  private val logger = Logger.getLogger("InferSchema")

  /**
   * This is much similar the InferSchema written for JSON
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(sampleRdd: RDD[Array[String]], header: Array[String],
            sdf: SimpleDateFormat): StructType = {
    logger.debug("Sample RDD Size : " + sampleRdd.count)
    logger.debug("Header : " + header)
    val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
    val rootTypes: Array[DataType] = sampleRdd.aggregate(startType)(inferRowType(sdf),
      mergeRowTypes)

    val structFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      StructField(thisHeader, rootType, nullable = true)
    }

    StructType(structFields)
  }

  private def inferRowType(sdf:SimpleDateFormat)
                          (rowSoFar: Array[DataType], next: Array[String]): Array[DataType] = {
    logger.debug("Rows so far : " + rowSoFar)
    logger.debug("Next row to be infered : " + next)
    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {
      rowSoFar(i) = inferField(rowSoFar(i), next(i), sdf)
      i+=1
    }
    rowSoFar
  }

  private def mergeRowTypes(
      first: Array[DataType],
      second: Array[DataType]): Array[DataType] = {
    first.zipAll(second, NullType, NullType).map { case ((a, b)) =>
      val tpe = findTightestCommonType(a, b).getOrElse(StringType)
      tpe match {
        case _: NullType => StringType
        case other => other
      }
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  private def inferField(typeSoFar: DataType, field: String, sdf: SimpleDateFormat): DataType = {
    if (field == null || field.isEmpty || field.stripMargin.toLowerCase() == "null") {
      typeSoFar
    } else {
      typeSoFar match {
        case NullType => tryParseInteger(field, sdf)
        case IntegerType => tryParseInteger(field, sdf)
        case LongType => tryParseLong(field, sdf)
        case DoubleType => tryParseDouble(field, sdf)
        case TimestampType => tryParseTimestamp(field, sdf)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }


  private def tryParseInteger(field: String, sdf: SimpleDateFormat): DataType = {
    if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else {
      tryParseLong(field, sdf)
    }
  }

  private def tryParseLong(field: String, sdf: SimpleDateFormat): DataType = {
    if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else {
      tryParseDouble(field, sdf)
    }
  }

  private def tryParseDouble(field: String, sdf: SimpleDateFormat): DataType = {
    if ((allCatch opt field.toDouble).isDefined) {
      DoubleType
    } else {
      tryParseTimestamp(field, sdf)
    }
  }


  def tryParseTimestamp(field: String, sdf: SimpleDateFormat): DataType = {
    if (sdf != null) {
      if ((allCatch opt sdf.parse(field)).isDefined){
        TimestampType
      } else {
        tryParseBoolean(field)
      }
    } else {
      if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
        TimestampType
      } else {
        tryParseBoolean(field)
      }
    }
  }

  def tryParseBoolean(field: String): DataType = {
    if ((allCatch opt field.toBoolean).isDefined) {
      BooleanType
    } else {
      stringType()
    }
  }

  private def stringType(): DataType = {
    StringType
  }

  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType)

  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}