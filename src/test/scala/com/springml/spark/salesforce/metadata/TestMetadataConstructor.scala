package com.springml.spark.salesforce.metadata

import org.apache.spark.sql.types.{StructType, StringType, IntegerType, LongType,
  FloatType, DateType, TimestampType, BooleanType, StructField}
import org.scalatest.FunSuite
import com.springml.spark.salesforce.Utils

/**
 */
class TestMetadataConstructor extends FunSuite {
    test("Test Metadata generation") {
    val columnNames = List("c1", "c2", "c3", "c4")
    val columnStruct = columnNames.map(colName => StructField(colName, StringType, true))
    val schema = StructType(columnStruct)

    val schemaString = MetadataConstructor.generateMetaString(schema,"sampleDataSet", Utils.metadataConfig(null))
    assert(schemaString.length > 0)
    assert(schemaString.contains("sampleDataSet"))
  }

  test("Test Metadata generation With Custom MetadataConfig") {
    val columnNames = List("c1", "c2", "c3", "c4")
    val intField = StructField("intCol", IntegerType, true)
    val longField = StructField("longCol", LongType, true)
    val floatField = StructField("floatCol", FloatType, true)
    val dateField = StructField("dateCol", DateType, true)
    val timestampField = StructField("timestampCol", TimestampType, true)
    val stringField = StructField("stringCol", StringType, true)
    val someTypeField = StructField("someTypeCol", BooleanType, true)

    val columnStruct = Array[StructField] (intField, longField, floatField, dateField, timestampField, stringField, someTypeField)

    val schema = StructType(columnStruct)

    var metadataConfig = Map("string" -> Map("wave_type" -> "Text"))
    metadataConfig += ("integer" -> Map("wave_type" -> "Numeric", "precision" -> "10", "scale" -> "0", "defaultValue" -> "100"))
    metadataConfig += ("float" -> Map("wave_type" -> "Numeric", "precision" -> "10", "scale" -> "2"))
    metadataConfig += ("long" -> Map("wave_type" -> "Numeric", "precision" -> "18", "scale" -> "0"))
    metadataConfig += ("date" -> Map("wave_type" -> "Date", "format" -> "yyyy/MM/dd"))
    metadataConfig += ("timestamp" -> Map("wave_type" -> "Date", "format" -> "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))


    val schemaString = MetadataConstructor.generateMetaString(schema, "sampleDataSet", metadataConfig)
    assert(schemaString.length > 0)
    assert(schemaString.contains("sampleDataSet"))
    assert(schemaString.contains("Numeric"))
    assert(schemaString.contains("precision"))
    assert(schemaString.contains("scale"))
    assert(schemaString.contains("18"))
    assert(schemaString.contains("Text"))
    assert(schemaString.contains("Date"))
    assert(schemaString.contains("format"))
    assert(schemaString.contains("defaultValue"))
    assert(schemaString.contains("100"))
    assert(schemaString.contains("yyyy/MM/dd"))
    assert(schemaString.contains("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
  }
}