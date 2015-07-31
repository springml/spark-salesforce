package com.springml.spark.salesforce.metadata

/**
 * Model class for DataType details
 */
class SFDataType(var wave_type: String, 
              var precision: Int,
              var scale: Int,
              var format: String
              )