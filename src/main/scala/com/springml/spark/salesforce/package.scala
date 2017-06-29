/*
 * Copyright 2015 - 2017, oolong  
 * Author  :
 * 		Kagan Turgut
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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