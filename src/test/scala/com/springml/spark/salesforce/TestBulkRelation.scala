package com.springml.spark.salesforce

import org.scalatest.{ FunSuite, BeforeAndAfterEach }
import org.mockito.Mockito._
import org.scalatest.easymock.EasyMockSugar
import com.springml.salesforce.wave.api.BulkAPI
import com.springml.salesforce.wave.api.ForceAPI
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext, Row, SparkSession }
import com.springml.spark.salesforce.Parameters.MergedParameters
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.AWSCredentialsProvider
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

class TestBulkRelation extends FunSuite with org.scalatest.mockito.MockitoSugar with BeforeAndAfterEach {

  val params = mock[MergedParameters]
  val aws = mock[AWSCredentialsProvider]
  val bulkAPI = mock[BulkAPI]
  val soql = "SELECT Id, Name, Type FROM Opportunity";

  var spark: SparkSession = _

  //  val s3ClientFactory: AWSCredentialsProvider => AmazonS3Client = new AmazonS3Client(awsCredentials);

  //  ignore("test bulk read") {
  //    val dr = BulkRelation(params, s3ClientFactory, None)(spark.sqlContext)
  //    val rdd = dr.buildScan()
  //    spark.stop()
  //  }

  override def beforeEach() {
    //    when(params.soql) thenReturn (Option(soql))
    //    when(params.rootTempDir) thenReturn ("s3n://oolong.staging/temp")
    //    when(params.bulk) thenReturn (true)
    //    when(params.bulk) thenReturn (true)
    //    when(params.user) thenReturn ("james@ooequipment.com")
    //    when(params.password) thenReturn ("oolong200molbX310CHFjJHR8djRKpiB1")

    spark = SparkSession.builder()
      .master("local")
      .appName("Test Bulk Relation")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJZ4OFWMBV2MQCB2A")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
  
  }

  //  override def afterEach() {
  //    spark.stop()
  //  }

  ignore("directly from salesforce") {
      val dfr = spark.sqlContext.
        read.
        format("com.springml.spark.salesforce").
        option("username", "james@ooequipment.com").
        option("password", "oolong200molbX310CHFjJHR8djRKpiB1").
        option("tempdir", "s3n://oolong.staging/temp").
        option("soql", soql).
        option("bulk", true).
        option("version", "36.0").
        load();
      dfr.foreach(
          r => println(r)
          );
      println("Finished")
  }
  
  ignore ("basics") {
    val schema = StructType(Array( StructField("Id",StringType,true),  StructField("Name",StringType,true),  StructField("Type",StringType,true)))
    val files = List("s3n://oolong.staging/temp/0df7677c-ecaa-4d5e-a948-6b94b0dd600a/7503600000C4vDmAAJ/7513600000DGePyAAL")
    val df =   spark.sqlContext.read
        //   .format(classOf[SalesforceBatchFileFormat].getName)
        .format("com.databricks.spark.csv")
        .schema(schema)
        .option("header", "false")
        //      .load(filesToRead: _*)
        .load(files: _*)
        ///.queryExecution.executedPlan.execute().asInstanceOf[RDD[Row]]
    
    df.foreach(
        a=>println(a)
        );
  }

}
