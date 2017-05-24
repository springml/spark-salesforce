package com.springml.spark.salesforce

import com.amazonaws.auth.{ AWSCredentialsProvider, BasicSessionCredentials }

/**
 * All user-specifiable parameters for spark-redshift, along with their validation rules and
 * defaults.
 */
private[salesforce] object Parameters {

  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    // Notes:
    // * tempdir, dbtable and url have no default and they *must* be provided
    // * sortkeyspec has no default, but is optional
    // * distkey has no default, but is optional unless using diststyle KEY
    // * jdbcdriver has no default, but is optional
    "forward_spark_s3_credentials" -> "false",
    "login" -> "https://login.salesforce.com",
    "version" -> "36.0",
    "csvnullstring" -> "@NULL@",
    "overwrite" -> "false",
    "diststyle" -> "EVEN",
    "preactions" -> ";",
    "postactions" -> ";",
    "pageSize" -> "1000",
    "sampleSize" -> "1000",
    "maxRetry" -> "5",
    "inferSchema" -> "true",
    "replaceDatasetNameWithId" -> "false",
    "upsert" -> "false",
    "monitorJob" -> "false",
    "aws_iam_role" ->"arn:aws:iam::699237797221:role/myRedshiftRole"   
    //    "pkChunking" -> "false"
    //    val username = param(parameters, "SF_USERNAME", "username")
    //    val password = param(parameters, "SF_PASSWORD", "password")
    //    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    //    val version = parameters.getOrElse("version", "36.0")
    //    val saql = parameters.get("saql")
    //    val soql = parameters.get("soql")
    //    val resultVariable = parameters.get("resultVariable")
    //    val pageSize = parameters.getOrElse("pageSize", "1000")
    //    val sampleSize = parameters.getOrElse("sampleSize", "1000")
    //    val maxRetry = parameters.getOrElse("maxRetry", "5")
    //    val inferSchema = parameters.getOrElse("inferSchema", "false")
    //    val bulk = parameters.getOrElse("bulk", "false")
    //    val pkChunking = parameters.getOrElse("pkChunking", "false")
    //    val tempDir = parameters.getOrElse("tempDir","undefined");
    //    val datasetName = parameters.get("datasetName")
    //    val sfObject = parameters.get("sfObject")
    //    val appName = parameters.getOrElse("appName", null)
    //    val usersMetadataConfig = parameters.get("metadataConfig")
    //    val upsert = parameters.getOrElse("upsert", "false")
    //    val metadataFile = parameters.get("metadataFile")
    //    val monitorJob = parameters.getOrElse("monitorJob", "false")
    )

  val VALID_TEMP_FORMATS = Set("AVRO", "CSV", "CSV GZIP")

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String], save: Boolean): MergedParameters = {
    if (userParameters.contains("bulk") && !userParameters.contains("tempdir")) {
      throw new IllegalArgumentException("'tempdir' is required for all Bulk Query API")
    }
    if (userParameters.contains("bulk") && !userParameters.contains("soql")) {
      throw new IllegalArgumentException("'soql' is required for all Bulk Query API")
    }
    if (userParameters.contains("pkChunking") && !userParameters.contains("tempdir")) {
      throw new IllegalArgumentException("'tempdir' is required for all Bulk Query API")
    }
    if (!userParameters.contains("username") && !sys.env.get("SF_USERNAME").isDefined) {
      throw new IllegalArgumentException("A salesforce 'username' must be provided for authentication")
    }
    if (!userParameters.contains("password") && !sys.env.get("SF_USERNAME").isDefined) {
      throw new IllegalArgumentException("A salesforce 'password' must be provided for authentication")
    }
    if (userParameters.contains("saql") && userParameters.contains("soql")) {
      throw new IllegalArgumentException("Either one of 'saql' or 'soql' is expected for creating dataframe, not both")
    }
    if (!userParameters.contains("saql") && !userParameters.contains("soql")) {
      throw new IllegalArgumentException("Either one of 'saql' or 'soql' is expected for creating dataframe")
    }

    MergedParameters(DEFAULT_PARAMETERS ++ userParameters, save)
  }

  /**
   * Adds validators and accessors to string map
   */
  case class MergedParameters(parameters: Map[String, String], save: Boolean) {

    if (save) {
      require(datasetName.isDefined && upsert && !metadataFile.isDefined, "metadataFile has to be provided for upsert")

      require(!(soql.isDefined || saql.isDefined) && datasetName.isDefined || sfObject.isDefined,
        "You must specify either 'datasetName', 'sfObject' for upsert to Salesforce")

      require(Seq(
        datasetName.isDefined,
        sfObject.isDefined).count(_ == true) == 1,
        "'sfObjct' and 'datasetName' are mutually-exclusive; please specify only one.")
    } else {
      require(temporaryAWSCredentials.isDefined || iamRole.isDefined || forwardSparkS3Credentials,
        "You must specify a method for authenticating Spark-Salesforce's connection to S3 (aws_iam_role," +
          " forward_spark_s3_credentials, or temporary_aws_*. For a discussion of the differences" +
          " between these options, please see the README.")

      require(Seq(
        temporaryAWSCredentials.isDefined,
        iamRole.isDefined,
        forwardSparkS3Credentials).count(_ == true) == 1,
        "The aws_iam_role, forward_spark_s3_credentials, and temporary_aws_*. options are " +
          "mutually-exclusive; please specify only one.")
    }

    /**
     * A root directory to be used for intermediate data exchange, expected to be on S3, or
     * somewhere that can be written to and read from by Redshift. Make sure that AWS credentials
     * are available for S3.
     */
    def rootTempDir: String = parameters("tempdir")

    /**
     * The format in which to save temporary files in S3. Defaults to "AVRO"; the other allowed
     * values are "CSV" and "CSV GZIP" for CSV and gzipped CSV, respectively.
     */
    def tempFormat: String = parameters("tempformat").toUpperCase

    /**
     * The String value to write for nulls when using CSV.
     * This should be a value which does not appear in your actual data.
     */
    def nullString: String = parameters("csvnullstring")

    /**
     * Creates a per-query subdirectory in the [[rootTempDir]], with a random UUID.
     */
    def createPerQueryTempDir(): String = Utils.makeTempPath(rootTempDir)

    //    /**
    //     * The Redshift query to be used as the target when loading data.
    //     */
    //    def query: Option[String] = parameters.get("query").orElse {
    //      parameters.get("dbtable")
    //        .map(_.trim)
    //        .filter(t => t.startsWith("(") && t.endsWith(")"))
    //        .map(t => t.drop(1).dropRight(1))
    //    }

    /**
     * User and password to be used to authenticate to SalesForce
     */
    def credentials: Option[(String, String)] = {
      val user = parameters.get("username").getOrElse(sys.env.get("SF_USERNAME").get);
      val password = parameters.get("password").getOrElse(sys.env.get("SF_PASSWORD").get);
      Option(user, password)
    }

    //    /**
    //     * List of semi-colon separated SQL statements to run before write operations.
    //     * This can be useful for running DELETE operations to clean up data
    //     *
    //     * If the action string contains %s, the table name will be substituted in, in case a staging
    //     * table is being used.
    //     *
    //     * Defaults to empty.
    //     */
    //    def preActions: Array[String] = parameters("preactions").split(";")
    //
    //    /**
    //     * List of semi-colon separated SQL statements to run after successful write operations.
    //     * This can be useful for running GRANT operations to make your new tables readable to other
    //     * users and groups.
    //     *
    //     * If the action string contains %s, the table name will be substituted in, in case a staging
    //     * table is being used.
    //     *
    //     * Defaults to empty.
    //     */
    //    def postActions: Array[String] = parameters("postactions").split(";")

    /**
     * The IAM role that Redshift should assume for COPY/UNLOAD operations.
     */
    def iamRole: Option[String] = parameters.get("aws_iam_role")

    /**
     * If true then this library will automatically discover the credentials that Spark is
     * using to connect to S3 and will forward those credentials to Redshift over JDBC.
     */
    def forwardSparkS3Credentials: Boolean = parameters("forward_spark_s3_credentials").toBoolean

    /**
     * Temporary AWS credentials which are passed to spark-salesforce. These only need to be supplied by
     * the user when Hadoop is configured to authenticate to S3 via IAM roles assigned to EC2
     * instances.
     */
    def temporaryAWSCredentials: Option[AWSCredentialsProvider] = {
      for (
        accessKey <- parameters.get("temporary_aws_access_key_id");
        secretAccessKey <- parameters.get("temporary_aws_secret_access_key");
        sessionToken <- parameters.get("temporary_aws_session_token")
      ) yield {
        AWSCredentialsUtils.staticCredentialsProvider(
          new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken))
      }
    }

    def soql: Option[String] = parameters.get("soql")

    def saql: Option[String] = parameters.get("saql")

    // This is only needed for Spark version 1.5.2 or lower
    def encodeFields = parameters.get("encodeFields")

    def inferSchema = "true".equals(parameters("inferSchema"))

    def user = parameters.get("username").getOrElse(sys.env.get("SF_USERNAME").get);
    def password = parameters.get("password").getOrElse(sys.env.get("SF_PASSWORD").get);

    def version = parameters("version")

    def login = parameters("login")

    def resultVariable = parameters.get("resultVariable")

    def pageSize = parameters.get("pageSize").get.toInt

    def sampleSize = parameters.get("sampleSize").get.toInt

    def maxRetry = parameters.get("maxRetry").get.toInt

    def replaceDatasetNameWithId = "true".equalsIgnoreCase(parameters("replaceDatasetNameWithId"))

    def bulk = parameters.get("bulk").isDefined && "true".equalsIgnoreCase(parameters("bulk")) || parameters.get("pkChunking").isDefined

    def pkChunking = parameters.get("pkChunking")

    def datasetName = parameters.get("datasetName")

    def sfObject = parameters.get("sfObject")

    def appName = parameters.get("appName")

    def userMetaConfig = parameters.get("metadataConfig")

    def upsert = "true".equalsIgnoreCase(parameters.getOrElse("upsert", "false"))

    def metadataFile = parameters.get("metadataFile")

    def monitorJob = "true".equalsIgnoreCase(parameters.getOrElse("monitorJob", "false"))

    def objectName: String = {
      soql match {
        case Some(query) => {
          val begin = query.toLowerCase().indexOf(" from ") + 6
          val end = query.indexOf(" ", begin)
          if (end == (-1))
            query.substring(begin)
          else
            query.substring(begin, end)
        }
        case _ => "Could not extract object name from SOQL query"
      }
    }

  }
}