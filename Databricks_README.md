# Spark Salesforce Library

A library for connecting Spark with Salesforce and Salesforce Wave.

## Requirements

This library requires Spark 2.x.

For Spark 1.x support, please check [spark1.x](https://github.com/springml/spark-salesforce/tree/spark1.x) branch.


## Linking
You can link against this library in your program at the following ways:

## Importing library into Databricks
1) run `sbt assembly` to build a single, executable JAR file. The JAR will exist under target/scala-2.XX/spark-salesforce-assembly-X.X.X.jar
2) Upload JAR file into Databricks workspace.


## Features
* **Dataset Creation** - Create dataset in [Salesforce Wave](http://www.salesforce.com/in/analytics-cloud/overview/) Wave from [Spark DataFrames](http://spark.apache.org/docs/latest/sql-programming-guide.html)
* **Read Salesforce Wave Dataset** - User has to provide SAQL to read data from Salesforce Wave. The query result will be constructed as dataframe
* **Read Salesforce Object** - User has to provide SOQL to read data from Salesforce object. The query result will be constructed as dataframe
* **Update Salesforce Object** - Salesforce object will be updated with the details present in dataframe

### Options
* `username`: Salesforce Wave Username. This user should have privilege to upload datasets or execute SAQL or execute SOQL
* `password`: Salesforce Wave Password. Please append security token along with password.For example, if a user’s password is mypassword, and the security token is XXXXXXXXXX, the user must provide mypasswordXXXXXXXXXX
* `login`: (Optional) Salesforce Login URL. Default value https://login.salesforce.com
* `datasetName`: (Optional) Name of the dataset to be created in Salesforce Wave. Required for Dataset Creation
* `sfObject`: (Optional) Salesforce Object to be updated. (e.g.) Contact
* `metadataConfig`: (Optional) Metadata configuration which will be used to construct [Salesforce Wave Dataset Metadata] (https://resources.docs.salesforce.com/sfdc/pdf/bi_dev_guide_ext_data_format.pdf). Metadata configuration has to be provided in JSON format
* `saql`: (Optional) SAQL query to used to query Salesforce Wave. Mandatory for reading Salesforce Wave dataset
* `soql`: (Optional) SOQL query to used to query Salesforce Object. Mandatory for reading Salesforce Object like Opportunity
* `version`: (Optional) Salesforce API Version. Default 35.0
* `inferSchema`: (Optional) Inferschema from the query results. Sample rows will be taken to find the datatype
* `dateFormat`: (Optional) A string that indicates the format that follow java.text.SimpleDateFormat to use when reading timestamps. This applies to TimestampType. By default, it is null which means trying to parse timestamp by java.sql.Timestamp.valueOf()
* `resultVariable`: (Optional) result variable used in SAQL query. To paginate SAQL queries this package will add the required offset and limit. For example, in this SAQL query `q = load \"<dataset_id>/<dataset_version_id>\"; q = foreach q generate  'Name' as 'Name',  'Email' as 'Email';` **q** is the result variable
* `pageSize`: (Optional) Page size for each query to be executed against Salesforce Wave. Default value is 2000. This option can only be used if `resultVariable` is set
* `externalIdFieldName`: (Optional) The name of the field used as the external ID for Salesforce Object. This value is only used when doing an update or upsert. Default "Id"


### Scala API
```scala
// Writing Dataset
// Using spark-csv package to load dataframes
val df = spark.
                read.
                format("com.databricks.spark.csv").
                option("header", "true").
                load("your_csv_location")
df.
   write.
   format("com.springml.spark.salesforce").
   option("username", "your_salesforce_username").
   option("password", "your_salesforce_password_with_secutiry_token"). //<salesforce login password><security token>
   option("datasetName", "your_dataset_name").
   save()

// Reading Dataset
val saql = "q = load \"<dataset_id>/<dataset_version_id>\"; q = foreach q generate  'Name' as 'Name',  'Email' as 'Email';"
val sfWaveDF = spark.
                read.
                format("com.springml.spark.salesforce").
                option("username", "your_salesforce_username").
                option("password", "your_salesforce_password_with_secutiry_token"). //<salesforce login password><security token>
                option("saql", saql)
                option("inferSchema", "true").
                load()

// Reading Salesforce Object
val soql = "select id, name, amount from opportunity"
val sfDF = spark.
                read.
                format("com.springml.spark.salesforce").
                option("username", "your_salesforce_username").
                option("password", "your_salesforce_password_with_secutiry_token"). //<salesforce login password><security token>
                option("soql", soql).
                option("version", "37.0").
                load()

// Upsert Salesforce Object
// CSV should contain Id column followed other fields to be Updated
// Sample -
// Id,Description
// 003B00000067Rnx,Superman
// 003B00000067Rnw,SpiderMan
val df = spark.
                read.
                format("com.databricks.spark.csv").
                option("header", "true").
                load("your_csv_location")
df.
   write.
   format("com.springml.spark.salesforce").
   option("username", "your_salesforce_username").
   option("password", "your_salesforce_password_with_secutiry_token"). //<salesforce login password><security token>
   option("sfObject", "Contact").
   option("upsert", "true").
   save()

```


### Java API
```java
// Writing Dataset
DataFrame df = spark
                    .read()
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .load("your_csv_location");
df.write()
      .format("com.springml.spark.salesforce")
		  .option("username", "your_salesforce_username")
		  .option("password", "your_salesforce_password_with_secutiry_token") //<salesforce login password><security token>
		  .option("datasetName", "your_dataset_name")
		  .save();

// Reading Dataset
String saql = "q = load \"<dataset_id>/<dataset_version_id>\"; q = foreach q generate  'Name' as 'Name',  'Email' as 'Email';"
DataFrame sfWaveDF = spark.
          read().
          format("com.springml.spark.salesforce").
          option("username", "your_salesforce_username").
          option("password", "your_salesforce_password_with_secutiry_token"). //<salesforce login password><security token>
          option("saql", saql)
          option("inferSchema", "true").
          load()

// Reading Salesforce Object
String soql = "select id, name, amount from opportunity"
DataFrame sfDF = spark.
          read.
          format("com.springml.spark.salesforce").
          option("username", "your_salesforce_username").
          option("password", "your_salesforce_password_with_secutiry_token"). //<salesforce login password><security token>
          option("soql", soql).
          option("version", "37.0").
          load()

// Update Salesforce Object
// CSV should contain Id column followed other fields to be Updated
// Sample -
// Id,Description
// 003B00000067Rnx,Superman
// 003B00000067Rnw,SpiderMan
DataFrame df = spark
                    .read()
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .load("your_csv_location");
df.write().format("com.springml.spark.salesforce")
      .option("username", "your_salesforce_username")
      .option("password", "your_salesforce_password_with_secutiry_token")//<salesforce login password><security token>
      .option("sfObject", "Contact")
      .save();

```


### R API
```r
# Writing Dataset
df <- read.df("your_csv_location", source = "com.databricks.spark.csv", inferSchema = "true")
write.df(df, path="", source='com.springml.spark.salesforce', mode="append", datasetName="your_dataset_name", username="your_salesforce_username", password="your_salesforce_password_with_secutiry_token") #<salesforce login password><security token>

# Reading Dataset
saql <- "q = load \"<dataset_id>/<dataset_version_id>\"; q = foreach q generate  'Name' as 'Name',  'Email' as 'Email';"
sfWaveDF <- read.df(source="com.springml.spark.salesforce", username=your_salesforce_username, password=your_salesforce_password_with_secutiry_token, saql=saql) #<salesforce login password><security token>

# Reading Salesforce Object
soql <- "select id, name, amount from opportunity"
dfDF <- read.df(source="com.springml.spark.salesforce", username=your_salesforce_username, password=your_salesforce_password_with_secutiry_token, soql=soql) #<salesforce login password><security token>

# Update Salesforce Object
# CSV should contain Id column followed other fields to be Updated
# Sample -
# Id,Description
# 003B00000067Rnx,Superman
# 003B00000067Rnw,SpiderMan
df <- read.df("your_csv_location", source = "com.databricks.spark.csv", header = "true")
write.df(df, path="", source='com.springml.spark.salesforce', mode="append", sfObject="Contacct", username="your_salesforce_username", password="your_salesforce_password_with_secutiry_token") #<salesforce login password><security token>

```


## Metadata Configuration
This library constructs [Salesforce Wave Dataset Metadata] (https://resources.docs.salesforce.com/sfdc/pdf/bi_dev_guide_ext_data_format.pdf) using Metadata Configuration present in [resources](https://github.com/springml/spark-salesforce/blob/master/src/main/resources/metadata_config.json). User may modifiy the default behaviour. User can modify already defined datatypes or user may add additional datatypes. For example, user can change the scale to 5 for float datatype

Metadata configuration has to be provided in JSON format via "metadataConfig" option. The structure of the JSON is
```json

{
  "<df_data_type>": {
  "wave_type": "<wave_data_type>",
  "precision": "<precision>",
  "scale": "<scale>",
  "format": "<format>",
  "defaultValue": "<defaultValue>"
  }
}
```

* **df_data_type**: Dataframe datatype for which the Wave datatype to be mapped.
* **wave_data_type**: Salesforce wave supports Text, Numeric and Date types.
* **precision**: The maximum number of digits in a numeric value, or the length of a text value
* **scale**: The number of digits to the right of the decimal point in a numeric value. Must be less than the precision value
* **format**: The format of the numeric or date value.
* **defaultValue**: The default value of the field, if any. If not provided for Numeric fields, 0 is used as defaultValue

More details on Salesforce Wave Metadata can be found [here] (https://resources.docs.salesforce.com/sfdc/pdf/bi_dev_guide_ext_data_format.pdf)

#### Sample JSON
```json

{
  "float": {
  "wave_type": "Numeric",
  "precision": "10",
  "scale": "2",
  "format": "##0.00",
  "defaultValue": "0.00"
  }
}
```

#### Sample to provide metadata config
This sample is to change the format of the timestamp datatype.

```scala
// Default format is yyyy-MM-dd'T'HH:mm:ss.SSS'Z' and
// the this sample changes to yyyy/MM/dd'T'HH:mm:ss
val modifiedTimestampConfig = """{"timestamp":{"wave_type":"Date","format":"yyyy/MM/dd'T'HH:mm:ss"}}"""
// Using spark-csv package to load dataframes
val df = spark.read.format("com.databricks.spark.csv").
                          option("header", "true").
                          load("your_csv_location")
df.
   write.
    format("com.springml.spark.salesforce").
    option("username", "your_salesforce_username").
    option("password", "your_salesforce_password_with_secutiry_token").
    option("datasetName", "your_dataset_name").
    option("metadataConfig", modifiedTimestampConfig).
    save()

```


### Using this package in databricks

#### Create Spark Salesforce Package Library
* Login into your databricks instance
* Click Create-->Library and select "Maven Coordinate" as source
* Click "Search Spark Packages and Maven Central" button
* Select "spark-salesforce" and click "Create Library" button
* Library called "spark-salesforce_2.11-1.1.0" will be created
* Now attach it to your clusters

#### Upload Databricks table into Salesforce Wave
* Click Create-->Notebook in your databricks instance
* Select language that you want to use, select your cluster and click "Create" to create a notebook
* Write code to create dataframe. Below scala code is to create dataframe from your table
```scala
val df = spark.sql("select * from <your_table_name>")
```
* Write code to upload the dataframe as dataset into Salesforce Wave. Below scala code is to upload a dataframe into Salesforce Wave
```scala
df.
   write.
   format("com.springml.spark.salesforce").
   option("username", "your_salesforce_username").
   option("password", "your_salesforce_password_with_secutiry_token").
   option("datasetName", "your_dataset_name").
   save()
```


#### Short Demo Video
[![Spark Salesforce Package Demo](http://i.imgur.com/1W9VYV7.png?1)](https://www.youtube.com/watch?v=i6CyOJkG2_Y "Spark Salesforce Package Demo")


### Note
Salesforce wave does require atleast one "Text" field. So please make sure the dataframe has atleast one string type.

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
