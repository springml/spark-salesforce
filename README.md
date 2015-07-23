# Spark Salesforce Wave Library

A library for uploading dataframes into Salesforce Wave.

## Requirements

This library requires Spark 1.4+

## Linking
You can link against this library in your program at the following ways:

### Maven Dependency
```
<dependency>
    <groupId>com.springml</groupId>
    <artifactId>spark-salesforce-wave_2.10</artifactId>
    <version>1.0.0</version>
</dependency>
```


## Using with Spark shell
This package can be added to Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars com.springml:spark-salesforce-wave_2.10:1.0.0
```

## Features
This package can be used to create dataset in Salesforce Wave from [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html) to [Salesforce Wave](http://www.salesforce.com/in/analytics-cloud/overview/).
When uploadin the API requires following options:
* `username`: Salesforce Wave Username. This user should have privilege to upload datasets
* `password`: Salesforce Wave Password. Please append security token along with password.For example, if a user’s password is mypassword, and the security token is XXXXXXXXXX, the user must provide mypasswordXXXXXXXXXX
* `datasetName`: Name of the dataset to be created in Salesforce Wave

### Scala API
Spark 1.4+:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
// Using spark-csv package to load dataframes
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("your_csv_location")
df.
   write.
   format("com.springml.spark.salesforce").
   option("username", "your_salesforce_username").
   option("password", "your_salesforce_password_with_secutiry_token").
   option("datasetName", "your_dataset_name").
   save()
```


### Java API
Spark 1.4+:
```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load("your_csv_location");
df.write().format("com.springml.spark.salesforce")
		  .option("username", "your_salesforce_username")
		  .option("password", "your_salesforce_password_with_secutiry_token")
		  .option("datasetName", "your_dataset_name")
		  .save();
```


## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
