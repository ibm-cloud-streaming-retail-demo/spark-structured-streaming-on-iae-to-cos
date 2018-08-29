package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object Main {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Structured Streaming from Message Hub to COS")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val accessKey = "CHANGEME"
    val secretKey = "CHANGEME"
    val bucketName = "CHANGEME"
    val endpoint = "s3.eu-geo.objectstorage.service.networklayer.com" // probably CHANGEME

    // arbitrary name for refering to the cos settings from this code
    val serviceName = "myservicename"

    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)

    val s3Url = s"s3a://${bucketName}/"

    val schema = new StructType()
      .add("InvoiceNo", LongType)
      .add("StockCode", LongType)
      .add("Description", StringType)
      .add("Quantity", ShortType)
      .add("InvoiceDate", LongType)
      .add("UnitPrice", DoubleType)
      .add("CustomerID", IntegerType)
      .add("Country", StringType)
      .add("LineNo", ShortType)
      .add("InvoiceTime", StringType)
      .add("StoreID", ShortType)
      .add("TransactionID", StringType)

    val df = spark.readStream.
      format("kafka").
      // v- probably CHANGEME -v
      option("kafka.bootstrap.servers", "kafka03-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka04-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka01-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka02-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka05-prod01.messagehub.services.eu-de.bluemix.net:9093").
      option("subscribe", "transactions_load").
      option("kafka.security.protocol", "SASL_SSL").
      option("kafka.sasl.mechanism", "PLAIN").
      option("kafka.ssl.protocol", "TLSv1.2").
      option("kafka.ssl.enabled.protocols", "TLSv1.2").
      load()

    val dataDf = df.selectExpr("CAST(value AS STRING) as json").
      select( from_json($"json", schema=schema).as("data")).
      select("data.*")

    dataDf.
      writeStream.
      format("json").
      trigger(Trigger.ProcessingTime(30.seconds)).
      option("checkpointLocation", s"${s3Url}/checkpoint").
      option("path",               s"${s3Url}/data").
      start()

    //Wait for all streams to finish
    spark.streams.awaitAnyTermination()
  }
}