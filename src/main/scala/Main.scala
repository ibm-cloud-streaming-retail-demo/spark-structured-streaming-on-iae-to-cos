package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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

    val bucketName = conf.get("spark.s3_bucket")

    // arbitrary name for refering to the cos settings from this code
    val serviceName = "myservicename"

    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    sc.hadoopConfiguration.set("fs.s3a.access.key", conf.get("spark.s3_accesskey"))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", conf.get("spark.s3_secretkey"))
    sc.hadoopConfiguration.set("fs.s3a.endpoint", conf.get("spark.s3_endpoint"))

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
      option("kafka.bootstrap.servers", conf.get("spark.kafka_bootstrap_servers")).
      option("subscribe", "transactions_load").
      option("kafka.security.protocol", "SASL_SSL").
      option("kafka.sasl.mechanism", "PLAIN").
      option("kafka.ssl.protocol", "TLSv1.2").
      option("kafka.ssl.enabled.protocols", "TLSv1.2").
      load()

    val dataDf = df.selectExpr("CAST(value AS STRING) as json").
      select( from_json($"json", schema=schema).as("data")).
      select("data.*").
      filter($"InvoiceNo".isNotNull).
      withColumn("InvoiceDateString", from_unixtime($"InvoiceDate" / 1000)).
      withColumn("InvoiceYear", year(from_unixtime($"InvoiceDate"/ 1000))).
      withColumn("InvoiceMonth", month(from_unixtime($"InvoiceDate" / 1000))).
      withColumn("InvoiceDay", dayofmonth(from_unixtime($"InvoiceDate" / 1000))).
      withColumn("InvoiceHour", hour(from_unixtime($"InvoiceDate" / 1000)))

    val trigger_time_ms = conf.get("spark.trigger_time_ms").toInt

    dataDf.
      writeStream.
      format("parquet").
      trigger(Trigger.ProcessingTime(trigger_time_ms)).
      option("checkpointLocation", s"${s3Url}/checkpoint").
      option("path",               s"${s3Url}/data").
      partitionBy("InvoiceYear", "InvoiceMonth", "InvoiceDay", "InvoiceHour").
      start()

    //Wait for all streams to finish
    spark.streams.awaitAnyTermination()
  }
}
