# spark-structured-streaming-on-iae-to-cos

```

cat << EOF > jaas.conf
KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    serviceName="kafka"
    username="CHANGEME"
    password="CHANGEME";
};
EOF

cat << EOF > start_spark.sh
spark-shell --master local[1] \
       --files jaas.conf \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
       --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --num-executors 1  --executor-cores 1 
EOF

./start_spark.sh

///////////////////////////////

val accessKey = "CHANGEME"
val secretKey = "CHANGEME"
val bucketName = "CHANGEME"
val endpoint = "s3.eu-geo.objectstorage.service.networklayer.com" // probably CHANGEME

// arbitrary name for refering to the cos settings from this code
val serviceName = "myservicename"

// stocator isn't working
//sc.hadoopConfiguration.set(s"fs.cos.${serviceName}.access.key", accessKey)
//sc.hadoopConfiguration.set(s"fs.cos.${serviceName}.secret.key", secretKey)
//sc.hadoopConfiguration.set(s"fs.cos.${serviceName}.endpoint", endpoint)

sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
sc.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)

// stocator isn't working
//val s3Url = s"cos://${bucketName}.${serviceName}/"

val s3Url = s"s3a://${bucketName}/"

// use a demo record to infer the schema
val jsonstr = """{"InvoiceNo": 5384972, "StockCode": 21329, "Description": "DINOSAURS  WRITING SET ", "Quantity": 1, "InvoiceDate": 1534812240000, "UnitPrice": 1.65, "CustomerID": 14684, "Country": "United Kingdom", "LineNo": 14, "InvoiceTime": "00:44:00", "StoreID": 0, "TransactionID": "5384972140180821"}"""

val jsonSchema = spark.read.json(Seq(jsonstr).toDS).schema

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
                    select( from_json($"json", schema=jsonSchema).as("data")).
                    select("data.*")

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

dataDf.
  writeStream.
  format("json").
  trigger(Trigger.ProcessingTime(30.seconds)).
  option("checkpointLocation", s"${s3Url}/checkpoint").
  option("path",               s"${s3Url}/data").
  start()


//////////

// TODO: https://github.com/polomarcus/Spark-Structured-Streaming-Examples/blob/master/src/main/scala/cassandra/StreamSinkProvider/CassandraSink.scala


```
