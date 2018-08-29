Build the scala class and package to jar and cooy it to IAE!

```
sbt package
scp target/scala-2.11/spark-structured-streaming-on-iae-to-cos_2.11-1.0.jar clsadmin@yourcluster:./
```

Create a jaas.conf for spark to authenticate to Message Hub (paste this script into your IAE SSH session and edit the file to chnage the CHANGEME values):

```
cat << EOF > jaas.conf
KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    serviceName="kafka"
    username="CHANGEME"
    password="CHANGEME";
};
EOF
```

Create a script with some variables needed by spark (paste this script into your IAE SSH session and edit the file to chnage the CHANGEME values):

```
cat << EOF > config_vars.sh

# S3 #
S3_ACCESSKEY=CHANGEME
S3_SECRETKEY=CHANGEME
S3_PRIVATE_ENDPOINT=CHANGEME # E.g. s3.eu-geo.objectstorage.service.networklayer.com
S3_BUCKET=CHANGEME

# KAFKA #
KAFKA_BOOTSTRAP_SERVERS=HOST1:PORT1,HOST2:PORT2,... # E.g. kafka03-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka04-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka01-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka02-prod01.messagehub.services.eu-de.bluemix.net:9093,kafka05-prod01.messagehub.services.eu-de.bluemix.net:9093

EOF
```

Create a script to start spark interactively via yarn (paste this script into your IAE SSH session):

```
cat << 'EOF' > start_yarn_client.sh
source config_vars.sh

spark-submit --class main.Main \
       --master yarn \
       --deploy-mode client \
       --files jaas.conf \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
       --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --conf spark.s3_accesskey=$S3_ACCESSKEY \
       --conf spark.s3_secretkey=$S3_SECRETKEY \
       --conf spark.s3_endpoint=$S3_PRIVATE_ENDPOINT \
       --conf spark.s3_bucket=$S3_BUCKET \
       --conf spark.kafka_bootstrap_servers=$KAFKA_BOOTSTRAP_SERVERS \
       --num-executors 1 \
       --executor-cores 1 \
       spark-structured-streaming-on-iae-to-cos_2.11-1.0.jar
EOF
chmod +x start_yarn_client.sh
```

Run the interactive script, you should see spark saving to COS and output sent to the terminal:

```
bash -x start_yarn_client.sh
```

Create a script to start spark in the background via yarn (paste this script into your IAE SSH session):

```
cat << 'EOF' > start_yarn_cluster.sh
source config_vars.sh

spark-submit --class main.Main \
       --master yarn \
       --deploy-mode cluster \
       --files jaas.conf \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
       --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --conf spark.s3_accesskey=$S3_ACCESSKEY \
       --conf spark.s3_secretkey=$S3_SECRETKEY \
       --conf spark.s3_endpoint=$S3_PRIVATE_ENDPOINT \
       --conf spark.s3_bucket=$S3_BUCKET \
       --conf spark.kafka_bootstrap_servers=$KAFKA_BOOTSTRAP_SERVERS \
       --conf spark.yarn.submit.waitAppCompletion=false \
       --num-executors 1 \
       --executor-cores 1 \
       spark-structured-streaming-on-iae-to-cos_2.11-1.0.jar
EOF
chmod +x start_yarn_cluster.sh
```

Run the script. It will terminate after submitting the job to yarn.

```
bash -x start_yarn_cluster.sh
```

Verify that the spark job is running on yarn:

```
yarn application -list
```
