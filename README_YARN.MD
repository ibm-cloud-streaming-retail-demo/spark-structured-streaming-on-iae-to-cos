```
sbt package

scp target/scala-2.11/spark-structured-streaming-on-iae-to-cos_2.11-1.0.jar clsadmin@yourcluster:./

cat << EOF > jaas.conf
KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    serviceName="kafka"
    username="CHANGEME"
    password="CHANGEME";
};
EOF


spark-submit --class main.Main \
       --master yarn --deploy-mode cluster \
       --files jaas.conf \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
       --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" \
       --num-executors 1 \
       --executor-cores 1 \
       spark-structured-streaming-on-iae-to-cos_2.11-1.0.jar
```
