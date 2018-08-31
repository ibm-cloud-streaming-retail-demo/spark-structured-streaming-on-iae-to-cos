First, configure IAE to work with COS [[docs](https://console.bluemix.net/docs/services/AnalyticsEngine/configure-COS-S3-object-storage.html#configuring-clusters-to-work-with-ibm-cos-s3-object-stores)], where the docs describe changing the core-site properties use:

```
fs.s3a.access.key=CHANGEME
fs.s3a.secret.key=CHANGEME
fs.s3a.endpoint=CHANGEME
```

Now use the hive (replace CHANGEME with your bucket name) ...

```
CREATE EXTERNAL TABLE invoiceitems (
  InvoiceNo INT,
  StockCode INT,
  Description STRING,
  Quantity INT,
  InvoiceDate BIGINT,
  UnitPrice DOUBLE,
  CustomerID INT,
  Country STRING,
  LineNo INT,
  InvoiceTime STRING,
  StoreID INT,
  TransactionID STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3a://CHANGEME/data/'; 
```

```
select * from invoiceitems where invoiceno is not null limit 5;
```

To check load status without having to login to an ssh session each time, use [ssh-copy-id](https://www.ssh.com/ssh/copy-id) to copy your ssh public key to the cluster and then run:

```
ssh clsadmin@yourcluster "hive -e 'select count(*) from invoiceitems'"
```

Count the number of transactions in the last hour (TODO invoicedate and unix_timestamp should both be UTC)

```
ssh clsadmin@yourcluster "hive -e 'select count(*) from invoiceitems where invoicedate > (unix_timestamp() - 3600)*1000'"
```

NOTE:

- invoicedate is in milliseconds whereas unix_timestamp is seconds, so we needed to convert the unix_timestamp to milliseconds (*1000)
- the [Yarn Cluster job](./README_YARN.MD) by default saves a new batch of data every 10 minutes (--conf spark.trigger_time_ms=600000)
