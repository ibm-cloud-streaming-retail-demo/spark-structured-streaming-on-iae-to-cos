First, configure IAE to work with COS [[docs](https://console.bluemix.net/docs/services/AnalyticsEngine/configure-COS-S3-object-storage.html#configuring-clusters-to-work-with-ibm-cos-s3-object-stores)], where the docs describe changing the core-site properties use:

```
fs.s3a.access.key=CHANGEME
fs.s3a.secret.key=CHANGEME
fs.s3a.endpoint=CHANGEME
```

Now use the hive ...

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
  TransactionID String
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3a://streamingdata/data/';
```

```
select * from invoiceitems limit 5;
```
