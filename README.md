Instructions for [Running Spark Interactively](./README_INTERACTIVE.MD)

Instructions for [Running Spark on Yarn](./README_YARN.MD)

---

[Watson Studio Query COS data](./WatsonStudio_QueryCOS.ipynb)

[Hive Query COS data](./README_HIVE.MD)

[Watson Studio ML Model on COS data](./README_WS_ML_MODEL.MD) coming soon ..

---
IMPORTANT:

- Data ingest pipelines using Spark Structured Streaming need to design how they will handle the 'small file anti-pattern' - https://evoeftimov.wordpress.com/2017/08/29/spark-streaming-parquet-and-too-many-small-output-files/
- Alternative approach avoiding the small files anti-pattern: https://community.hortonworks.com/questions/68154/small-files-problem-with-spark-streaming-for-inges.html (see the accepted answer)
