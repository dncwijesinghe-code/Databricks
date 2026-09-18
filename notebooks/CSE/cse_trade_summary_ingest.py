# Databricks notebook source
# MAGIC %md
# MAGIC # CSE Trade Summary → `dataplatform_dev.bronze_dev.reqTradeSummery`
# MAGIC
# MAGIC Pulls the Colombo Stock Exchange trade summary and **appends** it to the bronze table.
# MAGIC
# MAGIC - The endpoint requires **POST** (a GET returns `405`).
# MAGIC - The table is created automatically on the first run.
# MAGIC - Every run appends a new batch; nothing is overwritten or deduplicated.
# MAGIC - Each row carries `_batch_id` / `_ingested_at` so runs stay distinguishable.

# COMMAND ----------

CATALOG  = "dataplatform_dev"
SCHEMA   = "bronze_dev"
TABLE    = "reqTradeSummery"
FQN      = f"{CATALOG}.{SCHEMA}.{TABLE}"

API_URL  = "https://www.cse.lk/api/tradeSummary"
JSON_KEY = "reqTradeSummery"   # top-level key in the API response
TIMEOUT  = 60

# COMMAND ----------

# MAGIC %md ## 1. Fetch

# COMMAND ----------

import requests, uuid

response = requests.post(
    API_URL,
    timeout=TIMEOUT,
    headers={"User-Agent": "databricks-bronze-ingest/1.0", "Accept": "application/json"},
)
response.raise_for_status()
response.encoding = "utf-8"          # names contain non-ASCII characters

records = response.json().get(JSON_KEY, [])
if not records:
    raise ValueError(f"No records under '{JSON_KEY}' - aborting so an empty batch is not appended.")

batch_id = str(uuid.uuid4())
print(f"Fetched {len(records)} records | batch_id = {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Shape into a table
# MAGIC
# MAGIC The schema is declared explicitly rather than inferred. Inference would read types from
# MAGIC whatever arrives in a single run, so a field that happened to be all-null that day could
# MAGIC land as a different type and break the append.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType,
)

schema = StructType([
    StructField("id",                  LongType(),    True),
    StructField("name",                StringType(),  True),
    StructField("symbol",              StringType(),  True),
    StructField("logoUrl",             StringType(),  True),   # nullable in source
    StructField("quantity",            LongType(),    True),
    StructField("percentageChange",    DoubleType(),  True),
    StructField("change",              DoubleType(),  True),
    StructField("price",               DoubleType(),  True),
    StructField("previousClose",       DoubleType(),  True),
    StructField("high",                DoubleType(),  True),
    StructField("low",                 DoubleType(),  True),
    StructField("lastTradedTime",      LongType(),    True),   # epoch milliseconds
    StructField("issueDate",           StringType(),  True),   # e.g. "01/JAN/1984", nullable
    StructField("turnover",            DoubleType(),  True),
    StructField("sharevolume",         LongType(),    True),
    StructField("tradevolume",         LongType(),    True),
    StructField("marketCap",           DoubleType(),  True),
    StructField("marketCapPercentage", DoubleType(),  True),
    StructField("open",                DoubleType(),  True),
    StructField("closingPrice",        DoubleType(),  True),
    StructField("crossingVolume",      LongType(),    True),
    StructField("crossingTradeVol",    LongType(),    True),
    StructField("status",              IntegerType(), True),
])

# Project each record to a fixed field order. Missing keys become None instead of
# shifting every following column.
field_names = [f.name for f in schema.fields]
rows = [tuple(rec.get(name) for name in field_names) for rec in records]

df = spark.createDataFrame(rows, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Derived and lineage columns
# MAGIC
# MAGIC `issueDate` and `lastTradedTime` are kept exactly as delivered — bronze keeps raw fidelity —
# MAGIC with a usable timestamp added alongside rather than replacing the original.

# COMMAND ----------

df_out = (
    df
    .withColumn("lastTradedTimestamp", (F.col("lastTradedTime") / 1000).cast("timestamp"))
    .withColumn("_batch_id",    F.lit(batch_id))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_url",  F.lit(API_URL))
)

display(df_out)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Append
# MAGIC
# MAGIC `mode("append").saveAsTable(...)` creates the Delta table on the first run and appends on
# MAGIC every run after, so no separate `CREATE TABLE` branch is needed.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

table_existed = spark.catalog.tableExists(FQN)

(
    df_out.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "false")   # fail loudly if the API changes shape
    .saveAsTable(FQN)
)

print(("Appended to existing table " if table_existed else "Created table ") + FQN)
print(f"Rows written this run: {df_out.count()}")

# COMMAND ----------

# MAGIC %md ## 4. Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT _batch_id,
           MIN(_ingested_at) AS ingested_at,
           COUNT(*)          AS row_count
    FROM {FQN}
    GROUP BY _batch_id
    ORDER BY ingested_at DESC
"""))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {FQN}"))

