# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanse `reqTradeSummery` → `dataplatform_dev.silver_dev`
# MAGIC
# MAGIC Reads the raw bronze table, cleanses it, and rebuilds the silver table.
# MAGIC
# MAGIC | Step | Rule |
# MAGIC |---|---|
# MAGIC | Trim | Leading/trailing whitespace stripped from every string column |
# MAGIC | Blank → NULL | A value that is only whitespace becomes `NULL`, not `""` |
# MAGIC | Deduplicate | Rows identical across all **market-data** columns collapse to one |
# MAGIC
# MAGIC **On duplicates:** every ingestion run stamps a fresh `_batch_id` and `_ingested_at`, so no
# MAGIC two rows are ever byte-identical across runs. Dedup therefore compares the market-data
# MAGIC columns only and ignores ingestion metadata — otherwise it would never match anything.
# MAGIC The **earliest** ingestion of each distinct state is kept, so genuine price movement is
# MAGIC preserved while repeated polls of an unchanged market collapse.
# MAGIC
# MAGIC The notebook is a full rebuild (`overwrite`), so it is idempotent — running it twice
# MAGIC produces the same silver table.

# COMMAND ----------

CATALOG      = "dataplatform_dev"
SOURCE_TABLE = f"{CATALOG}.bronze_dev.reqTradeSummery"
TARGET_TABLE = f"{CATALOG}.silver_dev.reqTradeSummery"

# Ingestion lineage - excluded from the duplicate comparison, carried through to silver.
META_COLS = ["_batch_id", "_ingested_at", "_source_url"]

# You asked for leading/trailing spaces only, so this is OFF by default.
# 8 rows currently have *internal* double spaces in `name` (e.g. "ABC  PLC").
# Set to True to also collapse runs of internal whitespace to a single space.
COLLAPSE_INNER_WHITESPACE = False

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

bronze = spark.table(SOURCE_TABLE)
rows_in = bronze.count()
print(f"Source : {SOURCE_TABLE}")
print(f"Rows in: {rows_in}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Trim whitespace
# MAGIC
# MAGIC String columns are discovered from the schema rather than hard-coded, so a new text field
# MAGIC added upstream is cleansed automatically instead of being silently skipped.

# COMMAND ----------

string_cols = [
    f.name for f in bronze.schema.fields
    if isinstance(f.dataType, StringType) and f.name not in META_COLS
]
print("String columns cleansed:", string_cols)

trimmed = bronze
for c in string_cols:
    cleaned = F.trim(F.col(c))
    if COLLAPSE_INNER_WHITESPACE:
        cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    # A field that held only whitespace trims to "" - store NULL instead, so
    # "missing" is represented one way rather than two.
    trimmed = trimmed.withColumn(c, F.when(cleaned == "", None).otherwise(cleaned))

# COMMAND ----------

# MAGIC %md ## 2. Deduplicate

# COMMAND ----------

business_cols = [c for c in trimmed.columns if c not in META_COLS]
print(f"Comparing {len(business_cols)} market-data columns (ignoring {META_COLS})")

# Rank each identical market-data state by ingestion time and keep the first sighting.
window = Window.partitionBy(*business_cols).orderBy(F.col("_ingested_at").asc())

deduped = (
    trimmed
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

# MAGIC %md ## 3. Write silver

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_dev")

silver = deduped.withColumn("_cleansed_at", F.current_timestamp())
silver.cache()
rows_out = silver.count()

(
    silver.write
    .format("delta")
    .mode("overwrite")               # full rebuild - idempotent, replayable from bronze
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Target      : {TARGET_TABLE}")
print(f"Rows in     : {rows_in}")
print(f"Rows out    : {rows_out}")
print(f"Duplicates  : {rows_in - rows_out} removed")

# COMMAND ----------

# MAGIC %md ## 4. Verify

# COMMAND ----------

# Whitespace should be gone entirely; any non-zero value here means a rule was missed.
checks = [
    F.sum(F.when(F.col(c) != F.trim(F.col(c)), 1).otherwise(0)).alias(f"{c}_untrimmed")
    for c in string_cols
]
display(spark.table(TARGET_TABLE).select(*checks))

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*)                  AS total_rows,
           COUNT(DISTINCT symbol)    AS distinct_symbols,
           COUNT(DISTINCT _batch_id) AS source_batches
    FROM {TARGET_TABLE}
"""))

