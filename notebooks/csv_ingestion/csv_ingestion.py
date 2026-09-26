# Databricks notebook source
# MAGIC %md
# MAGIC # CSV from Google Drive → `dataplatform_dev.bronze_dev.ef_csv_<filename>`
# MAGIC
# MAGIC Config-driven ingestion. Nothing about a particular feed is hard-coded here — the three
# MAGIC JSON files under `config/` describe the connection, the source file and the destination.
# MAGIC
# MAGIC | File | Holds |
# MAGIC |---|---|
# MAGIC | `connection_config.json` | Drive credentials (by secret reference) and API behaviour |
# MAGIC | `source_config.json` | File identity, delimiter, header rules, column list, primary key |
# MAGIC | `destination_config.json` | Catalog, schema, table naming, write mode, table schema |
# MAGIC
# MAGIC **Credentials are never stored in config.** `connection_config.json` names a Databricks
# MAGIC secret scope and key; the service-account JSON itself lives only in that scope.

# COMMAND ----------

# MAGIC %pip install --quiet google-api-python-client google-auth
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("config_dir", "config", "Config directory")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (skip write)")

CONFIG_DIR = dbutils.widgets.get("config_dir")
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"

# COMMAND ----------

import json, os, io, re, uuid


def load_config(name):
    path = os.path.join(CONFIG_DIR, name)
    if not os.path.exists(path):  # fall back to a path relative to the notebook
        path = os.path.join(os.getcwd(), CONFIG_DIR, name)
    with io.open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)
    # Keys beginning with "_" are documentation for whoever edits the file, not settings.
    return {k: v for k, v in cfg.items() if not k.startswith("_")}


connection = load_config("connection_config.json")
source = load_config("source_config.json")
destination = load_config("destination_config.json")

print("Configs loaded from:", os.path.abspath(CONFIG_DIR))

# COMMAND ----------

# MAGIC %md ## 1. Authenticate to Google Drive

# COMMAND ----------

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

auth = connection["auth"]
if auth.get("method") != "service_account":
    raise NotImplementedError(
        "Only service_account auth is implemented, got {!r}".format(auth.get("method"))
    )

# The key is read from the secret scope. It is never printed and never written to disk.
sa_json = dbutils.secrets.get(scope=auth["secret_scope"], key=auth["secret_key"])

credentials = service_account.Credentials.from_service_account_info(
    json.loads(sa_json),
    scopes=connection["api"]["scopes"],
)
drive = build("drive", "v3", credentials=credentials, cache_discovery=False)
print("Authenticated as:", auth.get("service_account_email", "(email not recorded in config)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolve and download the file
# MAGIC
# MAGIC A name lookup that matches more than one file fails rather than guessing, so a feed can
# MAGIC never silently switch to a different file that happens to share its name.

# COMMAND ----------

file_cfg = source["file"]
file_id = (file_cfg.get("file_id") or "").strip()

if not file_id:
    name = (file_cfg.get("file_name") or "").strip()
    folder_id = (file_cfg.get("folder_id") or "").strip()
    if not name:
        raise ValueError("source_config.file needs either file_id or file_name.")

    query = "name = '{}' and trashed = false".format(name)
    if folder_id:
        query += " and '{}' in parents".format(folder_id)

    matches = (
        drive.files()
        .list(
            q=query,
            fields="files(id, name, mimeType, modifiedTime, size)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
        .get("files", [])
    )

    if not matches:
        raise FileNotFoundError(
            "No Drive file named {!r}{}. Confirm it is shared with {}.".format(
                name,
                " in folder " + folder_id if folder_id else "",
                auth.get("service_account_email"),
            )
        )
    if len(matches) > 1:
        raise ValueError(
            "{} files named {!r} matched - set file_id to disambiguate.".format(len(matches), name)
        )
    file_id = matches[0]["id"]

meta = (
    drive.files()
    .get(fileId=file_id, fields="id, name, mimeType, size, modifiedTime", supportsAllDrives=True)
    .execute()
)
print("File   : {}  ({} bytes)".format(meta["name"], meta.get("size", "n/a")))
print("Type   : {}".format(meta["mimeType"]))
print("Changed: {}".format(meta.get("modifiedTime")))

# COMMAND ----------

# A native Google Sheet has no bytes to download and must be exported as CSV instead.
if meta["mimeType"] == "application/vnd.google-apps.spreadsheet":
    request = drive.files().export_media(fileId=file_id, mimeType="text/csv")
else:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)

buffer = io.BytesIO()
downloader = MediaIoBaseDownload(
    buffer, request, chunksize=connection["api"].get("chunk_size_bytes", 10 * 1024 * 1024)
)
done = False
while not done:
    status, done = downloader.next_chunk(num_retries=connection["api"].get("max_retries", 3))

raw_bytes = buffer.getvalue()
print("Downloaded {:,} bytes".format(len(raw_bytes)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse
# MAGIC
# MAGIC Every column is read as text and cast explicitly afterwards. Letting the parser infer types
# MAGIC means the schema depends on whichever rows happened to arrive that day — a column that is
# MAGIC all digits one week and blank the next would change type and break the append.

# COMMAND ----------

import pandas as pd

fmt = source["format"]
hdr = source["header"]
declared = source["columns"]
declared_names = [c["name"] for c in declared]

read_args = dict(
    sep=fmt.get("delimiter", ","),
    encoding=fmt.get("encoding", "utf-8"),
    quotechar=fmt.get("quote_char", '"'),
    dtype=str,  # no type inference
    keep_default_na=False,
    na_values=fmt.get("null_values", [""]),
    skipinitialspace=bool(fmt.get("trim_whitespace", True)),
)
if fmt.get("escape_char"):
    read_args["escapechar"] = fmt["escape_char"]

if hdr.get("has_header", True):
    # header_row_number is 1-based in config; pandas counts from 0.
    read_args["header"] = int(hdr.get("header_row_number", 1)) - 1
else:
    read_args["header"] = None
    read_args["names"] = declared_names

pdf = pd.read_csv(io.BytesIO(raw_bytes), **read_args)

skip_after = int(hdr.get("skip_rows_after_header", 0))
if skip_after:
    pdf = pdf.iloc[skip_after:]

if fmt.get("trim_whitespace", True):
    for col in pdf.columns:
        if pdf[col].dtype == object:
            pdf[col] = pdf[col].str.strip()

print("Parsed {:,} rows x {} columns".format(len(pdf), len(pdf.columns)))
print("Columns found:", list(pdf.columns))

# COMMAND ----------

# MAGIC %md ## 4. Validate against the declared column list

# COMMAND ----------

rules = source.get("validation", {})
found = list(pdf.columns)
missing = [c for c in declared_names if c not in found]
extra = [c for c in found if c not in declared_names]

if missing and rules.get("fail_on_missing_columns", True):
    raise ValueError("Columns declared in source_config but absent from the CSV: {}".format(missing))
if extra and rules.get("fail_on_extra_columns", False):
    raise ValueError("Columns present in the CSV but not declared: {}".format(extra))
if extra:
    print("Ignoring {} undeclared column(s): {}".format(len(extra), extra))

if rules.get("enforce_column_list", True):
    pdf = pdf[[c for c in declared_names if c in found]]  # declared order, declared columns only

# COMMAND ----------

# MAGIC %md ## 5. Cast to the declared types

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Build as all-strings first so the cast is explicit and visible, then convert.
string_schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
pdf_obj = pdf.astype(object).where(pd.notnull(pdf), None)
df = spark.createDataFrame(pdf_obj, schema=string_schema)

for col in declared:
    name, dtype = col["name"], col["type"]
    if name not in df.columns:
        continue
    if dtype.lower() in ("date", "timestamp") and col.get("format"):
        conv = F.to_date if dtype.lower() == "date" else F.to_timestamp
        df = df.withColumn(name, conv(F.col(name), col["format"]))
    else:
        df = df.withColumn(name, F.col(name).cast(dtype))  # DDL strings incl. decimal(p,s)

# A failed cast yields NULL rather than an error, so a not-nullable column that gained
# NULLs is reported here instead of silently corrupting the table.
for col in declared:
    if not col.get("nullable", True) and col["name"] in df.columns:
        bad = df.filter(F.col(col["name"]).isNull()).count()
        if bad:
            raise ValueError(
                "Column {!r} is declared not-nullable but has {} NULL(s) after casting to {} "
                "- check the source data or the declared type.".format(col["name"], bad, col["type"])
            )

# COMMAND ----------

# MAGIC %md ## 6. Primary key checks

# COMMAND ----------

pk = source.get("primary_key") or []
if pk:
    if rules.get("fail_on_null_primary_key", True):
        null_pk = df.filter(" OR ".join("`{}` IS NULL".format(c) for c in pk)).count()
        if null_pk:
            raise ValueError("{} row(s) have a NULL primary key {}.".format(null_pk, pk))

    total = df.count()
    distinct = df.select(*pk).distinct().count()
    dupes = total - distinct
    if dupes:
        if rules.get("deduplicate_on_primary_key", False):
            df = df.dropDuplicates(pk)
            print("Removed {} duplicate row(s) on {}".format(dupes, pk))
        elif rules.get("enforce_primary_key_unique", True):
            raise ValueError(
                "{} duplicate value(s) for primary key {}. Set "
                "validation.deduplicate_on_primary_key to true to drop them instead.".format(dupes, pk)
            )
    else:
        print("Primary key {} is unique across {:,} rows".format(pk, total))

# COMMAND ----------

# MAGIC %md ## 7. Ingestion metadata

# COMMAND ----------

batch_id = str(uuid.uuid4())

if destination.get("add_ingestion_metadata", True):
    df = (
        df.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file_name", F.lit(meta["name"]))
        .withColumn("_source_file_id", F.lit(file_id))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resolve the table name
# MAGIC
# MAGIC `ef_csv_<filename>`: extension dropped, non-alphanumeric folded to underscore, lowercased.

# COMMAND ----------


def table_name_from_file(file_name, prefix):
    stem = os.path.splitext(file_name)[0]
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", stem).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    if not slug:
        raise ValueError("Cannot derive a table name from {!r}.".format(file_name))
    if slug[0].isdigit():
        slug = "t_" + slug  # an identifier cannot start with a digit
    return prefix + slug


table = destination.get("table_name_override") or table_name_from_file(
    meta["name"], destination.get("table_name_prefix", "ef_csv_")
)
fqn = "{}.{}.{}".format(destination["catalog"], destination["schema"], table)

print("Target table:", fqn)
display(df)

# COMMAND ----------

# MAGIC %md ## 9. Write

# COMMAND ----------

if DRY_RUN:
    print("DRY RUN - would write {:,} rows to {}. Nothing was written.".format(df.count(), fqn))
else:
    if destination.get("create_if_not_exists", True):
        spark.sql(
            "CREATE SCHEMA IF NOT EXISTS {}.{}".format(destination["catalog"], destination["schema"])
        )

    existed = spark.catalog.tableExists(fqn)
    writer = (
        df.write.format("delta")
        .mode(destination.get("write_mode", "append"))
        .option("mergeSchema", "false")
    )

    if destination.get("partition_by"):
        writer = writer.partitionBy(*destination["partition_by"])
    for key, value in (destination.get("table_properties") or {}).items():
        writer = writer.option(key, value)

    writer.saveAsTable(fqn)

    print(("Appended to " if existed else "Created ") + fqn)
    print("Rows written : {:,}".format(df.count()))
    print("Batch id     : {}".format(batch_id))

# COMMAND ----------

# MAGIC %md ## 10. Verify

# COMMAND ----------

if not DRY_RUN:
    display(
        spark.sql(
            """
        SELECT _batch_id,
               _source_file_name,
               MIN(_ingested_at) AS ingested_at,
               COUNT(*)          AS row_count
        FROM {}
        GROUP BY _batch_id, _source_file_name
        ORDER BY ingested_at DESC
    """.format(
                fqn
            )
        )
    )
