# Databricks notebook source
# MAGIC %md
# MAGIC # CSV from Google Drive → Unity Catalog
# MAGIC
# MAGIC Config-driven ingestion. Nothing about a particular feed is hard-coded here — a single
# MAGIC `ingestion_config.json` describes everything, and the `feed` widget picks which one to run.
# MAGIC
# MAGIC That file has three parts:
# MAGIC
# MAGIC | Section | Holds |
# MAGIC |---|---|
# MAGIC | `connection` | Drive credentials (by secret reference) and API behaviour |
# MAGIC | `defaults` | Format, header and validation rules shared by every feed |
# MAGIC | `feeds` | One block per feed, overriding only what differs |
# MAGIC
# MAGIC ### Adding a folder
# MAGIC
# MAGIC Add a block under `feeds` with its `folder_id`, how to pick the file, the `columns`, and the
# MAGIC destination `table`. Nothing in this notebook changes.
# MAGIC
# MAGIC Merging is recursive, so a feed overriding `format.quote_char` inherits the rest of
# MAGIC `format` untouched.
# MAGIC
# MAGIC ### Selecting the source file
# MAGIC
# MAGIC Three ways, in precedence order:
# MAGIC
# MAGIC 1. **`file_id`** — an exact Drive id. Unambiguous; always wins.
# MAGIC 2. **`match`** — a glob pattern within `folder_id`, taking either the newest match or all
# MAGIC    of them. Use this for a drop-folder where filenames vary.
# MAGIC 3. **`file_name`** — a literal name, optionally scoped to `folder_id`.
# MAGIC
# MAGIC With `strategy: "all"`, every match is ingested in one run. They all append to the feed's
# MAGIC `destination.table` unless `append_filename_suffix` is set, which gives each file its own
# MAGIC `<table>_<filename>` table instead.
# MAGIC
# MAGIC **Credentials are never stored in config.** The `connection.auth` block names a Databricks
# MAGIC secret scope and key; the service-account JSON itself lives only in that scope.

# COMMAND ----------

# MAGIC %pip install --quiet google-api-python-client google-auth
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("feed", "dataset1", "Feed (key under 'feeds')")
dbutils.widgets.text("config_file", "ingestion_config.json", "Config file")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (skip write)")

FEED = dbutils.widgets.get("feed").strip()
CONFIG_FILE = dbutils.widgets.get("config_file").strip()
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"

# COMMAND ----------

import json, os, io, re, uuid, fnmatch, copy


def strip_docs(node):
    """Drop keys beginning with '_' - they document the file for whoever edits it."""
    if isinstance(node, dict):
        return {k: strip_docs(v) for k, v in node.items() if not k.startswith("_")}
    if isinstance(node, list):
        return [strip_docs(v) for v in node]
    return node


def deep_merge(base, override):
    """Recursive merge so a feed can override one nested key and inherit the rest.

    Lists replace rather than concatenate: a feed's 'columns' is its whole column list, and
    appending to an inherited list is never what you want here.
    """
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


path = CONFIG_FILE if os.path.exists(CONFIG_FILE) else os.path.join(os.getcwd(), CONFIG_FILE)
with io.open(path, encoding="utf-8") as fh:
    raw = strip_docs(json.load(fh))

feeds = raw.get("feeds") or {}
if FEED not in feeds:
    raise ValueError(
        "Unknown feed {!r}. Defined feeds: {}".format(FEED, sorted(feeds) or "none")
    )

defaults = raw.get("defaults") or {}
feed_cfg = feeds[FEED]

connection = raw["connection"]
source = deep_merge(defaults.get("source") or {}, feed_cfg.get("source"))
destination = deep_merge(defaults.get("destination") or {}, feed_cfg.get("destination"))

if not destination.get("table"):
    raise ValueError("Feed {!r} must set destination.table.".format(FEED))
if not source.get("columns"):
    raise ValueError("Feed {!r} must set source.columns.".format(FEED))

print("Config : {}".format(os.path.abspath(path)))
print("Feed   : {}".format(FEED))
print("Target : {}.{}.{}".format(destination["catalog"], destination["schema"], destination["table"]))

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

FILE_FIELDS = "id, name, mimeType, size, modifiedTime"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select the source file(s)
# MAGIC
# MAGIC Drive's `in parents` matches direct children only, so subfolders are walked explicitly
# MAGIC when `recursive` is set. Pattern matching happens here rather than in the Drive query
# MAGIC because the API supports only `contains`, not real globbing.

# COMMAND ----------


def list_folder(folder_id, recursive=False):
    """Every non-trashed file under folder_id, descending into subfolders if asked."""
    collected, pending, seen = [], [folder_id], set()

    while pending:
        current = pending.pop()
        if current in seen:  # a folder can be reachable by more than one path
            continue
        seen.add(current)

        page_token = None
        while True:
            response = (
                drive.files()
                .list(
                    q="'{}' in parents and trashed = false".format(current),
                    fields="nextPageToken, files({})".format(FILE_FIELDS),
                    pageSize=1000,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            for entry in response.get("files", []):
                if entry["mimeType"] == "application/vnd.google-apps.folder":
                    if recursive:
                        pending.append(entry["id"])
                else:
                    collected.append(entry)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

    return collected


def resolve_files(file_cfg):
    """Return the list of Drive files to ingest, in the precedence order documented above."""
    file_id = (file_cfg.get("file_id") or "").strip()
    if file_id:
        meta = drive.files().get(fileId=file_id, fields=FILE_FIELDS, supportsAllDrives=True).execute()
        return [meta]

    folder_id = (file_cfg.get("folder_id") or "").strip()
    match = file_cfg.get("match") or {}

    if match.get("enabled"):
        if not folder_id:
            raise ValueError("match mode needs source_config.file.folder_id.")
        pattern = match.get("pattern") or "*"
        candidates = [
            f for f in list_folder(folder_id, recursive=bool(match.get("recursive")))
            if fnmatch.fnmatch(f["name"], pattern)
        ]
        if not candidates:
            raise FileNotFoundError(
                "No file in folder {} matched {!r}. Confirm the folder is shared with {}.".format(
                    folder_id, pattern, auth.get("service_account_email")
                )
            )

        cutoff = match.get("min_modified_time")
        if cutoff:
            candidates = [f for f in candidates if f.get("modifiedTime", "") >= cutoff]
            if not candidates:
                raise FileNotFoundError("No match modified on or after {}.".format(cutoff))

        candidates.sort(key=lambda f: f.get("modifiedTime", ""), reverse=True)

        strategy = (match.get("strategy") or "newest").lower()
        if strategy == "newest":
            return candidates[:1]
        if strategy == "all":
            return candidates
        raise ValueError("Unknown match.strategy {!r} - expected 'newest' or 'all'.".format(strategy))

    name = (file_cfg.get("file_name") or "").strip()
    if not name:
        raise ValueError("source_config.file needs file_id, file_name, or match.enabled with a pattern.")

    query = "name = '{}' and trashed = false".format(name)
    if folder_id:
        query += " and '{}' in parents".format(folder_id)

    matches = (
        drive.files()
        .list(
            q=query,
            fields="files({})".format(FILE_FIELDS),
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
            "{} files named {!r} matched - set file_id, or scope it with folder_id.".format(
                len(matches), name
            )
        )
    return matches


files_to_ingest = resolve_files(source["file"])

print("{} file(s) selected:".format(len(files_to_ingest)))
for f in files_to_ingest:
    print("  {:40} {:>12}  modified {}".format(f["name"], f.get("size", "n/a"), f.get("modifiedTime")))

# COMMAND ----------

# MAGIC %md ## 3. Per-file ingestion

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

fmt = source["format"]
hdr = source["header"]
declared = source["columns"]
declared_names = [c["name"] for c in declared]
rules = source.get("validation", {})


def download(meta):
    """Bytes for a Drive file. A native Google Sheet must be exported rather than downloaded."""
    if meta["mimeType"] == "application/vnd.google-apps.spreadsheet":
        request = drive.files().export_media(fileId=meta["id"], mimeType="text/csv")
    else:
        request = drive.files().get_media(fileId=meta["id"], supportsAllDrives=True)

    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(
        buffer, request, chunksize=connection["api"].get("chunk_size_bytes", 10 * 1024 * 1024)
    )
    done = False
    while not done:
        _, done = downloader.next_chunk(num_retries=connection["api"].get("max_retries", 3))
    return buffer.getvalue()


def parse(raw_bytes):
    """Parse to a DataFrame of strings. Type inference is deliberately avoided - see below."""
    read_args = dict(
        sep=fmt.get("delimiter", ","),
        encoding=fmt.get("encoding", "utf-8"),
        quotechar=fmt.get("quote_char", '"'),
        dtype=str,
        keep_default_na=False,
        na_values=fmt.get("null_values", [""]),
        skipinitialspace=bool(fmt.get("trim_whitespace", True)),
    )
    if fmt.get("escape_char"):
        read_args["escapechar"] = fmt["escape_char"]

    if hdr.get("has_header", True):
        read_args["header"] = int(hdr.get("header_row_number", 1)) - 1  # config is 1-based
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
    return pdf


def validate_columns(pdf, file_name):
    found = list(pdf.columns)
    missing = [c for c in declared_names if c not in found]
    extra = [c for c in found if c not in declared_names]

    if missing and rules.get("fail_on_missing_columns", True):
        raise ValueError("{}: declared columns absent from the CSV: {}".format(file_name, missing))
    if extra and rules.get("fail_on_extra_columns", False):
        raise ValueError("{}: undeclared columns present: {}".format(file_name, extra))
    if extra:
        print("    ignoring {} undeclared column(s): {}".format(len(extra), extra))

    if rules.get("enforce_column_list", True):
        pdf = pdf[[c for c in declared_names if c in found]]
    return pdf


def to_spark(pdf, file_name):
    """All-strings first, then an explicit cast per declared type."""
    string_schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    df = spark.createDataFrame(pdf.astype(object).where(pd.notnull(pdf), None), schema=string_schema)

    for col in declared:
        name, dtype = col["name"], col["type"]
        if name not in df.columns:
            continue
        if dtype.lower() in ("date", "timestamp") and col.get("format"):
            conv = F.to_date if dtype.lower() == "date" else F.to_timestamp
            df = df.withColumn(name, conv(F.col(name), col["format"]))
        else:
            df = df.withColumn(name, F.col(name).cast(dtype))

    # A failed cast yields NULL rather than raising, so a not-nullable column that gained
    # NULLs is reported here instead of silently corrupting the table.
    for col in declared:
        if not col.get("nullable", True) and col["name"] in df.columns:
            bad = df.filter(F.col(col["name"]).isNull()).count()
            if bad:
                raise ValueError(
                    "{}: column {!r} is declared not-nullable but has {} NULL(s) after casting "
                    "to {}.".format(file_name, col["name"], bad, col["type"])
                )
    return df


def check_primary_key(df, file_name):
    pk = source.get("primary_key") or []
    if not pk:
        return df

    if rules.get("fail_on_null_primary_key", True):
        null_pk = df.filter(" OR ".join("`{}` IS NULL".format(c) for c in pk)).count()
        if null_pk:
            raise ValueError("{}: {} row(s) have a NULL primary key {}.".format(file_name, null_pk, pk))

    total = df.count()
    dupes = total - df.select(*pk).distinct().count()
    if dupes:
        if rules.get("deduplicate_on_primary_key", False):
            df = df.dropDuplicates(pk)
            print("    removed {} duplicate row(s) on {}".format(dupes, pk))
        elif rules.get("enforce_primary_key_unique", True):
            raise ValueError(
                "{}: {} duplicate value(s) for primary key {}. Set "
                "validation.deduplicate_on_primary_key to true to drop them.".format(
                    file_name, dupes, pk
                )
            )
    return df


def check_table_schema(df, file_name):
    """Assert the DataFrame matches destination.table_schema, when one is declared.

    source_config and destination_config each describe part of the shape, so they can drift
    apart. Catching that here means a mismatch fails the run rather than quietly creating a
    table shaped differently from what the config claims.
    """
    expected = destination.get("table_schema")
    if not expected:
        # Derive it: source columns, then metadata. Because there is only one config file, the
        # schema has no second copy to drift out of step with - this now checks the built
        # DataFrame against the declared columns rather than reconciling two files.
        metadata_types = {
            "_batch_id": "string",
            "_ingested_at": "timestamp",
            "_source_file_name": "string",
            "_source_file_id": "string",
        }
        expected = list(declared)
        if destination.get("add_ingestion_metadata", True):
            expected = expected + [
                {"name": c, "type": metadata_types.get(c, "string"), "nullable": False}
                for c in destination.get("metadata_columns", [])
            ]

    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected_names = [c["name"] for c in expected]

    missing = [n for n in expected_names if n not in actual]
    extra = [n for n in actual if n not in expected_names]
    if missing or extra:
        raise ValueError(
            "{}: table_schema does not match the data. Missing {}, unexpected {}. "
            "Reconcile destination_config.table_schema with source_config.columns.".format(
                file_name, missing or "none", extra or "none"
            )
        )

    wrong = [
        "{} declared {} but built as {}".format(c["name"], c["type"], actual[c["name"]])
        for c in expected
        if actual[c["name"]].lower() != c["type"].lower()
    ]
    if wrong:
        raise ValueError("{}: type mismatch - {}".format(file_name, "; ".join(wrong)))


def resolve_table(file_name):
    """The configured table name, optionally suffixed with the filename for per-drop tables."""
    table = destination["table"]
    if not destination.get("append_filename_suffix"):
        return table

    stem = os.path.splitext(file_name)[0]
    slug = re.sub(r"_+", "_", re.sub(r"[^0-9a-zA-Z]+", "_", stem).strip("_")).lower()
    if not slug:
        raise ValueError("Cannot derive a table suffix from {!r}.".format(file_name))
    return "{}_{}".format(table, slug)


def ingest(meta):
    """Download, parse, validate and write one Drive file. Returns a summary dict."""
    print("  {}".format(meta["name"]))
    raw = download(meta)
    pdf = parse(raw)
    pdf = validate_columns(pdf, meta["name"])
    df = to_spark(pdf, meta["name"])
    df = check_primary_key(df, meta["name"])

    batch_id = str(uuid.uuid4())
    if destination.get("add_ingestion_metadata", True):
        df = (
            df.withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file_name", F.lit(meta["name"]))
            .withColumn("_source_file_id", F.lit(meta["id"]))
        )

    check_table_schema(df, meta["name"])

    table = resolve_table(meta["name"])
    fqn = "{}.{}.{}".format(destination["catalog"], destination["schema"], table)
    rows = df.count()

    if DRY_RUN:
        print("    DRY RUN - would write {:,} rows to {}".format(rows, fqn))
        return {"file": meta["name"], "table": fqn, "rows": rows, "batch_id": None, "written": False}

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

    print("    {} {} ({:,} rows)".format("appended to" if existed else "created", fqn, rows))
    return {"file": meta["name"], "table": fqn, "rows": rows, "batch_id": batch_id, "written": True}


# COMMAND ----------

# MAGIC %md ## 4. Run

# COMMAND ----------

results = []
for meta in files_to_ingest:
    results.append(ingest(meta))

print()
print("{} file(s), {:,} row(s) total".format(len(results), sum(r["rows"] for r in results)))
display(spark.createDataFrame(pd.DataFrame(results)))

# COMMAND ----------

# MAGIC %md ## 5. Verify

# COMMAND ----------

if not DRY_RUN:
    for fqn in sorted({r["table"] for r in results}):
        print(fqn)
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
