# Databricks notebook source
# MAGIC %md
# MAGIC # CSV from Google Drive → Unity Catalog
# MAGIC
# MAGIC Fully config-driven. The notebook holds logic only; every value it uses comes from one of
# MAGIC two JSON files.
# MAGIC
# MAGIC | File | Changes when | Holds |
# MAGIC |---|---|---|
# MAGIC | `connection_config.json` | a new source account or target schema appears | named `sources` and `destinations`, each defined once |
# MAGIC | `source_config.json` | a feed is added or changed | per-feed file selection, parsing, columns, primary key |
# MAGIC | `destination_config.json` | a feed is added or changed | per-feed table and write mode |
# MAGIC | `runtime_config.json` | rarely | Drive API details, parser fallbacks, metadata catalogue, writer options, naming rules |
# MAGIC
# MAGIC ### Connections are referenced, not repeated
# MAGIC
# MAGIC Many files can arrive from one Drive account and land in one catalog, so a feed names a
# MAGIC connection rather than restating it: `connection` in its source block points at
# MAGIC `connection_config.sources`, and in its destination block at `connection_config.destinations`,
# MAGIC which supplies `catalog` and `schema`. A feed may still override `catalog` or `schema`
# MAGIC directly.
# MAGIC
# MAGIC ### How a run changes the table
# MAGIC
# MAGIC A feed's `destination.load_type` states the intent; `runtime_config.load_types` defines what
# MAGIC each one does.
# MAGIC
# MAGIC | `load_type` | Effect | Re-running the same file |
# MAGIC |---|---|---|
# MAGIC | `full` | Delete and load — replaces the table's contents | table mirrors the file |
# MAGIC | `append` | Adds the rows to what is already there | duplicates them |
# MAGIC | `delta` | Upsert on the primary key | idempotent |
# MAGIC
# MAGIC `delta` treats the file as a set of changes: matched keys are updated, new keys inserted, and
# MAGIC rows already in the table but absent from the file are **left alone**. Use `full` when the
# MAGIC file is meant to be the whole picture. It requires `primary_key`, and key uniqueness must be
# MAGIC enforced or deduplicated — the notebook refuses otherwise, because duplicate keys make a
# MAGIC merge fail with an unhelpful error.
# MAGIC
# MAGIC ### Adding an ingestion
# MAGIC
# MAGIC Add a block under `feeds` in **both** `source_config.json` (folder, how to pick the file,
# MAGIC `columns`) and `destination_config.json` (`table`), using the same feed key. If it reuses an
# MAGIC existing source and destination, the inherited `connection` defaults already point at them
# MAGIC and nothing else changes.
# MAGIC
# MAGIC The notebook warns when a feed key exists in one file but not the other, and fails with a
# MAGIC message naming the missing side if the selected feed is half-defined.
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
# MAGIC `<table><sep><filename>` table instead.
# MAGIC
# MAGIC **Credentials are never stored in config.** The `connection.auth` block names a Databricks
# MAGIC secret scope and key; the service-account JSON itself lives only in that scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Load configuration
# MAGIC
# MAGIC The package list below is mirrored in `runtime_config.json` for reference, but a `%pip`
# MAGIC magic runs before any Python does, so it cannot be read from there. It is the one thing
# MAGIC that has to be edited in two places.

# COMMAND ----------

# MAGIC %pip install --quiet google-api-python-client google-auth
# MAGIC %restart_python

# COMMAND ----------

import json, os, io, re, uuid, fnmatch, copy

# The only path the notebook names itself - everything else is read from the file it points at.
dbutils.widgets.text("runtime_config_file", "runtime_config.json", "Runtime config file")


def resolve_path(name):
    return name if os.path.exists(name) else os.path.join(os.getcwd(), name)


def strip_docs(node):
    """Drop keys beginning with '_' - they document the file for whoever edits it."""
    if isinstance(node, dict):
        return {k: strip_docs(v) for k, v in node.items() if not k.startswith("_")}
    if isinstance(node, list):
        return [strip_docs(v) for v in node]
    return node


def load_json(name):
    with io.open(resolve_path(name), encoding="utf-8") as fh:
        return strip_docs(json.load(fh))


RUNTIME = load_json(dbutils.widgets.get("runtime_config_file").strip())

W = RUNTIME["widgets"]
dbutils.widgets.text("feed", W["feed_default"], "Feed (key under 'feeds')")
dbutils.widgets.text(
    "connection_config_file", W["connection_config_file_default"], "Connection config file"
)
dbutils.widgets.text("source_config_file", W["source_config_file_default"], "Source config file")
dbutils.widgets.text(
    "destination_config_file", W["destination_config_file_default"], "Destination config file"
)
dbutils.widgets.dropdown("dry_run", W["dry_run_default"], ["true", "false"], "Dry run (skip write)")

FEED = dbutils.widgets.get("feed").strip()
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"

DRIVE_CFG = RUNTIME["drive"]
PARSER_CFG = RUNTIME["parser"]
WRITER_CFG = RUNTIME["writer"]
NAMING_CFG = RUNTIME["table_naming"]
# Keyed by name for lookup. It is a list in the file because every metadata column name starts
# with an underscore, and underscore-prefixed *keys* are stripped as documentation on load.
META_CATALOGUE = {c["name"]: c for c in RUNTIME["metadata_columns"]}

# COMMAND ----------


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


source_path = dbutils.widgets.get("source_config_file").strip()
dest_path = dbutils.widgets.get("destination_config_file").strip()

src_cfg = load_json(source_path)
dst_cfg = load_json(dest_path)

src_feeds = src_cfg.get("feeds") or {}
dst_feeds = dst_cfg.get("feeds") or {}

# Splitting source from destination means the two feed lists can drift apart. Report that
# precisely, rather than letting a half-defined feed fail somewhere less obvious.
only_source = sorted(set(src_feeds) - set(dst_feeds))
only_dest = sorted(set(dst_feeds) - set(src_feeds))
if only_source or only_dest:
    print(
        "WARNING: feeds defined on one side only - source-only {}, destination-only {}".format(
            only_source or "none", only_dest or "none"
        )
    )

if FEED not in src_feeds:
    raise ValueError(
        "Feed {!r} is not in {}. Defined there: {}".format(
            FEED, os.path.basename(source_path), sorted(src_feeds) or "none"
        )
    )
if FEED not in dst_feeds:
    raise ValueError(
        "Feed {!r} is in {} but not in {}. Defined there: {}".format(
            FEED, os.path.basename(source_path), os.path.basename(dest_path),
            sorted(dst_feeds) or "none",
        )
    )

source = deep_merge(src_cfg.get("defaults") or {}, src_feeds[FEED])
destination = deep_merge(dst_cfg.get("defaults") or {}, dst_feeds[FEED])

# Resolve the named connections. Defined once in connection_config.json, so any number of feeds
# reading from the same Drive account or writing to the same catalog share one definition.
conn_path = dbutils.widgets.get("connection_config_file").strip()
conn_cfg = load_json(conn_path)


def named_connection(kind, name):
    pool = conn_cfg.get(kind) or {}
    if name not in pool:
        raise ValueError(
            "Feed {!r} names {} connection {!r}, which is not defined in {}. Available: {}".format(
                FEED, kind[:-1], name, os.path.basename(conn_path), sorted(pool) or "none"
            )
        )
    return pool[name]


source_connection_name = source.get("connection")
dest_connection_name = destination.get("connection")
if not source_connection_name:
    raise ValueError("Feed {!r} must name a source connection.".format(FEED))
if not dest_connection_name:
    raise ValueError("Feed {!r} must name a destination connection.".format(FEED))

connection = named_connection("sources", source_connection_name)
dest_connection = named_connection("destinations", dest_connection_name)

# catalog and schema come from the destination connection; a feed may still override either.
destination = deep_merge(
    {k: v for k, v in dest_connection.items() if k in ("catalog", "schema")}, destination
)
for key in ("catalog", "schema"):
    if not destination.get(key):
        raise ValueError(
            "Feed {!r}: neither destination connection {!r} nor the feed supplies {}.".format(
                FEED, dest_connection_name, key
            )
        )

# What the run does to the table: full (delete and load), append, or delta (upsert on the key).
LOAD_TYPE = destination.get("load_type") or WRITER_CFG["fallback_load_type"]
if LOAD_TYPE not in RUNTIME["load_types"]:
    raise ValueError(
        "Feed {!r} sets load_type {!r}, which is not defined in runtime_config.load_types. "
        "Available: {}".format(FEED, LOAD_TYPE, sorted(RUNTIME["load_types"]))
    )
LOAD_SPEC = RUNTIME["load_types"][LOAD_TYPE]

if LOAD_SPEC.get("requires_primary_key") and not (source.get("primary_key") or []):
    raise ValueError(
        "Feed {!r} uses load_type {!r}, which matches rows on the primary key, but "
        "source.primary_key is empty.".format(FEED, LOAD_TYPE)
    )

# A merge fails outright if one source row could match a target row more than once, so an
# upsert feed must guarantee key uniqueness rather than leaving duplicates to surface later.
if LOAD_SPEC.get("merge"):
    _rules = deep_merge(RUNTIME["validation_fallbacks"], source.get("validation"))
    if not (_rules["enforce_primary_key_unique"] or _rules["deduplicate_on_primary_key"]):
        raise ValueError(
            "Feed {!r} uses load_type {!r} but neither enforce_primary_key_unique nor "
            "deduplicate_on_primary_key is set. Duplicate keys would make the merge fail "
            "with an unhelpful error.".format(FEED, LOAD_TYPE)
        )

if not destination.get("table"):
    raise ValueError("Feed {!r} must set destination.table.".format(FEED))
if not source.get("columns"):
    raise ValueError("Feed {!r} must set source.columns.".format(FEED))

# Anything a feed leaves unset falls back to runtime_config rather than to a literal in the code.
fmt = deep_merge(PARSER_CFG["fallbacks"], source.get("format"))
hdr = deep_merge(PARSER_CFG["fallbacks"], source.get("header"))
rules = deep_merge(RUNTIME["validation_fallbacks"], source.get("validation"))
api = deep_merge(DRIVE_CFG["fallbacks"], connection.get("api"))

declared = source["columns"]
declared_names = [c["name"] for c in declared]

print("Feed        : {}".format(FEED))
print("Connections : source {!r} -> destination {!r}".format(
    source_connection_name, dest_connection_name
))
print(
    "Target      : {}.{}.{}".format(
        destination["catalog"], destination["schema"], destination["table"]
    )
)
print("Config      : {} | {} | {} | {}".format(
    os.path.basename(resolve_path(dbutils.widgets.get("runtime_config_file").strip())),
    os.path.basename(resolve_path(conn_path)),
    os.path.basename(resolve_path(source_path)),
    os.path.basename(resolve_path(dest_path)),
))

# COMMAND ----------

# MAGIC %md ## 1. Authenticate to Google Drive

# COMMAND ----------

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

auth = connection["auth"]
if auth.get("method") != DRIVE_CFG["auth_method"]:
    raise NotImplementedError(
        "Only {!r} auth is implemented, got {!r}".format(DRIVE_CFG["auth_method"], auth.get("method"))
    )

# The key is read from the secret scope. It is never printed and never written to disk.
sa_json = dbutils.secrets.get(scope=auth["secret_scope"], key=auth["secret_key"])

credentials = service_account.Credentials.from_service_account_info(
    json.loads(sa_json), scopes=api["scopes"]
)
drive = build(
    DRIVE_CFG["api_name"], DRIVE_CFG["api_version"], credentials=credentials, cache_discovery=False
)
print("Authenticated as:", auth.get("service_account_email", "(email not recorded in config)"))

FILE_FIELDS = ", ".join(DRIVE_CFG["file_fields"])
MIME = DRIVE_CFG["mime_types"]
QUERIES = DRIVE_CFG["queries"]
SHARED_DRIVE_ARGS = {
    "supportsAllDrives": DRIVE_CFG["support_all_drives"],
    "includeItemsFromAllDrives": DRIVE_CFG["include_items_from_all_drives"],
}

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
                    q=QUERIES["children_of_folder"].format(folder_id=current),
                    fields="nextPageToken, files({})".format(FILE_FIELDS),
                    pageSize=DRIVE_CFG["page_size"],
                    pageToken=page_token,
                    **SHARED_DRIVE_ARGS
                )
                .execute()
            )
            for entry in response.get("files", []):
                if entry["mimeType"] == MIME["folder"]:
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
        return [
            drive.files()
            .get(fileId=file_id, fields=FILE_FIELDS, supportsAllDrives=DRIVE_CFG["support_all_drives"])
            .execute()
        ]

    folder_id = (file_cfg.get("folder_id") or "").strip()
    match = file_cfg.get("match") or {}

    if match.get("enabled"):
        if not folder_id:
            raise ValueError("match mode needs source.file.folder_id.")
        pattern = match.get("pattern") or "*"
        candidates = [
            f
            for f in list_folder(folder_id, recursive=bool(match.get("recursive")))
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
        raise ValueError("source.file needs file_id, file_name, or match.enabled with a pattern.")

    query = QUERIES["by_name"].format(name=name)
    if folder_id:
        query += QUERIES["parent_clause"].format(folder_id=folder_id)

    matches = (
        drive.files()
        .list(q=query, fields="files({})".format(FILE_FIELDS), **SHARED_DRIVE_ARGS)
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


def download(meta):
    """Bytes for a Drive file. A native Google Sheet must be exported rather than downloaded."""
    if meta["mimeType"] == MIME["google_sheet"]:
        request = drive.files().export_media(fileId=meta["id"], mimeType=MIME["sheet_export_as"])
    else:
        request = drive.files().get_media(
            fileId=meta["id"], supportsAllDrives=DRIVE_CFG["support_all_drives"]
        )

    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request, chunksize=api["chunk_size_bytes"])
    done = False
    while not done:
        _, done = downloader.next_chunk(num_retries=api["max_retries"])
    return buffer.getvalue()


def parse(raw_bytes):
    """Parse to a DataFrame of strings. Type inference is deliberately avoided - see below."""
    read_args = dict(
        sep=fmt["delimiter"],
        encoding=fmt["encoding"],
        quotechar=fmt["quote_char"],
        dtype=str,  # no type inference
        keep_default_na=False,
        na_values=fmt["null_values"],
        skipinitialspace=bool(fmt["trim_whitespace"]),
    )
    if fmt.get("escape_char"):
        read_args["escapechar"] = fmt["escape_char"]

    if hdr["has_header"]:
        offset = 1 if PARSER_CFG["header_row_is_one_based"] else 0
        read_args["header"] = int(hdr["header_row_number"]) - offset
    else:
        read_args["header"] = None
        read_args["names"] = declared_names

    pdf = pd.read_csv(io.BytesIO(raw_bytes), **read_args)

    skip_after = int(hdr["skip_rows_after_header"])
    if skip_after:
        pdf = pdf.iloc[skip_after:]

    if fmt["trim_whitespace"]:
        for col in pdf.columns:
            if pdf[col].dtype == object:
                pdf[col] = pdf[col].str.strip()
    return pdf


def validate_columns(pdf, file_name):
    found = list(pdf.columns)
    missing = [c for c in declared_names if c not in found]
    extra = [c for c in found if c not in declared_names]

    if missing and rules["fail_on_missing_columns"]:
        raise ValueError("{}: declared columns absent from the CSV: {}".format(file_name, missing))
    if extra and rules["fail_on_extra_columns"]:
        raise ValueError("{}: undeclared columns present: {}".format(file_name, extra))
    if extra:
        print("    ignoring {} undeclared column(s): {}".format(len(extra), extra))

    if rules["enforce_column_list"]:
        pdf = pdf[[c for c in declared_names if c in found]]
    return pdf


def to_spark(pdf, file_name):
    """All-strings first, then an explicit cast per declared type."""
    string_schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    df = spark.createDataFrame(pdf.astype(object).where(pd.notnull(pdf), None), schema=string_schema)

    temporal = [t.lower() for t in PARSER_CFG["temporal_types"]]
    # Which Spark function parses which type is logic, not configuration - but keyed by name
    # rather than by position, so reordering temporal_types cannot silently swap them.
    converters = {"date": F.to_date, "timestamp": F.to_timestamp}

    for col in declared:
        name, dtype = col["name"], col["type"]
        if name not in df.columns:
            continue
        if dtype.lower() in temporal and col.get("format"):
            conv = converters.get(dtype.lower())
            if conv is None:
                raise ValueError(
                    "runtime_config lists {!r} as a temporal type but this notebook has no "
                    "parser for it. Known: {}.".format(dtype, sorted(converters))
                )
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

    if rules["fail_on_null_primary_key"]:
        null_pk = df.filter(" OR ".join("`{}` IS NULL".format(c) for c in pk)).count()
        if null_pk:
            raise ValueError("{}: {} row(s) have a NULL primary key {}.".format(file_name, null_pk, pk))

    total = df.count()
    dupes = total - df.select(*pk).distinct().count()
    if dupes:
        if rules["deduplicate_on_primary_key"]:
            df = df.dropDuplicates(pk)
            print("    removed {} duplicate row(s) on {}".format(dupes, pk))
        elif rules["enforce_primary_key_unique"]:
            raise ValueError(
                "{}: {} duplicate value(s) for primary key {}. Set "
                "validation.deduplicate_on_primary_key to true to drop them.".format(
                    file_name, dupes, pk
                )
            )
    return df


def selected_metadata():
    """The metadata columns this feed wants, as (name, spec) from the runtime catalogue."""
    if not destination.get("add_ingestion_metadata", True):
        return []
    chosen = []
    for name in destination.get("metadata_columns", []):
        spec = META_CATALOGUE.get(name)
        if spec is None:
            raise ValueError(
                "destination.metadata_columns names {!r}, which is not in "
                "runtime_config.metadata_columns.".format(name)
            )
        chosen.append((name, spec))
    return chosen


def add_metadata(df, meta, batch_id):
    """Attach the selected metadata columns, each built from its declared 'value' kind."""
    producers = {
        "batch_id": lambda: F.lit(batch_id),
        "current_timestamp": F.current_timestamp,
        "source_file_name": lambda: F.lit(meta["name"]),
        "source_file_id": lambda: F.lit(meta["id"]),
    }
    for name, spec in selected_metadata():
        kind = spec["value"]
        if kind not in producers:
            raise ValueError(
                "Metadata column {!r} declares value {!r}, which this notebook cannot "
                "produce. Known kinds: {}.".format(name, kind, sorted(producers))
            )
        df = df.withColumn(name, producers[kind]())
    return df


def check_table_schema(df, file_name):
    """Assert the built DataFrame matches the declared shape.

    With a single config file the schema has no second copy to drift out of step with, so this
    checks the DataFrame against source.columns plus the selected metadata rather than
    reconciling two files. An explicit destination.table_schema still overrides.
    """
    expected = destination.get("table_schema")
    if not expected:
        expected = list(declared) + [
            {"name": name, "type": spec["type"], "nullable": spec.get("nullable", False)}
            for name, spec in selected_metadata()
        ]

    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected_names = [c["name"] for c in expected]

    missing = [n for n in expected_names if n not in actual]
    extra = [n for n in actual if n not in expected_names]
    if missing or extra:
        raise ValueError(
            "{}: the built data does not match the declared schema. Missing {}, unexpected {}. "
            "Reconcile source.columns and destination.metadata_columns.".format(
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
    slug = re.sub(NAMING_CFG["non_alphanumeric_pattern"], "_", stem).strip("_")
    slug = re.sub(NAMING_CFG["collapse_underscores_pattern"], "_", slug)
    if NAMING_CFG["lowercase"]:
        slug = slug.lower()
    if not slug:
        raise ValueError("Cannot derive a table suffix from {!r}.".format(file_name))
    if slug[0].isdigit():
        slug = NAMING_CFG["digit_prefix"] + slug
    return "{}{}{}".format(table, NAMING_CFG["suffix_separator"], slug)


def write_plain(df, fqn, write_mode):
    """A straight Delta write - used for full and append loads, and a delta load's first run."""
    writer = df.write.format(WRITER_CFG["format"]).mode(write_mode)
    for key, value in (WRITER_CFG.get("options") or {}).items():
        writer = writer.option(key, value)
    if destination.get("partition_by"):
        writer = writer.partitionBy(*destination["partition_by"])
    for key, value in (destination.get("table_properties") or {}).items():
        writer = writer.option(key, value)
    writer.saveAsTable(fqn)


def merge_into(df, fqn):
    """Upsert on the primary key: matched rows updated, unmatched inserted.

    Rows already in the table but absent from this file are left untouched - the file is treated
    as a set of changes, not as the full picture. Use load_type 'full' for the latter.
    """
    merge_cfg = RUNTIME["merge"]
    pk = source["primary_key"]

    view = "{}{}".format(merge_cfg["temp_view_prefix"], uuid.uuid4().hex[:12])
    df.createOrReplaceTempView(view)
    try:
        on_clause = merge_cfg["on_separator"].join(
            merge_cfg["on_template"].format(column=c) for c in pk
        )
        before = spark.table(fqn).count()
        spark.sql(merge_cfg["sql"].format(target=fqn, source_view=view, on_clause=on_clause))
        after = spark.table(fqn).count()
    finally:
        spark.catalog.dropTempView(view)

    inserted = after - before
    updated = df.count() - inserted
    return "merged into (on {}: {} inserted, {} updated in)".format(
        ", ".join(pk), inserted, updated
    )


def ingest(meta):
    """Download, parse, validate and write one Drive file. Returns a summary dict."""
    print("  {}".format(meta["name"]))
    pdf = validate_columns(parse(download(meta)), meta["name"])
    df = check_primary_key(to_spark(pdf, meta["name"]), meta["name"])

    batch_id = str(uuid.uuid4())
    df = add_metadata(df, meta, batch_id)
    check_table_schema(df, meta["name"])

    fqn = "{}.{}.{}".format(
        destination["catalog"], destination["schema"], resolve_table(meta["name"])
    )
    rows = df.count()

    if DRY_RUN:
        print("    DRY RUN - would {} {:,} rows to {}".format(LOAD_TYPE, rows, fqn))
        return {
            "file": meta["name"],
            "table": fqn,
            "rows": rows,
            "batch_id": None,
            "load_type": LOAD_TYPE,
            "written": False,
        }

    if destination.get("create_if_not_exists", True):
        spark.sql(
            WRITER_CFG["create_schema_sql"].format(
                catalog=destination["catalog"], schema=destination["schema"]
            )
        )

    existed = spark.catalog.tableExists(fqn)

    if LOAD_SPEC["merge"] and existed:
        action = merge_into(df, fqn)
    else:
        # A merge into a table that does not exist yet has nothing to match on, so the first
        # run writes the rows plainly and later runs upsert.
        write_plain(df, fqn, LOAD_SPEC["write_mode"])
        if LOAD_SPEC["merge"]:
            action = "created (first load, nothing to merge into)"
        else:
            action = ("replaced contents of" if LOAD_SPEC["write_mode"] == "overwrite" else "appended to") \
                if existed else "created"

    print("    {} {} ({:,} rows, load_type={})".format(action, fqn, rows, LOAD_TYPE))
    return {
        "file": meta["name"],
        "table": fqn,
        "rows": rows,
        "batch_id": batch_id,
        "load_type": LOAD_TYPE,
        "written": True,
    }


# COMMAND ----------

# MAGIC %md ## 4. Run

# COMMAND ----------

results = [ingest(meta) for meta in files_to_ingest]

print()
print("{} file(s), {:,} row(s) total".format(len(results), sum(r["rows"] for r in results)))
display(spark.createDataFrame(pd.DataFrame(results)))

# COMMAND ----------

# MAGIC %md ## 5. Verify

# COMMAND ----------


def metadata_named(value_kind):
    """Find the selected metadata column that carries a given kind, for the verify query."""
    for name, spec in selected_metadata():
        if spec["value"] == value_kind:
            return name
    return None


batch_col = metadata_named("batch_id")
file_col = metadata_named("source_file_name")
time_col = metadata_named("current_timestamp")

if not DRY_RUN and all([batch_col, file_col, time_col]):
    for fqn in sorted({r["table"] for r in results}):
        print(fqn)
        display(
            spark.sql(
                RUNTIME["verify_query"].format(
                    table=fqn, batch_col=batch_col, file_col=file_col, time_col=time_col
                )
            )
        )
elif not DRY_RUN:
    print("Skipped: the verify query needs batch_id, source_file_name and current_timestamp "
          "metadata columns, and this feed does not select all three.")
