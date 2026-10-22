# Bronze: append-only streaming tables, one per landing source, loaded with Auto Loader.
#
# Every bronze row keeps where it came from (_metadata.file_path / file_modification_time) and when it
# was ingested, so silver can dedupe, pick the newest run, and trace any number back to a file.
#
# Why Auto Loader for all of these (and not COPY INTO): these folders get new files every hour/day and
# the pipeline should only read what's new. Auto Loader tracks processed files in the checkpoint,
# infers and evolves the schema, and rescues values that don't fit. COPY INTO is used for the small
# reference files instead (see notebooks/03_load_reference.py and docs/ingestion-choices.md).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("aicost.catalog")
LANDING = spark.conf.get("aicost.landing_root")            # /Volumes/<catalog>/landing/files
FILE_EVENTS = spark.conf.get("aicost.use_file_events", "false")   # "true" once the external location has file events on


def autoloader(fmt: str, path: str, **options):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # new field -> restart once, then included
              .option("cloudFiles.useManagedFileEvents", FILE_EVENTS))      # false = directory listing
    for k, v in options.items():
        reader = reader.option(k, v)
    return (reader.load(path)
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("ingest_time", F.current_timestamp()))


@dp.table(
    name=f"{CATALOG}.bronze.gateway_logs",
    comment="SYNTHETIC. APIM LLM gateway logs as archived by a diagnostic setting (hourly PT1H.json). "
            "Append-only; duplicates and corrupt values are kept here and handled in silver.",
    table_properties={"quality": "bronze"},
)
def gateway_logs():
    return autoloader(
        "json", f"{LANDING}/gateway_logs/",
        # Token counts must be integers. A value like "n/a" doesn't fit, so instead of failing it is
        # set to NULL and the raw value is kept in _rescued_data (on by default with inference).
        **{"cloudFiles.schemaHints": "properties.PromptTokens BIGINT, properties.CompletionTokens BIGINT, "
                                     "properties.TotalTokens BIGINT, properties.ResponseCode INT, time STRING",
           # The archive path has y=/m=/d=/h=/m= folders. "m" appears twice (month and minute), so
           # partition-column inference is switched off; the record's own timestamp is used instead.
           "cloudFiles.partitionColumns": ""},
    )


@dp.table(
    name=f"{CATALOG}.bronze.feedback_events",
    comment="SYNTHETIC. Thumbs up/down events from the apps (nested JSON). A user can change a vote: "
            "the same feedback_id arrives again with a later event_time.",
    table_properties={"quality": "bronze"},
)
def feedback_events():
    return autoloader("json", f"{LANDING}/feedback/")


@dp.table(
    name=f"{CATALOG}.bronze.outcomes",
    comment="SYNTHETIC. Business outcomes uploaded by app owners (CSV). Header casing differs by owner.",
    table_properties={"quality": "bronze"},
)
def outcomes():
    return autoloader(
        "csv", f"{LANDING}/outcomes/",
        header="true",
        # Keep text as text in bronze; silver parses US dates and numbers explicitly.
        **{"cloudFiles.inferColumnTypes": "false",
           # Finance's file says App_ID, others say app_id. Case-insensitive matching puts both in
           # one column instead of rescuing Finance's values.
           "readerCaseSensitive": "false"},
    )


@dp.table(
    name=f"{CATALOG}.bronze.cost_export_rows",
    comment="SYNTHETIC. Every row of every FOCUS export run (CSV+Gzip), all runs kept. Each daily run "
            "restates the whole month, so silver keeps only the newest complete run per month.",
    table_properties={"quality": "bronze"},
)
def cost_export_rows():
    return (autoloader("csv", f"{LANDING}/cost_exports/", header="true", pathGlobFilter="*.csv.gz",
                       **{"cloudFiles.inferColumnTypes": "false"})
            .withColumn("run_id", F.regexp_extract("source_file", r"/([0-9a-f-]{36})/[^/]+$", 1)))


@dp.table(
    name=f"{CATALOG}.bronze.cost_export_manifests",
    comment="SYNTHETIC. manifest.json for each export run: run id, submitted time, expected row count.",
    table_properties={"quality": "bronze"},
)
def cost_export_manifests():
    return autoloader("json", f"{LANDING}/cost_exports/", multiLine="true", pathGlobFilter="manifest.json")
