# Databricks notebook source
# Load the small reference files with COPY INTO (not Auto Loader), on purpose:
#   - a handful of small files that change rarely (an app list, a price card per capture)
#   - COPY INTO is idempotent: files it already loaded are skipped, so rerunning is safe
#   - it's plain SQL that runs on a SQL warehouse or serverless, with no stream or checkpoint to manage
# Auto Loader would also work; it pays off with many files or continuous arrival (the bronze sources).
# See docs/ingestion-choices.md.

# COMMAND ----------
dbutils.widgets.text("catalog", "aicost_dev")
CATALOG = dbutils.widgets.get("catalog")
REF = f"/Volumes/{CATALOG}/landing/files/reference"

# COMMAND ----------
display(spark.sql(f"""
COPY INTO {CATALOG}.ops.ref_apps
FROM (SELECT app_id, app_name, team_id, team_name, use_case, apim_subscription_id,
             _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
      FROM '{REF}/apps/')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
"""))

# COMMAND ----------
display(spark.sql(f"""
COPY INTO {CATALOG}.ops.price_card_history
FROM (SELECT model, token_type, meter_name, unit_of_measure,
             CAST(retail_price_usd AS DECIMAL(18,8)) AS retail_price_usd, region,
             CAST(effective_from AS DATE) AS effective_from, source, CAST(captured_on AS DATE) AS captured_on,
             _metadata.file_path AS _source_file, current_timestamp() AS _loaded_at
      FROM '{REF}/price_card/')
FILEFORMAT = CSV
PATTERN = 'price_card_*.csv'
FORMAT_OPTIONS ('header' = 'true')
"""))

# COMMAND ----------
# Note: COPY INTO skips a file it has already loaded even if the file was later modified. If the app
# list changes, write a new dated file (apps_YYYYMMDD.csv) or rerun once with
# COPY_OPTIONS ('force' = 'true') and dedupe. The price card already gets a new file per capture.
display(spark.table(f"{CATALOG}.ops.price_card_history").orderBy("captured_on", "model", "token_type"))
