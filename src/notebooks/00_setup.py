# Databricks notebook source
# One-time setup per environment: catalog, schemas, the landing volume, and the reference tables
# that COPY INTO loads. Safe to rerun (everything is IF NOT EXISTS).
#
# Landing volume, two ways:
#   external  (the real target) - the volume points at your ADLS Gen2 container through an external
#             location. Files stay in your storage account; dropping the volume doesn't delete them.
#             Needs a storage credential (Azure access connector) created once by a metastore admin.
#   managed   (fallback to start today) - Databricks stores the files in the catalog's managed storage.
#             Same /Volumes path, so nothing else changes when you switch later.

# COMMAND ----------
dbutils.widgets.text("catalog", "aicost_dev")
dbutils.widgets.dropdown("landing_mode", "managed", ["managed", "external"])
dbutils.widgets.text("storage_url", "", "abfss://landing@<account>.dfs.core.windows.net/aicost_dev")
dbutils.widgets.text("storage_credential", "", "Storage credential name (external mode)")
dbutils.widgets.text("catalog_managed_location", "", "Optional MANAGED LOCATION for the catalog")

CATALOG = dbutils.widgets.get("catalog").strip()
MODE = dbutils.widgets.get("landing_mode")
URL = dbutils.widgets.get("storage_url").strip().rstrip("/")
CRED = dbutils.widgets.get("storage_credential").strip()
MANAGED_LOC = dbutils.widgets.get("catalog_managed_location").strip()

import os, sys
sys.path.append(os.path.abspath(".."))          # src/, so `aicost` imports from the Git folder
from aicost import settings as S

# COMMAND ----------
# Catalog. On a new Azure workspace with no metastore-level storage you must give it a managed location.
loc = f" MANAGED LOCATION '{MANAGED_LOC}'" if MANAGED_LOC else ""
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}{loc} COMMENT 'AI cost & value pipeline (synthetic data)'")
for schema, comment in S.SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"schema  {CATALOG}.{schema}")

# COMMAND ----------
# Landing volume
if MODE == "external":
    if not (URL and CRED):
        raise ValueError("external mode needs storage_url and storage_credential")
    # The external location governs who can read/write that path; the volume is how pipelines see it.
    loc_name = f"{CATALOG}_landing"
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS {loc_name} URL '{URL}'
                  WITH (STORAGE CREDENTIAL `{CRED}`) COMMENT 'Landing zone for the AI cost pipeline'""")
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {CATALOG}.landing.{S.LANDING_VOLUME} LOCATION '{URL}'")
else:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.{S.LANDING_VOLUME}")
print(f"volume  {S.landing_root(CATALOG)}  ({MODE})")
display(spark.sql(f"DESCRIBE VOLUME {CATALOG}.landing.{S.LANDING_VOLUME}"))

# COMMAND ----------
# Reference tables. COPY INTO needs the target to exist; explicit types so a bad file can't change them.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.ops.ref_apps (
  app_id STRING, app_name STRING, team_id STRING, team_name STRING, use_case STRING,
  apim_subscription_id STRING, _source_file STRING, _loaded_at TIMESTAMP)
  COMMENT 'SYNTHETIC. App -> team -> use case. Loaded by COPY INTO from landing/reference/apps.'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.ops.price_card_history (
  model STRING, token_type STRING, meter_name STRING, unit_of_measure STRING, retail_price_usd DECIMAL(18,8),
  region STRING, effective_from DATE, source STRING, captured_on DATE, _source_file STRING, _loaded_at TIMESTAMP)
  COMMENT 'Azure retail prices for the models in use, one file per capture. Loaded by COPY INTO.'""")
print("tables  ops.ref_apps, ops.price_card_history")
