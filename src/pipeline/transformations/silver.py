# Silver: cleaned, deduplicated, conformed tables.
#
#   ai_requests      streaming table   one row per gateway request (deduped within a watermark), liquid clustered
#   feedback         streaming table   latest vote per feedback_id (AUTO CDC, SCD type 1)
#   outcomes         materialized view one row per outcome, US dates parsed, re-uploads collapsed
#   outcome_requests materialized view outcomes exploded to one row per linked request
#   cost_runs        materialized view every export run with its manifest and completeness
#   cost_latest      materialized view FOCUS rows from the newest complete run per month
#   dim_app          materialized view app -> team -> use case (from the COPY INTO reference table)
#   dim_price        materialized view price card with effective date ranges
#
# Expectations: warn (keep + count), drop (remove + count), fail (stop the update). Each is chosen by
# what a bad row means for the numbers downstream; see the comment on each one.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import ai_transforms as T     # the pipeline root folder is on sys.path automatically

CATALOG = spark.conf.get("aicost.catalog")
B, S = f"{CATALOG}.bronze", f"{CATALOG}.silver"


# ---- requests ---------------------------------------------------------------------------------
@dp.temporary_view(comment="Gateway logs flattened to one row per log record (duplicates still present).")
def gateway_requests_flat():
    return T.clean_gateway_logs(spark.readStream.table(f"{B}.gateway_logs"))


@dp.table(
    name=f"{S}.ai_requests",
    comment="SYNTHETIC. One row per LLM request through the gateway. Deduped by request_id.",
    cluster_by=["event_date", "app_id"],     # dashboards filter by date and app; liquid clustering co-locates them
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
# drop: a negative token count is a logging bug; keeping it would understate cost
@dp.expect_or_drop("non_negative_tokens", "prompt_tokens >= 0 AND completion_tokens >= 0")
@dp.expect_or_drop("has_request_id", "request_id IS NOT NULL")
# warn: the row is still usable (prompt tokens recovered from the total); we just want the count
@dp.expect("prompt_tokens_logged_cleanly", "NOT prompt_tokens_recovered")
@dp.expect("known_app", "app_id LIKE 'app-%'")
def ai_requests():
    return T.dedupe_requests(spark.readStream.table("gateway_requests_flat"))


# ---- feedback (votes can change: keep the latest per feedback_id) -------------------------------
@dp.temporary_view()
@dp.expect_or_drop("valid_rating", "rating_value IN (1, -1)")
@dp.expect_or_drop("has_request", "request_id IS NOT NULL")
def feedback_clean():
    return T.clean_feedback(spark.readStream.table(f"{B}.feedback_events"))


dp.create_streaming_table(
    name=f"{S}.feedback",
    comment="SYNTHETIC. Latest rating per feedback_id. A changed vote overwrites the earlier one (SCD type 1).",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=f"{S}.feedback",
    source="feedback_clean",
    keys=["feedback_id"],
    sequence_by=F.col("event_time"),      # out-of-order events: the later vote wins
    stored_as_scd_type=1,
)


# ---- outcomes ---------------------------------------------------------------------------------
@dp.materialized_view(name=f"{S}.outcomes", comment="SYNTHETIC. One row per business outcome.")
@dp.expect_or_drop("parsable_date", "outcome_date IS NOT NULL")
@dp.expect("has_minutes", "minutes_saved IS NOT NULL")
def outcomes():
    return T.clean_outcomes(spark.read.table(f"{B}.outcomes"))


@dp.materialized_view(name=f"{S}.outcome_requests",
                      comment="SYNTHETIC. Outcomes exploded to one row per linked request_id.")
def outcome_requests():
    return T.explode_outcome_requests(spark.read.table(f"{S}.outcomes"))


# ---- cost exports -----------------------------------------------------------------------------
@dp.materialized_view(name=f"{S}.cost_runs",
                      comment="Every FOCUS export run: manifest metadata plus how many of its rows have landed.")
# warn: an incomplete run is normal while files are still arriving; it's just not used yet
@dp.expect("run_complete", "is_complete")
def cost_runs():
    return T.cost_runs(spark.read.table(f"{B}.cost_export_rows"), spark.read.table(f"{B}.cost_export_manifests"))


@dp.materialized_view(
    name=f"{S}.cost_latest",
    comment="SYNTHETIC. FOCUS cost rows from the newest complete export run of each billing month.",
    cluster_by=["charge_date"],
)
# fail: every cost figure downstream assumes USD; a second currency would silently corrupt sums
@dp.expect_or_fail("usd_only", "currency = 'USD'")
@dp.expect("has_charge_date", "charge_date IS NOT NULL")
def cost_latest():
    runs = spark.read.table(f"{S}.cost_runs")
    return T.build_cost_latest(spark.read.table(f"{B}.cost_export_rows"), T.latest_complete_runs(runs))


# ---- dimensions (reference tables are loaded by COPY INTO outside the pipeline) -----------------
@dp.materialized_view(name=f"{S}.dim_app", comment="App -> team -> use case.")
@dp.expect_or_fail("app_id_present", "app_id IS NOT NULL")   # fail: every gold join keys on it
def dim_app():
    return (spark.read.table(f"{CATALOG}.ops.ref_apps")
            .select(F.lower("app_id").alias("app_id"), "app_name", "team_id", "team_name", "use_case")
            .dropDuplicates(["app_id"]))


@dp.materialized_view(name=f"{S}.dim_price", comment="Azure retail price per model/token type with effective dates.")
@dp.expect_or_drop("positive_price", "retail_price_usd > 0")
def dim_price():
    return T.build_dim_price(spark.read.table(f"{CATALOG}.ops.price_card_history"))
