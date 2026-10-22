# Gold: business-facing materialized views for the dashboard.
#
#   ai_spend_daily            what AI costs, per day/app/team/model (gateway estimate: tokens x retail price)
#   ai_adoption               who uses it: approximate daily active users and requests
#   ai_efficiency             how well it runs: latency (avg, p95), tokens per request, throttling, caching
#   ai_quality                do people like it: feedback rate and thumbs-up share (requests LEFT JOIN feedback)
#   ai_unit_economics         is it worth it: monthly cost vs outcomes and estimated value
#   cost_reconciliation_daily does the gateway explain the Azure bill? (gap, shadow usage, unpriced models)
#
# All are materialized views: batch results that the pipeline keeps up to date, recomputing
# incrementally when it can. A plain view (no stored result) sits on top of the reconciliation for
# alerting, created outside the pipeline in notebooks/04_gold_views.py.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities import ai_transforms as T

CATALOG = spark.conf.get("aicost.catalog")
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"
HOURLY_RATE = float(spark.conf.get("aicost.loaded_hourly_rate", "55"))      # an assumption, shown in the output
GATEWAY_RESOURCE = spark.conf.get("aicost.gateway_resource", "aoai-zeal-prod-eus2")
RECON_TOLERANCE = spark.conf.get("aicost.recon_tolerance_pct", "0.05")


@dp.temporary_view(comment="Each request with the retail price in effect that day (broadcast join to the price card).")
def requests_priced():
    return T.price_requests(spark.read.table(f"{S}.ai_requests"), spark.read.table(f"{S}.dim_price"))


@dp.materialized_view(name=f"{G}.ai_spend_daily", comment="SYNTHETIC. Estimated LLM spend by day, app, team and model.")
# warn: a model with no price is real spend we can't estimate; keep the rows so the gap is visible
@dp.expect("model_in_price_card", "model_in_price_card")
def ai_spend_daily():
    return T.ai_spend_daily(spark.read.table("requests_priced"), spark.read.table(f"{S}.dim_app"))


@dp.materialized_view(name=f"{G}.ai_adoption", comment="SYNTHETIC. Approximate daily active users and requests per app.")
def ai_adoption():
    return T.ai_adoption(spark.read.table(f"{S}.ai_requests"), spark.read.table(f"{S}.dim_app"))


@dp.materialized_view(name=f"{G}.ai_efficiency", comment="SYNTHETIC. Latency, token use, throttling and caching per app/model/day.")
def ai_efficiency():
    return T.ai_efficiency(spark.read.table(f"{S}.ai_requests"))


@dp.materialized_view(name=f"{G}.ai_quality", comment="SYNTHETIC. Feedback rate and satisfaction per app/model/day.")
def ai_quality():
    return T.ai_quality(spark.read.table(f"{S}.ai_requests"), spark.read.table(f"{S}.feedback"))


@dp.materialized_view(
    name=f"{G}.ai_unit_economics",
    comment=f"SYNTHETIC. Monthly cost vs outcomes per app. Value assumes a loaded rate of ${HOURLY_RATE:.0f}/hour.",
)
def ai_unit_economics():
    return T.ai_unit_economics(spark.read.table("requests_priced"), spark.read.table(f"{S}.outcome_requests"),
                               spark.read.table(f"{S}.dim_app"), HOURLY_RATE)


@dp.materialized_view(
    name=f"{G}.cost_reconciliation_daily",
    comment="SYNTHETIC. Azure-billed LLM cost vs gateway-estimated cost per day, with the gap explained.",
)
# warn: a gap is a finding, not bad data. The count of days outside tolerance shows up in the event log,
# and the alert view in 04_gold_views lists them for the job's condition task.
@dp.expect("gap_within_tolerance", f"gap_pct IS NULL OR abs(gap_pct) <= {RECON_TOLERANCE}")
def cost_reconciliation_daily():
    return T.cost_reconciliation_daily(spark.read.table(f"{S}.cost_latest"), spark.read.table("requests_priced"),
                                       GATEWAY_RESOURCE)
