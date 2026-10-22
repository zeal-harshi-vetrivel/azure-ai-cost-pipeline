# Databricks notebook source
# Plain views on top of the pipeline's gold tables. A view stores no data: it's a saved query that runs
# when read, so it is always current with the materialized view underneath and costs nothing to keep.
# (Contrast: materialized views store results and are refreshed by the pipeline; streaming tables only
# ever append new input.) These live outside the pipeline so analysts can change them without a
# pipeline update.

# COMMAND ----------
dbutils.widgets.text("catalog", "aicost_dev")
dbutils.widgets.text("recon_tolerance_pct", "0.05")
CATALOG = dbutils.widgets.get("catalog")
TOL = float(dbutils.widgets.get("recon_tolerance_pct"))
G = f"{CATALOG}.gold"

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_cost_reconciliation_alerts
COMMENT 'Days in the last 30 where the gateway resource bill and the gateway estimate differ by more than {TOL:.0%}.'
AS SELECT event_date, billed_gateway_resource_usd, gateway_est_cost_usd, gap_usd, gap_pct,
          unpriced_requests, off_gateway_cost_usd,
          CASE WHEN unpriced_requests > 0 THEN 'model missing from the price card'
               WHEN gap_usd < 0 THEN 'estimate above the bill (discounts the logs cannot see?)'
               ELSE 'investigate' END AS likely_cause
   FROM {G}.cost_reconciliation_daily
   WHERE abs(gap_pct) > {TOL} AND event_date >= current_date() - INTERVAL 30 DAYS
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_ai_spend_by_team_month
COMMENT 'Monthly estimated LLM spend per team, for the FinOps dashboard.'
AS SELECT trunc(event_date, 'MM') AS month, team_name,
          sum(est_cost_usd) AS est_cost_usd, sum(requests) AS requests,
          sum(unpriced_tokens) AS unpriced_tokens
   FROM {G}.ai_spend_daily GROUP BY ALL
""")

# COMMAND ----------
alerts = spark.table(f"{G}.v_cost_reconciliation_alerts")
n = alerts.count()
display(alerts.orderBy("event_date"))
# Hand the count to later job tasks (an If/else condition task can branch on it).
dbutils.jobs.taskValues.set(key="recon_alert_days", value=n)
print(f"{n} day(s) outside tolerance")
