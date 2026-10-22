# Databricks notebook source
# Generate SYNTHETIC landing files: gateway logs, feedback, outcomes, FOCUS cost exports, reference files.
# Plays the outside systems so the pipeline has something realistic to ingest. See src/aicost/simulator.py.
#
# Runs on job compute (plain Python + pandas, no Spark needed). Typical use:
#   backfill   through = 2026-10-21            (first run)
#   daily      through = "" and advance_days = 1 (the job does this; one simulated day per run)
#   lab        volume_multiplier = 5, skew_app_share = 0.8 (troubleshooting lab, later phase)

# COMMAND ----------
dbutils.widgets.text("catalog", "aicost_dev")
dbutils.widgets.text("through", "", "Simulate through (YYYY-MM-DD); blank = advance_days")
dbutils.widgets.text("advance_days", "1")
dbutils.widgets.text("requests_per_day", "3000")
dbutils.widgets.text("volume_multiplier", "1.0")
dbutils.widgets.text("skew_app_share", "")
dbutils.widgets.dropdown("reset", "false", ["false", "true"])

import os, sys
sys.path.append(os.path.abspath(".."))
from aicost import settings as S, simulator

w = dbutils.widgets.get
cfg = simulator.Config(
    root=S.landing_root(w("catalog")),
    through=w("through").strip() or None,
    advance_days=int(w("advance_days") or 1),
    requests_per_day=int(w("requests_per_day")),
    volume_multiplier=float(w("volume_multiplier")),
    skew_app_share=float(w("skew_app_share")) if w("skew_app_share").strip() else None,
    reset=w("reset") == "true",
)
print(cfg)

# COMMAND ----------
state = simulator.run(cfg)
print(f"\nsimulated through {state['last_day']}")
