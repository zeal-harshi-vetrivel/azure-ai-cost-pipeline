"""Shared fixtures: a local Spark session and one simulated landing folder for all tests.

Bronze is emulated with plain batch reads of the landing files (Auto Loader only runs on Databricks),
adding the same lineage columns the pipeline adds.
"""

import json
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "src" / "pipeline")]

from aicost import simulator  # noqa: E402

THROUGH = "2026-10-20"


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.ansi.enabled", "true")          # serverless runs with ANSI mode on
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    root = tmp_path_factory.mktemp("landing")
    cfg = simulator.Config(root=str(root), start="2026-09-01", through=THROUGH,
                           requests_per_day=3000, volume_multiplier=0.15)
    state = simulator.run(cfg)
    return root, state


def _lineage(df):
    return (df.withColumn("source_file", F.col("_metadata.file_path"))
              .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
              .withColumn("ingest_time", F.current_timestamp()))


@pytest.fixture(scope="session")
def bronze(spark, landing):
    root, _ = landing
    gw = _lineage(spark.read.option("recursiveFileLookup", "true").json(str(root / "gateway_logs")))
    fb = _lineage(spark.read.option("recursiveFileLookup", "true").json(str(root / "feedback")))
    oc = spark.read.option("header", "true").option("recursiveFileLookup", "true").csv(str(root / "outcomes"))
    oc = _lineage(oc.toDF(*[c.lower() for c in oc.columns]))   # Auto Loader: readerCaseSensitive=false
    rows = (_lineage(spark.read.option("header", "true").option("recursiveFileLookup", "true")
                     .option("pathGlobFilter", "*.csv.gz").csv(str(root / "cost_exports")))
            .withColumn("run_id", F.regexp_extract("source_file", r"/([0-9a-f-]{36})/[^/]+$", 1)))
    man = _lineage(spark.read.option("multiLine", "true").option("recursiveFileLookup", "true")
                   .option("pathGlobFilter", "manifest.json").json(str(root / "cost_exports")))
    apps = spark.read.option("header", "true").csv(str(root / "reference" / "apps"))
    price = (spark.read.option("header", "true").csv(str(root / "reference" / "price_card"))
             .withColumn("retail_price_usd", F.col("retail_price_usd").cast("decimal(18,8)"))
             .withColumn("effective_from", F.to_date("effective_from"))
             .withColumn("captured_on", F.to_date("captured_on")))
    return {"gateway_logs": gw, "feedback": fb, "outcomes": oc, "cost_rows": rows, "manifests": man,
            "apps": apps, "price": price}
