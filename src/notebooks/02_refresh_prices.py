# Databricks notebook source
# Pull current Azure OpenAI retail prices from the public Azure Retail Prices REST API
# (https://prices.azure.com/api/retail/prices, no auth needed) and land them as files:
#
#   landing/reference/retail_prices/<YYYYMMDD>/page_NNN.json   raw API pages, kept for audit/replay
#   landing/reference/price_card/price_card_<YYYYMMDD>.csv      only the meters our models use
#
# 03_load_reference then picks up the new price card with COPY INTO. Prices themselves are REAL;
# everything else in this project is synthetic.
#
# Retries: the API pages with NextPageLink and can throttle (429) or blip (5xx). Each call retries with
# exponential backoff here; the job task also has retries for anything worse (e.g. DNS down).

# COMMAND ----------
dbutils.widgets.text("catalog", "aicost_dev")
dbutils.widgets.text("region", "eastus2")
CATALOG = dbutils.widgets.get("catalog")
REGION = dbutils.widgets.get("region")

import csv, io, json, os, sys, time
from datetime import date
import requests

sys.path.append(os.path.abspath(".."))
from aicost import settings as S

API = "https://prices.azure.com/api/retail/prices"
TODAY = date.today()
REF = f"{S.landing_root(CATALOG)}/{S.LANDING['reference']}"

# COMMAND ----------
def get_with_retry(url, params=None, attempts=5):
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            if i == attempts - 1:
                raise
            wait = 2 ** i
            print(f"  attempt {i + 1} failed ({e}); retrying in {wait}s")
            time.sleep(wait)


def fetch_all(filter_expr):
    url, params, pages = API, {"$filter": filter_expr}, []
    while url:
        page = get_with_retry(url, params)
        pages.append(page)
        url, params = page.get("NextPageLink"), None       # the next link already carries the filter
    return pages


flt = (f"serviceName eq 'Foundry Models' and armRegionName eq '{REGION}' "
       f"and priceType eq 'Consumption'")
pages = fetch_all(flt)
items = [i for p in pages for i in p["Items"]]
print(f"{len(items)} price rows in {len(pages)} pages")

# COMMAND ----------
# Land the raw pages (dbutils.fs.put writes straight to the volume)
raw_dir = f"{REF}/retail_prices/{TODAY:%Y%m%d}"
for n, p in enumerate(pages):
    dbutils.fs.put(f"{raw_dir}/page_{n:03d}.json", json.dumps(p), overwrite=True)

# Map our models' meters to today's prices. Models marked in_price_card=False are left out on purpose
# (see settings.py): the pipeline should show that their usage can't be priced.
by_meter = {i["meterName"]: i for i in items if i.get("unitOfMeasure") == S.PRICE_UNIT}
rows, missing = [], []
for model, m in S.MODELS.items():
    if not m.get("in_price_card", True):
        continue
    for kind, (meter, _) in m["meters"].items():
        it = by_meter.get(meter)
        if not it:
            missing.append(meter)
            continue
        rows.append({"model": model, "token_type": kind, "meter_name": meter,
                     "unit_of_measure": it["unitOfMeasure"], "retail_price_usd": f"{it['retailPrice']:.8f}",
                     "region": REGION, "effective_from": it["effectiveStartDate"][:10],
                     "source": "Azure Retail Prices API", "captured_on": str(TODAY)})
if missing:
    print("WARNING: meters not found (renamed or retired?):", missing)
if not rows:
    raise RuntimeError("No prices matched; not writing an empty price card")

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=list(rows[0]))
w.writeheader(); w.writerows(rows)
dbutils.fs.put(f"{REF}/price_card/price_card_{TODAY:%Y%m%d}.csv", buf.getvalue(), overwrite=True)
display(spark.createDataFrame(rows))
