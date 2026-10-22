# AI cost & value pipeline

Tracks what an organisation's LLM apps cost, who uses them, how well they work, and whether they pay off,
and checks the gateway's numbers against the Azure bill. Built on Azure Databricks (Lakeflow pipelines,
Unity Catalog, ADLS Gen2), and laid out to cover the Databricks Data Engineer Associate exam guide
([docs/exam-map.md](docs/exam-map.md)).

> **All data is synthetic.** Schemas follow real sources (Azure FOCUS cost exports, the APIM
> `ApiManagementGatewayLlmLog` table) and prices are real Azure retail prices, but the apps, users
> and usage are made up by `src/aicost/simulator.py`.

```
 simulator / Azure ──► landing volume (ADLS) ──► bronze ──────────► silver ─────────────► gold ──► dashboard
   gateway_logs/  JSON hourly          Auto Loader      ai_requests (deduped,        ai_spend_daily
   feedback/      JSON daily           streaming        clustered)                   ai_adoption
   outcomes/      CSV                  tables           feedback (AUTO CDC)          ai_efficiency
   cost_exports/  FOCUS CSV.gz         (append-only)    outcomes, outcome_requests   ai_quality
   reference/     apps, price card ──► COPY INTO ─────► cost_runs, cost_latest       ai_unit_economics
 Azure Retail Prices API ──► REST notebook ──┘          dim_app, dim_price           cost_reconciliation_daily
```

## What the gold tables answer

| Table | Question |
| --- | --- |
| `ai_spend_daily` | What does each app/team/model cost per day? (tokens × the retail price in effect that day) |
| `ai_adoption` | How many people use each app? (approximate distinct users) |
| `ai_efficiency` | Latency (avg, p95), tokens per request, throttling, cached-token share |
| `ai_quality` | Share of requests rated, and share rated thumbs-up |
| `ai_unit_economics` | Monthly cost vs reported outcomes, cost per outcome, estimated value (assumes a $55/hour loaded rate; a parameter, not a fact) |
| `cost_reconciliation_daily` | Does the gateway explain the Azure bill? Gap on the gateway's Azure OpenAI resource, plus LLM spend that never went through the gateway |

The simulator plants problems these tables should surface: a runaway retry loop (Oct 8), a model
missing from the price card (gpt-4.1, from Oct 15), shadow usage on a sandbox resource, duplicate,
late and corrupt log records, a schema change in the logs (Oct 10), late charges restated on days 1–5,
and vote changes in feedback. Tests check the pipeline finds each one.

**Reading the value numbers:** LLM token costs are tiny next to staff time, so value-to-cost ratios come
out large. Cost per outcome and its trend are the more honest measures.

## Repo layout

| Path | What it is |
| --- | --- |
| `src/aicost/settings.py` | Landing folders, the simulated apps, model → Azure meter and price, planted anomalies |
| `src/aicost/simulator.py` | Writes the synthetic landing files (also runs from the command line) |
| `src/notebooks/00_setup.py` | Catalog, schemas, landing volume (external on ADLS, or managed to start), reference tables |
| `src/notebooks/01_simulate.py` | Runs the simulator (backfill or one day at a time) |
| `src/notebooks/02_refresh_prices.py` | Pulls Azure OpenAI prices from the Retail Prices API, lands a price card |
| `src/notebooks/03_load_reference.py` | COPY INTO for the app list and price cards |
| `src/notebooks/04_gold_views.py` | Plain views for alerting and the dashboard; sets a task value with the alert count |
| `src/pipeline/transformations/` | The pipeline: `bronze.py`, `silver.py`, `gold.py` (dataset declarations + expectations) |
| `src/pipeline/utilities/ai_transforms.py` | All silver/gold logic as DataFrame functions, shared with the tests |
| `resources/` | Pipeline and job definitions for the bundle |
| `databricks.yml` | Bundle: `dev` (catalog `aicost_dev`) and `prod` (catalog `aicost`) targets |
| `tests/` | Local end-to-end tests against simulated data with known answers |
| `docs/` | Exam coverage map, ingestion choices |

## Setup (Azure Databricks)

1. **Get the code in.** Push this repo to GitHub and add it as a **Git folder** in the workspace.
2. **Landing storage.** Two options; the `/Volumes/...` path is the same either way:
   - *Start now (managed):* run `src/notebooks/00_setup` with `landing_mode = managed`.
   - *ADLS (the target design):* create an ADLS Gen2 account (hierarchical namespace on) with a
     container such as `landing`; create an **Access Connector for Azure Databricks** and give its managed
     identity *Storage Blob Data Contributor* on the account; in Catalog Explorer create a **storage
     credential** from the connector. Then run `00_setup` with `landing_mode = external`, `storage_url =
     abfss://landing@<account>.dfs.core.windows.net/aicost_dev`, `storage_credential = <name>`.
   - If catalog creation fails with a storage error, the metastore has no default storage: pass
     `catalog_managed_location` (an `abfss://` path under an external location).
3. **Backfill data:** run `01_simulate` with `through = <yesterday>`.
4. **Reference data:** run `02_refresh_prices`, then `03_load_reference`.
5. **Deploy and run:**
   ```
   databricks bundle validate -t dev
   databricks bundle deploy   -t dev
   databricks bundle run      -t dev ai_cost_pipeline      # just the pipeline
   databricks bundle run      -t dev ai_cost_daily         # the whole daily job
   ```
   Needs a recent Databricks CLI (pipeline `root_path` and `glob` libraries are newer bundle settings).
6. **Views:** `04_gold_views` (the job runs it after the pipeline).

## Tests

```
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt     # Java 17+ for PySpark
.venv/bin/python -m pytest
```
The tests simulate Sept 1 – Oct 20 at reduced volume, emulate bronze with batch reads, run the real
silver/gold functions, and compare against the simulator's recorded truth (row counts after dedupe,
monthly billed totals, reconciliation gaps explained to the cent). Spark runs with ANSI mode on, as
serverless does.

## Known limits / not yet verified

- Not yet deployed to a workspace. The pipeline's Auto Loader, AUTO CDC and expectations only run on
  Databricks; locally the same transformation functions are tested with batch reads.
- Field names in gateway records follow the Log Analytics table. Logs archived to storage by a
  diagnostic setting may differ in casing; schema hints and case-insensitive handling are the place to adjust.
- The FOCUS export carries only the columns this project uses, not the full FOCUS 1.0r2 set.
- Non-AI lines in the export (API Management, Storage, AI Search) use made-up daily amounts.

## Roadmap

- **Phase 2 (Jobs, CI/CD):** If/else condition on reconciliation alerts, For-each backfill, file-arrival and
  table-update triggers, job compute for the simulator, unit-economics snapshot table, GitHub Actions.
- **Phase 3 (Governance, troubleshooting):** groups and grants, service principal `run_as`, row filter by
  team, column mask and ABAC tag on `user_id`, time travel/RESTORE runbook, skew/spill lab.
