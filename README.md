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

## How the synthetic data is made

`src/aicost/simulator.py` plays four outside systems and writes their files into the landing volume in
the formats those systems use. `src/aicost/settings.py` holds the apps, models, prices and anomaly
switches. There is no real traffic or real spend anywhere in this project.

**Real vs invented**

| Real (copied from Azure) | Invented (my assumptions) |
| --- | --- |
| Field names and file layouts: the APIM `ApiManagementGatewayLlmLog` table, the diagnostic-settings archive path, the FOCUS 1.0r2 export (CSV + Gzip, manifest, "create new report") | The organisation: 6 apps, 6 teams, the users (`userNNNN.<app>@example.com`) and how many each app has |
| Token prices: Azure Retail Prices API, `eastus2`, Global, captured 2026-10-22 | Request volume, growth, time-of-day shape, token counts, latency, error and throttle rates |
| Meter names and the model list (gpt-4o, gpt-4o-mini, gpt-4.1-mini, gpt-4.1, text-embedding-3-small) | Feedback rates, minutes saved per outcome, the $55/hour rate |
| | Non-AI cost lines (API Management, Storage, AI Search), subscription ID, billing account, resource names |

**How a day is generated** (`generate_requests`)

1. Request count for the day = 3,000 × adoption growth (+1.2% per day since the start date) × weekday
   factor (weekends 30%) × noise (normal, 5% spread). Scalable with `--volume-multiplier`.
2. Each request is assigned an app by weight, then a model by that app's model mix (`settings.APPS`).
3. Arrival time follows an hourly profile that peaks 8am–6pm Central.
4. Prompt and completion tokens are log-normal around a per-app median (`TOKEN_PROFILE`). Embedding calls
   have no completion. Cached prompt tokens exist only for gpt-4.1-mini (0–60% of the prompt).
5. 1.5% of calls are throttled (429) and 0.3% fail (500). Failed calls carry no tokens and are not billed.
6. Latency is a formula on token counts (faster for mini models and embeddings) with random noise.

Each day uses its own random generator seeded from `seed + date`, so a given configuration always
produces the same files.

**What gets written** (all from the same requests, so the sources agree with each other)

| Folder | Source it imitates | Notes |
| --- | --- | --- |
| `gateway_logs/` | APIM LLM logs archived to storage | One `PT1H.json` per hour, JSON lines |
| `feedback/` | Apps' thumbs up/down events | 7% of successful chat calls get a rating; 5% of those later change their vote (same id, later event) |
| `outcomes/<app_id>/` | Owners' uploaded business outcomes (CSV) | Each outcome links to several request ids; US dates; finance uses Title_Case headers |
| `cost_exports/` | Azure Cost Management FOCUS daily export | Month-to-date through yesterday, one run per day; on days 1–5 an extra run restates the prior month with late charges |
| `reference/` | App list and price card | The price card deliberately omits gpt-4.1 |

**Why gateway and bill reconcile.** Cost is computed as tokens × the real retail price for the meter,
from the same requests that become gateway logs. So the two agree, except where `settings.ANOMALIES`
breaks them on purpose (unpriced model, shadow usage on a sandbox resource, duplicate/late/corrupt log
records, schema change, restated charges). That is what the tests check against.

**Ground truth.** After each day the simulator records what a perfect pipeline should find (unique
requests, duplicates, billed cost split into priced / unpriced / shadow) in `_simulator/state.json`
under the landing root. The tests compare pipeline output with it.

**Labelling.** Every gateway and feedback record carries `"_synthetic": true`, each export manifest has
`"synthetic": true`, the landing root gets a `SYNTHETIC_DATA.md`, names such as `ai-platform (synthetic)`
are used, and the table comments in the pipeline start with `SYNTHETIC.`

**Running it**

```
cd src && python -m aicost.simulator --root ./landing --through 2026-10-21 --reset
```
Or run the `01_simulate` notebook on Databricks (backfill, or advance one day per run). State is kept, so
repeat runs continue where the last stopped. Options: `--requests-per-day`, `--volume-multiplier`,
`--skew-app-share` (send most traffic to one app, for the Spark skew lab), `--reset`. The seed is a
field on `Config` (default 7), not a command-line flag.

**Limits of the simulation.** The distributions are my choices, not measured from real traffic. Real
logs will have more variety, odd records and field-casing differences than this generator produces, and
real value-per-request is not known. Results from this data show that the pipeline works, not what any
organisation's AI spend looks like.

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
