# CLAUDE.md: context for continuing this project

Read this first. It's the hand-off from the Cowork sessions that designed and built Phase 1 (Oct 21–22, 2026).

## Who and why
- Owner: Harshi Vetrivel (Zeal IT Consultants). Studying for the **Databricks Certified Data Engineer
  Associate** exam (May 2026 guide). Already studied: lakehouse basics, Spark, medallion, notebooks + Git
  folders, Unity Catalog.
- Origin: a colleague (George) asked what Azure cost data can be exported into Databricks, since clients
  use various Azure AI services. That produced the first repo, **azure-cost-pipeline** (private; Databricks
  Free Edition; FOCUS exports, manifest-driven ingest). It was tested on one real export from Zeal's
  Skunkworks subscription (FOCUS 1.0r2, CSV+Gzip; storage account zbskunkworks, container cost-management,
  directory exports).
- This repo (**azure-ai-cost-pipeline**, GitHub `zeal-harshi-vetrivel/azure-ai-cost-pipeline`) is the
  **exam-aligned rebuild**. It widens scope from cost to AI usage and value, so the pipeline is worth having,
  and places each exam section in a real feature. Goal: cover as much of the exam guide as possible
  **without adding features the project doesn't need**.
- Runs in Harshi's **personal Azure Databricks workspace** (serverless available; ADLS shows up as a
  connector) with a personal ADLS Gen2 account. A personal subscription has too little spend for real
  cost exports, so data is **simulated but anchored on real schemas and real prices**.

## Working preferences
- Tight, plain explanations; official docs only; be honest about gaps and anything unverified.
- **All synthetic data must be labelled synthetic** (table comments, `_synthetic` fields, manifest flag,
  `SYNTHETIC_DATA.md` in landing).
- Never commit data. Real exports contain company spend and subscription IDs (`.gitignore` blocks data files).
- Commit/push only when asked. Work on feature branches + PRs (S5 practice).

## Architecture (agreed)
```
simulator / Azure -> landing volume (ADLS, external) -> bronze (Auto Loader ST) -> silver -> gold (MVs) -> AI/BI dashboard
REST (Retail Prices API) -> price card file -> COPY INTO -> ops.price_card_history -> silver.dim_price
```
- Catalog per target: `aicost_dev` / `aicost`. Schemas: landing (volume `files`), bronze, silver, gold, ops.
- Landing folders: `cost_exports/` (FOCUS, `<export>/<YYYYMMDD-YYYYMMDD>/<runId>/part_0_0001.csv.gz` +
  `manifest.json` last, CreateNewReport so all runs kept), `gateway_logs/` (APIM diagnostic-settings archive
  layout `resourceId=/.../y=/m=/d=/h=/m=00/PT1H.json`), `feedback/YYYY/MM/DD/`, `outcomes/<app_id>/`,
  `reference/{apps,price_card,retail_prices}/`.
- Bronze streaming tables: gateway_logs, feedback_events, outcomes, cost_export_rows, cost_export_manifests
  (all keep source_file, file_modification_time, ingest_time).
- Silver: ai_requests (ST, watermark dedupe, `cluster_by` event_date, app_id), feedback (AUTO CDC SCD1 on
  feedback_id by event_time), outcomes and outcome_requests (MV, exploded), cost_runs and cost_latest
  (newest *complete* run per month via manifest row count), dim_app, dim_price (effective_to via lead).
- Gold MVs: ai_spend_daily, ai_adoption, ai_efficiency, ai_quality, ai_unit_economics,
  cost_reconciliation_daily. Plain views (notebook 04): v_cost_reconciliation_alerts,
  v_ai_spend_by_team_month; 04 sets task value `recon_alert_days`.
- Logic lives in `src/pipeline/utilities/ai_transforms.py` (pure DataFrame functions). Pipeline files only
  declare datasets and expectations. Tests import the same module.

## Phase 1 status: built, tested locally, NOT yet deployed
- One commit, `0bfbc6d` (+ this file). Push to GitHub was blocked: the Cowork session lacked repo access.
  If the repo is still empty, push first.
- `pytest` passes 12 tests (PySpark 4.0, ANSI on) comparing pipeline output with simulator truth: dedupe
  counts, monthly billed totals, reconciliation gaps explained to the cent.
- Bundle passes the Databricks CLI v0.270 JSON schema check; `bundle validate` not run against the workspace.

## Planted anomalies (what gold should reveal; see `settings.ANOMALIES`)
| Anomaly | Where it shows |
| --- | --- |
| Runaway retry loop, finance app, gpt-4o, 2026-10-08 02–05h | ai_spend_daily spike |
| gpt-4.1 used by legal from 2026-10-15 but not in price card | `model_in_price_card` warn; reconciliation gap ~20%; alert view |
| Shadow usage on sandbox AOAI resource (no gateway logs) | `off_gateway_cost_usd` |
| Duplicate (0.4%), late (1%), corrupt PromptTokens "n/a" (0.05%), negative tokens (0.03%) | dedupe; `_rescued_data` + recovery from TotalTokens; expect_or_drop |
| Schema drift 2026-10-10: CachedTokens + x-session-id appear | addNewColumns; before it, the gateway estimate overstates gpt-4.1-mini cost (cached discount invisible), negative gap ≈1% |
| Late charges restated days 1–5 of next month | cost_latest picks newest run; month-end day grows |
| Feedback vote changes (5%) | AUTO CDC latest wins |
| Outcome files with Title_Case headers (finance), US dates `M/d/yyyy` | readerCaseSensitive=false; to_date parse |

## Decisions and findings
- **Reconciliation compares like with like**: gateway AOAI resource bill vs gateway estimate. Off-gateway
  (shadow) LLM spend is a separate column. Otherwise every day alerts (shadow ≈15% of the bill).
- Unit economics: value = minutes saved × **assumed** $55/h (pipeline conf `aicost.loaded_hourly_rate`).
  Ratios come out huge because tokens are cheap; README says cost per outcome is the honest measure.
- Price card only includes models in `settings.MODELS` without `in_price_card: False`. That's deliberate,
  so gpt-4.1 stays unpriced.
- Reference data uses COPY INTO (small, idempotent; it skips already-loaded files **even if modified**). Use a
  new dated file or `force`.
- Non-AI export lines (APIM, Storage, AI Search) are made-up amounts; the AI prices are real.

## Real prices used (Azure Retail Prices API, eastus2, Global, captured 2026-10-22, USD per 1K tokens)
serviceName `Foundry Models`, productName `Azure OpenAI`:
gpt-4o-0806 Inp 0.0025 / Outp 0.01 · gpt-4o-mini-0718 Inp 0.00015 / Outp 0.0006 ·
gpt 4.1 mini Inp 0.0004 / cached 0.0001 / Outp 0.0016 · text-embedding-3-small 0.00002 ·
gpt 4.1 Inp 0.002 / Outp 0.008. Exact meter names are in `src/aicost/settings.py`.

## Verified API facts (from docs, Oct 2026)
- `from pyspark import pipelines as dp` (dlt still works). `@dp.table`, `@dp.materialized_view`,
  `@dp.temporary_view`, `dp.create_streaming_table(..., cluster_by=..., row_filter=..., private=...)`,
  `dp.create_auto_cdc_flow(target, source, keys, sequence_by, stored_as_scd_type=1, ...)` (replaces
  apply_changes). Expectations: `expect` / `expect_or_drop` / `expect_or_fail` (+ `expect_all*` dict).
- Pipeline root folder is auto-added to `sys.path` (`from utilities import ...`).
- Auto Loader: `cloudFiles.partitionColumns ""` ignores Hive-style dirs; `readerCaseSensitive` default true;
  `inferColumnTypes` default false; `schemaEvolutionMode` default addNewColumns; file events via
  `cloudFiles.useManagedFileEvents` (needs an external location with file events; volume paths recommended;
  >7 days idle forces a full listing).
- APIM `ApiManagementGatewayLlmLog` columns: PromptTokens, CompletionTokens, TotalTokens, DeploymentName,
  ModelName, OperationName, RequestId, CorrelationId, IsStreamCompletion, Region, ApiVersion, TimeGenerated, …
  (no CachedTokens; that field is the simulator's "policy change").

## Not yet verified (check on first deploy)
- `dropDuplicatesWithinWatermark` inside a pipeline streaming table.
- Nested schema hints (`properties.PromptTokens BIGINT`) and `partitionColumns ""` with the PT1H layout.
- Field casing of real APIM diagnostic-archive records vs the Log Analytics names used here.
- Simulator writing to `/Volumes` (atomic rename has a fallback).
- Production mode may want `run_as` set before `-t prod` deploys.

## Next steps
1. Push; add the repo as a Git folder; run `00_setup` (`landing_mode=managed` first, external ADLS later:
   access connector → storage credential → `landing_mode=external`).
2. `01_simulate` with `through=<yesterday>`, then `02_refresh_prices`, then `03_load_reference`;
   `databricks bundle deploy -t dev`, then run `ai_cost_pipeline`, then `04_gold_views`. Fix whatever the
   unverified items break.
3. Build the AI/BI dashboard on gold (SQL warehouse).
4. **Phase 2 (S4/S5):** If/else condition task on `recon_alert_days` → alert notebook; For-each backfill
   over months; file-arrival trigger on gateway_logs and a table-update trigger for the gold refresh, plus a
   time-vs-data-driven note; job compute for the simulator; unit-economics snapshot *table*; feature-branch
   PR workflow; optional GitHub Actions running pytest + `bundle validate`.
5. **Phase 3 (S6/S7/S1):** groups finops_analysts / team_leads / data_engineers; service principal `run_as`;
   GRANT/REVOKE/DENY; row filter (team leads see own team); column mask on hashed user_id; ABAC governed tag
   `pii`; managed↔external conversion exercise; time travel/RESTORE runbook; predictive optimization;
   skew/spill lab (`skew_app_share=0.8`, `volume_multiplier`), AQE skew join or salting, shuffle partitions,
   broadcast threshold; run-history comparison after scale-up.
- Keep out (notes only): cross joins, JDBC, forced streaming where batch fits. Optional: a Lakeflow Connect
  SharePoint/Drive connector for outcomes.

## Commands
```
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt   # Java 17+
.venv/bin/python -m pytest                                              # ~1 min
cd src && python -m aicost.simulator --root /tmp/landing --through 2026-10-21 --reset
databricks bundle validate -t dev && databricks bundle deploy -t dev && databricks bundle run -t dev ai_cost_daily
```

## Related material
- Reference guide (Azure cost data → Databricks), as a Claude Doc: https://claude.ai/artifact/X88bMTkRCsc1QgEvarEo8L
  and a Google Doc: https://docs.google.com/document/d/1dBdv-yNj-DMMrl2O6ncyTLTzdfHekyZlNMBaykk-ysQ/edit
- Claude project "Cost Metrics Export Investigation": the exam guide PDF, `claude/exam-aligned-project-plan.md`,
  `claude/phase-1-status.md`, and the old azure-cost-pipeline files.
- `docs/exam-map.md` (section → file → status), `docs/ingestion-choices.md`.
