# Exam guide coverage (Databricks Certified Data Engineer Associate, May 2026)

Where each exam section shows up in this repo. **Done** = built in Phase 1. **Planned** = later phase.

## S1 Platform (6%)
| Topic | Where | Status |
| --- | --- | --- |
| Compute choice: serverless pipeline, serverless job tasks, SQL warehouse for dashboards | `resources/*.yml`, README | Done (job compute for the simulator: Phase 2) |
| Delta time travel / RESTORE to undo a bad load | runbook notebook | Planned |

## S2 Ingestion (21%)
| Topic | Where | Status |
| --- | --- | --- |
| Auto Loader: schema inference, schema hints (incl. nested), `addNewColumns`, `_rescued_data` | `src/pipeline/transformations/bronze.py` | Done |
| `readerCaseSensitive`, `pathGlobFilter`, partition-column inference off | `bronze.py` | Done |
| Directory listing vs file events | `bronze.py`, `docs/ingestion-choices.md` | Done (switch is a variable) |
| COPY INTO, idempotency | `src/notebooks/03_load_reference.py` | Done |
| REST API client with retries, landing raw responses | `src/notebooks/02_refresh_prices.py` | Done |
| Choosing a method per source | `docs/ingestion-choices.md` | Done |
| Lakeflow Connect managed connector | outcomes from SharePoint/Drive | Optional |

## S3 Transformation (22%)
| Topic | Where | Status |
| --- | --- | --- |
| Cleaning: types, `try_cast`, US dates, nulls, case | `utilities/ai_transforms.py` | Done |
| Left join (requests ⟕ feedback), broadcast join (price card), multi-key join (outcomes ⋈ requests), explode | `ai_transforms.py` | Done |
| Dedupe: streaming within a watermark; newest run per month (window) | `dedupe_requests`, `latest_complete_runs` | Done |
| `approx_count_distinct`, `percentile_approx`, averages | `ai_adoption`, `ai_efficiency` | Done |
| Object types: streaming tables, materialized views, temporary views, a persisted view | `bronze/silver/gold.py`, `04_gold_views.py` | Done (snapshot *table* for unit economics: Phase 2) |
| Expectations: warn / drop / fail | `silver.py`, `gold.py` | Done |
| AUTO CDC (SCD type 1) | `silver.feedback` | Done |

## S4 Lakeflow Jobs (16%)
| Topic | Where | Status |
| --- | --- | --- |
| Multi-task DAG with a pipeline task, retries, task values | `resources/ai_cost_daily_job.yml`, `04_gold_views.py` | Done (basic) |
| If/else condition task on `recon_alert_days`, For-each backfill | job | Planned (Phase 2) |
| File-arrival and table-update triggers; time vs data-driven note | job | Planned (Phase 2) |

## S5 CI/CD (10%)
| Topic | Where | Status |
| --- | --- | --- |
| Bundle with dev/prod targets and variables | `databricks.yml` | Done |
| `bundle validate / deploy / run` | README | Done (not yet run against the workspace) |
| Feature branches + PRs from a Git folder; GitHub Actions | repo | Planned |

## S6 Troubleshooting (10%)
| Topic | Where | Status |
| --- | --- | --- |
| Liquid clustering (`ai_requests` on event_date, app_id; `cost_latest` on charge_date) | `silver.py` | Done |
| Predictive optimization | catalog setting | Planned |
| Skew / spill lab (`skew_app_share`, `volume_multiplier` already in the simulator), AQE, shuffle partitions | lab notebook | Planned |
| Run-history comparison after scaling volume | job runs | Planned |

## S7 Governance (15%)
| Topic | Where | Status |
| --- | --- | --- |
| External location + external volume vs managed | `00_setup.py` | Done |
| Groups, GRANT/REVOKE, service principal `run_as` | governance notebook, `databricks.yml` prod | Planned (Phase 3) |
| Row filter by team, column mask on `user_id`, ABAC tag policy | governance notebook | Planned (Phase 3) |
