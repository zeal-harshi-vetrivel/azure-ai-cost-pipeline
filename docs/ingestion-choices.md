# Why each source is ingested the way it is

The exam asks you to pick an ingestion method for a source and justify it. These are this project's picks.

| Source | Shape | Method | Why this one |
| --- | --- | --- | --- |
| `gateway_logs/` (APIM LLM logs) | Many small JSON files, a new one every hour, nested, schema changes over time | **Auto Loader** in the pipeline (streaming table) | Only new files should be read each run. Auto Loader remembers what it processed, infers the nested schema, adds new fields (`schemaEvolutionMode = addNewColumns`), and puts values that don't fit the type into `_rescued_data` instead of failing. |
| `feedback/` | JSON events, one file per day, a vote can change later | **Auto Loader** → **AUTO CDC** (SCD type 1) | Same reasons as above to land it; AUTO CDC then keeps the latest vote per `feedback_id`, ordered by `event_time`, even when events arrive out of order. |
| `outcomes/` | CSV from several owners, header casing differs, US dates | **Auto Loader** with `readerCaseSensitive = false` | New files arrive per owner per day. Case-insensitive reading puts `App_ID` and `app_id` in one column instead of rescuing one owner's data. Kept as text in bronze; silver parses `M/d/yyyy`. |
| `cost_exports/` (FOCUS) | Daily month-to-date runs, CSV+Gzip, `manifest.json` written last | **Auto Loader** for rows and manifests (two streams, `pathGlobFilter`) | Bronze keeps every run (append-only, cheap, auditable). Choosing the newest *complete* run per month is a batch decision, so it happens in a silver materialized view using the manifest's row count and submitted time. |
| `reference/apps`, `reference/price_card` | A few small files that rarely change | **COPY INTO** (notebook 03) | Idempotent (files already loaded are skipped), plain SQL, no stream or checkpoint to look after. Auto Loader would work but adds machinery a handful of files doesn't need. |
| Azure Retail Prices API | Public REST API, paged JSON | **REST client in a notebook** (notebook 02), scheduled by the job with retries | No connector exists for it. The notebook lands raw pages as files (replayable), then writes a price card file that COPY INTO picks up. |

## Directory listing vs file events

Auto Loader finds new files in one of two ways:

- **Directory listing** (default here, `aicost.use_file_events = false`): lists the folder each run. Nothing to set up. Cost grows with the number of files in the folder.
- **File events** (`cloudFiles.useManagedFileEvents = true`): Databricks keeps a cache of file events for the external location, so a run reads only what changed. Needs the landing volume to be *external* and file events turned on for its external location. Worth it once the hourly gateway folder holds thousands of files. If a stream doesn't run for more than 7 days, it falls back to a full listing once.

Switch by setting the bundle variable `use_file_events: "true"` after enabling file events on the external location.

## Considered and not used

- **Lakeflow Connect managed connectors** (SharePoint / Google Drive): a fit for the outcomes CSVs if owners kept them in SharePoint. Optional later; needs a connection set up by a workspace admin.
- **JDBC**: no source here is a database.
- **Streaming the reference data**: it changes a few times a year; batch is the right tool.
