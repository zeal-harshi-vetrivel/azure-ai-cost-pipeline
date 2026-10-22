"""Shared settings: catalog layout, landing folders, the simulated apps, and model → price meters.

Everything environment-specific (catalog name, storage URL) comes in as a parameter from the
bundle target (dev/prod), so this file holds only things that are the same everywhere.
"""

# ---- Unity Catalog layout --------------------------------------------------------------------
# One catalog per bundle target (dev: aicost_dev, prod: aicost). Schemas inside it:
SCHEMAS = {
    "landing": "Raw files. Holds the 'files' volume (external on ADLS, or managed as a fallback).",
    "bronze":  "Append-only streaming tables written by the pipeline, one per source.",
    "silver":  "Cleaned, deduplicated, conformed tables.",
    "gold":    "Business-facing materialized views and views for dashboards.",
    "ops":     "Reference tables loaded by COPY INTO, price snapshots, run logs.",
}
LANDING_VOLUME = "files"          # /Volumes/<catalog>/landing/files


def landing_root(catalog: str) -> str:
    return f"/Volumes/{catalog}/landing/{LANDING_VOLUME}"


# Folders inside the landing volume, one per source. The pipeline and the simulator both use these.
LANDING = {
    "cost_exports": "cost_exports",   # FOCUS daily exports: <export>/<YYYYMMDD-YYYYMMDD>/<run id>/part_*.csv.gz + manifest.json
    "gateway_logs": "gateway_logs",   # APIM LLM logs, diagnostic-settings layout: .../y=/m=/d=/h=/m=00/PT1H.json
    "feedback":     "feedback",       # thumbs up/down events from the apps (JSON lines)
    "outcomes":     "outcomes",       # business outcomes reported by app owners (CSV, US dates)
    "reference":    "reference",      # apps.csv, price_card/*.csv, retail_prices/ (raw API pages)
}

# ---- Simulated organisation --------------------------------------------------------------------
# Synthetic. App IDs double as the APIM subscription each app calls the gateway with.
APPS = [
    # app_id,           app_name,                team,          use_case,              weight, models (share)
    ("app-support-copilot", "Support Copilot",   "customer-care", "ticket drafting",      0.34, {"gpt-4o-mini": 0.8, "gpt-4o": 0.2}),
    ("app-sales-email",     "Sales Email Writer", "sales",        "email generation",     0.16, {"gpt-4.1-mini": 1.0}),
    ("app-contract-review", "Contract Reviewer",  "legal",        "document review",      0.08, {"gpt-4o": 1.0}),
    ("app-kb-search",       "Knowledge Search",   "it",           "RAG search",           0.24, {"text-embedding-3-small": 0.55, "gpt-4o-mini": 0.45}),
    ("app-finance-recon",   "Invoice Matcher",    "finance",      "invoice extraction",   0.10, {"gpt-4.1-mini": 0.7, "gpt-4o": 0.3}),
    ("app-hr-helpdesk",     "HR Helpdesk Bot",    "hr",           "policy Q&A",           0.08, {"gpt-4o-mini": 1.0}),
]
TEAMS = {
    "customer-care": "Customer Care", "sales": "Sales", "legal": "Legal",
    "it": "IT", "finance": "Finance", "hr": "People",
}

# ---- Models and their Azure meters -------------------------------------------------------------
# Global Standard deployments in eastus2. meter names and prices are exactly as returned by the
# Azure Retail Prices API (serviceName 'Foundry Models', productName 'Azure OpenAI') on 2026-10-22.
# The price refresh notebook re-pulls them; this snapshot only seeds the first price card.
MODELS = {
    "gpt-4o": {
        "model_version": "2024-08-06",
        "meters": {"input":  ("gpt-4o-0806-Inp-glbl Tokens", 0.0025),
                   "output": ("gpt-4o-0806-Outp-glbl Tokens", 0.01)},
    },
    "gpt-4o-mini": {
        "model_version": "2024-07-18",
        "meters": {"input":  ("gpt-4o-mini-0718-Inp-glbl Tokens", 0.00015),
                   "output": ("gpt-4o-mini-0718-Outp-glbl Tokens", 0.0006)},
    },
    "gpt-4.1-mini": {
        "model_version": "2025-04-14",
        "meters": {"input":        ("gpt 4.1 mini Inp glbl Tokens", 0.0004),
                   "cached_input": ("gpt 4.1 mini cached Inp glbl Tokens", 0.0001),
                   "output":       ("gpt 4.1 mini Outp glbl Tokens", 0.0016)},
    },
    "text-embedding-3-small": {
        "model_version": "1",
        "meters": {"input": ("text-embedding-3-small-glbl Tokens", 0.00002)},
    },
    # Deliberate gap: Legal starts routing some traffic to gpt-4.1 mid-month, but nobody adds it to
    # the price card. Azure still bills it (so it shows in the cost export); the gateway-side cost
    # estimate can't price it. The pipeline should surface this, not hide it.
    "gpt-4.1": {
        "model_version": "2025-04-14",
        "in_price_card": False,
        "meters": {"input":  ("gpt 4.1 Inp glbl Tokens", 0.002),
                   "output": ("gpt 4.1 Outp glbl Tokens", 0.008)},
    },
}

# ---- Deliberate anomalies (all synthetic; each one is something a gold table should reveal) ------
ANOMALIES = {
    "runaway_loop":      {"date": "2026-10-08", "app_id": "app-finance-recon", "model": "gpt-4o",
                          "extra_requests": 4000, "hours": (2, 5)},   # retry loop overnight
    "unpriced_model":    {"from": "2026-10-15", "app_id": "app-contract-review", "model": "gpt-4.1",
                          "share": 0.4},
    "shadow_usage":      {"model": "gpt-4o", "daily_tokens": (400_000, 120_000)},  # (input, output) on the sandbox resource, no gateway logs
    "schema_drift":      {"from": "2026-10-10"},      # gateway starts logging CachedTokens and a session id
    "duplicate_rate":    0.004,                       # log records delivered twice
    "late_rate":         0.01,                        # records that land in the next hour's file
    "corrupt_rate":      0.0005,                      # PromptTokens logged as a string -> rescued data
    "negative_rate":     0.0003,                      # negative CompletionTokens -> dropped by expectation
}
PRICE_REGION = "eastus2"
PRICE_SNAPSHOT_DATE = "2026-10-22"
PRICE_UNIT = "1K"                 # Retail Prices API unitOfMeasure for these meters

# Azure resources the AI traffic runs on (they appear in the FOCUS cost export).
AOAI_RESOURCES = {
    "prod": "aoai-zeal-prod-eus2",     # everything behind the gateway
    "sandbox": "aoai-zeal-sbx-eus2",   # direct calls that bypass the gateway (shadow usage)
}
APIM_NAME = "apim-zeal-ai-gateway"

SUBSCRIPTION_ID = "00000000-0000-0000-0000-00000000a1c0"   # synthetic
SUBSCRIPTION_NAME = "ai-platform (synthetic)"
RESOURCE_GROUP = "rg-ai-platform"
