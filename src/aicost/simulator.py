"""Synthetic AI usage + cost generator. ALL DATA IT WRITES IS SYNTHETIC.

It plays four outside systems and writes their files the way those systems do:

  gateway_logs/   Azure API Management LLM logs, archived to storage by a diagnostic setting
                  (hourly PT1H.json files of JSON lines). Field names follow the
                  ApiManagementGatewayLlmLog table (PromptTokens, CompletionTokens, DeploymentName...),
                  plus a few gateway fields (ResponseCode, TotalTime, BackendTime, ApimSubscriptionId).
  feedback/       thumbs up/down events the apps send (nested JSON lines, one file per day).
  outcomes/       business outcomes app owners upload (CSV, US dates, header casing varies by owner).
  cost_exports/   Azure Cost Management FOCUS daily month-to-date exports (CSV + Gzip, manifest last,
                  "create new report" so every run's folder is kept).

Requests are generated first; cost = tokens x the real Azure retail price for that meter. So the
gateway logs and the cost export reconcile, except for deliberate gaps (see settings.ANOMALIES):
an unpriced model, shadow usage that bypasses the gateway, duplicate/late/corrupt log records, and
late charges restated on days 1-5 of the next month.

Runs as a library (the 01_simulate notebook calls run()) and from the command line:
  python -m aicost.simulator --root ./landing --through 2026-10-21
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from aicost import settings as S

# ---- tunables ---------------------------------------------------------------------------------
# Per app: (prompt-token median, completion-token median). Embedding calls have no completion.
TOKEN_PROFILE = {
    "app-support-copilot": (1200, 350), "app-sales-email": (600, 450), "app-contract-review": (6000, 900),
    "app-kb-search": (2500, 300), "app-finance-recon": (3000, 250), "app-hr-helpdesk": (900, 250),
}
USERS_PER_APP = {
    "app-support-copilot": 220, "app-sales-email": 90, "app-contract-review": 25,
    "app-kb-search": 600, "app-finance-recon": 40, "app-hr-helpdesk": 500,
}
OUTCOMES = {  # app -> (outcome_type, requests per outcome, minutes saved median, header style)
    "app-support-copilot": ("ticket_resolved",   6, 4,  "lower"),
    "app-sales-email":     ("email_sent",        2, 2,  "lower"),
    "app-contract-review": ("contract_reviewed", 15, 20, "lower"),
    "app-finance-recon":   ("invoice_matched",   3, 3,  "Title"),   # this owner's sheet uses Title_Case headers
}
FEEDBACK_RATE = 0.07
FEEDBACK_CHANGE_RATE = 0.05
THROTTLE_RATE, ERROR_RATE = 0.015, 0.003
EXPORT_NAME = "ai-platform-focus-daily"
LATE_TOKENS_PER_DAY = 50_000      # late sandbox charges added to a month's last day, per restatement day
NON_AI_DAILY = [  # synthetic flat daily charges so the export isn't only AI (not real prices)
    ("API Management", "Integration", "API Management", "Standard v2", "Standard v2 Unit", 1.0, "1 Day", 23.10,
     S.APIM_NAME, "microsoft.apimanagement/service"),
    ("Storage Accounts", "Storage", "Storage", "Hot LRS", "Hot LRS Data Stored", 210.0, "1 GB/Month", 0.0007,
     "stzealailanding", "microsoft.storage/storageaccounts"),
    ("Azure AI Search", "AI and Machine Learning", "Azure Cognitive Search", "Basic", "Basic Unit", 24.0, "1 Hour", 0.34,
     "srch-zeal-kb", "microsoft.search/searchservices"),
]


# ---- small helpers ----------------------------------------------------------------------------
def month_start(d: date) -> date:
    return d.replace(day=1)

def next_month(d: date) -> date:
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1)

def month_end(d: date) -> date:
    return next_month(d) - timedelta(days=1)

def iso(ts: pd.Timestamp | datetime) -> str:
    return pd.Timestamp(ts).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def _uuid(rng: np.random.Generator) -> str:
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))

def _uuids(rng: np.random.Generator, n: int) -> list[str]:
    raw = rng.bytes(16 * n)
    return [str(uuid.UUID(bytes=raw[i * 16:(i + 1) * 16], version=4)) for i in range(n)]

def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    try:
        tmp.rename(path)
    except OSError:              # some mounts can't rename; fall back to a plain write
        path.write_text(text)
        tmp.unlink(missing_ok=True)


@dataclass
class Config:
    root: str                                   # landing root (a /Volumes/... path or a local folder)
    start: str = "2026-09-01"
    through: str | None = None                  # simulate every day up to and including this date
    advance_days: int | None = 1                # ...or this many days past the last simulated day
    requests_per_day: int = 3000                # weekday volume on day 1, before growth
    volume_multiplier: float = 1.0              # scale-up for the troubleshooting lab
    skew_app_share: float | None = None         # e.g. 0.8 = one app gets 80% of traffic (Spark UI lab)
    seed: int = 7
    reset: bool = False
    anomalies: dict = field(default_factory=lambda: dict(S.ANOMALIES))


# ---- one simulated day of gateway traffic ------------------------------------------------------
def _app_weights(cfg: Config) -> tuple[list[str], np.ndarray]:
    ids = [a[0] for a in S.APPS]
    w = np.array([a[4] for a in S.APPS], dtype=float)
    if cfg.skew_app_share:
        others = (1 - cfg.skew_app_share) * w[1:] / w[1:].sum()
        w = np.concatenate([[cfg.skew_app_share], others])
    return ids, w / w.sum()


def generate_requests(cfg: Config, d: date) -> pd.DataFrame:
    """Every gateway request on day d, one row each, before any logging glitches."""
    rng = np.random.default_rng(cfg.seed + d.toordinal())
    start = date.fromisoformat(cfg.start)
    growth = 1 + 0.012 * (d - start).days                      # adoption grows ~1.2%/day
    weekday = 1.0 if d.weekday() < 5 else 0.3
    n = int(cfg.requests_per_day * cfg.volume_multiplier * growth * weekday * rng.normal(1, 0.05))
    app_ids, app_w = _app_weights(cfg)
    apps = {a[0]: a for a in S.APPS}

    app = rng.choice(app_ids, size=n, p=app_w)
    model = np.empty(n, dtype=object)
    for a in app_ids:
        idx = np.where(app == a)[0]
        mix = apps[a][5]
        model[idx] = rng.choice(list(mix), size=len(idx), p=list(mix.values()))

    unpriced = cfg.anomalies.get("unpriced_model")
    if unpriced and d >= date.fromisoformat(unpriced["from"]):
        idx = np.where(app == unpriced["app_id"])[0]
        model[idx[rng.random(len(idx)) < unpriced["share"]]] = unpriced["model"]

    # business-hours-shaped arrival times (Central time 8am-6pm peak, expressed in UTC)
    hour = rng.choice(24, size=n, p=_hour_profile())
    seconds = hour * 3600 + rng.integers(0, 3600, n)

    runaway = cfg.anomalies.get("runaway_loop")
    if runaway and d == date.fromisoformat(runaway["date"]):
        k = int(runaway["extra_requests"] * cfg.volume_multiplier)
        h0, h1 = runaway["hours"]
        app = np.concatenate([app, np.full(k, runaway["app_id"], dtype=object)])
        model = np.concatenate([model, np.full(k, runaway["model"], dtype=object)])
        seconds = np.concatenate([seconds, rng.integers(h0 * 3600, h1 * 3600, k)])
        n += k

    users = np.array([f"user{rng.integers(0, USERS_PER_APP[a]):04d}.{a.split('-')[1]}@example.com"
                      for a in app])
    is_embed = np.array([m.startswith("text-embedding") for m in model])
    p_med = np.array([TOKEN_PROFILE[a][0] for a in app]) * np.where(is_embed, 0.12, 1.0)
    c_med = np.array([TOKEN_PROFILE[a][1] for a in app])
    prompt = np.maximum(5, rng.lognormal(np.log(p_med), 0.55)).astype(int)
    completion = np.where(is_embed, 0, np.maximum(1, rng.lognormal(np.log(c_med), 0.6))).astype(int)
    cached = np.where(np.array([m == "gpt-4.1-mini" for m in model]),
                      (prompt * rng.uniform(0, 0.6, n)).astype(int), 0)

    status = np.where(rng.random(n) < THROTTLE_RATE, 429, 200)
    status = np.where(rng.random(n) < ERROR_RATE, 500, status)
    ok = status == 200
    prompt, completion, cached = prompt * ok, completion * ok, cached * ok   # failed calls aren't billed

    mini = np.array(["mini" in m for m in model])
    backend = np.where(is_embed, 60 + prompt * 0.02,
                       250 + completion * np.where(mini, 9, 18) + prompt * 0.05) * rng.lognormal(0, 0.25, n)
    backend = np.where(status == 429, rng.integers(5, 30, n), backend)
    total = backend + rng.integers(12, 45, n)

    ts = pd.Timestamp(d, tz="UTC") + pd.to_timedelta(seconds, unit="s") + \
        pd.to_timedelta(rng.integers(0, 1000, n), unit="ms")
    return pd.DataFrame({
        "request_id": _uuids(rng, n), "correlation_id": _uuids(rng, n), "time": ts,
        "app_id": app, "model": model, "user_id": users,
        "prompt_tokens": prompt, "completion_tokens": completion, "cached_tokens": cached,
        "status": status, "backend_ms": backend.round().astype(int), "total_ms": total.round().astype(int),
        "is_stream": (~is_embed) & (rng.random(n) < 0.6),
        "operation": np.where(is_embed, "embeddings", "chat.completions"),
    }).sort_values("time", ignore_index=True)


def _hour_profile() -> np.ndarray:
    # UTC hours; peak 13:00-23:00 UTC = 8am-6pm Central
    p = np.array([2, 1.5, 1, 1, 1, 1, 1, 1.5, 2, 2.5, 3, 4, 6, 9, 10, 10, 9, 10, 10, 9, 8, 6, 4, 3], float)
    return p / p.sum()


# ---- writers ----------------------------------------------------------------------------------
def write_gateway_logs(cfg: Config, d: date, req: pd.DataFrame) -> dict:
    rng = np.random.default_rng(cfg.seed + d.toordinal() * 3)
    an = cfg.anomalies
    drift = d >= date.fromisoformat(an["schema_drift"]["from"]) if an.get("schema_drift") else False
    res_id = (f"/SUBSCRIPTIONS/{S.SUBSCRIPTION_ID}/RESOURCEGROUPS/{S.RESOURCE_GROUP.upper()}"
              f"/PROVIDERS/MICROSOFT.APIMANAGEMENT/SERVICE/{S.APIM_NAME.upper()}")
    n = len(req)
    arrival = req["time"].dt.hour.to_numpy().copy()
    late = rng.random(n) < an.get("late_rate", 0)
    arrival = np.where(late, np.minimum(arrival + 1, 23), arrival)
    corrupt = rng.random(n) < an.get("corrupt_rate", 0)
    negative = rng.random(n) < an.get("negative_rate", 0)
    dup = rng.random(n) < an.get("duplicate_rate", 0)

    lines: dict[int, list[str]] = {h: [] for h in range(24)}
    for i, r in enumerate(req.itertuples(index=False)):
        mv = S.MODELS[r.model]["model_version"]
        props = {
            "RequestId": r.request_id, "CorrelationId": r.correlation_id,
            "ApimSubscriptionId": r.app_id, "OperationName": r.operation,
            "ApiVersion": "2024-10-21", "DeploymentName": r.model, "ModelName": r.model,
            "ModelVersion": mv, "Region": "East US 2", "IsStreamCompletion": bool(r.is_stream),
            "PromptTokens": int(r.prompt_tokens), "CompletionTokens": int(r.completion_tokens),
            "TotalTokens": int(r.prompt_tokens + r.completion_tokens),
            "ResponseCode": int(r.status), "TotalTime": int(r.total_ms), "BackendTime": int(r.backend_ms),
            "RequestHeaders": {"x-user-id": r.user_id, "x-app-version": "1.4.2"},
        }
        if drift:   # the gateway policy was updated: cached tokens and a session id start appearing
            props["CachedTokens"] = int(r.cached_tokens)
            props["RequestHeaders"]["x-session-id"] = f"s-{zlib.crc32(f'{r.user_id}|{r.time:%Y%m%d%H}'.encode()) % 10**8:08d}"
        if corrupt[i]:
            props["PromptTokens"] = "n/a"
        if negative[i]:
            props["CompletionTokens"] = -abs(props["CompletionTokens"]) - 1
        rec = {"time": iso(r.time), "resourceId": res_id, "category": "GatewayLlmLogs",
               "operationName": "Microsoft.ApiManagement/GatewayLlmLogs", "properties": props,
               "_synthetic": True}
        line = json.dumps(rec, separators=(",", ":"))
        lines[int(arrival[i])].append(line)
        if dup[i]:                                    # delivered twice, possibly in the next file
            lines[int(min(arrival[i] + rng.integers(0, 2), 23))].append(line)

    base = Path(cfg.root) / S.LANDING["gateway_logs"] / f"resourceId={res_id}"
    files = 0
    for h, ls in lines.items():
        if not ls:
            continue
        p = base / f"y={d:%Y}" / f"m={d:%m}" / f"d={d:%d}" / f"h={h:02d}" / "m=00" / "PT1H.json"
        _write_text_atomic(p, "\n".join(ls) + "\n")
        files += 1
    return {"records": int(n + dup.sum()), "unique_requests": n, "duplicates": int(dup.sum()),
            "corrupt": int(corrupt.sum()), "negative": int(negative.sum()), "files": files}


def write_feedback(cfg: Config, d: date, req: pd.DataFrame) -> dict:
    rng = np.random.default_rng(cfg.seed + d.toordinal() * 5)
    chat = req[(req["status"] == 200) & (req["operation"] == "chat.completions")]
    fb = chat[rng.random(len(chat)) < FEEDBACK_RATE].reset_index(drop=True)
    p_up = 0.78 + np.where(fb["model"] == "gpt-4o", 0.06, 0) - np.where(fb["model"].str.contains("mini"), 0.04, 0) \
        - np.where(fb["app_id"] == "app-hr-helpdesk", 0.10, 0)
    up = rng.random(len(fb)) < p_up
    down_reasons = np.array(["incorrect", "incomplete", "too_slow", "off_topic"])
    events = []
    ids = _uuids(rng, len(fb))
    for i, r in enumerate(fb.itertuples(index=False)):
        t = r.time + pd.Timedelta(seconds=int(rng.integers(20, 900)))
        ev = {"feedback_id": ids[i], "event_time": iso(t), "rating": "up" if up[i] else "down",
              "reason": None if up[i] else str(rng.choice(down_reasons)),
              "app": {"id": r.app_id, "version": "1.4.2"}, "request": {"id": r.request_id},
              "user": {"id": r.user_id}, "_synthetic": True}
        events.append(ev)
        if rng.random() < FEEDBACK_CHANGE_RATE:        # user changes their mind: same id, later event
            ev2 = dict(ev, event_time=iso(t + pd.Timedelta(minutes=int(rng.integers(1, 120)))),
                       rating="down" if up[i] else "up", reason=None if not up[i] else "incorrect")
            events.append(ev2)
    p = Path(cfg.root) / S.LANDING["feedback"] / f"{d:%Y/%m/%d}" / f"feedback_{d:%Y%m%d}.json"
    _write_text_atomic(p, "".join(json.dumps(e, separators=(",", ":")) + "\n" for e in events))
    return {"events": len(events), "feedback_ids": len(fb)}


def write_outcomes(cfg: Config, d: date, req: pd.DataFrame) -> dict:
    rng = np.random.default_rng(cfg.seed + d.toordinal() * 11)
    written = 0
    for app_id, (otype, per, minutes, style) in OUTCOMES.items():
        ok = req[(req["app_id"] == app_id) & (req["status"] == 200)]["request_id"].to_numpy()
        n = len(ok) // per
        if n == 0:
            continue
        rng.shuffle(ok)
        rows = []
        for i in range(n):
            k = max(1, int(rng.poisson(per)))
            ids = ok[i * per: i * per + min(k, per)]
            rows.append({"outcome_id": f"{app_id[4:8]}-{d:%Y%m%d}-{i:05d}", "app_id": app_id,
                         "outcome_date": f"{d.month}/{d.day}/{d.year}",       # US format, no zero padding
                         "outcome_type": otype,
                         "minutes_saved": round(float(rng.lognormal(np.log(minutes), 0.4)), 1),
                         "request_ids": "|".join(ids)})
        df = pd.DataFrame(rows)
        if style == "Title":
            df.columns = [c.replace("_id", "_ID").title().replace("_Id", "_ID") for c in df.columns]
        p = Path(cfg.root) / S.LANDING["outcomes"] / app_id / f"outcomes_{d:%Y%m%d}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(p, index=False)
        written += len(df)
    return {"outcomes": written}


# ---- cost export ------------------------------------------------------------------------------
def daily_usage(cfg: Config, d: date, req: pd.DataFrame) -> list[dict]:
    """Billed tokens per resource/model/meter for day d (what Azure meters, regardless of logging)."""
    ok = req[req["status"] == 200]
    g = ok.groupby("model")[["prompt_tokens", "completion_tokens", "cached_tokens"]].sum()
    out = []
    for model, r in g.iterrows():
        meters = S.MODELS[model]["meters"]
        uncached = int(r.prompt_tokens - r.cached_tokens) if "cached_input" in meters else int(r.prompt_tokens)
        parts = {"input": uncached, "output": int(r.completion_tokens),
                 "cached_input": int(r.cached_tokens) if "cached_input" in meters else 0}
        for kind, tokens in parts.items():
            if tokens > 0 and kind in meters:
                out.append({"resource": "prod", "model": model, "kind": kind, "tokens": tokens})
    shadow = cfg.anomalies.get("shadow_usage")
    if shadow:
        rng = np.random.default_rng(cfg.seed + d.toordinal() * 13)
        f = (0.3 if d.weekday() >= 5 else 1.0) * cfg.volume_multiplier * cfg.requests_per_day / 3000
        tin, tout = (int(t * f * rng.uniform(0.7, 1.3)) for t in shadow["daily_tokens"])
        out += [{"resource": "sandbox", "model": shadow["model"], "kind": "input", "tokens": tin},
                {"resource": "sandbox", "model": shadow["model"], "kind": "output", "tokens": tout}]
    return out


def _focus_row(day: date, resource: str, rtype: str, service: str, category: str, meter_cat: str,
               meter_sub: str, meter: str, qty: float, unit: str, price: float, tags: dict | None) -> dict:
    cost = round(qty * price, 10)
    return {
        "BilledCost": cost, "EffectiveCost": cost, "ListCost": cost, "ContractedCost": cost,
        "BillingAccountId": "0000000", "BillingAccountName": "Zeal (synthetic)", "BillingCurrency": "USD",
        "BillingPeriodStart": f"{month_start(day)}T00:00:00Z", "BillingPeriodEnd": f"{next_month(day)}T00:00:00Z",
        "ChargeCategory": "Usage", "ChargeClass": "", "ChargeDescription": meter, "ChargeFrequency": "Usage-Based",
        "ChargePeriodStart": f"{day}T00:00:00Z", "ChargePeriodEnd": f"{day + timedelta(days=1)}T00:00:00Z",
        "CommitmentDiscountType": "", "ConsumedQuantity": round(qty, 6), "ConsumedUnit": unit,
        "ContractedUnitPrice": price, "ListUnitPrice": price, "PricingQuantity": round(qty, 6),
        "PricingUnit": unit, "ProviderName": "Microsoft", "PublisherName": "Microsoft",
        "RegionName": "East US 2", "ResourceId": f"/subscriptions/{S.SUBSCRIPTION_ID}/resourcegroups/"
                                                 f"{S.RESOURCE_GROUP}/providers/{rtype}/{resource}",
        "ResourceName": resource, "ServiceCategory": category, "ServiceName": service,
        "SubAccountId": S.SUBSCRIPTION_ID, "SubAccountName": S.SUBSCRIPTION_NAME,
        "Tags": json.dumps(tags) if tags else "",
        "x_ResourceGroupName": S.RESOURCE_GROUP, "x_ResourceType": rtype,
        "x_SkuMeterCategory": meter_cat, "x_SkuMeterSubcategory": meter_sub, "x_SkuMeterName": meter,
    }


def cost_rows_for_day(day: date, usage: list[dict]) -> list[dict]:
    rows = []
    for u in usage:
        meter, price = S.MODELS[u["model"]]["meters"][u["kind"]]
        resource = S.AOAI_RESOURCES[u["resource"]]
        tags = {"env": "prod", "project": "ai-gateway"} if u["resource"] == "prod" else None
        rows.append(_focus_row(day, resource, "microsoft.cognitiveservices/accounts", "Azure OpenAI",
                               "AI and Machine Learning", "Foundry Models", "Azure OpenAI", meter,
                               u["tokens"] / 1000, S.PRICE_UNIT, price, tags))
    for (svc, cat, mcat, msub, meter, qty, unit, price, res, rtype) in NON_AI_DAILY:
        rows.append(_focus_row(day, res, rtype, svc, cat, mcat, msub, meter, qty, unit, price,
                               {"env": "prod", "project": "ai-gateway"}))
    return rows


def write_export_run(cfg: Config, period: date, rows: list[dict], run_date: date, through: date,
                     slot: int) -> dict:
    rng = np.random.default_rng(cfg.seed + run_date.toordinal() * 17 + slot)
    run_id = _uuid(rng)
    folder = f"{period:%Y%m%d}-{month_end(period):%Y%m%d}"
    rel_dir = f"{S.LANDING['cost_exports']}/{EXPORT_NAME}/{folder}/{run_id}"
    run_dir = Path(cfg.root) / rel_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows).sample(frac=1, random_state=int(rng.integers(0, 2**31))).reset_index(drop=True)
    fname = "part_0_0001.csv.gz"
    with gzip.open(run_dir / fname, "wt", newline="") as fh:
        df.to_csv(fh, index=False, float_format="%.10f")
    submitted = datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone.utc) + \
        timedelta(hours=int(rng.integers(3, 9)), minutes=int(rng.integers(0, 60)), seconds=slot)
    manifest = {
        "manifestVersion": "2024-04-01", "byteCount": (run_dir / fname).stat().st_size, "blobCount": 1,
        "dataRowCount": len(df), "synthetic": True,
        "exportConfig": {"exportName": EXPORT_NAME, "dataVersion": "1.0r2", "apiVersion": "2023-07-01-preview",
                         "type": "FocusCost", "timeFrame": "MonthToDate", "granularity": "Daily"},
        "deliveryConfig": {"partitionData": True, "dataOverwriteBehavior": "CreateNewReport",
                           "fileFormat": "Csv", "compressionMode": "Gzip",
                           "rootFolderPath": S.LANDING["cost_exports"]},
        "runInfo": {"executionType": "Scheduled", "submittedTime": submitted.isoformat().replace("+00:00", "Z"),
                    "runId": run_id, "startDate": f"{period}T00:00:00", "endDate": f"{through}T00:00:00Z"},
        "blobs": [{"blobName": f"{rel_dir}/{fname}", "byteCount": (run_dir / fname).stat().st_size,
                   "dataRowCount": len(df)}],
    }
    _write_text_atomic(run_dir / "manifest.json", json.dumps(manifest, indent=2))   # manifest lands last
    return {"run_id": run_id, "rows": len(df), "billed_cost": round(float(df["BilledCost"].sum()), 4),
            "covers_through": str(through), "submitted": manifest["runInfo"]["submittedTime"]}


def export_runs_on(run_date: date, start: date, usage_by_day: dict) -> list[tuple[date, list[dict], date]]:
    """The export runs Azure makes on run_date: month-to-date through yesterday, and on days 1-5
    a restatement of the prior month that picks up late charges."""
    runs = []
    yesterday = run_date - timedelta(days=1)
    if yesterday < start:
        return runs
    def month_rows(period, through, late_day=0):
        rows = []
        d = max(period, start)
        while d <= through:
            rows += cost_rows_for_day(d, usage_by_day.get(str(d), []))
            d += timedelta(days=1)
        if late_day:   # late-arriving sandbox charges for the month's last day, growing over days 1-5
            rows += cost_rows_for_day(through, [{"resource": "sandbox", "model": "gpt-4o", "kind": "input",
                                                 "tokens": LATE_TOKENS_PER_DAY * late_day}])[:1]
        return rows
    if run_date.day > 1:
        runs.append((month_start(run_date), month_rows(month_start(run_date), yesterday), yesterday))
    prior = month_start(month_start(run_date) - timedelta(days=1))
    if run_date.day <= 5 and month_end(prior) >= start:
        runs.append((prior, month_rows(prior, month_end(prior), late_day=run_date.day), month_end(prior)))
    return runs


# ---- reference files --------------------------------------------------------------------------
def write_reference(cfg: Config) -> None:
    ref = Path(cfg.root) / S.LANDING["reference"]
    ref.mkdir(parents=True, exist_ok=True)
    apps = pd.DataFrame([{"app_id": a[0], "app_name": a[1], "team_id": a[2], "team_name": S.TEAMS[a[2]],
                          "use_case": a[3], "apim_subscription_id": a[0]} for a in S.APPS])
    (ref / "apps").mkdir(exist_ok=True)
    apps.to_csv(ref / "apps" / "apps.csv", index=False)
    rows = []
    for model, m in S.MODELS.items():
        if not m.get("in_price_card", True):
            continue
        for kind, (meter, price) in m["meters"].items():
            rows.append({"model": model, "token_type": kind, "meter_name": meter, "unit_of_measure": S.PRICE_UNIT,
                         "retail_price_usd": price, "region": S.PRICE_REGION,
                         "effective_from": "2026-01-01", "source": "Azure Retail Prices API",
                         "captured_on": S.PRICE_SNAPSHOT_DATE})
    (ref / "price_card").mkdir(exist_ok=True)
    pd.DataFrame(rows).to_csv(ref / "price_card" / f"price_card_{S.PRICE_SNAPSHOT_DATE.replace('-', '')}.csv",
                              index=False, float_format="%.8f")


# ---- driver -----------------------------------------------------------------------------------
def run(cfg: Config) -> dict:
    root = Path(cfg.root)
    state_path = root / "_simulator" / "state.json"
    if cfg.reset:
        for sub in list(S.LANDING.values()) + ["_simulator"]:
            shutil.rmtree(root / sub, ignore_errors=True)
        print("reset: removed all simulated landing files")
    start = date.fromisoformat(cfg.start)
    state = json.loads(state_path.read_text()) if state_path.exists() else \
        {"start": cfg.start, "last_day": str(start - timedelta(days=1)), "usage": {}, "days": {}, "exports": {}}
    if state["start"] != cfg.start:
        raise ValueError("start changed since the last run; rerun with reset=True")

    write_reference(cfg)
    (root / "SYNTHETIC_DATA.md").write_text(
        "Every file under this folder was generated by aicost.simulator. It is synthetic: realistic "
        "schemas and real Azure retail prices, but made-up apps, users and usage.\n")

    last = date.fromisoformat(state["last_day"])
    through = date.fromisoformat(cfg.through) if cfg.through else last + timedelta(days=cfg.advance_days or 1)
    d = last + timedelta(days=1)
    while d <= through:
        req = generate_requests(cfg, d)
        gw = write_gateway_logs(cfg, d, req)
        fb = write_feedback(cfg, d, req)
        oc = write_outcomes(cfg, d, req)
        state["usage"][str(d)] = daily_usage(cfg, d, req)
        state["days"][str(d)] = {**gw, **fb, **oc, **_truth_costs(state["usage"][str(d)]),
                                 "cached_savings": _cached_savings(req)}
        # Azure exports day d's charges in the run on d+1
        for slot, (period, rows, covered) in enumerate(export_runs_on(d + timedelta(days=1), start, state["usage"])):
            info = write_export_run(cfg, period, rows, d + timedelta(days=1), covered, slot)
            state["exports"].setdefault(str(period), []).append(info)
            if period != month_start(d + timedelta(days=1)):     # a restatement: record its late charges
                late_tokens = LATE_TOKENS_PER_DAY * (d + timedelta(days=1)).day
                state.setdefault("late_charges", {})[str(covered)] = round(
                    late_tokens / 1000 * S.MODELS["gpt-4o"]["meters"]["input"][1], 4)
        print(f"{d}  requests={gw['unique_requests']:,}  log files={gw['files']}  feedback={fb['events']}  "
              f"outcomes={oc['outcomes']}  billed=${state['days'][str(d)]['billed_total']:,.2f}")
        state["last_day"] = str(d)
        d += timedelta(days=1)

    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=1))
    return state


def _truth_costs(usage: list[dict]) -> dict:
    """What a perfect pipeline should find: AI cost split by why it is/isn't explained by the gateway."""
    out = {"gateway_priced": 0.0, "gateway_unpriced": 0.0, "shadow": 0.0}
    for u in usage:
        price = S.MODELS[u["model"]]["meters"][u["kind"]][1]
        cost = u["tokens"] / 1000 * price
        if u["resource"] == "sandbox":
            out["shadow"] += cost
        elif S.MODELS[u["model"]].get("in_price_card", True):
            out["gateway_priced"] += cost
        else:
            out["gateway_unpriced"] += cost
    out = {k: round(v, 4) for k, v in out.items()}
    out["billed_total"] = round(sum(out.values()), 4)
    return out


def _cached_savings(req: pd.DataFrame) -> float:
    """Discount from cached prompt tokens. Before the gateway logs CachedTokens (schema drift date),
    a gateway-side estimate can't see it and overstates cost by this much."""
    total = 0.0
    for model, m in S.MODELS.items():
        if "cached_input" in m["meters"]:
            tokens = float(req.loc[(req["model"] == model) & (req["status"] == 200), "cached_tokens"].sum())
            total += tokens / 1000 * (m["meters"]["input"][1] - m["meters"]["cached_input"][1])
    return round(total, 4)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate synthetic AI gateway, feedback, outcome and cost files")
    ap.add_argument("--root", required=True)
    ap.add_argument("--start", default="2026-09-01")
    ap.add_argument("--through")
    ap.add_argument("--advance-days", type=int, default=1)
    ap.add_argument("--requests-per-day", type=int, default=3000)
    ap.add_argument("--volume-multiplier", type=float, default=1.0)
    ap.add_argument("--skew-app-share", type=float)
    ap.add_argument("--reset", action="store_true")
    a = ap.parse_args(argv)
    run(Config(root=a.root, start=a.start, through=a.through, advance_days=a.advance_days,
               requests_per_day=a.requests_per_day, volume_multiplier=a.volume_multiplier,
               skew_app_share=a.skew_app_share, reset=a.reset))


if __name__ == "__main__":
    main()
