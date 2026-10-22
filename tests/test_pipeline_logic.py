"""End-to-end checks of the silver/gold logic against simulated data with known answers.

The simulator records what a correct pipeline should find (state["days"], state["exports"]), so these
tests compare pipeline output with that truth rather than with hand-picked numbers.
"""

from datetime import date

import pytest
from pyspark.sql import functions as F

from utilities import ai_transforms as T


def _expectation(df, constraint):
    """Mimic expect_or_drop: keep rows where the constraint is true."""
    return df.where(constraint)


@pytest.fixture(scope="module")
def silver(bronze):
    flat = T.clean_gateway_logs(bronze["gateway_logs"])
    req = _expectation(T.dedupe_requests(flat), "prompt_tokens >= 0 AND completion_tokens >= 0").cache()
    fb = T.clean_feedback(bronze["feedback"])
    oc = T.clean_outcomes(bronze["outcomes"]).cache()
    runs = T.cost_runs(bronze["cost_rows"], bronze["manifests"]).cache()
    cost = T.build_cost_latest(bronze["cost_rows"], T.latest_complete_runs(runs)).cache()
    dim_app = bronze["apps"]
    dim_price = T.build_dim_price(bronze["price"])
    priced = T.price_requests(req, dim_price).cache()
    return {"flat": flat, "requests": req, "feedback": fb, "outcomes": oc, "runs": runs, "cost": cost,
            "dim_app": dim_app, "dim_price": dim_price, "priced": priced}


def test_requests_are_deduped(silver, landing):
    _, state = landing
    days = state["days"].values()
    flat = silver["flat"].count()
    assert flat == sum(d["records"] for d in days)                       # duplicates present before
    expected = sum(d["unique_requests"] for d in days) - sum(d["negative"] for d in days)
    assert silver["requests"].count() == expected                          # gone after, minus dropped rows
    assert silver["requests"].groupBy("request_id").count().where("count > 1").count() == 0


def test_corrupt_prompt_tokens_are_recovered(silver, landing):
    _, state = landing
    rec = silver["requests"].where("prompt_tokens_recovered")
    assert rec.count() > 0
    assert rec.where("prompt_tokens IS NULL").count() == 0
    assert silver["flat"].where("prompt_tokens_recovered").select("request_id").distinct().count() == \
        sum(d["corrupt"] for d in state["days"].values())


def test_schema_drift_column_appears(silver):
    r = silver["requests"]
    assert r.where("event_date < '2026-10-10' AND session_id IS NOT NULL").count() == 0
    assert r.where("event_date >= '2026-10-10' AND session_id IS NOT NULL").count() > 0


def test_cost_latest_uses_newest_complete_run(silver, landing):
    _, state = landing
    got = {str(r.billing_month): float(r.total)
           for r in silver["cost"].groupBy("billing_month").agg(F.sum("billed_cost").alias("total")).collect()}
    for period, runs in state["exports"].items():
        newest = max(runs, key=lambda x: x["submitted"])
        assert got[period] == pytest.approx(newest["billed_cost"], abs=0.01), period
    assert silver["runs"].where("NOT is_complete").count() == 0


def test_september_includes_late_restated_charges(silver, landing):
    _, state = landing
    sep = state["exports"]["2026-09-01"]
    assert len({r["covers_through"] for r in sep}) >= 2          # MTD runs plus restatements
    assert max(sep, key=lambda x: x["submitted"])["covers_through"] == "2026-09-30"


def test_outcomes_parse_us_dates_and_title_case_headers(silver):
    oc = silver["outcomes"]
    assert oc.where("outcome_date IS NULL").count() == 0
    assert oc.where("app_id = 'app-finance-recon'").count() > 0
    ex = T.explode_outcome_requests(oc)
    assert ex.count() == oc.select(F.sum(F.size("request_ids"))).first()[0]


def test_dim_price_effective_ranges(spark):
    hist = spark.createDataFrame(
        [("m", "input", "x", 0.002, "1K", date(2026, 1, 1), date(2026, 1, 1)),
         ("m", "input", "x", 0.001, "1K", date(2026, 6, 1), date(2026, 5, 20)),
         ("m", "input", "x", 0.0011, "1K", date(2026, 6, 1), date(2026, 5, 25))],   # newer capture wins
        "model string, token_type string, meter_name string, retail_price_usd double, unit_of_measure string, "
        "effective_from date, captured_on date")
    rows = sorted(T.build_dim_price(hist).collect(), key=lambda r: r.effective_from)
    assert [r.retail_price_usd for r in rows] == [0.002, 0.0011]
    assert rows[0].effective_to == date(2026, 5, 31) and rows[1].effective_to == date(9999, 12, 31)


def test_unpriced_model_is_flagged(silver):
    p = silver["priced"]
    assert p.where("model = 'gpt-4.1'").where("is_priced").count() == 0
    assert p.where("model = 'gpt-4.1' AND event_date < '2026-10-15'").count() == 0
    spend = T.ai_spend_daily(p, silver["dim_app"])
    assert spend.where("NOT model_in_price_card").select("model").distinct().collect()[0].model == "gpt-4.1"


def test_reconciliation_gap_is_explained(silver, landing):
    """On the gateway resource: billed - estimate = unpriced model - cached discount the logs couldn't see
    (before the drift date) + the few rows dropped as bad. Off-gateway = shadow usage + late charges."""
    _, state = landing
    rec = {str(r.event_date): r for r in
           T.cost_reconciliation_daily(silver["cost"], silver["priced"], "aoai-zeal-prod-eus2").collect()}
    checked = 0
    for day, truth in state["days"].items():
        if day not in rec or rec[day].billed_llm_cost_usd == 0:
            continue          # charges for the last simulated day aren't exported yet
        r = rec[day]
        late = state.get("late_charges", {}).get(day, 0)      # restated on days 1-5 of the next month
        assert float(r.billed_llm_cost_usd) == pytest.approx(truth["billed_total"] + late, abs=0.01), day
        assert float(r.off_gateway_cost_usd) == pytest.approx(truth["shadow"] + late, abs=0.01), day
        explained = truth["gateway_unpriced"]
        if day < "2026-10-10":     # before the gateway logged CachedTokens, the estimate overstates cost
            explained -= truth["cached_savings"]
        residual = float(r.gap_usd) - explained
        assert -0.01 <= residual <= 0.01 * float(r.billed_gateway_resource_usd) + 0.01, (day, residual)
        checked += 1
    assert checked >= 45
    days = sorted(rec)
    flagged = {d for d in days if rec[d].gap_pct is not None and abs(float(rec[d].gap_pct)) > 0.05}
    assert "2026-10-16" in flagged                    # unpriced gpt-4.1 traffic
    assert not any("2026-10-10" <= d < "2026-10-15" for d in flagged)    # clean week


def test_runaway_loop_shows_in_spend(silver):
    spend = T.ai_spend_daily(silver["priced"], silver["dim_app"])
    fin = {str(r.event_date): float(r.est_cost_usd or 0) for r in
           spend.where("app_id = 'app-finance-recon'").groupBy("event_date")
           .agg(F.sum("est_cost_usd").alias("est_cost_usd")).collect()}
    others = [v for k, v in fin.items() if k != "2026-10-08" and date.fromisoformat(k).weekday() == 3]
    assert fin["2026-10-08"] > 3 * max(others)


def test_quality_and_adoption(silver):
    q = T.ai_quality(silver["requests"], silver["feedback"])
    tot = q.agg(F.sum("rated_requests"), F.sum("requests")).first()
    assert 0.04 < tot[0] / tot[1] < 0.10
    a = T.ai_adoption(silver["requests"], silver["dim_app"])
    assert a.where("active_users_approx <= 0").count() == 0


def test_unit_economics(silver):
    ue = T.ai_unit_economics(silver["priced"], T.explode_outcome_requests(silver["outcomes"]),
                             silver["dim_app"], 55.0)
    rows = {(str(r.month), r.app_id): r for r in ue.collect()}
    sup = rows[("2026-10-01", "app-support-copilot")]
    assert sup.outcomes > 0 and sup.value_to_cost_ratio > 1
    assert rows[("2026-10-01", "app-kb-search")].outcomes is None      # this team doesn't report outcomes
