"""S181 — Periodic live-filter validation.

Every Monday at 17:00 ET, scans each active V16+S180 block rule and reports:
  - How many trades were blocked in the last 30 days
  - What those trades' average P&L would have been if NOT blocked
  - WR of the blocked-trade pool
  - Whether the block is still net-saving money (block staying valid)
    or net-losing money (block over-blocking winners — candidate for removal)

User's instruction (2026-05-24): "always block first, then re-assess periodically.
This is better than waiting indefinitely for more sample. If the regime shifts and
a rule starts blocking winners, we'll see it and can remove it."

Each rule is a dict:
  {
    "id": "S180", "name": "GEX-TARGET PM long block",
    "predicate": lambda row: True/False (would this row be blocked?),
    "shipped": "2026-05-24",
    "expected": "blocked PnL should stay <= 0 (block saves money)",
  }

Output: console log + Telegram message to alerts channel + HTML to Tel Res (weekly).
"""
from __future__ import annotations

import os
import requests
import types
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
from typing import Callable

from sqlalchemy import text

NY = ZoneInfo("America/New_York")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

_engine = None
_send_telegram = None


def init(engine, send_telegram_fn=None) -> None:
    global _engine, _send_telegram
    _engine = engine
    _send_telegram = send_telegram_fn
    _ensure_history_table()


def _ensure_history_table() -> None:
    """Create filter_validation_runs table if missing.
    One row per (rule_id, run_ts) — full history for trend analysis."""
    if _engine is None:
        return
    try:
        with _engine.begin() as cx:
            cx.execute(text("""
                CREATE TABLE IF NOT EXISTS filter_validation_runs (
                    id           BIGSERIAL PRIMARY KEY,
                    run_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    rule_id      TEXT NOT NULL,
                    rule_name    TEXT NOT NULL,
                    shipped_date TEXT,
                    window_days  INT NOT NULL,
                    n_blocked    INT NOT NULL,
                    win_count    INT,
                    wr_pct       NUMERIC,
                    total_pnl_pt NUMERIC,
                    total_pnl_usd NUMERIC,
                    avg_pnl_pt   NUMERIC,
                    verdict      TEXT NOT NULL,
                    note         TEXT
                )
            """))
            cx.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_fvr_rule_ts "
                "ON filter_validation_runs (rule_id, run_ts DESC)"
            ))
    except Exception as e:
        print(f"[filter-validation] table init error: {e}", flush=True)


def _persist_results(results: list[dict]) -> int:
    """Insert one row per rule into filter_validation_runs. Returns rows written."""
    if _engine is None or not results:
        return 0
    written = 0
    with _engine.begin() as cx:
        for r in results:
            if r.get("error"):
                continue
            try:
                cx.execute(text("""
                    INSERT INTO filter_validation_runs
                      (rule_id, rule_name, shipped_date, window_days, n_blocked,
                       win_count, wr_pct, total_pnl_pt, total_pnl_usd, avg_pnl_pt,
                       verdict, note)
                    VALUES
                      (:rid, :rname, :ship, :wd, :nb, :wc, :wr, :pt, :usd, :avg, :v, :note)
                """), {
                    "rid": r["id"], "rname": r["name"],
                    "ship": r.get("shipped"),
                    "wd": r.get("window_days", 30),
                    "nb": r.get("n_blocked", 0),
                    "wc": r.get("win_count"),
                    "wr": r.get("wr"),
                    "pt": r.get("total_pnl_pt"),
                    "usd": r.get("total_pnl_usd"),
                    "avg": r.get("avg_pnl_pt"),
                    "v": r["verdict"],
                    "note": r.get("note"),
                })
                written += 1
            except Exception as e:
                print(f"[filter-validation] persist error for {r.get('id')}: {e}", flush=True)
    return written


def get_history(rule_id: str | None = None, limit: int = 20) -> list[dict]:
    """Pull recent validation history. Used by future portal trend view."""
    if _engine is None:
        return []
    with _engine.connect() as cx:
        if rule_id:
            rows = cx.execute(text("""
                SELECT run_ts, rule_id, rule_name, n_blocked, wr_pct,
                       total_pnl_pt, total_pnl_usd, verdict, note
                FROM filter_validation_runs
                WHERE rule_id = :rid
                ORDER BY run_ts DESC
                LIMIT :lim
            """), {"rid": rule_id, "lim": limit}).fetchall()
        else:
            rows = cx.execute(text("""
                SELECT run_ts, rule_id, rule_name, n_blocked, wr_pct,
                       total_pnl_pt, total_pnl_usd, verdict, note
                FROM filter_validation_runs
                ORDER BY run_ts DESC
                LIMIT :lim
            """), {"lim": limit}).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Rule definitions — one per active block in _passes_live_filter()
# Add new rules here when shipping new filter blocks.
# ---------------------------------------------------------------------------

def _is_long(row) -> bool:
    return (row.get("direction") or "").lower() in ("long", "bullish")


def _is_short(row) -> bool:
    return (row.get("direction") or "").lower() in ("short", "bearish")


def _et_hour(row) -> int | None:
    ts = row.get("ts_et")
    return ts.hour if ts else None


def _is_opex_friday(row) -> bool:
    ts = row.get("ts_et")
    if not ts:
        return False
    return ts.weekday() == 4 and 15 <= ts.day <= 21


_BLOCK_RULES: list[dict] = [
    {
        "id": "S180", "name": "GEX-TARGET PM long block",
        "live_line": None,   # composite (paradigm + time + direction) — no single line to neutralise
        "shipped": "2026-05-24",
        "expected": "blocked PnL <= 0 (saves money)",
        "setups": ("Skew Charm", "DD Exhaustion", "ES Absorption"),
        "predicate": lambda r: (
            r.get("paradigm") == "GEX-TARGET"
            and _is_long(r)
            and (_et_hour(r) or 0) >= 13
        ),
    },
    {
        "id": "V16-R5", "name": "SC long GEX-LIS block (all alignments)",
        "live_line": "    if sn == 'Skew Charm' and isLong and para == 'GEX-LIS': return False",
        "shipped": "2026-05-17",
        "expected": "blocked PnL <= 0 (saves money)",
        "setups": ("Skew Charm",),
        "predicate": lambda r: (
            r.get("setup_name") == "Skew Charm"
            and _is_long(r)
            and r.get("paradigm") == "GEX-LIS"
        ),
    },
    {
        "id": "V16-R2", "name": "SC long monthly OpEx Friday block",
        "live_line": "    if sn == 'Skew Charm' and isLong and isOpex(): return False",
        "shipped": "2026-05-17",
        "expected": "blocked PnL <= 0 (saves money)",
        "setups": ("Skew Charm",),
        "predicate": lambda r: (
            r.get("setup_name") == "Skew Charm"
            and _is_long(r)
            and _is_opex_friday(r)
        ),
    },
    {
        "id": "V16-R10", "name": "ES Absorption bearish PM block (hr>=14)",
        "live_line": None,   # lives inside the ES Absorption branch, several lines
        "shipped": "2026-05-17",
        "expected": "blocked PnL <= 0 (saves money)",
        "setups": ("ES Absorption",),
        "predicate": lambda r: (
            r.get("setup_name") == "ES Absorption"
            and _is_short(r)
            and (_et_hour(r) or 0) >= 14
        ),
    },
    {
        "id": "V14-SIDIAL-EXT", "name": "SIDIAL-EXTREME longs block",
        "live_line": "        if para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900: return False",
        "shipped": "2026-04-12",
        "expected": "blocked PnL <= 0 (saves money)",
        "setups": ("Skew Charm", "DD Exhaustion", "ES Absorption"),
        "predicate": lambda r: (
            _is_long(r) and r.get("paradigm") == "SIDIAL-EXTREME"
        ),
    },
    {
        # S263 — the whole-Friday gate, armed on real money 2026-08-15
        # (REAL_TRADE_NO_FRIDAY=true). This entry IS the standing revert check the
        # user asked for: the weekly verdict logic already flags DEGRADING when a
        # rule's blocked set turns net-POSITIVE, which is exactly "Fridays started
        # working again, reconsider". No separate monitor needed.
        #
        # Restricted to live_pass so it measures the trades TSRT would really have
        # taken, not every Friday signal. v7 is excluded automatically — GEX Long is
        # not in the setup list this module queries, and v7 keeps trading Fridays on
        # purpose (+6.47 pt/trade, 77% WR; blocking it would have cost $712).
        "id": "S263-FRI", "name": "Whole-Friday block (main book, v7 exempt)",
        "live_line": None,   # not a live_filter rule at all — it is a real_trader gate
        "post_gate": True,   # applies AFTER V16 passes, so match on PASSING signals
        "shipped": "2026-08-15",
        "expected": "blocked PnL <= 0 (saves money); DEGRADING = Fridays recovered, review the gate",
        "setups": ("Skew Charm", "DD Exhaustion", "ES Absorption",
                   "AG Short", "Vanna Pivot Bounce"),
        "predicate": lambda r: (
            bool(r.get("live_pass"))
            and r.get("ts_et") is not None
            and r["ts_et"].weekday() == 4
        ),
    },
]


def _rule_variant(live_line: str):
    """Return a copy of live_filter with ONE rule neutralised, or None if the
    source line no longer matches.

    WHY THIS EXISTS (2026-08-15). The old method matched signals by SHAPE — "is
    this a SIDIAL-EXTREME long?" — and called them blocked. That is wrong twice
    over and it produced two false DEGRADING alarms:

      V14-SIDIAL-EXT   shape says 39t +$1,128 (blocking winners!)
                       truth      3t   -$190 (blocking losers, correctly)
      V16-R5           shape says 44t   +$329
                       truth     25t   -$244

    1. STALE SHAPE. The SIDIAL rule was narrowed to 14:00-15:00 on 2026-05-30
       (S195) but the predicate still tested every hour, so 24 of its 39 "blocked"
       signals were actually being TRADED.
    2. BAD ATTRIBUTION. A signal rejected by three rules was credited to all three.
       Removing any one of them would not have freed it, so its P&L belongs to none.

    The only definition that survives both: a signal is blocked BY THIS RULE if it
    fails passes_v16 AND passes a copy of passes_v16 with just this rule turned off.

    The line is neutralised (`: return False` -> `: pass`) rather than deleted,
    because deleting the sole body of an `if` breaks indentation.

    FAILS LOUD: if the live rule is edited and the line stops matching, the rule
    reports STALE-RULE in Telegram instead of quietly reporting wrong numbers.
    """
    try:
        src = _LIVE_FILTER_SRC()
        if src is None or (live_line + "\n") not in src:
            return None
        if not live_line.rstrip().endswith("return False"):
            return None
        patched = src.replace(live_line + "\n",
                              live_line[:live_line.rindex("return False")] + "pass\n")
        mod = types.ModuleType("live_filter_variant")
        mod.__file__ = "live_filter_variant.py"
        exec(compile(patched, "live_filter_variant.py", "exec"), mod.__dict__)
        return mod
    except Exception as e:
        print(f"[filter-validation] variant build failed: {e}", flush=True)
        return None


def _LIVE_FILTER_SRC():
    try:
        import app.live_filter as _lf
        with open(_lf.__file__, encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def evaluate_rules(window_days: int = 30) -> list[dict]:
    """Pull setup_log for last N days, evaluate each rule against actual outcomes.

    Returns list of dicts per rule with:
      - n_blocked, win_count, total_pnl_pt, avg_pnl, wr, verdict
    """
    if _engine is None:
        return [{"error": "module not initialized"}]

    cutoff = datetime.now(NY) - timedelta(days=window_days)
    with _engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, setup_name, direction, grade, paradigm,
                   greek_alignment, vix, live_pass, ts, overvix,
                   v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side,
                   basket_pct,
                   ts AT TIME ZONE 'America/New_York' AS ts_et,
                   outcome_result, outcome_pnl
            FROM setup_log
            WHERE ts >= :cutoff
              AND outcome_pnl IS NOT NULL
              AND setup_name IN ('Skew Charm', 'DD Exhaustion',
                                 'ES Absorption', 'AG Short', 'Vanna Pivot Bounce')
        """), {"cutoff": cutoff}).fetchall()

    all_rows = [dict(r._mapping) for r in rows]

    # Counterfactual attribution (2026-08-15). See _rule_variant for why shape
    # matching was wrong. Compute passes_v16 once for every row, then per rule ask
    # "does it pass with ONLY this rule switched off". Rules with live_line=None
    # keep the old shape match but are marked so the report says which is which.
    from app.live_filter import passes_v16 as _v16, load_gaps as _lg
    _base_ok, _gaps = {}, None
    try:
        with _engine.connect() as _c:
            _gaps = _lg(_c)
        for r in all_rows:
            _base_ok[r["id"]] = bool(_v16(r, _gaps))
    except Exception as e:
        print(f"[filter-validation] counterfactual setup failed: {e}", flush=True)
        _base_ok = None

    results = []
    for rule in _BLOCK_RULES:
        method = "shape"
        try:
            variant = _rule_variant(rule["live_line"]) if rule.get("live_line") else None
            if rule.get("live_line") and variant is None:
                results.append({
                    "id": rule["id"], "name": rule["name"], "shipped": rule["shipped"],
                    "window_days": window_days, "n_blocked": 0, "verdict": "STALE-RULE",
                    "note": "live_filter source line no longer matches — rule "
                            "definition here is out of date, numbers withheld",
                })
                continue
            if variant is not None and _base_ok is not None:
                blocked = [r for r in all_rows
                           if not _base_ok.get(r["id"], True)
                           and variant.passes_v16(r, _gaps)]
                method = "counterfactual"
            elif rule.get("post_gate"):
                # A real_trader gate (e.g. the Friday block) runs AFTER the filter,
                # so its blocked set is drawn from signals that PASS V16 — the
                # opposite of a live_filter rule. Requiring "fails V16" here would
                # always return zero.
                blocked = [r for r in all_rows if rule["predicate"](r)]
                method = "post-gate"
            else:
                blocked = [r for r in all_rows if rule["predicate"](r)]
                if _base_ok is not None:
                    # even shape-matched rules must at least BE rejected by V16
                    blocked = [r for r in blocked if not _base_ok.get(r["id"], True)]
                    method = "shape+v16"
        except Exception as e:
            results.append({"id": rule["id"], "name": rule["name"], "error": str(e)})
            continue

        n = len(blocked)
        if n == 0:
            results.append({
                "id": rule["id"], "name": rule["name"], "shipped": rule["shipped"],
                "window_days": window_days, "n_blocked": 0,
                "verdict": "DORMANT", "method": method,
                "note": f"No matching signals fired in last {window_days} days",
            })
            continue

        pnls = [float(r["outcome_pnl"]) for r in blocked]
        wins = sum(1 for p in pnls if p > 0)
        total = sum(pnls)
        avg = total / n
        wr = wins / n * 100

        # Verdict logic:
        #   - VALIDATED: total_pnl <= 0 (rule blocks net-losers → keep)
        #   - DEGRADING: total_pnl > 0 and avg > 1pt and n >= 5 (rule blocks winners → review)
        #   - WATCH: total_pnl > 0 but small sample or marginal (n < 5 or avg <= 1pt)
        if total <= 0:
            verdict = "VALIDATED"
        elif n >= 5 and avg > 1.0:
            verdict = "DEGRADING"
        else:
            verdict = "WATCH"

        results.append({
            "id": rule["id"], "name": rule["name"], "shipped": rule["shipped"],
            "window_days": window_days, "n_blocked": n,
            "win_count": wins, "wr": wr, "total_pnl_pt": total,
            "total_pnl_usd": total * 5, "avg_pnl_pt": avg,
            "verdict": verdict, "method": method,
        })

    return results


def format_telegram_summary(results: list[dict]) -> str:
    """Compact Telegram-friendly summary."""
    _wd = next((r.get("window_days") for r in results if r.get("window_days")), 90)
    lines = [f"🛡️ <b>Filter Validation — {datetime.now(NY).strftime('%Y-%m-%d')}</b>",
             f"Active block rules vs recent {_wd}-day data:\n"]
    has_alert = False
    for r in results:
        if r.get("error"):
            lines.append(f"⚠️ {r['id']} {r['name']}: ERROR {r['error']}")
            has_alert = True
            continue
        if r["verdict"] == "DORMANT":
            lines.append(f"💤 {r['id']} {r['name']}: dormant ({r['note']})")
            continue
        emoji = {"VALIDATED": "✅", "DEGRADING": "🚨", "WATCH": "👁️"}[r["verdict"]]
        if r["verdict"] == "DEGRADING":
            has_alert = True
        lines.append(
            f"{emoji} <b>{r['id']}</b> {r['name']}: "
            f"{r['n_blocked']}t · WR {r['wr']:.0f}% · "
            f"would have been {r['total_pnl_pt']:+.1f}pt (${r['total_pnl_usd']:+.0f}) · "
            f"{r['verdict']}"
        )
    if has_alert:
        lines.append("\n🚨 <b>Action needed</b>: review DEGRADING rules above.")
    return "\n".join(lines)


def run_today() -> None:
    """Scheduled wrapper — runs Mondays 17:00 ET."""
    now = datetime.now(NY)
    if now.weekday() != 0:  # Monday only
        return
    if not (dtime(16, 30) <= now.time() <= dtime(23, 59)):
        return
    try:
        results = evaluate_rules(window_days=90)  # 90d (was 30) — spans multiple vol-regimes so a single low-vol month doesn't false-flag DEGRADING (2026-06-30)
        # Persist to filter_validation_runs for trend tracking across weeks
        n_written = _persist_results(results)
        print(f"[filter-validation] weekly check ({len(results)} rules, {n_written} persisted):",
              flush=True)
        for r in results:
            print(f"  {r}", flush=True)
        msg = format_telegram_summary(results)
        # Telegram alert
        if TG_TOKEN and TG_CHAT:
            r = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT, "text": msg,
                      "parse_mode": "HTML", "disable_web_page_preview": "true"},
                timeout=10,
            )
            if r.status_code != 200:
                print(f"[filter-validation] telegram send failed: {r.status_code}", flush=True)
    except Exception as e:
        print(f"[filter-validation] run_today error: {e}", flush=True)


# CLI: python -m app.filter_validation [--days 30]
if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--persist", action="store_true",
                        help="Insert results into filter_validation_runs (skip for dry preview)")
    parser.add_argument("--history", type=str, default=None,
                        help="Show last 20 runs for given rule_id (e.g. S180)")
    args = parser.parse_args()
    db = os.environ.get("DATABASE_URL")
    if not db:
        raise SystemExit("DATABASE_URL required")
    eng = create_engine(db)
    init(eng)
    if args.history:
        rows = get_history(rule_id=args.history, limit=20)
        if not rows:
            print(f"No history for rule_id={args.history}")
        else:
            print(f"History for {args.history} (newest first):")
            for r in rows:
                print(f"  {r['run_ts']}  n={r['n_blocked']}  wr={r['wr_pct']}  "
                      f"pnl_pt={r['total_pnl_pt']}  verdict={r['verdict']}")
        raise SystemExit(0)
    results = evaluate_rules(window_days=args.days)
    print(format_telegram_summary(results))
    if args.persist:
        n = _persist_results(results)
        print(f"\n[persisted {n} rows to filter_validation_runs]")
    print()
    print("=== Full per-rule output ===")
    for r in results:
        print(r)
