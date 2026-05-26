"""FIFO close-price reconciliation for real_trade_orders (S179).

Bot tracks `close_fill_price` per OID. TS books FIFO at the position level.
When >= 2 positions are open on the same account and >= 2 closes fire
near-simultaneously, the bot's OID-based per-lid attribution diverges from
broker FIFO truth. Broker totals (BalanceDetail.RealizedProfitLoss) stay
correct; only per-setup attribution is shuffled.

This module re-pairs per-lid close prices to FIFO order using broker truth.
Runs daily at 16:15 ET (alongside S81 trade_reconcile). Live trading code
untouched. Idempotent: re-running yields the same result.

Audit fields added to state on first reconcile:
  - close_fill_price                    (overwritten with FIFO truth)
  - close_fill_price_pre_fifo_reconcile (preserves bot's per-OID value)
  - fifo_close_oid                      (broker order ID FIFO-paired to lid)
  - fifo_reconciled_at                  (ISO timestamp)

Self-contained. No imports from main.py. Receives engine, get_token_fn,
send_telegram_fn via init(). Mirrors trade_reconcile.py module style.
"""
from __future__ import annotations

import json
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

NY = ZoneInfo("America/New_York")
REAL_BASE = "https://api.tradestation.com/v3"

# Hardcoded account direction map. Matches real_trader._ACCOUNT_DIRECTION.
# Longs account exits via Sell, shorts account exits via Buy.
_ACCOUNT_EXIT_SIDE = {
    "210VYX65": "Sell",  # longs
    "210VYX91": "Buy",   # shorts
}
ACCOUNTS = tuple(_ACCOUNT_EXIT_SIDE.keys())

_engine = None
_get_token = None
_send_telegram = None


def init(engine, get_token_fn, send_telegram_fn) -> None:
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn


def _ts_get(path: str) -> dict | None:
    if not _get_token:
        return None
    try:
        token = _get_token()
        r = requests.get(REAL_BASE + path, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if r.status_code != 200:
            return None
        return r.json() if r.text else None
    except Exception as e:
        print(f"[fifo-reconcile] TS GET error: {e}", flush=True)
        return None


def _get_filled_exits(acct: str, date_et: str) -> list[dict]:
    """Pull all FLL MES exit orders on this account for the given ET date.

    Combines /orders (intraday live) + /historicalorders (today often appears
    in /orders only — TS quirk noted in real_trader._get_order_fill_price).
    Filters to FLL + exit-side (Sell for longs acct, Buy for shorts acct) + MES
    symbol + ClosedDateTime starting with the target date.
    """
    exit_side = _ACCOUNT_EXIT_SIDE.get(acct)
    if not exit_side:
        return []
    live = _ts_get(f"/brokerage/accounts/{acct}/orders?pageSize=600") or {}
    # For historical backfill we need wider window — pull `since` = (target_date - 1)
    # so even days many weeks back work. TS API supports up to ~90 days.
    try:
        d_anchor = datetime.strptime(date_et, "%Y-%m-%d") - timedelta(days=1)
        since_str = d_anchor.strftime("%m-%d-%Y")
    except Exception:
        since_str = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%m-%d-%Y")
    hist = _ts_get(
        f"/brokerage/accounts/{acct}/historicalorders?since={since_str}&pageSize=600"
    ) or {}
    by_oid: dict[str, dict] = {}
    for o in (live.get("Orders") or []) + (hist.get("Orders") or []):
        oid = str(o.get("OrderID") or "")
        if oid and oid not in by_oid:
            by_oid[oid] = o

    exits: list[dict] = []
    for o in by_oid.values():
        if o.get("Status") != "FLL":
            continue
        legs = o.get("Legs") or [{}]
        if not legs:
            continue
        leg = legs[0]
        if leg.get("BuyOrSell") != exit_side:
            continue
        if "MES" not in str(leg.get("Symbol", "")):
            continue
        cd = o.get("ClosedDateTime") or ""
        if not cd.startswith(date_et):
            continue
        # Pull execution price from the leg
        ep = leg.get("ExecutionPrice") or o.get("FilledPrice")
        try:
            ep = float(ep) if ep is not None else None
        except (TypeError, ValueError):
            ep = None
        if ep is None or ep <= 0:
            continue
        exits.append({
            "oid": str(o.get("OrderID")),
            "execution_price": ep,
            "closed_at": cd,
            "order_type": o.get("OrderType"),
        })
    exits.sort(key=lambda x: x["closed_at"])  # chronological broker fill order
    return exits


def _get_closed_lids(acct: str, date_et: str) -> list[tuple[int, dict]]:
    """Pull closed real_trade_orders for (acct, date), sorted by ts_placed ASC
    (FIFO entry order)."""
    if _engine is None:
        return []
    from datetime import date as _date_cls
    target = _date_cls.fromisoformat(date_et)
    with _engine.begin() as cx:
        rows = cx.execute(text("""
            SELECT setup_log_id, state
            FROM real_trade_orders
            WHERE state->>'account_id' = :acct
              AND state->>'status' = 'closed'
              AND state->>'fill_price' IS NOT NULL
              AND state->>'ts_placed' IS NOT NULL
              AND (state->>'ts_placed')::timestamp::date = :target
            ORDER BY (state->>'ts_placed')::timestamp
        """), {"acct": acct, "target": target}).fetchall()
    out: list[tuple[int, dict]] = []
    for sid, st in rows:
        if isinstance(st, str):
            try:
                st = json.loads(st)
            except Exception:
                continue
        try:
            float(st.get("fill_price"))
        except (TypeError, ValueError):
            continue
        out.append((sid, st))
    return out


def reconcile_account_date(acct: str, date_et: str, dry_run: bool = False) -> dict[str, Any]:
    """Run FIFO reconcile for one (account, date). Returns summary dict.

    Pairing rule: lids sorted by entry time ASC <--> exits sorted by close time ASC.
    Index-by-index match. Aborts loudly on count mismatch (refuses to mis-pair).
    """
    summary: dict[str, Any] = {
        "acct": acct, "date": date_et,
        "lids_count": 0, "exits_count": 0,
        "changes": [], "warnings": [], "errors": [],
    }
    if _engine is None:
        summary["errors"].append("module not initialized (no engine)")
        return summary

    try:
        lids = _get_closed_lids(acct, date_et)
        exits = _get_filled_exits(acct, date_et)
    except Exception as e:
        summary["errors"].append(f"data fetch failed: {e}")
        return summary

    summary["lids_count"] = len(lids)
    summary["exits_count"] = len(exits)

    if not lids:
        return summary  # nothing to do

    if len(lids) != len(exits):
        summary["warnings"].append(
            f"COUNT MISMATCH: {len(lids)} closed lids vs {len(exits)} FLL exits — "
            f"refusing to FIFO-pair (would mis-attribute). Investigate manually."
        )
        return summary

    # FIFO pairing: lids[i] (oldest entry) <-- exits[i] (earliest close)
    pairs = list(zip(lids, exits))
    for (lid, state), exit_o in pairs:
        fifo_price = exit_o["execution_price"]
        fifo_oid = exit_o["oid"]
        current = state.get("close_fill_price")
        try:
            current_f = float(current) if current is not None else None
        except (TypeError, ValueError):
            current_f = None

        if current_f is not None and abs(current_f - fifo_price) < 0.001:
            continue  # already correct — idempotent skip

        change = {
            "lid": lid,
            "setup": state.get("setup_name"),
            "direction": state.get("direction"),
            "fill_price": state.get("fill_price"),
            "old_close": current,
            "new_close": fifo_price,
            "old_oid": state.get("close_order_id") or state.get("stop_order_id"),
            "new_oid": fifo_oid,
            "closed_at": exit_o["closed_at"],
        }
        summary["changes"].append(change)

        if not dry_run:
            try:
                # Preserve original (only on first reconcile — don't overwrite audit trail)
                if "close_fill_price_pre_fifo_reconcile" not in state:
                    state["close_fill_price_pre_fifo_reconcile"] = current
                state["close_fill_price"] = fifo_price
                state["fifo_close_oid"] = fifo_oid
                state["fifo_reconciled_at"] = datetime.now(NY).isoformat()
                with _engine.begin() as cx:
                    cx.execute(text(
                        "UPDATE real_trade_orders SET state = :st, updated_at = NOW() "
                        "WHERE setup_log_id = :sid"
                    ), {"st": json.dumps(state), "sid": lid})
            except Exception as e:
                summary["errors"].append(f"DB update failed for lid={lid}: {e}")

    return summary


def reconcile_today() -> list[dict[str, Any]]:
    """Run FIFO reconcile for all whitelisted accounts for today (ET).
    Called by the daily 16:15 ET cron. Idempotent.
    """
    today_et = datetime.now(NY).strftime("%Y-%m-%d")
    summaries: list[dict[str, Any]] = []
    for acct in ACCOUNTS:
        s = reconcile_account_date(acct, today_et, dry_run=False)
        summaries.append(s)
        if s.get("changes"):
            print(f"[fifo-reconcile] {acct} {today_et}: {len(s['changes'])} lids rewritten "
                  f"(out of {s['lids_count']} closed)", flush=True)
            for ch in s["changes"]:
                print(f"  lid={ch['lid']} {ch['setup']} "
                      f"{ch['old_close']} -> {ch['new_close']} (oid {ch['new_oid']})",
                      flush=True)
        for w in s.get("warnings", []):
            print(f"[fifo-reconcile] WARN {acct} {today_et}: {w}", flush=True)
            _alert_telegram(f"⚠️ FIFO reconcile WARN on {acct} {today_et}\n{w}")
        for e in s.get("errors", []):
            print(f"[fifo-reconcile] ERROR {acct} {today_et}: {e}", flush=True)
    return summaries


def reconcile_history(start_date: str, end_date: str,
                     dry_run: bool = False) -> list[dict[str, Any]]:
    """Backfill FIFO reconcile across a date range. Weekdays only."""
    summaries: list[dict[str, Any]] = []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri only
            date_str = d.strftime("%Y-%m-%d")
            for acct in ACCOUNTS:
                try:
                    summaries.append(reconcile_account_date(acct, date_str, dry_run))
                except Exception as e:
                    summaries.append({
                        "acct": acct, "date": date_str,
                        "errors": [f"unhandled: {e}"],
                        "changes": [], "warnings": [],
                        "lids_count": 0, "exits_count": 0,
                    })
        d += timedelta(days=1)
    return summaries


def run_today() -> None:
    """Scheduled wrapper — guard against weekends and pre-close runs."""
    now = datetime.now(NY)
    if now.weekday() >= 5:
        return
    # Guard adjusted 2026-05-27: was dtime(16,10) which silently rejected the
    # 16:03 ET cron schedule (lid=3211 didn't auto-repair Tue). Match scheduler.
    if not (dtime(16, 0) <= now.time() <= dtime(23, 59)):
        return  # only after market close
    # Env gate: ship behind FIFO_RECONCILE_ENABLED (default false for first day observation)
    import os
    if os.getenv("FIFO_RECONCILE_ENABLED", "false").lower() != "true":
        print("[fifo-reconcile] disabled via env (set FIFO_RECONCILE_ENABLED=true to activate)",
              flush=True)
        return
    try:
        reconcile_today()
    except Exception as e:
        print(f"[fifo-reconcile] run_today error: {e}", flush=True)


def _alert_telegram(msg: str) -> None:
    if not _send_telegram:
        return
    try:
        _send_telegram(msg)
    except Exception:
        pass


# CLI entry for manual backfill: python -m app.fifo_reconcile --backfill START END [--dry]
if __name__ == "__main__":
    import argparse, os
    from sqlalchemy import create_engine

    parser = argparse.ArgumentParser(description="FIFO close-price reconcile (S179)")
    parser.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                        help="Date range YYYY-MM-DD YYYY-MM-DD")
    parser.add_argument("--today", action="store_true", help="Run today only")
    parser.add_argument("--dry", action="store_true", help="Dry run (no DB writes)")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL env var required")
    engine = create_engine(db_url)

    def _token_fn():
        # Lazy refresh for CLI usage
        import socket
        import urllib3.util.connection as urllib3_cn
        urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
        r = requests.post("https://signin.tradestation.com/oauth/token", data={
            "grant_type": "refresh_token",
            "client_id": os.environ["TS_CLIENT_ID"],
            "client_secret": os.environ["TS_CLIENT_SECRET"],
            "refresh_token": os.environ["TS_REFRESH_TOKEN"],
        }, timeout=60)
        r.raise_for_status()
        return r.json()["access_token"]

    init(engine, _token_fn, None)

    if args.today:
        summaries = [reconcile_account_date(a,
                     datetime.now(NY).strftime("%Y-%m-%d"), dry_run=args.dry) for a in ACCOUNTS]
    elif args.backfill:
        summaries = reconcile_history(args.backfill[0], args.backfill[1], dry_run=args.dry)
    else:
        parser.print_help()
        raise SystemExit(1)

    total_changes = 0
    total_lids = 0
    days_with_changes = 0
    days_seen: set[tuple[str, str]] = set()
    for s in summaries:
        key = (s["acct"], s["date"])
        days_seen.add(key)
        total_lids += s.get("lids_count", 0)
        if s.get("changes"):
            days_with_changes += 1
            total_changes += len(s["changes"])
            print(f"\n=== {s['acct']} {s['date']} ({s['lids_count']} lids) ===")
            for ch in s["changes"]:
                shift = ch["new_close"] - (ch["old_close"] or ch["new_close"])
                print(f"  lid={ch['lid']:5d} {ch['setup']:25s} {ch['direction']:8s} "
                      f"close {ch['old_close']} -> {ch['new_close']} "
                      f"(shift {shift:+.2f}) oid={ch['new_oid']}")
        for w in s.get("warnings", []):
            print(f"  WARN {s['acct']} {s['date']}: {w}")
        for e in s.get("errors", []):
            print(f"  ERROR {s['acct']} {s['date']}: {e}")

    mode = "DRY-RUN" if args.dry else "APPLIED"
    print(f"\n=== {mode}: {total_changes} lid rewrites across {days_with_changes} acct-days "
          f"(of {len(days_seen)} acct-days seen, {total_lids} total lids) ===")
