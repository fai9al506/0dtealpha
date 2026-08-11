"""Daily portal-vs-real trade reconcile (S81).

Runs at 16:15 ET each trading day. For each whitelisted real-trader trade
today, joins setup_log (portal SPX P&L) with real_trade_orders (broker MES
fills) and flags any trade with abs(gap) >= 4 pts ($20 MES). Also reconciles
each broker account's RealizedProfitLoss vs sum of tracked trades.

Trigger built after May 4 2026 audit revealed -$262 of hidden bug cost
(lid=2447 wrong-side instant fill, lid=2433 ghost orphan, lid=2449 stop reject)
that was invisible at daily-total level. Per-trade reconcile catches the same
pattern same-day instead of weeks later.

Self-contained module. No imports from main.py. Receives `engine`,
`get_token_fn`, `send_telegram_fn` via init().
"""
from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Any
from zoneinfo import ZoneInfo

import requests

NY = ZoneInfo("America/New_York")
MES_DOLLAR_PER_PT = 5.0
GAP_FLAG_PTS = 4.0  # flag trades with abs(gap) >= this in points
ACCT_GAP_FLAG_DOLLARS = 20.0
WHITELIST_SETUPS = (
    "Skew Charm",
    "AG Short",
    "Vanna Pivot Bounce",
    "VIX Divergence",
    "ES Absorption",
    "DD Exhaustion",
)
ACCOUNTS = ("210VYX65", "210VYX91")
REAL_BASE = "https://api.tradestation.com/v3"

_engine = None
_get_token = None
_send_telegram = None


def init(engine, get_token_fn, send_telegram_fn) -> None:
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn


def _broker_realized_pnl(account_id: str) -> float | None:
    """Pull broker daily realized P&L (matches what _day_line uses)."""
    if not _get_token:
        return None
    try:
        token = _get_token()
        r = requests.get(
            f"{REAL_BASE}/brokerage/accounts/{account_id}/balances",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        balances = data.get("Balances", [])
        if isinstance(balances, list) and balances:
            b = balances[0]
        elif isinstance(balances, dict):
            b = balances
        else:
            return None
        detail = b.get("BalanceDetail", {}) or {}
        val = detail.get("RealizedProfitLoss")
        return float(val) if val is not None else None
    except Exception as e:
        print(f"[reconcile] broker P&L error {account_id}: {e}", flush=True)
        return None


def _own_exit_price(state: dict[str, Any]) -> float | None:
    """Per-lid exit price reflecting THIS signal's own trail/stop — NOT the
    FIFO-reshuffled value.

    On multi-concurrent days, app/fifo_reconcile.py (S210) overwrites
    `close_fill_price` / `stop_fill_price` with broker-FIFO-paired prices that
    are shuffled ACROSS the concurrent lids. The day/account totals stay
    conserved, but a single lid's `close_fill_price` can then show another
    lid's exit. For the per-lid gap comparison we want each signal's OWN exit,
    so prefer the `*_pre_fifo_reconcile` audit fields when present and fall
    back to the live fields otherwise. (Stop fill takes precedence over close
    fill, mirroring the consumer order used elsewhere.)
    """
    for k in ("stop_fill_price_pre_fifo_reconcile", "stop_fill_price",
              "close_fill_price_pre_fifo_reconcile", "close_fill_price"):
        v = state.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _was_fifo_reshuffled(state: dict[str, Any]) -> bool:
    """True if FIFO reconcile actually MOVED this lid's exit price (i.e. the
    pre-reconcile value differs from the live value). Only happens on
    multi-concurrent days where per-lid attribution is shuffled."""
    for live_k, pre_k in (("stop_fill_price", "stop_fill_price_pre_fifo_reconcile"),
                          ("close_fill_price", "close_fill_price_pre_fifo_reconcile")):
        pre = state.get(pre_k)
        live = state.get(live_k)
        if pre is None or live is None:
            continue
        try:
            if abs(float(pre) - float(live)) > 0.001:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _concurrent_lids(rows) -> set[int]:
    """Find lids that ran CONCURRENTLY with another lid in the SAME account on
    the same ET day (open intervals overlap). Those positions NET together at
    the broker, so per-lid execution-gap flags are not meaningful (FIFO
    attribution shuffles the per-lid exit). Returns the set of such lids.

    Interval = [entry_et, entry_et + outcome_elapsed_min]. Entry comes from
    setup_log.ts; duration from setup_log.outcome_elapsed_min (fallback 0 when
    null → treated as an instantaneous point, still overlap-detectable).
    """
    # Build per-account list of (lid, start_min, end_min) using minutes-of-day.
    by_acct: dict[str, list[tuple[int, float, float]]] = {}
    for row in rows:
        lid = row[0]
        et = row[1]
        state = row[6] or {}
        acct = state.get("account_id", "?")
        if et is None:
            continue
        try:
            start_min = et.hour * 60 + et.minute + et.second / 60.0
        except Exception:
            continue
        elapsed = row[7]
        try:
            dur = float(elapsed) if elapsed is not None else 0.0
        except (TypeError, ValueError):
            dur = 0.0
        end_min = start_min + max(dur, 0.0)
        by_acct.setdefault(acct, []).append((lid, start_min, end_min))

    concurrent: set[int] = set()
    for acct, intervals in by_acct.items():
        if acct == "?" or len(intervals) < 2:
            continue
        for i in range(len(intervals)):
            lid_i, s_i, e_i = intervals[i]
            for j in range(len(intervals)):
                if i == j:
                    continue
                lid_j, s_j, e_j = intervals[j]
                # overlap if one starts before the other ends (inclusive)
                if s_i <= e_j and s_j <= e_i:
                    concurrent.add(lid_i)
                    concurrent.add(lid_j)
                    break
    return concurrent


def _classify_gap(state: dict[str, Any], gap_pts: float) -> str:
    """Map gap pattern to suspected root cause."""
    fill = state.get("fill_price")
    # Use THIS lid's own exit (pre-FIFO-reconcile when present) for the
    # wrong-side fingerprint — the FIFO-shuffled value belongs to a sibling.
    exit_p = _own_exit_price(state)
    reason = state.get("close_reason", "")
    direction = state.get("direction", "").lower()
    is_long = direction in ("long", "bullish")

    if exit_p is None or fill is None:
        return "no exit recorded → ghost candidate (broker may still hold position)"
    # Wrong-side instant fill: stop on wrong side of entry, fired within seconds
    if not is_long and exit_p < fill:
        # short with exit BELOW entry — only legit if trail was earned
        if gap_pts < -3:
            return "instant-fill at wrong-side stop (S80 wrong-side bug fingerprint)"
    if is_long and exit_p > fill:
        if gap_pts < -3:
            return "instant-fill at wrong-side stop (S80 wrong-side bug fingerprint)"
    if reason == "stop_rejected_async":
        return "broker rejected stop → market-closed near entry"
    if reason in ("modify_rejected", "trail_market_exit"):
        return "trail modify failed → market-closed at adverse price"
    if reason == "ghost_reconcile":
        return "ghost: bot thought flat, broker had position"
    if reason == "eod_flatten":
        return "EOD flatten — gap normal if mid-trade at close"
    return "basis drift / SPX-vs-MES execution gap (no specific bug pattern)"


def run_reconcile(target_date: str | None = None) -> dict[str, Any]:
    """Run the reconcile for a given trade_date (default = today ET).
    Returns summary dict. Sends Telegram if any flag found."""
    if _engine is None:
        return {"error": "not initialized"}

    if target_date is None:
        target_date = datetime.now(NY).strftime("%Y-%m-%d")

    flags: list[str] = []
    trade_lines: list[str] = []

    setup_filter = ",".join(f"'{s}'" for s in WHITELIST_SETUPS)

    sql = f"""
        SELECT sl.id,
               (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.setup_name, sl.direction, sl.outcome_pnl, sl.outcome_max_profit,
               rto.state, sl.outcome_elapsed_min
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '{target_date}'
          AND sl.setup_name IN ({setup_filter})
          AND rto.setup_log_id IS NOT NULL
        ORDER BY sl.ts
    """

    portal_total_pts = 0.0
    real_total_pts_by_acct: dict[str, float] = {a: 0.0 for a in ACCOUNTS}
    # Dollars, not points × $5 — basket sizing means a lid can be 2 MES, and points
    # alone silently halve those trades. Before this (2026-08-11) every 2x day raised a
    # phantom account ⚠️: 2026-08-11 shorts read "broker $+303 · tracked $+155" — $155
    # was the same three trades priced at 1 MES. Nothing was wrong with the money.
    real_total_dollars_by_acct: dict[str, float] = {a: 0.0 for a in ACCOUNTS}
    real_count_by_acct: dict[str, int] = {a: 0 for a in ACCOUNTS}
    flagged_count = 0

    with _engine.connect() as conn:
        from sqlalchemy import text
        rows = conn.execute(text(sql)).fetchall()

    # Identify lids that ran concurrently with a sibling in the same account.
    # Their per-lid exit price is FIFO-reshuffled (S210), so the per-lid gap is
    # not meaningful — we suppress/relabel execution-gap flags for them.
    concurrent_lids = _concurrent_lids(rows)

    for row in rows:
        lid = row[0]
        et = row[1]
        setup = row[2]
        direction = row[3]
        portal_pnl = float(row[4]) if row[4] is not None else 0.0
        state = row[6] or {}

        fill = state.get("fill_price")
        # Per-lid comparison uses THIS signal's OWN exit (pre-FIFO-reconcile when
        # present); the account total below uses the FIFO-correct live value.
        exit_p = _own_exit_price(state)
        acct_exit_p = state.get("stop_fill_price") or state.get("close_fill_price")
        acct = state.get("account_id", "?")
        is_long = direction.lower() in ("long", "bullish")
        is_concurrent = lid in concurrent_lids
        # Contracts actually filled (basket sizing gives 2 on confirmed days). Default 1
        # so a missing/garbage value under-states rather than invents money.
        try:
            qty = int(state.get("quantity") or 1)
        except (TypeError, ValueError):
            qty = 1
        if qty < 1:
            qty = 1

        portal_total_pts += portal_pnl

        # Count this trade against its account regardless of ghost status (2026-05-06 fix:
        # was excluding ghosts, causing per-account count != header count).
        if acct in real_count_by_acct:
            real_count_by_acct[acct] += 1

        if fill is None or exit_p is None:
            # Ghost candidate — no real exit recorded (count already incremented above)
            gap_pts = -portal_pnl  # treat as full miss
            flagged_count += 1
            why = _classify_gap(state, gap_pts)
            trade_lines.append(
                f"⚠️ lid={lid} {setup} {direction[:1].upper()} "
                f"acct={acct[-4:]}\n"
                f"   portal={portal_pnl:+.1f}p, real=NO_EXIT, gap=??\n"
                f"   → {why}"
            )
            continue

        # Per-lid real P&L (own exit) for the gap comparison.
        if is_long:
            real_pts = exit_p - fill
        else:
            real_pts = fill - exit_p

        # Per-account total uses the FIFO-correct (conserved) exit price so the
        # account/day totals stay exactly right even on concurrent days.
        if acct in real_total_pts_by_acct and acct_exit_p is not None:
            try:
                acct_exit_f = float(acct_exit_p)
                acct_real_pts = (acct_exit_f - fill) if is_long else (fill - acct_exit_f)
                real_total_pts_by_acct[acct] += acct_real_pts
                real_total_dollars_by_acct[acct] += acct_real_pts * qty * MES_DOLLAR_PER_PT
            except (TypeError, ValueError):
                pass

        gap_pts = real_pts - portal_pnl
        # Dollar impact is what the account actually felt, so scale by size.
        gap_dollars = gap_pts * qty * MES_DOLLAR_PER_PT

        if abs(gap_pts) >= GAP_FLAG_PTS:
            if is_concurrent:
                # On concurrent days the per-lid exit attribution is FIFO-shuffled
                # across siblings → a per-lid "gap" is an artifact, not an
                # execution problem. Relabel clearly instead of crying basis drift.
                flagged_count += 1
                dir_arrow = "↗" if is_long else "↘"
                why = ("FIFO-attribution (per-lid P&amp;L not meaningful on "
                       "concurrent days; trust the day/account total)")
                trade_lines.append(
                    f"🔵 <b>{setup}</b> {dir_arrow} #{lid} <i>{acct[-4:]}</i>\n"
                    f"   Portal {portal_pnl:+.1f}p  →  Lid-exit {real_pts:+.1f}p  "
                    f"<b>(Δ ${gap_dollars:+.0f})</b>\n"
                    f"   <i>{why}</i>"
                )
            else:
                flagged_count += 1
                why = _classify_gap(state, gap_pts)
                gap_emoji = "🟢" if gap_dollars > 0 else "🔴"
                dir_arrow = "↗" if is_long else "↘"
                trade_lines.append(
                    f"{gap_emoji} <b>{setup}</b> {dir_arrow} #{lid} <i>{acct[-4:]}</i>\n"
                    f"   Portal {portal_pnl:+.1f}p  →  Real {real_pts:+.1f}p  "
                    f"<b>(gap ${gap_dollars:+.0f})</b>\n"
                    f"   <i>{why}</i>"
                )

    # Account-level reconcile vs broker
    acct_lines: list[str] = []
    acct_flag = False
    acct_label = {'210VYX65': 'longs', '210VYX91': 'shorts'}
    total_broker = 0.0
    broker_total_available = False
    for acct in ACCOUNTS:
        broker_pnl = _broker_realized_pnl(acct)
        tracked_dollars = real_total_dollars_by_acct[acct]
        n = real_count_by_acct[acct]
        label = acct_label.get(acct, acct[-4:])
        if broker_pnl is None:
            acct_lines.append(
                f"  • <b>{label}</b> ({acct[-4:]}): {n}t · tracked ${tracked_dollars:+.0f} · broker <i>API err</i>"
            )
            continue
        broker_total_available = True
        total_broker += broker_pnl
        diff = broker_pnl - tracked_dollars
        marker = ""
        if abs(diff) >= ACCT_GAP_FLAG_DOLLARS:
            acct_flag = True
            marker = " ⚠️"
        broker_emoji = "🟢" if broker_pnl >= 0 else "🔴"
        acct_lines.append(
            f"  {broker_emoji} <b>{label}</b> ({acct[-4:]}): {n}t · "
            f"broker <b>${broker_pnl:+.0f}</b> · tracked ${tracked_dollars:+.0f} · diff ${diff:+.0f}{marker}"
        )

    summary = {
        "date": target_date,
        "trades": len(rows),
        "portal_total_pts": portal_total_pts,
        "flagged_trades": flagged_count,
        "tracked_real_dollars": sum(real_total_dollars_by_acct.values()),
        "broker_real_dollars": total_broker,
    }

    # Header — use broker total (truth, includes ghosts) when available;
    # fallback to bot-tracked sum if broker API errored. (2026-05-06 fix: was using
    # bot-tracked which excludes ghost trades, hid -$107 ghost loss in May 6 reconcile.)
    real_dollars = total_broker if broker_total_available else summary['tracked_real_dollars']
    day_emoji = "🟢" if real_dollars > 0 else ("🔴" if real_dollars < 0 else "⚪")

    # Build Telegram message
    if flagged_count > 0 or acct_flag:
        lines = [
            f"<b>📊 TSRT Daily Reconcile</b> · {target_date}",
            f"━━━━━━━━━━━━━━━━━━",
            f"{day_emoji} <b>Day P&amp;L: ${real_dollars:+.0f}</b> across {len(rows)} trades",
            # Portal sim is a per-1-MES measure by design — label it, so it is not read
            # as a broker-$ figure sitting one line above the real per-account dollars.
            f"<i>Portal sim: {portal_total_pts:+.1f}p (${portal_total_pts*MES_DOLLAR_PER_PT:+.0f} @1 MES)</i>",
            "",
        ]
        if trade_lines:
            lines.append(f"<b>⚠️ Flagged ({flagged_count} trade{'s' if flagged_count!=1 else ''}):</b>")
            lines.extend(trade_lines)
            lines.append("")
        lines.append(f"<b>📋 Per account:</b>")
        lines.extend(acct_lines)

        msg = "\n".join(lines)
        if _send_telegram:
            try:
                _send_telegram(msg)
            except Exception as e:
                print(f"[reconcile] telegram send error: {e}", flush=True)
        print(f"[reconcile] {target_date} flagged={flagged_count} acct_flag={acct_flag}",
              flush=True)
    else:
        print(f"[reconcile] {target_date} clean: {len(rows)} trades, "
              f"no gaps >= ${GAP_FLAG_PTS * MES_DOLLAR_PER_PT}",
              flush=True)
        # Send a brief clean-day confirmation so user knows it ran
        if _send_telegram and len(rows) > 0:
            try:
                _send_telegram(
                    f"<b>✅ TSRT Reconcile</b> · {target_date}\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"{day_emoji} <b>Day P&amp;L: ${real_dollars:+.0f}</b> across {len(rows)} trades · "
                    f"All gaps clean (&lt; ${GAP_FLAG_PTS * MES_DOLLAR_PER_PT:.0f})"
                )
            except Exception:
                pass

    return summary


def run_today() -> None:
    """Scheduled wrapper — guard against weekends/holidays (no trades)."""
    now = datetime.now(NY)
    if now.weekday() >= 5:
        return  # Sat/Sun
    if not (dtime(16, 0) <= now.time() <= dtime(23, 59)):
        return  # only run after market close
    try:
        run_reconcile()
    except Exception as e:
        print(f"[reconcile] run_today error: {e}", flush=True)
