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


def _classify_gap(state: dict[str, Any], gap_pts: float) -> str:
    """Map gap pattern to suspected root cause."""
    fill = state.get("fill_price")
    exit_p = state.get("stop_fill_price") or state.get("close_fill_price")
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
               rto.state
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '{target_date}'
          AND sl.setup_name IN ({setup_filter})
          AND rto.setup_log_id IS NOT NULL
        ORDER BY sl.ts
    """

    portal_total_pts = 0.0
    real_total_pts_by_acct: dict[str, float] = {a: 0.0 for a in ACCOUNTS}
    real_count_by_acct: dict[str, int] = {a: 0 for a in ACCOUNTS}
    flagged_count = 0

    with _engine.connect() as conn:
        from sqlalchemy import text
        rows = conn.execute(text(sql)).fetchall()

    for row in rows:
        lid = row[0]
        et = row[1]
        setup = row[2]
        direction = row[3]
        portal_pnl = float(row[4]) if row[4] is not None else 0.0
        state = row[6] or {}

        fill = state.get("fill_price")
        exit_p = state.get("stop_fill_price") or state.get("close_fill_price")
        acct = state.get("account_id", "?")
        is_long = direction.lower() in ("long", "bullish")

        portal_total_pts += portal_pnl

        if fill is None or exit_p is None:
            # Ghost candidate — no real exit recorded
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

        if is_long:
            real_pts = exit_p - fill
        else:
            real_pts = fill - exit_p

        if acct in real_total_pts_by_acct:
            real_total_pts_by_acct[acct] += real_pts
            real_count_by_acct[acct] += 1

        gap_pts = real_pts - portal_pnl
        gap_dollars = gap_pts * MES_DOLLAR_PER_PT

        if abs(gap_pts) >= GAP_FLAG_PTS:
            flagged_count += 1
            why = _classify_gap(state, gap_pts)
            gap_emoji = "🟢" if gap_dollars > 0 else "🔴"
            dir_arrow = "↗" if direction.lower() in ("long","bullish") else "↘"
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
    for acct in ACCOUNTS:
        broker_pnl = _broker_realized_pnl(acct)
        tracked_dollars = real_total_pts_by_acct[acct] * MES_DOLLAR_PER_PT
        n = real_count_by_acct[acct]
        label = acct_label.get(acct, acct[-4:])
        if broker_pnl is None:
            acct_lines.append(
                f"  • <b>{label}</b> ({acct[-4:]}): {n}t · tracked ${tracked_dollars:+.0f} · broker <i>API err</i>"
            )
            continue
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
        "tracked_real_dollars": sum(real_total_pts_by_acct.values()) * MES_DOLLAR_PER_PT,
    }

    # Header — combined day P&L from broker totals
    real_dollars = summary['tracked_real_dollars']
    day_emoji = "🟢" if real_dollars > 0 else ("🔴" if real_dollars < 0 else "⚪")

    # Build Telegram message
    if flagged_count > 0 or acct_flag:
        lines = [
            f"<b>📊 TSRT Daily Reconcile</b> · {target_date}",
            f"━━━━━━━━━━━━━━━━━━",
            f"{day_emoji} <b>Day P&amp;L: ${real_dollars:+.0f}</b> across {len(rows)} trades",
            f"<i>Portal sim: {portal_total_pts:+.1f}p (${portal_total_pts*MES_DOLLAR_PER_PT:+.0f})</i>",
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
