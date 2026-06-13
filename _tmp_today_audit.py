"""Tue 2026-05-26 — TSRT day audit. Did we hit $391? What drove it?"""
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

conn = psycopg2.connect(DB); cur = conn.cursor()

# Today's real trades
print("=" * 100)
print("TSRT real_trade_orders TODAY (2026-05-26):")
print("=" * 100)
cur.execute("""
    SELECT sl.id, sl.setup_name, sl.direction, sl.grade, sl.paradigm,
           sl.greek_alignment, sl.vix,
           (sl.ts AT TIME ZONE 'America/New_York') AS et_ts,
           sl.outcome_result, sl.outcome_pnl,
           rto.state->>'fill_price' AS fill,
           rto.state->>'close_fill_price' AS close_fp,
           rto.state->>'account_id' AS acct,
           rto.state->>'close_reason' AS close_reason
    FROM setup_log sl
    JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE sl.ts::date = '2026-05-26'
    ORDER BY sl.ts
""")

trades = []
for r in cur.fetchall():
    lid, setup, direction, grade, para, align, vix, et_ts, out, pnl, fill, close_fp, acct, reason = r
    is_long = direction in ("long","bullish")
    sign = 1 if is_long else -1
    real_pnl_pt = None
    real_pnl_usd = None
    if fill and close_fp:
        real_pnl_pt = sign * (float(close_fp) - float(fill))
        real_pnl_usd = real_pnl_pt * 5
    trades.append({
        "lid": lid, "setup": setup, "dir": direction, "grade": grade, "para": para,
        "align": align, "vix": vix, "et_ts": et_ts, "outcome": out,
        "portal_pnl": float(pnl) if pnl is not None else None,
        "fill": float(fill) if fill else None, "close_fp": float(close_fp) if close_fp else None,
        "real_pnl_pt": real_pnl_pt, "real_pnl_usd": real_pnl_usd,
        "acct": acct, "reason": reason,
    })

print(f"Total real trades today: {len(trades)}\n")
print(f"{'lid':>5s} {'time':>5s} {'setup':22s} {'dir':6s} {'g':4s} {'paradigm':18s} "
      f"{'align':>5s} {'fill':>8s} {'close':>8s} {'real$':>8s} {'reason':24s}")
total_real_usd = 0.0
total_portal_pt = 0.0
for t in trades:
    real_usd_s = f"{t['real_pnl_usd']:+.2f}" if t['real_pnl_usd'] is not None else "—"
    fill_s = f"{t['fill']:.2f}" if t['fill'] else "—"
    close_s = f"{t['close_fp']:.2f}" if t['close_fp'] else "—"
    print(f"{t['lid']:5d} {t['et_ts'].strftime('%H:%M'):>5s} {t['setup']:22s} "
          f"{t['dir']:6s} {t['grade'] or '-':4s} {(t['para'] or '-'):18s} "
          f"{(str(t['align']) if t['align'] is not None else '-'):>5s} "
          f"{fill_s:>8s} {close_s:>8s} {real_usd_s:>8s} {(t['reason'] or '-')[:24]:24s}")
    if t['real_pnl_usd'] is not None:
        total_real_usd += t['real_pnl_usd']
    if t['portal_pnl'] is not None:
        total_portal_pt += t['portal_pnl']

print(f"\nTOTAL real broker (computed from fill/close): ${total_real_usd:+.2f}")
print(f"TOTAL portal sim (SPX outcome_pnl): {total_portal_pt:+.1f}pt = ${total_portal_pt*5:+.2f}")

# Broker truth check via BalanceDetail per account
print("\n" + "=" * 100)
print("Broker realized P&L by account (TS BalanceDetail.RealizedProfitLoss):")
print("=" * 100)

# By setup
print("\nBy setup:")
from collections import defaultdict
by_setup = defaultdict(list)
for t in trades:
    if t['real_pnl_usd'] is not None:
        by_setup[t['setup']].append(t)
for setup, ts in sorted(by_setup.items(), key=lambda x: -sum(t['real_pnl_usd'] for t in x[1])):
    total = sum(t['real_pnl_usd'] for t in ts)
    wins = sum(1 for t in ts if t['real_pnl_usd'] > 0)
    print(f"  {setup:22s} {len(ts)}t  {wins}W  ${total:+.2f}")

# By paradigm
print("\nBy paradigm:")
by_para = defaultdict(list)
for t in trades:
    if t['real_pnl_usd'] is not None:
        by_para[t['para'] or 'NULL'].append(t)
for para, ts in sorted(by_para.items(), key=lambda x: -sum(t['real_pnl_usd'] for t in x[1])):
    total = sum(t['real_pnl_usd'] for t in ts)
    wins = sum(1 for t in ts if t['real_pnl_usd'] > 0)
    print(f"  {para:18s} {len(ts)}t  {wins}W  ${total:+.2f}")

# What S180 blocked today (GEX-TARGET PM longs)
print("\n" + "=" * 100)
print("S180 GEX-TARGET PM long block — did it fire today?")
print("=" * 100)
cur.execute("""
    SELECT id, setup_name, direction, paradigm,
           (ts AT TIME ZONE 'America/New_York') AS et_ts,
           outcome_pnl, real_trade_skip_reason
    FROM setup_log
    WHERE ts::date = '2026-05-26'
      AND direction IN ('long','bullish')
      AND paradigm = 'GEX-TARGET'
      AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption')
    ORDER BY ts
""")
gt_pm = cur.fetchall()
for r in gt_pm:
    et = r[4]
    hr_ge_13 = et.hour >= 13
    print(f"  lid={r[0]} {r[1]:22s} {r[2]:6s} et={et.strftime('%H:%M')} "
          f"would-be-blocked={hr_ge_13}  portal_pnl={float(r[5]) if r[5] else 0:+.1f}  "
          f"skip={r[6] or '-'}")

# Historical biggest single-day comparison
print("\n" + "=" * 100)
print("Historical TOP 10 single-day TSRT broker P&L (from real_trade_orders):")
print("=" * 100)
cur.execute("""
    WITH per_lid AS (
        SELECT setup_log_id,
               created_at::date AS d,
               state->>'fill_price' AS fill,
               state->>'close_fill_price' AS close_fp,
               state->>'direction' AS direction
        FROM real_trade_orders
        WHERE state->>'status' = 'closed'
          AND state->>'fill_price' IS NOT NULL
          AND state->>'close_fill_price' IS NOT NULL
    )
    SELECT d,
           COUNT(*) AS n,
           SUM(CASE WHEN direction IN ('long','bullish')
                    THEN (close_fp::numeric - fill::numeric) * 5
                    ELSE (fill::numeric - close_fp::numeric) * 5 END) AS day_usd
    FROM per_lid
    GROUP BY d
    ORDER BY day_usd DESC
    LIMIT 10
""")
for r in cur.fetchall():
    d, n, usd = r
    print(f"  {d}  n={n:3d}  ${float(usd):+8.2f}")

conn.close()
