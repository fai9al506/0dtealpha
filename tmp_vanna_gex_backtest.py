"""
GEX Long with Vanna Filter Backtest
=====================================
Instead of blocking GEX Long entirely, allow it only when vanna ALL > 0.
Compare: block GEX vs vanna-filtered GEX vs unfiltered GEX.
Also test vanna filter on other setups.
"""

import os, psycopg2
from datetime import timedelta
from collections import defaultdict

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

# Get all trades with outcomes
cur.execute("""
    SELECT id, setup_name, direction, grade,
           ts AT TIME ZONE 'America/New_York' as ts_et,
           outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss,
           outcome_elapsed_min,
           spot, paradigm
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")

trades = []
for r in cur.fetchall():
    direction_norm = "long" if r[2].lower() in ("long", "bullish") else "short"
    elapsed = float(r[9] or 30)
    trades.append({
        "id": r[0], "setup": r[1], "direction": direction_norm, "grade": r[3],
        "ts": r[4], "result": r[5], "pnl": float(r[6] or 0),
        "max_profit": float(r[7] or 0), "max_loss": float(r[8] or 0),
        "elapsed_min": elapsed,
        "spot": float(r[10] or 0), "paradigm": r[11] or "",
        "end_ts": r[4] + timedelta(minutes=elapsed),
    })

# Get vanna ALL data for each trade (nearest volland snapshot)
# Load all vanna exposure points in batch
cur.execute("""
    SELECT ts_utc AT TIME ZONE 'America/New_York' as ts_et,
           SUM(value) as total_vanna
    FROM volland_exposure_points
    WHERE greek = 'vanna'
      AND expiration_option = 'ALL'
    GROUP BY ts_utc
    ORDER BY ts_utc
""")
vanna_snapshots = [(r[0], float(r[1])) for r in cur.fetchall()]
print(f"Vanna ALL snapshots: {len(vanna_snapshots)}")

# Also get vanna by other expiration types
vanna_by_type = {}
for exp_type in ['ALL', 'THIRTY_NEXT_DAYS', 'THIS_WEEK', 'TODAY']:
    cur.execute("""
        SELECT ts_utc AT TIME ZONE 'America/New_York' as ts_et,
               SUM(value) as total_vanna
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND expiration_option = %s
        GROUP BY ts_utc
        ORDER BY ts_utc
    """, (exp_type,))
    vanna_by_type[exp_type] = [(r[0], float(r[1])) for r in cur.fetchall()]
    print(f"  Vanna {exp_type}: {len(vanna_by_type[exp_type])} snapshots")

conn.close()

def find_nearest_vanna(trade_ts, snapshots, max_gap_min=10):
    """Find nearest vanna snapshot within max_gap_min minutes of trade timestamp."""
    best = None
    best_gap = float('inf')
    for snap_ts, val in snapshots:
        gap = abs((trade_ts - snap_ts).total_seconds()) / 60
        if gap < best_gap:
            best_gap = gap
            best = val
        elif gap > best_gap + 30:
            break  # past the minimum, stop searching
    if best_gap <= max_gap_min:
        return best
    return None

# Attach vanna data to each trade
for t in trades:
    t["vanna_all"] = find_nearest_vanna(t["ts"], vanna_by_type.get("ALL", []))
    t["vanna_30d"] = find_nearest_vanna(t["ts"], vanna_by_type.get("THIRTY_NEXT_DAYS", []))
    t["vanna_week"] = find_nearest_vanna(t["ts"], vanna_by_type.get("THIS_WEEK", []))
    t["vanna_today"] = find_nearest_vanna(t["ts"], vanna_by_type.get("TODAY", []))

# ============ GEX LONG ANALYSIS BY VANNA REGIME ============
print("\n" + "=" * 80)
print("GEX LONG: PERFORMANCE BY VANNA REGIME")
print("=" * 80)

gex_trades = [t for t in trades if t["setup"] == "GEX Long"]
print(f"\nTotal GEX Long trades: {len(gex_trades)}")

for vanna_key, label in [("vanna_all", "Vanna ALL"), ("vanna_30d", "Vanna 30D"),
                          ("vanna_week", "Vanna Week"), ("vanna_today", "Vanna Today")]:
    has_data = [t for t in gex_trades if t[vanna_key] is not None]
    positive = [t for t in has_data if t[vanna_key] > 0]
    negative = [t for t in has_data if t[vanna_key] <= 0]
    no_data = [t for t in gex_trades if t[vanna_key] is None]

    print(f"\n  {label} ({len(has_data)}/{len(gex_trades)} have data):")
    if positive:
        w = sum(1 for t in positive if t["result"] == "WIN")
        l = sum(1 for t in positive if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in positive)
        wr = w / max(w + l, 1) * 100
        print(f"    Positive: {len(positive)} trades, {w}W/{l}L, {wr:.0f}% WR, {pnl:+.1f} pts")
    if negative:
        w = sum(1 for t in negative if t["result"] == "WIN")
        l = sum(1 for t in negative if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in negative)
        wr = w / max(w + l, 1) * 100
        print(f"    Negative: {len(negative)} trades, {w}W/{l}L, {wr:.0f}% WR, {pnl:+.1f} pts")
    if no_data:
        w = sum(1 for t in no_data if t["result"] == "WIN")
        l = sum(1 for t in no_data if t["result"] == "LOSS")
        pnl = sum(t["pnl"] for t in no_data)
        print(f"    No data:  {len(no_data)} trades, {w}W/{l}L, {pnl:+.1f} pts")

# ============ ALL SETUPS BY VANNA REGIME ============
print("\n\n" + "=" * 80)
print("ALL SETUPS: VANNA ALL REGIME IMPACT")
print("=" * 80)

for setup_name in ["GEX Long", "AG Short", "DD Exhaustion", "ES Absorption", "BofA Scalp", "Paradigm Reversal"]:
    st = [t for t in trades if t["setup"] == setup_name]
    has_data = [t for t in st if t["vanna_all"] is not None]
    if not has_data:
        print(f"\n{setup_name}: no vanna data")
        continue

    positive = [t for t in has_data if t["vanna_all"] > 0]
    negative = [t for t in has_data if t["vanna_all"] <= 0]

    print(f"\n{setup_name} ({len(has_data)}/{len(st)} have vanna ALL data):")
    for label, subset in [("Vanna+", positive), ("Vanna-", negative)]:
        if subset:
            w = sum(1 for t in subset if t["result"] == "WIN")
            l = sum(1 for t in subset if t["result"] == "LOSS")
            e = sum(1 for t in subset if t["result"] == "EXPIRED")
            pnl = sum(t["pnl"] for t in subset)
            wr = w / max(w + l, 1) * 100
            avg = pnl / len(subset)
            print(f"  {label}: {len(subset):>3} trades, {w:>2}W/{l:>2}L/{e:>2}E, "
                  f"{wr:>5.1f}% WR, {pnl:>+8.1f} pts (avg {avg:>+5.1f})")


# ============ FULL BACKTEST WITH VANNA-FILTERED GEX ============
print("\n\n" + "=" * 80)
print("SINGLE POSITION BACKTEST: VANNA-FILTERED GEX vs BLOCKED GEX")
print("=" * 80)

def is_bofa_paradigm(paradigm):
    p = (paradigm or "").upper()
    return "BOFA" in p and "PURE" in p

def passes_filters_block_gex(t):
    """Block GEX entirely"""
    if t["setup"] == "GEX Long":
        return False
    if t["setup"] == "DD Exhaustion":
        if t["ts"].hour >= 14:
            return False
        if is_bofa_paradigm(t["paradigm"]):
            return False
    return True

def passes_filters_vanna_gex(t):
    """Allow GEX only when vanna ALL > 0"""
    if t["setup"] == "GEX Long":
        vanna = t.get("vanna_all")
        if vanna is None or vanna <= 0:
            return False
    if t["setup"] == "DD Exhaustion":
        if t["ts"].hour >= 14:
            return False
        if is_bofa_paradigm(t["paradigm"]):
            return False
    return True

def passes_filters_vanna_all_setups(t):
    """Apply vanna regime to GEX + DD"""
    if t["setup"] == "GEX Long":
        vanna = t.get("vanna_all")
        if vanna is None or vanna <= 0:
            return False
    if t["setup"] == "DD Exhaustion":
        if t["ts"].hour >= 14:
            return False
        if is_bofa_paradigm(t["paradigm"]):
            return False
        # DD likes NEGATIVE vanna (opposite of GEX)
        vanna = t.get("vanna_all")
        if vanna is not None and vanna > 0:
            # DD with positive vanna — check if it hurts
            pass  # don't filter yet, just test
    return True

def simulate_single_pos(trades, filter_fn, name):
    """Single position mode with given filter."""
    taken = []
    position = None
    daily_pnl = defaultdict(float)

    for t in trades:
        day = t["ts"].date()
        if not filter_fn(t):
            continue

        if position is not None:
            if t["ts"] >= position["end_ts"]:
                position = None
            else:
                continue  # position open, skip

        taken.append(t)
        position = t
        daily_pnl[day] += t["pnl"]

    total_pnl = sum(t["pnl"] for t in taken)
    wins = sum(1 for t in taken if t["result"] == "WIN")
    losses = sum(1 for t in taken if t["result"] == "LOSS")
    expired = sum(1 for t in taken if t["result"] == "EXPIRED")
    wr = wins / max(wins + losses, 1) * 100

    running = 0; peak = 0; max_dd = 0
    for day in sorted(daily_pnl):
        running += daily_pnl[day]
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)

    worst_day = min(daily_pnl.values()) if daily_pnl else 0
    best_day = max(daily_pnl.values()) if daily_pnl else 0
    losing_days = sum(1 for v in daily_pnl.values() if v < 0)
    avg_daily = total_pnl / max(len(daily_pnl), 1)

    return {
        "name": name, "taken": taken, "total": len(taken),
        "total_pnl": total_pnl, "wins": wins, "losses": losses,
        "expired": expired, "wr": wr, "max_dd": max_dd,
        "worst_day": worst_day, "best_day": best_day,
        "losing_days": losing_days, "trading_days": len(daily_pnl),
        "avg_daily": avg_daily, "daily_pnl": dict(daily_pnl),
    }

r_block = simulate_single_pos(trades, passes_filters_block_gex, "Block GEX entirely")
r_vanna = simulate_single_pos(trades, passes_filters_vanna_gex, "GEX only when Vanna+ ")
r_all = simulate_single_pos(trades, passes_filters_vanna_all_setups, "Vanna filter on GEX+DD")

# Also test: no filters at all, just single position
r_none = simulate_single_pos(trades, lambda t: True, "No filters (single pos)")

results = [r_none, r_block, r_vanna, r_all]

print(f"\n{'Strategy':<35} {'Trades':>6} {'W':>3} {'L':>3} {'WR%':>6} {'PnL':>8} "
      f"{'MaxDD':>7} {'Worst':>7} {'Days':>4} {'$/Day':>7}")
print("-" * 100)
for r in results:
    print(f"{r['name']:<35} {r['total']:>6} {r['wins']:>3} {r['losses']:>3} "
          f"{r['wr']:>5.1f}% {r['total_pnl']:>+8.1f} {r['max_dd']:>7.1f} "
          f"{r['worst_day']:>+7.1f} {r['trading_days']:>4} {r['avg_daily']:>+7.1f}")

# ============ DETAILED: WHICH GEX TRADES PASS VANNA FILTER? ============
print("\n\n" + "=" * 80)
print("GEX LONG TRADES THAT PASS VANNA FILTER (vanna ALL > 0)")
print("=" * 80)

gex_pass = [t for t in gex_trades if t.get("vanna_all") is not None and t["vanna_all"] > 0]
gex_fail = [t for t in gex_trades if t.get("vanna_all") is None or t["vanna_all"] <= 0]

print(f"\nPASS ({len(gex_pass)} trades):")
for t in gex_pass:
    print(f"  #{t['id']} {str(t['ts'])[:16]} dir={t['direction']} result={t['result']} "
          f"pnl={t['pnl']:+.1f} vanna={t['vanna_all']:+.0f}")

print(f"\nFAIL/BLOCKED ({len(gex_fail)} trades):")
for t in gex_fail:
    v = t.get('vanna_all')
    v_str = f"{v:+.0f}" if v is not None else "N/A"
    print(f"  #{t['id']} {str(t['ts'])[:16]} dir={t['direction']} result={t['result']} "
          f"pnl={t['pnl']:+.1f} vanna={v_str}")

# Summary
pass_pnl = sum(t["pnl"] for t in gex_pass)
fail_pnl = sum(t["pnl"] for t in gex_fail)
print(f"\n  PASS P&L: {pass_pnl:+.1f} pts ({len(gex_pass)} trades)")
print(f"  FAIL P&L: {fail_pnl:+.1f} pts ({len(gex_fail)} trades)")
print(f"  Filter saves: {abs(fail_pnl):.1f} pts by blocking {len(gex_fail)} losers")

# ============ DAILY COMPARISON ============
print("\n\n" + "=" * 80)
print("DAILY P&L: BLOCK GEX vs VANNA-FILTERED GEX")
print("=" * 80)

all_days = sorted(set(list(r_block["daily_pnl"].keys()) + list(r_vanna["daily_pnl"].keys())))
print(f"\n{'Date':<12} {'BlockGEX':>8} {'VannaGEX':>8} {'Diff':>8}")
print("-" * 40)
for day in all_days:
    b = r_block["daily_pnl"].get(day, 0)
    v = r_vanna["daily_pnl"].get(day, 0)
    diff = v - b
    marker = " <<" if abs(diff) > 5 else ""
    print(f"{str(day):<12} {b:>+8.1f} {v:>+8.1f} {diff:>+8.1f}{marker}")
print("-" * 40)
print(f"{'TOTAL':<12} {r_block['total_pnl']:>+8.1f} {r_vanna['total_pnl']:>+8.1f} "
      f"{r_vanna['total_pnl'] - r_block['total_pnl']:>+8.1f}")

# Monthly projection
print("\n\n" + "=" * 80)
print("MONTHLY PROJECTION (20 trading days)")
print("=" * 80)
for r in results:
    monthly = r["avg_daily"] * 20
    print(f"\n  {r['name']}")
    print(f"    {r['avg_daily']:+.1f} pts/day = {monthly:+.1f} pts/month")
    for c, label in [(1, "1 ES"), (2, "2 ES"), (4, "4 ES")]:
        print(f"    {label}: ${monthly * 50 * c:>+,.0f}/month")
