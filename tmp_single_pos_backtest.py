"""
Single Position Mode Backtest
==============================
Walk through all 226 trades chronologically and simulate multiple strategies.
For each, track daily P&L, trades taken, win rate, max drawdown.

Strategies:
  A) Portal (all trades run independently — baseline)
  B) Single position, first-come-first-served
  C) Single position + setup priority (ES Abs > AG > DD > BofA > Paradigm > GEX)
  D) Single position + priority + filters (no GEX, DD time<14:00, no BOFA paradigm)
  E) No-reversal: take first, but if losing trade and better signal fires, tighten stop
  F) Strategy D but allow same-direction stacking (don't block same-dir signals)
"""

import os, psycopg2
from datetime import timedelta, time as dtime
from collections import defaultdict

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

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

conn.close()

PRIORITY = {
    "ES Absorption": 1,
    "AG Short": 2,
    "DD Exhaustion": 3,
    "BofA Scalp": 4,
    "Paradigm Reversal": 5,
    "GEX Long": 6,
}

def is_bofa_paradigm(paradigm):
    p = (paradigm or "").upper()
    return "BOFA" in p and "PURE" in p

def passes_filters(t):
    """Strategy D filters: no GEX, DD time<14:00, no BOFA paradigm for DD"""
    if t["setup"] == "GEX Long":
        return False
    if t["setup"] == "DD Exhaustion":
        hour = t["ts"].hour
        if hour >= 14:
            return False
        if is_bofa_paradigm(t["paradigm"]):
            return False
    return True


def simulate(trades, name, use_priority=False, use_filters=False, allow_same_dir=False):
    """Simulate single position mode. Returns results dict."""
    taken = []
    skipped = []
    position = None  # current open trade
    daily_pnl = defaultdict(float)
    daily_trades = defaultdict(int)
    daily_wins = defaultdict(int)
    daily_losses = defaultdict(int)

    for t in trades:
        day = t["ts"].date()

        # Apply filters if enabled
        if use_filters and not passes_filters(t):
            skipped.append((t, "filtered"))
            continue

        if position is not None:
            # Position is open — should we take this trade?
            pos_end = position["end_ts"]

            if t["ts"] >= pos_end:
                # Position already closed naturally
                position = None
            else:
                # Position still open
                same_dir = (t["direction"] == position["direction"])

                if same_dir and allow_same_dir:
                    # Same direction — allow (but in real execution this is complex)
                    skipped.append((t, "same_dir_skip"))  # skip for now
                    continue

                if use_priority:
                    # Check if new trade has higher priority AND fires within 2 min of position
                    new_pri = PRIORITY.get(t["setup"], 99)
                    cur_pri = PRIORITY.get(position["setup"], 99)
                    time_gap = (t["ts"] - position["ts"]).total_seconds() / 60

                    if time_gap <= 2 and new_pri < cur_pri and same_dir:
                        # Swap: undo position, take this one (only same direction swaps)
                        daily_pnl[day] -= position["pnl"]
                        daily_trades[day] -= 1
                        if position["result"] == "WIN":
                            daily_wins[day] -= 1
                        elif position["result"] == "LOSS":
                            daily_losses[day] -= 1
                        taken.remove(position)

                        taken.append(t)
                        daily_pnl[day] += t["pnl"]
                        daily_trades[day] += 1
                        if t["result"] == "WIN":
                            daily_wins[day] += 1
                        elif t["result"] == "LOSS":
                            daily_losses[day] += 1
                        position = t
                        continue

                # Skip — position is open, no reversal
                skipped.append((t, "position_open"))
                continue

        # No position open — take this trade
        taken.append(t)
        position = t
        daily_pnl[day] += t["pnl"]
        daily_trades[day] += 1
        if t["result"] == "WIN":
            daily_wins[day] += 1
        elif t["result"] == "LOSS":
            daily_losses[day] += 1

    # Calculate stats
    total_pnl = sum(t["pnl"] for t in taken)
    wins = sum(1 for t in taken if t["result"] == "WIN")
    losses = sum(1 for t in taken if t["result"] == "LOSS")
    expired = sum(1 for t in taken if t["result"] == "EXPIRED")
    wr = wins / max(wins + losses, 1) * 100

    # Max drawdown
    running = 0
    peak = 0
    max_dd = 0
    for day in sorted(daily_pnl):
        running += daily_pnl[day]
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)

    # Skipped P&L
    skipped_pnl = sum(t["pnl"] for t, reason in skipped)
    filtered_count = sum(1 for t, reason in skipped if reason == "filtered")

    return {
        "name": name,
        "taken": taken,
        "total": len(taken),
        "total_pnl": total_pnl,
        "wins": wins, "losses": losses, "expired": expired, "wr": wr,
        "max_dd": max_dd,
        "daily_pnl": dict(daily_pnl),
        "daily_trades": dict(daily_trades),
        "skipped": len(skipped),
        "skipped_pnl": skipped_pnl,
        "filtered": filtered_count,
        "trading_days": len(daily_pnl),
        "avg_daily": total_pnl / max(len(daily_pnl), 1),
    }


# ============ RUN ALL STRATEGIES ============

# Strategy A: Portal baseline
portal = {
    "name": "A) Portal (all trades)",
    "total": len(trades),
    "total_pnl": sum(t["pnl"] for t in trades),
    "wins": sum(1 for t in trades if t["result"] == "WIN"),
    "losses": sum(1 for t in trades if t["result"] == "LOSS"),
    "expired": sum(1 for t in trades if t["result"] == "EXPIRED"),
}
portal["wr"] = portal["wins"] / max(portal["wins"] + portal["losses"], 1) * 100
daily_portal = defaultdict(float)
for t in trades:
    daily_portal[t["ts"].date()] += t["pnl"]
running = 0; peak = 0; max_dd = 0
for day in sorted(daily_portal):
    running += daily_portal[day]
    peak = max(peak, running)
    max_dd = max(max_dd, peak - running)
portal["max_dd"] = max_dd
portal["trading_days"] = len(daily_portal)
portal["avg_daily"] = portal["total_pnl"] / max(len(daily_portal), 1)
portal["daily_pnl"] = dict(daily_portal)

strat_b = simulate(trades, "B) Single pos, first-come")
strat_c = simulate(trades, "C) Single pos + priority", use_priority=True)
strat_d = simulate(trades, "D) Single pos + priority + filters", use_priority=True, use_filters=True)

# Strategy E: filtered only (no single position restriction, just filters)
filtered_trades = [t for t in trades if passes_filters(t)]
filtered_portal = {
    "name": "E) Portal + filters only (no pos limit)",
    "total": len(filtered_trades),
    "total_pnl": sum(t["pnl"] for t in filtered_trades),
    "wins": sum(1 for t in filtered_trades if t["result"] == "WIN"),
    "losses": sum(1 for t in filtered_trades if t["result"] == "LOSS"),
    "expired": sum(1 for t in filtered_trades if t["result"] == "EXPIRED"),
}
filtered_portal["wr"] = filtered_portal["wins"] / max(filtered_portal["wins"] + filtered_portal["losses"], 1) * 100
daily_fp = defaultdict(float)
for t in filtered_trades:
    daily_fp[t["ts"].date()] += t["pnl"]
running = 0; peak = 0; max_dd = 0
for day in sorted(daily_fp):
    running += daily_fp[day]
    peak = max(peak, running)
    max_dd = max(max_dd, peak - running)
filtered_portal["max_dd"] = max_dd
filtered_portal["trading_days"] = len(daily_fp)
filtered_portal["avg_daily"] = filtered_portal["total_pnl"] / max(len(daily_fp), 1)
filtered_portal["daily_pnl"] = dict(daily_fp)

strategies = [portal, strat_b, strat_c, strat_d, filtered_portal]

# ============ PRINT RESULTS ============

print("=" * 100)
print("STRATEGY COMPARISON")
print("=" * 100)
print()
print(f"{'Strategy':<45} {'Trades':>6} {'W':>4} {'L':>4} {'E':>4} {'WR%':>6} "
      f"{'Total PnL':>10} {'MaxDD':>8} {'Days':>4} {'Avg/Day':>8}")
print("-" * 100)
for s in strategies:
    print(f"{s['name']:<45} {s['total']:>6} {s['wins']:>4} {s['losses']:>4} "
          f"{s['expired']:>4} {s['wr']:>5.1f}% "
          f"{s['total_pnl']:>+10.1f} {s['max_dd']:>8.1f} "
          f"{s['trading_days']:>4} {s['avg_daily']:>+8.1f}")

# ============ DAILY BREAKDOWN ============
print("\n\n" + "=" * 100)
print("DAILY P&L COMPARISON")
print("=" * 100)

all_days = sorted(set(daily_portal.keys()))
print(f"\n{'Date':<12} {'Portal':>8} {'SingleB':>8} {'SingleC':>8} {'Filt+SP':>8} {'FiltOnly':>8}")
print("-" * 60)

for day in all_days:
    p = portal["daily_pnl"].get(day, 0)
    b = strat_b.get("daily_pnl", {}).get(day, 0)
    c = strat_c.get("daily_pnl", {}).get(day, 0)
    d = strat_d.get("daily_pnl", {}).get(day, 0)
    e = filtered_portal["daily_pnl"].get(day, 0)
    print(f"{str(day):<12} {p:>+8.1f} {b:>+8.1f} {c:>+8.1f} {d:>+8.1f} {e:>+8.1f}")

# Totals
print("-" * 60)
p_tot = sum(portal["daily_pnl"].values())
b_tot = sum(strat_b.get("daily_pnl", {}).values())
c_tot = sum(strat_c.get("daily_pnl", {}).values())
d_tot = sum(strat_d.get("daily_pnl", {}).values())
e_tot = sum(filtered_portal["daily_pnl"].values())
print(f"{'TOTAL':<12} {p_tot:>+8.1f} {b_tot:>+8.1f} {c_tot:>+8.1f} {d_tot:>+8.1f} {e_tot:>+8.1f}")

# ============ LOSING DAYS ANALYSIS ============
print("\n\n" + "=" * 100)
print("LOSING DAYS ANALYSIS")
print("=" * 100)

for s in strategies:
    dp = s.get("daily_pnl", {})
    losing_days = sum(1 for v in dp.values() if v < 0)
    losing_pnl = sum(v for v in dp.values() if v < 0)
    winning_days = sum(1 for v in dp.values() if v > 0)
    winning_pnl = sum(v for v in dp.values() if v > 0)
    print(f"\n{s['name']}:")
    print(f"  Winning days: {winning_days} ({winning_pnl:+.1f} pts)")
    print(f"  Losing days:  {losing_days} ({losing_pnl:+.1f} pts)")
    print(f"  Worst day:    {min(dp.values()):+.1f} pts" if dp else "  No trades")
    print(f"  Best day:     {max(dp.values()):+.1f} pts" if dp else "")

# ============ SETUP MIX IN BEST STRATEGY ============
print("\n\n" + "=" * 100)
print("SETUP MIX IN STRATEGY D (Single + Priority + Filters)")
print("=" * 100)

if hasattr(strat_d, '__getitem__') and "taken" in strat_d:
    setup_stats = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0, "losses": 0})
    for t in strat_d["taken"]:
        s = setup_stats[t["setup"]]
        s["count"] += 1
        s["pnl"] += t["pnl"]
        if t["result"] == "WIN":
            s["wins"] += 1
        elif t["result"] == "LOSS":
            s["losses"] += 1

    print(f"\n{'Setup':<20} {'Trades':>6} {'W':>4} {'L':>4} {'WR%':>6} {'PnL':>8} {'Avg':>8}")
    print("-" * 65)
    for setup in sorted(setup_stats, key=lambda s: setup_stats[s]["pnl"], reverse=True):
        st = setup_stats[setup]
        wr = st["wins"] / max(st["wins"] + st["losses"], 1) * 100
        avg = st["pnl"] / max(st["count"], 1)
        print(f"{setup:<20} {st['count']:>6} {st['wins']:>4} {st['losses']:>4} "
              f"{wr:>5.1f}% {st['pnl']:>+8.1f} {avg:>+8.1f}")

# ============ CAPTURE RATE ============
print("\n\n" + "=" * 100)
print("CAPTURE RATES")
print("=" * 100)
for s in strategies:
    capture = s["total_pnl"] / portal["total_pnl"] * 100 if portal["total_pnl"] != 0 else 0
    fewer_trades = portal["total"] - s["total"]
    print(f"  {s['name']:<45} {capture:>5.1f}% of portal  "
          f"({s['total']:>3} trades, {fewer_trades:>3} fewer)")

# ============ EQUITY CURVE ============
print("\n\n" + "=" * 100)
print("CUMULATIVE EQUITY CURVE (pts)")
print("=" * 100)
print(f"\n{'Date':<12} {'Portal':>8} {'Strat D':>8}")
print("-" * 30)
cum_p = 0
cum_d = 0
for day in all_days:
    cum_p += portal["daily_pnl"].get(day, 0)
    cum_d += strat_d.get("daily_pnl", {}).get(day, 0)
    print(f"{str(day):<12} {cum_p:>+8.1f} {cum_d:>+8.1f}")

# Monthly projection
print("\n\n" + "=" * 100)
print("MONTHLY PROJECTION (20 trading days)")
print("=" * 100)
for s in strategies:
    monthly = s["avg_daily"] * 20
    print(f"  {s['name']:<45} {s['avg_daily']:>+6.1f} pts/day  = {monthly:>+8.1f} pts/month")
    for contracts, label in [(1, "1 ES"), (2, "2 ES"), (4, "4 ES")]:
        dollar = monthly * 50 * contracts
        print(f"    {label}: ${dollar:>+10,.0f}/month")
