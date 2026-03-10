"""Three-way comparison: No Filter vs Bugged Implementation vs Fixed Implementation vs Study's Correct Simulation.

The study (Analysis #8) simulated the filter using CHARM COMPONENT specifically.
The implementation uses OVERALL ALIGNMENT score (-3 to +3).
The bug inverts the sign check for SHORT trades.
"""
import os, sys
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text
import json

engine = create_engine(os.environ["DATABASE_URL"])

# Pull all WIN/LOSS trades with Greek data + reconstruct charm component
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT s.id, s.ts, s.setup_name, s.direction, s.grade, s.score, s.spot,
               s.outcome_result, s.outcome_pnl, s.greek_alignment,
               s.paradigm, s.vanna_all, s.spot_vol_beta,
               s.max_plus_gex
        FROM setup_log s
        WHERE s.outcome_result IN ('WIN', 'LOSS')
          AND s.grade != 'LOG'
        ORDER BY s.ts ASC
    """)).mappings().all()

trades = [dict(r) for r in rows]
print(f"Total WIN/LOSS trades: {len(trades)}")
print(f"Date range: {trades[0]['ts'].strftime('%Y-%m-%d')} to {trades[-1]['ts'].strftime('%Y-%m-%d')}")

# Reconstruct individual Greek components for each trade
# alignment = charm_component + vanna_component + gex_component
# We can derive charm_component if we know the other two
for t in trades:
    t["is_long"] = t["direction"] in ("long", "bullish")
    t["is_short"] = not t["is_long"]

    # Reconstruct components (same logic as _compute_greek_alignment)
    vanna_component = None
    if t["vanna_all"] is not None:
        aligned = (t["vanna_all"] > 0) == t["is_long"]
        vanna_component = 1 if aligned else -1

    gex_component = None
    if t["spot"] and t["max_plus_gex"]:
        gex_bullish = t["spot"] <= t["max_plus_gex"]
        aligned = gex_bullish == t["is_long"]
        gex_component = 1 if aligned else -1

    # charm_component = alignment - vanna_component - gex_component
    alignment = t["greek_alignment"]
    if alignment is not None:
        known_sum = 0
        known_count = 0
        if vanna_component is not None:
            known_sum += vanna_component
            known_count += 1
        if gex_component is not None:
            known_sum += gex_component
            known_count += 1
        # charm = alignment - other components
        charm_component = alignment - known_sum
        # Sanity check: charm_component should be -1, 0 (missing), or +1
        if charm_component in (-1, 0, 1):
            t["charm_component"] = charm_component
        else:
            t["charm_component"] = None  # can't determine
    else:
        t["charm_component"] = None

    t["vanna_component"] = vanna_component
    t["gex_component"] = gex_component

# Check reconstruction quality
has_charm = sum(1 for t in trades if t["charm_component"] is not None)
print(f"Charm component reconstructed: {has_charm}/{len(trades)}")
print()

# ── FILTER FUNCTIONS ──────────────────────────────────────────────────────

# F2 and F3 are shared across all filter versions
def f2(t):
    """AG Short at alignment -3 → block."""
    if t["setup_name"] == "AG Short" and t["greek_alignment"] is not None:
        if t["greek_alignment"] == -3:
            return False
    return True

def f3(t):
    """DD Exhaustion weak-negative SVB → block."""
    if t["setup_name"] == "DD Exhaustion" and t["spot_vol_beta"] is not None:
        if -0.5 <= t["spot_vol_beta"] <= 0:
            return False
    return True

# --- Study's correct simulation (charm component check) ---
def study_f1(t):
    """Study's F1: block when CHARM specifically opposes direction."""
    c = t.get("charm_component")
    if c is None:
        return True  # pass if charm unknown
    if c < 0:
        return False  # charm opposes trade direction
    return True

def apply_study(t):
    return study_f1(t) and f2(t) and f3(t)

# --- Bugged implementation (current eval_trader.py) ---
def bugged_f1(t):
    """Bugged F1: uses overall alignment, sign inverted for shorts."""
    a = t["greek_alignment"]
    if a is None:
        return True
    if t["is_long"] and a < 0:
        return False  # correct for longs
    if t["is_short"] and a > 0:
        return False  # BUG: blocks good shorts
    return True

def apply_bugged(t):
    return bugged_f1(t) and f2(t) and f3(t)

# --- Fixed implementation (alignment < 0 = block) ---
def fixed_f1(t):
    """Fixed F1: block when overall alignment < 0 (Greeks oppose trade)."""
    a = t["greek_alignment"]
    if a is None:
        return True
    if a < 0:
        return False
    return True

def apply_fixed(t):
    return fixed_f1(t) and f2(t) and f3(t)


def stats(trade_list):
    n = len(trade_list)
    if n == 0:
        return {"n": 0, "wr": 0, "pnl": 0, "avg": 0, "wins": 0, "losses": 0}
    wins = sum(1 for t in trade_list if t["outcome_result"] == "WIN")
    losses = n - wins
    pnl = sum(t["outcome_pnl"] or 0 for t in trade_list)
    return {"n": n, "wr": wins/n*100, "pnl": pnl, "avg": pnl/n, "wins": wins, "losses": losses}

# ── RUN ALL FOUR ──────────────────────────────────────────────────────────
baseline = stats(trades)
study_pass = [t for t in trades if apply_study(t)]
bugged_pass = [t for t in trades if apply_bugged(t)]
fixed_pass = [t for t in trades if apply_fixed(t)]

study_s = stats(study_pass)
bugged_s = stats(bugged_pass)
fixed_s = stats(fixed_pass)

# Blocked trades stats
study_blocked = stats([t for t in trades if not apply_study(t)])
bugged_blocked = stats([t for t in trades if not apply_bugged(t)])
fixed_blocked = stats([t for t in trades if not apply_fixed(t)])

print("=" * 90)
print("THREE-WAY COMPARISON")
print("=" * 90)
print(f"  {'Metric':<20s}  {'No Filter':>12s}  {'Study Sim':>12s}  {'Bugged Impl':>12s}  {'Fixed Impl':>12s}")
print(f"  {'─'*20}  {'─'*12}  {'─'*12}  {'─'*12}  {'─'*12}")
print(f"  {'Trades':<20s}  {baseline['n']:>12d}  {study_s['n']:>12d}  {bugged_s['n']:>12d}  {fixed_s['n']:>12d}")
print(f"  {'Wins':<20s}  {baseline['wins']:>12d}  {study_s['wins']:>12d}  {bugged_s['wins']:>12d}  {fixed_s['wins']:>12d}")
print(f"  {'Losses':<20s}  {baseline['losses']:>12d}  {study_s['losses']:>12d}  {bugged_s['losses']:>12d}  {fixed_s['losses']:>12d}")
print(f"  {'Win Rate':<20s}  {baseline['wr']:>11.1f}%  {study_s['wr']:>11.1f}%  {bugged_s['wr']:>11.1f}%  {fixed_s['wr']:>11.1f}%")
print(f"  {'Total PnL':<20s}  {baseline['pnl']:>+12.1f}  {study_s['pnl']:>+12.1f}  {bugged_s['pnl']:>+12.1f}  {fixed_s['pnl']:>+12.1f}")
print(f"  {'Avg/trade':<20s}  {baseline['avg']:>+12.1f}  {study_s['avg']:>+12.1f}  {bugged_s['avg']:>+12.1f}  {fixed_s['avg']:>+12.1f}")
print()
print(f"  {'Blocked trades':<20s}  {'—':>12s}  {study_blocked['n']:>12d}  {bugged_blocked['n']:>12d}  {fixed_blocked['n']:>12d}")
print(f"  {'Blocked PnL':<20s}  {'—':>12s}  {study_blocked['pnl']:>+12.1f}  {bugged_blocked['pnl']:>+12.1f}  {fixed_blocked['pnl']:>+12.1f}")
print(f"  {'Blocked WR':<20s}  {'—':>12s}  {study_blocked['wr']:>11.1f}%  {bugged_blocked['wr']:>11.1f}%  {fixed_blocked['wr']:>11.1f}%")

print()
print("=" * 90)
print("IMPROVEMENT vs NO FILTER")
print("=" * 90)
print(f"  {'Study Simulation':30s}  PnL: {baseline['pnl']:+.1f} → {study_s['pnl']:+.1f}  ({study_s['pnl']-baseline['pnl']:+.1f} pts)")
print(f"  {'Bugged Implementation':30s}  PnL: {baseline['pnl']:+.1f} → {bugged_s['pnl']:+.1f}  ({bugged_s['pnl']-baseline['pnl']:+.1f} pts)")
print(f"  {'Fixed Implementation':30s}  PnL: {baseline['pnl']:+.1f} → {fixed_s['pnl']:+.1f}  ({fixed_s['pnl']-baseline['pnl']:+.1f} pts)")

print()
print("=" * 90)
print("PER-SETUP COMPARISON")
print("=" * 90)
setups = sorted(set(t["setup_name"] for t in trades))
for setup in setups:
    st = [t for t in trades if t["setup_name"] == setup]
    bs = stats(st)
    ss = stats([t for t in st if apply_study(t)])
    bg = stats([t for t in st if apply_bugged(t)])
    fx = stats([t for t in st if apply_fixed(t)])
    print(f"\n  {setup}:")
    print(f"    {'':15s}  {'N':>4s}  {'WR':>6s}  {'PnL':>8s}  {'Avg':>6s}")
    print(f"    {'No Filter':15s}  {bs['n']:>4d}  {bs['wr']:>5.1f}%  {bs['pnl']:>+8.1f}  {bs['avg']:>+6.1f}")
    print(f"    {'Study Sim':15s}  {ss['n']:>4d}  {ss['wr']:>5.1f}%  {ss['pnl']:>+8.1f}  {ss['avg']:>+6.1f}")
    print(f"    {'Bugged':15s}  {bg['n']:>4d}  {bg['wr']:>5.1f}%  {bg['pnl']:>+8.1f}  {bg['avg']:>+6.1f}")
    print(f"    {'Fixed':15s}  {fx['n']:>4d}  {fx['wr']:>5.1f}%  {fx['pnl']:>+8.1f}  {fx['avg']:>+6.1f}")

# ── DAILY EQUITY CURVE ───────────────────────────────────────────────────
from collections import defaultdict
daily = defaultdict(lambda: {"baseline": 0, "study": 0, "bugged": 0, "fixed": 0})
for t in trades:
    d = t["ts"].strftime("%Y-%m-%d")
    pnl = t["outcome_pnl"] or 0
    daily[d]["baseline"] += pnl
    if apply_study(t):
        daily[d]["study"] += pnl
    if apply_bugged(t):
        daily[d]["bugged"] += pnl
    if apply_fixed(t):
        daily[d]["fixed"] += pnl

print()
print("=" * 90)
print("DAILY EQUITY CURVE (cumulative)")
print("=" * 90)
print(f"  {'Date':<12s}  {'No Filter':>10s}  {'Study Sim':>10s}  {'Bugged':>10s}  {'Fixed':>10s}")
cum_b, cum_s, cum_bg, cum_fx = 0, 0, 0, 0
for d in sorted(daily.keys()):
    cum_b += daily[d]["baseline"]
    cum_s += daily[d]["study"]
    cum_bg += daily[d]["bugged"]
    cum_fx += daily[d]["fixed"]
    print(f"  {d:<12s}  {cum_b:>+10.1f}  {cum_s:>+10.1f}  {cum_bg:>+10.1f}  {cum_fx:>+10.1f}")

# Max drawdown calculation
def max_drawdown(daily_pnls):
    cum = 0
    peak = 0
    max_dd = 0
    for pnl in daily_pnls:
        cum += pnl
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    return max_dd

dates = sorted(daily.keys())
dd_baseline = max_drawdown([daily[d]["baseline"] for d in dates])
dd_study = max_drawdown([daily[d]["study"] for d in dates])
dd_bugged = max_drawdown([daily[d]["bugged"] for d in dates])
dd_fixed = max_drawdown([daily[d]["fixed"] for d in dates])

print()
print(f"  {'Max Drawdown':<12s}  {dd_baseline:>10.1f}  {dd_study:>10.1f}  {dd_bugged:>10.1f}  {dd_fixed:>10.1f}")

# Profit factor
def profit_factor(trade_list):
    gross_profit = sum(t["outcome_pnl"] for t in trade_list if (t["outcome_pnl"] or 0) > 0)
    gross_loss = abs(sum(t["outcome_pnl"] for t in trade_list if (t["outcome_pnl"] or 0) < 0))
    return gross_profit / gross_loss if gross_loss > 0 else float('inf')

print()
print("=" * 90)
print("RISK METRICS")
print("=" * 90)
print(f"  {'Metric':<20s}  {'No Filter':>12s}  {'Study Sim':>12s}  {'Bugged':>12s}  {'Fixed':>12s}")
print(f"  {'Max Drawdown':<20s}  {dd_baseline:>12.1f}  {dd_study:>12.1f}  {dd_bugged:>12.1f}  {dd_fixed:>12.1f}")
print(f"  {'Profit Factor':<20s}  {profit_factor(trades):>12.2f}  {profit_factor(study_pass):>12.2f}  {profit_factor(bugged_pass):>12.2f}  {profit_factor(fixed_pass):>12.2f}")

# Monthly projection (10 MES = $5/pt/contract * 10 = $50/pt)
n_days = len(dates)
for label, s in [("No Filter", baseline), ("Study Sim", study_s), ("Bugged", bugged_s), ("Fixed", fixed_s)]:
    daily_avg = s["pnl"] / n_days
    monthly = daily_avg * 21 * 50  # 21 trading days, $50/pt for 10 MES
    print(f"  {label + ' (10 MES/mo)':<20s}  ${monthly:>+11,.0f}")
