"""Analyze Greek filter F1 bug impact: bugged vs fixed filter on historical data.

Requires DATABASE_URL env var (Railway Postgres).
Run: railway run -s 0dtealpha -- python tmp_greek_filter_bug.py
  OR: set DATABASE_URL=... && python tmp_greek_filter_bug.py
"""
import os, sys
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

# Pull all WIN/LOSS trades with Greek alignment
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, ts, setup_name, direction, grade, score, spot,
               outcome_result, outcome_pnl, greek_alignment,
               paradigm, vanna_all, spot_vol_beta
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
          AND grade != 'LOG'
        ORDER BY ts ASC
    """)).mappings().all()

trades = [dict(r) for r in rows]
print(f"Total WIN/LOSS trades: {len(trades)}")
print(f"Date range: {trades[0]['ts'].strftime('%Y-%m-%d')} to {trades[-1]['ts'].strftime('%Y-%m-%d')}")

# Count with alignment
has_align = [t for t in trades if t["greek_alignment"] is not None]
print(f"With alignment data: {len(has_align)}")
print()

# Classify each trade as LONG or SHORT
for t in trades:
    t["is_long"] = t["direction"] in ("long", "bullish")
    t["is_short"] = not t["is_long"]

longs = [t for t in trades if t["is_long"]]
shorts = [t for t in trades if t["is_short"]]
print(f"LONG trades: {len(longs)}, SHORT trades: {len(shorts)}")
print()

# ── BUGGED FILTER (current eval_trader.py logic) ─────────────────────────
# F1: LONG + alignment < 0 → BLOCK (correct)
#     SHORT + alignment > 0 → BLOCK (INVERTED — blocks good shorts)
def bugged_f1(t):
    a = t["greek_alignment"]
    if a is None:
        return True  # pass (no data)
    if t["is_long"] and a < 0:
        return False  # blocked
    if t["is_short"] and a > 0:
        return False  # blocked (BUG: blocks good shorts)
    return True

# ── FIXED FILTER (alignment < 0 = Greeks oppose trade, regardless of direction) ──
def fixed_f1(t):
    a = t["greek_alignment"]
    if a is None:
        return True  # pass (no data)
    if a < 0:
        return False  # blocked: Greeks oppose the trade direction
    return True

# F2 and F3 are the same in both (no bug)
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

def apply_optimal_bugged(t):
    return bugged_f1(t) and f2(t) and f3(t)

def apply_optimal_fixed(t):
    return fixed_f1(t) and f2(t) and f3(t)


def stats(label, trade_list):
    n = len(trade_list)
    if n == 0:
        return {"label": label, "n": 0, "wr": 0, "pnl": 0, "avg": 0}
    wins = sum(1 for t in trade_list if t["outcome_result"] == "WIN")
    pnl = sum(t["outcome_pnl"] or 0 for t in trade_list)
    return {"label": label, "n": n, "wr": wins/n*100, "pnl": pnl, "avg": pnl/n}


def print_stats(s):
    print(f"  {s['label']:30s}  N={s['n']:>4d}  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  Avg={s['avg']:>+5.1f}")


# ── BASELINE (no filter) ─────────────────────────────────────────────────
print("=" * 80)
print("BASELINE (no filter)")
print("=" * 80)
print_stats(stats("All trades", trades))
print_stats(stats("  LONG trades", longs))
print_stats(stats("  SHORT trades", shorts))
print()

# ── BUGGED FILTER ────────────────────────────────────────────────────────
bugged_pass = [t for t in trades if apply_optimal_bugged(t)]
bugged_block = [t for t in trades if not apply_optimal_bugged(t)]
bugged_pass_long = [t for t in bugged_pass if t["is_long"]]
bugged_pass_short = [t for t in bugged_pass if t["is_short"]]
bugged_block_long = [t for t in bugged_block if t["is_long"]]
bugged_block_short = [t for t in bugged_block if t["is_short"]]

print("=" * 80)
print("BUGGED FILTER (current — F1 inverted for SHORTs)")
print("=" * 80)
print_stats(stats("Passed (traded)", bugged_pass))
print_stats(stats("  LONG passed", bugged_pass_long))
print_stats(stats("  SHORT passed", bugged_pass_short))
print_stats(stats("Blocked (avoided)", bugged_block))
print_stats(stats("  LONG blocked", bugged_block_long))
print_stats(stats("  SHORT blocked", bugged_block_short))
print()

# ── FIXED FILTER ─────────────────────────────────────────────────────────
fixed_pass = [t for t in trades if apply_optimal_fixed(t)]
fixed_block = [t for t in trades if not apply_optimal_fixed(t)]
fixed_pass_long = [t for t in fixed_pass if t["is_long"]]
fixed_pass_short = [t for t in fixed_pass if t["is_short"]]
fixed_block_long = [t for t in fixed_block if t["is_long"]]
fixed_block_short = [t for t in fixed_block if t["is_short"]]

print("=" * 80)
print("FIXED FILTER (F1: block when alignment < 0 for ALL directions)")
print("=" * 80)
print_stats(stats("Passed (traded)", fixed_pass))
print_stats(stats("  LONG passed", fixed_pass_long))
print_stats(stats("  SHORT passed", fixed_pass_short))
print_stats(stats("Blocked (avoided)", fixed_block))
print_stats(stats("  LONG blocked", fixed_block_long))
print_stats(stats("  SHORT blocked", fixed_block_short))
print()

# ── COMPARISON ───────────────────────────────────────────────────────────
print("=" * 80)
print("COMPARISON: BUGGED vs FIXED")
print("=" * 80)
bs = stats("Bugged", bugged_pass)
fs = stats("Fixed", fixed_pass)
print(f"  {'':30s}  {'Bugged':>12s}  {'Fixed':>12s}  {'Delta':>12s}")
print(f"  {'Trades passed':30s}  {bs['n']:>12d}  {fs['n']:>12d}  {fs['n']-bs['n']:>+12d}")
print(f"  {'Win Rate':30s}  {bs['wr']:>11.1f}%  {fs['wr']:>11.1f}%  {fs['wr']-bs['wr']:>+11.1f}%")
print(f"  {'Total PnL':30s}  {bs['pnl']:>+12.1f}  {fs['pnl']:>+12.1f}  {fs['pnl']-bs['pnl']:>+12.1f}")
print(f"  {'Avg per trade':30s}  {bs['avg']:>+12.1f}  {fs['avg']:>+12.1f}  {fs['avg']-bs['avg']:>+12.1f}")
print()

# ── DIFFERENCE: trades that change between bugged and fixed ──────────────
# Trades blocked by bugged but passed by fixed (good shorts now allowed)
freed = [t for t in trades if not apply_optimal_bugged(t) and apply_optimal_fixed(t)]
# Trades passed by bugged but blocked by fixed (bad shorts now blocked)
caught = [t for t in trades if apply_optimal_bugged(t) and not apply_optimal_fixed(t)]

print("=" * 80)
print("TRADES THAT CHANGE")
print("=" * 80)
print(f"\nFreed by fix (blocked by bug, now allowed):")
print_stats(stats("  All freed", freed))
for t in freed:
    wr = "WIN" if t["outcome_result"] == "WIN" else "LOSS"
    print(f"    {t['ts'].strftime('%m/%d %H:%M')} {t['setup_name']:20s} {t['direction']:8s} "
          f"align={t['greek_alignment']:+d} → {wr} {t['outcome_pnl']:+.1f}pts")

print(f"\nCaught by fix (passed by bug, now blocked):")
print_stats(stats("  All caught", caught))
for t in caught:
    wr = "WIN" if t["outcome_result"] == "WIN" else "LOSS"
    print(f"    {t['ts'].strftime('%m/%d %H:%M')} {t['setup_name']:20s} {t['direction']:8s} "
          f"align={t['greek_alignment']:+d} → {wr} {t['outcome_pnl']:+.1f}pts")

# Net impact
freed_pnl = sum(t["outcome_pnl"] or 0 for t in freed)
caught_pnl = sum(t["outcome_pnl"] or 0 for t in caught)
print(f"\n  Freed PnL (gains from allowing good shorts): {freed_pnl:+.1f} pts")
print(f"  Caught PnL (savings from blocking bad shorts): {caught_pnl:+.1f} pts")
print(f"  NET IMPROVEMENT: {freed_pnl - caught_pnl:+.1f} pts")

# ── PER-SETUP BREAKDOWN ─────────────────────────────────────────────────
print()
print("=" * 80)
print("PER-SETUP: BUGGED vs FIXED")
print("=" * 80)
setups = sorted(set(t["setup_name"] for t in trades))
for setup in setups:
    st = [t for t in trades if t["setup_name"] == setup]
    bp = [t for t in st if apply_optimal_bugged(t)]
    fp = [t for t in st if apply_optimal_fixed(t)]
    bs = stats(f"{setup} (bugged)", bp)
    fs = stats(f"{setup} (fixed)", fp)
    print(f"\n  {setup}:")
    print(f"    Bugged: N={bs['n']:>3d}  WR={bs['wr']:5.1f}%  PnL={bs['pnl']:>+8.1f}")
    print(f"    Fixed:  N={fs['n']:>3d}  WR={fs['wr']:5.1f}%  PnL={fs['pnl']:>+8.1f}  "
          f"Delta={fs['pnl']-bs['pnl']:>+8.1f}")

# ── SHORT-ONLY ANALYSIS ─────────────────────────────────────────────────
print()
print("=" * 80)
print("SHORT TRADES ONLY: alignment breakdown")
print("=" * 80)
for a in range(-3, 4):
    st = [t for t in shorts if t.get("greek_alignment") == a]
    if st:
        s = stats(f"SHORT align={a:+d}", st)
        bugged_action = "BLOCKED" if a > 0 else "PASSED"
        fixed_action = "BLOCKED" if a < 0 else "PASSED"
        print(f"  align={a:+d}: N={s['n']:>3d}  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  "
              f"Bugged={bugged_action:>7s}  Fixed={fixed_action:>7s}")
