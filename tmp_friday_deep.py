"""Deep dive into Friday longs (the only winning filter) + additional nuances."""

import json
from collections import defaultdict

with open("tmp_backtest_data.json") as f:
    trades = json.load(f)

# Friday = DOW 5
friday_trades = [t for t in trades if t["dow"] == 5]
friday_longs = [t for t in friday_trades if t["direction"] in ("long", "bullish")]
friday_shorts = [t for t in friday_trades if t["direction"] in ("short", "bearish")]

print("=" * 80)
print("FRIDAY DEEP DIVE")
print("=" * 80)

# Friday longs vs shorts
def stats(ts, label):
    if not ts: return
    wins = len([t for t in ts if t["outcome"] == "WIN"])
    losses = len([t for t in ts if t["outcome"] == "LOSS"])
    pnl = sum(t["outcome_pnl"] for t in ts)
    wr = wins/len(ts)*100 if ts else 0
    win_pnl = sum(t["outcome_pnl"] for t in ts if t["outcome"] == "WIN")
    loss_pnl = sum(abs(t["outcome_pnl"]) for t in ts if t["outcome"] == "LOSS")
    pf = win_pnl/loss_pnl if loss_pnl > 0 else float("inf")
    print(f"  {label:40s} | {len(ts):3d} trades | {wins}W/{losses}L | WR {wr:.0f}% | PnL {pnl:+.1f} | PF {pf:.2f}")

stats(friday_longs, "Friday LONGS")
stats(friday_shorts, "Friday SHORTS")

# Friday longs by setup
print("\n  Friday LONGS by setup:")
for setup in sorted(set(t["setup_name"] for t in friday_longs)):
    st = [t for t in friday_longs if t["setup_name"] == setup]
    stats(st, f"  {setup}")

# Friday longs by date
print("\n  Friday LONGS by date:")
for d in sorted(set(t["trade_date"] for t in friday_longs)):
    dt = [t for t in friday_longs if t["trade_date"] == d]
    pnl = sum(t["outcome_pnl"] for t in dt)
    wins = len([t for t in dt if t["outcome"] == "WIN"])
    losses = len([t for t in dt if t["outcome"] == "LOSS"])
    vix = dt[0].get("vix", "n/a")
    print(f"    {d}: {len(dt)} trades, {wins}W/{losses}L, PnL {pnl:+.1f}, VIX {vix}")

# Friday longs by paradigm
print("\n  Friday LONGS by paradigm:")
for p in sorted(set(t.get("paradigm","None") for t in friday_longs)):
    st = [t for t in friday_longs if t.get("paradigm","None") == p]
    if st:
        stats(st, f"  {p}")

# Compare: Non-Friday longs
non_fri_longs = [t for t in trades if t["dow"] != 5 and t["direction"] in ("long", "bullish")]
print("\n  For comparison:")
stats(non_fri_longs, "Non-Friday LONGS")

# Would V9-SC already block most Friday longs?
print("\n\n" + "=" * 80)
print("V9-SC INTERACTION: How many Friday longs does V9-SC already block?")
print("=" * 80)

def v9sc_passes(t):
    direction = t["direction"]
    setup = t["setup_name"]
    alignment = t.get("alignment") or 0
    vix = t.get("vix")
    overvix = t.get("overvix")
    if direction in ("long", "bullish"):
        if alignment < 2: return False
        is_sc = setup == "Skew Charm"
        vix_ok = vix is not None and vix <= 22
        overvix_ok = overvix is not None and overvix >= 2
        if not (is_sc or vix_ok or overvix_ok): return False
        return True
    else:
        if setup == "Skew Charm": return True
        if setup == "AG Short": return True
        if setup == "DD Exhaustion": return alignment != 0
        return False

fri_longs_v9sc = [t for t in friday_longs if v9sc_passes(t)]
fri_longs_blocked_v9sc = [t for t in friday_longs if not v9sc_passes(t)]
print(f"  Friday longs total: {len(friday_longs)}")
print(f"  V9-SC passes: {len(fri_longs_v9sc)}")
print(f"  V9-SC blocks: {len(fri_longs_blocked_v9sc)}")

if fri_longs_v9sc:
    stats(fri_longs_v9sc, "Friday longs PASSING V9-SC")
    print("  These are what the Friday-longs filter would additionally block on top of V9-SC")
    # Breakdown
    for setup in sorted(set(t["setup_name"] for t in fri_longs_v9sc)):
        st = [t for t in fri_longs_v9sc if t["setup_name"] == setup]
        stats(st, f"    {setup}")

print("\n\n" + "=" * 80)
print("ADDITIONAL PARADIGM FINDINGS")
print("=" * 80)

# AG-LIS deep dive (Discord said it's toxic for SC)
ag_lis = [t for t in trades if t.get("paradigm") == "AG-LIS"]
print("\nAG-LIS paradigm (Discord flagged as potentially toxic):")
stats(ag_lis, "AG-LIS all trades")
for setup in sorted(set(t["setup_name"] for t in ag_lis)):
    st = [t for t in ag_lis if t["setup_name"] == setup]
    stats(st, f"  {setup}")

# BOFA-MESSY (worst paradigm)
bm = [t for t in trades if t.get("paradigm") == "BOFA-MESSY"]
print("\nBOFA-MESSY paradigm (worst performer):")
stats(bm, "BOFA-MESSY all trades")
for setup in sorted(set(t["setup_name"] for t in bm)):
    st = [t for t in bm if t["setup_name"] == setup]
    stats(st, f"  {setup}")

# GEX-LIS (also bad)
gl = [t for t in trades if t.get("paradigm") == "GEX-LIS"]
print("\nGEX-LIS paradigm (second worst):")
stats(gl, "GEX-LIS all trades")
for setup in sorted(set(t["setup_name"] for t in gl)):
    st = [t for t in gl if t["setup_name"] == setup]
    stats(st, f"  {setup}")

# What if we block BOFA-MESSY + GEX-LIS?
block_bad_paradigms = {"BOFA-MESSY", "GEX-LIS"}
test_bp = [t for t in trades if t.get("paradigm","") not in block_bad_paradigms]
blocked_bp = [t for t in trades if t.get("paradigm","") in block_bad_paradigms]
print(f"\nBlock BOFA-MESSY + GEX-LIS:")
stats(test_bp, "Without BOFA-MESSY + GEX-LIS")
stats(blocked_bp, "BLOCKED trades")
delta = sum(t["outcome_pnl"] for t in test_bp) - sum(t["outcome_pnl"] for t in trades)
print(f"  PnL delta: {delta:+.1f}")

# VIX 26+ longs (our existing V9-SC handles some but not all)
vix26_longs = [t for t in trades if t.get("vix") is not None and t["vix"] >= 26 and t["direction"] in ("long", "bullish")]
print(f"\nVIX >= 26 LONGS: {len(vix26_longs)} trades")
stats(vix26_longs, "VIX>=26 longs")

# What about the lunch hour dip?
print("\n\n" + "=" * 80)
print("LUNCH HOUR (12:00-13:00) vs REST")
print("=" * 80)
lunch = [t for t in trades if t["hour"] == 12]
lunch_longs = [t for t in lunch if t["direction"] in ("long", "bullish")]
lunch_shorts = [t for t in lunch if t["direction"] in ("short", "bearish")]
stats(lunch, "12:xx all")
stats(lunch_longs, "12:xx longs")
stats(lunch_shorts, "12:xx shorts")

# Block lunch longs
test_nolunch = [t for t in trades if not (t["hour"] == 12 and t["direction"] in ("long", "bullish"))]
delta_lunch = sum(t["outcome_pnl"] for t in test_nolunch) - sum(t["outcome_pnl"] for t in trades)
print(f"\n  Block lunch longs: {delta_lunch:+.1f} pts delta")

# 14:xx analysis
h14 = [t for t in trades if t["hour"] == 14]
h14_longs = [t for t in h14 if t["direction"] in ("long", "bullish")]
h14_shorts = [t for t in h14 if t["direction"] in ("short", "bearish")]
stats(h14, "14:xx all")
stats(h14_longs, "14:xx longs")
stats(h14_shorts, "14:xx shorts")

print("\n\n" + "=" * 80)
print("BEST COMBO: Friday longs block + BOFA-MESSY/GEX-LIS block")
print("=" * 80)

best_combo = [
    t for t in trades
    if not (t["dow"] == 5 and t["direction"] in ("long", "bullish"))
    and t.get("paradigm","") not in block_bad_paradigms
]
blocked_combo = [t for t in trades if t not in best_combo]
stats(best_combo, "Best combo")
total_base = sum(t["outcome_pnl"] for t in trades)
total_combo = sum(t["outcome_pnl"] for t in best_combo)
print(f"  PnL delta: {total_combo - total_base:+.1f} pts")
print(f"  Trades blocked: {len(blocked_combo)}")

# What about Friday longs + GEX-LIS + BOFA-MESSY + lunch longs?
mega = [
    t for t in trades
    if not (t["dow"] == 5 and t["direction"] in ("long", "bullish"))
    and t.get("paradigm","") not in block_bad_paradigms
    and not (t["hour"] == 12 and t["direction"] in ("long", "bullish"))
]
total_mega = sum(t["outcome_pnl"] for t in mega)
stats(mega, "Mega combo (Fri+paradigm+lunch longs)")
print(f"  PnL delta: {total_mega - total_base:+.1f} pts")

print("\nDone.")
