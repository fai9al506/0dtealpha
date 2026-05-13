"""Follow-up: explore what's special about the H7 'negative' trades (charm NOT neutral)."""
import json
import numpy as np

FEATURES = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json"

with open(FEATURES) as f:
    raw = json.load(f)

recs = []
for r in raw:
    flat = {
        "trade_id": r["trade_id"], "ts": r["ts"], "setup": r["setup"],
        "direction": r["direction"], "grade": r["grade"], "paradigm": r["paradigm"],
        "alignment": r["alignment"], "pnl": r["pnl"], "vix": r["vix"],
    }
    flat.update(r["charm_features"])
    flat["is_long"] = r["direction"].lower() in ("long", "bullish")
    flat["is_short"] = r["direction"].lower() in ("short", "bearish")
    recs.append(flat)

# Histogram of |charm_at_spot| across all live trades
LIVE_SETUPS = ("Skew Charm", "AG Short", "DD Exhaustion", "GEX Long",
               "ES Absorption", "VIX Divergence", "Paradigm Reversal",
               "Vanna Pivot Bounce", "BofA Scalp")
live = [r for r in recs if r["setup"] in LIVE_SETUPS]

abs_charm = [abs(r.get("charm_at_spot", 0)) for r in live]
print(f"Total live trades: {len(live)}")
print(f"\n|charm_at_spot| distribution (millions):")
for p in [10, 25, 50, 75, 90, 95, 99]:
    pct_val = np.percentile(abs_charm, p) / 1e6
    print(f"  P{p:2d}: {pct_val:8.2f}M")
print(f"  Max: {max(abs_charm)/1e6:.2f}M")

# How many fail H7 (>= 25M)?
fail = [r for r in live if abs(r.get("charm_at_spot", 0)) >= 25e6]
print(f"\nTrades with |charm_at_spot| >= 25M: {len(fail)} ({len(fail)/len(live)*100:.1f}%)")
print(f"  WR: {sum(1 for r in fail if r['pnl']>0)/len(fail)*100:.1f}%")
print(f"  Mean PnL: {np.mean([r['pnl'] for r in fail]):+.2f}")
print(f"  Total PnL: {sum(r['pnl'] for r in fail):+.1f}")

# What characterizes these "fail H7" trades?
print(f"\nSetup breakdown of fail-H7 trades:")
from collections import Counter
counter = Counter(r["setup"] for r in fail)
for s, c in counter.most_common():
    setup_recs = [r for r in fail if r["setup"] == s]
    avg_pnl = np.mean([r["pnl"] for r in setup_recs])
    print(f"  {s:25s} n={c:3d} avg_pnl={avg_pnl:+6.2f}")

# How big is |charm_at_spot| typically?
threshold_25M = sum(1 for r in live if abs(r.get("charm_at_spot", 0)) >= 25e6)
threshold_50M = sum(1 for r in live if abs(r.get("charm_at_spot", 0)) >= 50e6)
threshold_100M = sum(1 for r in live if abs(r.get("charm_at_spot", 0)) >= 100e6)
threshold_200M = sum(1 for r in live if abs(r.get("charm_at_spot", 0)) >= 200e6)
print(f"\n|charm_at_spot| thresholds:")
for t, n in [(25, threshold_25M), (50, threshold_50M), (100, threshold_100M), (200, threshold_200M)]:
    print(f"  >= {t}M: {n} trades ({n/len(live)*100:.1f}%)")

# Check H7 with stricter threshold: at 100M instead of 25M
print(f"\nH7 with various thresholds (full live pool):")
for thr_mm in [10, 25, 50, 100, 200]:
    pos = [r for r in live if abs(r.get("charm_at_spot", 0)) < thr_mm*1e6]
    neg = [r for r in live if abs(r.get("charm_at_spot", 0)) >= thr_mm*1e6]
    if len(pos) < 30 or len(neg) < 30:
        print(f"  thr={thr_mm}M: n_pos={len(pos)} n_neg={len(neg)} (skip)")
        continue
    pos_wr = sum(1 for r in pos if r["pnl"]>0)/len(pos)
    neg_wr = sum(1 for r in neg if r["pnl"]>0)/len(neg)
    pos_mean = np.mean([r["pnl"] for r in pos])
    neg_mean = np.mean([r["pnl"] for r in neg])
    print(f"  thr={thr_mm:3d}M: n_pos={len(pos):4d} (WR={pos_wr*100:.1f}% mean={pos_mean:+.2f})  n_neg={len(neg):4d} (WR={neg_wr*100:.1f}% mean={neg_mean:+.2f})  diff={pos_mean-neg_mean:+.2f}")

# What about H7 stratified by setup direction?
print(f"\nH7 by setup+direction (threshold 25M):")
combos = {}
for r in live:
    key = (r["setup"], "short" if r["is_short"] else "long")
    combos.setdefault(key, []).append(r)

for (s, d), rs in sorted(combos.items(), key=lambda x: -len(x[1])):
    if len(rs) < 30:
        continue
    pos = [r for r in rs if abs(r.get("charm_at_spot", 0)) < 25e6]
    neg = [r for r in rs if abs(r.get("charm_at_spot", 0)) >= 25e6]
    if not pos or not neg:
        continue
    pos_wr = sum(1 for r in pos if r["pnl"]>0)/len(pos)
    neg_wr = sum(1 for r in neg if r["pnl"]>0)/len(neg)
    pos_mean = np.mean([r["pnl"] for r in pos])
    neg_mean = np.mean([r["pnl"] for r in neg])
    print(f"  {s:25s} {d:5s}: n_pos={len(pos):3d} (mean={pos_mean:+6.2f})  n_neg={len(neg):3d} (mean={neg_mean:+6.2f})  diff={pos_mean-neg_mean:+.2f}")
