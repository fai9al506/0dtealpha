"""Deep dive on C2 (SC short, no opposing charm wall in 30 pts) and related."""
import json, numpy as np
from collections import defaultdict

FEATURES = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json"
with open(FEATURES) as f:
    raw = json.load(f)
recs = []
for r in raw:
    flat = {**{k: r[k] for k in ["trade_id","ts","setup","direction","grade","paradigm","alignment","pnl","vix"]}, **r["charm_features"]}
    flat["is_long"] = r["direction"].lower() in ("long","bullish")
    flat["is_short"] = r["direction"].lower() in ("short","bearish")
    recs.append(flat)

sc_short = [r for r in recs if r["setup"] == "Skew Charm" and r["is_short"]]
print(f"SC SHORT total: {len(sc_short)}")

# Vary the threshold for "no against wall" — basically wall_against_dir is support wall (below spot for shorts)
# The candidate uses 30. Let's also check 10, 15, 20, 50
for cutoff in [None, 10, 15, 20, 30, 50, 100]:
    if cutoff is None:
        # No-wall at all (None)
        pos = [r for r in sc_short if r.get("wall_against_dir_dist") is None]
        label = "wall is None (no wall detected within 100pt)"
    else:
        pos = [r for r in sc_short if (r.get("wall_against_dir_dist") is None) or (r.get("wall_against_dir_dist") > cutoff)]
        label = f"wall > {cutoff} pts or None"
    neg = [r for r in sc_short if r not in pos]
    if not pos or not neg:
        continue
    pos_pnl = [r["pnl"] for r in pos]
    neg_pnl = [r["pnl"] for r in neg]
    pos_wr = sum(1 for p in pos_pnl if p>0)/len(pos)
    neg_wr = sum(1 for p in neg_pnl if p>0)/len(neg)
    print(f"  {label:50s}: n_pos={len(pos):3d} WR={pos_wr*100:5.1f}% mean={np.mean(pos_pnl):+5.2f} tot={sum(pos_pnl):+6.1f}  | n_neg={len(neg):3d} WR={neg_wr*100:5.1f}% mean={np.mean(neg_pnl):+5.2f} tot={sum(neg_pnl):+6.1f}")

# Walk the wall-distance distribution
print(f"\nSC short distribution of wall_against_dir_dist:")
dists = [r.get("wall_against_dir_dist") for r in sc_short]
n_none = sum(1 for d in dists if d is None)
print(f"  None: {n_none} ({n_none/len(sc_short)*100:.1f}%)")
present = [d for d in dists if d is not None]
print(f"  Present (n={len(present)}):")
for p in [10, 25, 50, 75, 90, 95]:
    print(f"    P{p}: {np.percentile(present, p):.1f}")

# Bucket
buckets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 50), (50, 100)]
print(f"\nSC short bucketed by wall_against_dir_dist:")
for lo, hi in buckets:
    bucket = [r for r in sc_short if r.get("wall_against_dir_dist") is not None and lo <= r["wall_against_dir_dist"] < hi]
    if not bucket:
        continue
    wr = sum(1 for r in bucket if r["pnl"]>0)/len(bucket)
    mean_pnl = np.mean([r["pnl"] for r in bucket])
    print(f"  [{lo:3d},{hi:3d}): n={len(bucket):3d}  WR={wr*100:5.1f}%  mean={mean_pnl:+5.2f}  total={sum(r['pnl'] for r in bucket):+6.1f}")
none_bucket = [r for r in sc_short if r.get("wall_against_dir_dist") is None]
if none_bucket:
    wr = sum(1 for r in none_bucket if r["pnl"]>0)/len(none_bucket)
    mean_pnl = np.mean([r["pnl"] for r in none_bucket])
    print(f"  None      : n={len(none_bucket):3d}  WR={wr*100:5.1f}%  mean={mean_pnl:+5.2f}  total={sum(r['pnl'] for r in none_bucket):+6.1f}")

# Compare to V14 base — is this an ADD to V14 or already captured?
# V14 SC shorts have a paradigm/regime gate. Let's see if the charm signal is independent or already absorbed.
print(f"\nSC short charm-wall signal STRATIFIED by paradigm:")
para_groups = defaultdict(list)
for r in sc_short:
    para_groups[r.get("paradigm") or "None"].append(r)
for p, rs in sorted(para_groups.items(), key=lambda x: -len(x[1])):
    if len(rs) < 30:
        continue
    pos = [r for r in rs if (r.get("wall_against_dir_dist") is None) or (r["wall_against_dir_dist"] > 30)]
    neg = [r for r in rs if not ((r.get("wall_against_dir_dist") is None) or (r.get("wall_against_dir_dist", 0) > 30))]
    if not pos or not neg:
        continue
    pos_pnl_t = sum(r["pnl"] for r in pos)
    neg_pnl_t = sum(r["pnl"] for r in neg)
    pos_wr = sum(1 for r in pos if r["pnl"]>0)/len(pos)
    neg_wr = sum(1 for r in neg if r["pnl"]>0)/len(neg)
    print(f"  paradigm={p:20s} n_pos={len(pos):3d} (WR={pos_wr*100:.1f}% tot={pos_pnl_t:+.1f}) n_neg={len(neg):3d} (WR={neg_wr*100:.1f}% tot={neg_pnl_t:+.1f}) diff={np.mean([r['pnl'] for r in pos])-np.mean([r['pnl'] for r in neg]):+.2f}")

# Same for grade
print(f"\nSC short charm-wall signal stratified by grade:")
g_groups = defaultdict(list)
for r in sc_short:
    g_groups[r.get("grade") or "None"].append(r)
for g, rs in sorted(g_groups.items(), key=lambda x: -len(x[1])):
    if len(rs) < 30:
        continue
    pos = [r for r in rs if (r.get("wall_against_dir_dist") is None) or (r["wall_against_dir_dist"] > 30)]
    neg = [r for r in rs if not ((r.get("wall_against_dir_dist") is None) or (r.get("wall_against_dir_dist", 0) > 30))]
    if not pos or not neg:
        continue
    pos_wr = sum(1 for r in pos if r["pnl"]>0)/len(pos)
    neg_wr = sum(1 for r in neg if r["pnl"]>0)/len(neg)
    pmean = np.mean([r["pnl"] for r in pos])
    nmean = np.mean([r["pnl"] for r in neg])
    print(f"  grade={g:6s} n_pos={len(pos):3d} (WR={pos_wr*100:.1f}% mean={pmean:+.2f}) n_neg={len(neg):3d} (WR={neg_wr*100:.1f}% mean={nmean:+.2f}) diff={pmean-nmean:+.2f}")

# Same for align
print(f"\nSC short charm-wall signal stratified by alignment:")
a_groups = defaultdict(list)
for r in sc_short:
    a_groups[r.get("alignment", 0)].append(r)
for a, rs in sorted(a_groups.items()):
    if len(rs) < 20:
        continue
    pos = [r for r in rs if (r.get("wall_against_dir_dist") is None) or (r["wall_against_dir_dist"] > 30)]
    neg = [r for r in rs if not ((r.get("wall_against_dir_dist") is None) or (r.get("wall_against_dir_dist", 0) > 30))]
    if not pos or not neg:
        continue
    pmean = np.mean([r["pnl"] for r in pos])
    nmean = np.mean([r["pnl"] for r in neg])
    print(f"  align={a:+d} n_pos={len(pos):3d} (mean={pmean:+.2f}) n_neg={len(neg):3d} (mean={nmean:+.2f}) diff={pmean-nmean:+.2f}")
