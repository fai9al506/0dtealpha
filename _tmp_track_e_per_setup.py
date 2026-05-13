"""
Track E — Per-setup hypothesis testing. Pooled tests dilute setup-specific edges.

We re-test the most promising hypotheses on a per-setup basis to find where
charm features actually deliver edge.
"""
import json
import numpy as np
from collections import defaultdict

FEATURES = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json"
OUT = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_per_setup_results.json"

np.random.seed(0)

with open(FEATURES) as f:
    raw = json.load(f)

# Flatten
recs = []
for r in raw:
    flat = {
        "trade_id": r["trade_id"], "ts": r["ts"], "setup": r["setup"],
        "direction": r["direction"], "grade": r["grade"], "paradigm": r["paradigm"],
        "alignment": r["alignment"], "pnl": r["pnl"], "vix": r["vix"],
        "spot": r["spot"],
    }
    flat.update(r["charm_features"])
    flat["is_long"] = r["direction"].lower() in ("long", "bullish")
    flat["is_short"] = r["direction"].lower() in ("short", "bearish")
    recs.append(flat)

# Live setups
LIVE_SETUPS = ("Skew Charm", "AG Short", "DD Exhaustion", "GEX Long",
               "ES Absorption", "VIX Divergence", "Paradigm Reversal",
               "Vanna Pivot Bounce", "BofA Scalp")


def bootstrap_diff_ci(pos, neg, n_boot=1500):
    """Return (diff_mean, ci_lo, ci_hi)."""
    if not pos or not neg or len(pos) < 5 or len(neg) < 5:
        return (0.0, 0.0, 0.0, False)
    diffs = []
    p_arr = np.array(pos)
    n_arr = np.array(neg)
    for _ in range(n_boot):
        a = p_arr[np.random.randint(0, len(p_arr), len(p_arr))]
        b = n_arr[np.random.randint(0, len(n_arr), len(n_arr))]
        diffs.append(a.mean() - b.mean())
    lo = float(np.percentile(diffs, 2.5))
    hi = float(np.percentile(diffs, 97.5))
    diff = float(np.mean(diffs))
    sig = lo > 0 or hi < 0
    return (diff, lo, hi, sig)


def test_filter(records, fn, name):
    pos = [r for r in records if fn(r)]
    neg = [r for r in records if not fn(r)]
    pos_pnl = [r["pnl"] for r in pos]
    neg_pnl = [r["pnl"] for r in neg]
    if not pos_pnl or not neg_pnl:
        return None
    pos_wr = sum(1 for r in pos if r["pnl"] > 0) / len(pos)
    neg_wr = sum(1 for r in neg if r["pnl"] > 0) / len(neg)
    diff, lo, hi, sig = bootstrap_diff_ci(pos_pnl, neg_pnl)
    return {
        "name": name,
        "n_pos": len(pos), "n_neg": len(neg),
        "wr_pos": pos_wr, "wr_neg": neg_wr,
        "pnl_pos_mean": float(np.mean(pos_pnl)),
        "pnl_pos_total": float(np.sum(pos_pnl)),
        "pnl_neg_mean": float(np.mean(neg_pnl)),
        "diff": diff, "ci_lo": lo, "ci_hi": hi, "sig": sig,
    }


def report(records, label):
    if not records:
        return
    print(f"\n{'=' * 70}")
    print(f"  {label}  (n={len(records)})")
    print(f"{'=' * 70}")

    # Hypotheses to test
    tests = [
        ("H1_shorts_wall_above_5_15", lambda r: r["is_short"] and (r.get("charm_wall_above_dist") or 99) >= 5 and (r.get("charm_wall_above_dist") or 99) <= 15),
        ("H2_longs_wall_below_5_15", lambda r: r["is_long"] and (r.get("charm_wall_below_dist") or 99) >= 5 and (r.get("charm_wall_below_dist") or 99) <= 15),
        ("H4_after_14_ET", lambda r: r.get("et_hour", 12) >= 14.0),
        ("H4b_morning_before_12", lambda r: r.get("et_hour", 12) < 12.0),
        ("H4c_midday_12_to_14", lambda r: 12.0 <= r.get("et_hour", 12) < 14.0),
        ("H6_gradient_with_dir", lambda r: (r["is_long"] and r.get("charm_gradient_15", 0) < -1e6) or (r["is_short"] and r.get("charm_gradient_15", 0) > 1e6)),
        ("H7_charm_neutral_spot", lambda r: abs(r.get("charm_at_spot", 0)) < 25e6),
        ("H10_against_wall_close", lambda r: (r.get("wall_against_dir_dist") or 99) <= 5),
        ("H11_with_wall_5_15", lambda r: (r.get("wall_with_dir_dist") or 99) >= 5 and (r.get("wall_with_dir_dist") or 99) <= 15),
        ("H12_no_against_wall_in_30", lambda r: r.get("wall_against_dir_dist") is None or r["wall_against_dir_dist"] > 30),
        ("H13_strong_with_dir_bias", lambda r: r.get("charm_bias_with_dir", 0) > 500e6),
        ("H14_charm_neutral_lis_strong", lambda r: abs(r.get("charm_at_spot", 0)) < 25e6 and abs(r.get("charm_at_lis", 0) or 0) > 200e6),
    ]
    out = []
    for name, fn in tests:
        res = test_filter(records, fn, name)
        if res is None:
            continue
        out.append(res)
        marker = "*** SIG" if res["sig"] else "       "
        print(f"  {name:35s} n+={res['n_pos']:4d} WR+={res['wr_pos']*100:5.1f}% PnL/+={res['pnl_pos_mean']:+7.2f} "
              f"Tot+={res['pnl_pos_total']:+8.1f} WR-={res['wr_neg']*100:5.1f}% PnL/-={res['pnl_neg_mean']:+7.2f} "
              f"diff={res['diff']:+7.2f} CI=[{res['ci_lo']:+5.2f},{res['ci_hi']:+5.2f}] {marker}")
    return out


# Overall live-relevant
live = [r for r in recs if r["setup"] in LIVE_SETUPS]
all_out = {}
all_out["OVERALL"] = report(live, "OVERALL — all live-relevant setups")

# Per-setup splits with adequate sample size
setup_counts = defaultdict(int)
for r in live:
    setup_counts[r["setup"]] += 1
print(f"\nSetup counts: {dict(sorted(setup_counts.items(), key=lambda x: -x[1]))}")

for setup_name in sorted(setup_counts.keys(), key=lambda s: -setup_counts[s]):
    if setup_counts[setup_name] < 50:
        continue
    setup_recs = [r for r in live if r["setup"] == setup_name]
    all_out[setup_name] = report(setup_recs, f"{setup_name} (n={len(setup_recs)})")

# Direction splits
for setup_name in ("Skew Charm", "DD Exhaustion", "ES Absorption"):
    for direction_label, dir_fn in [("LONG", lambda r: r["is_long"]), ("SHORT", lambda r: r["is_short"])]:
        sub = [r for r in live if r["setup"] == setup_name and dir_fn(r)]
        if len(sub) < 50:
            continue
        all_out[f"{setup_name} {direction_label}"] = report(sub, f"{setup_name} {direction_label} (n={len(sub)})")

# Save
with open(OUT, "w") as f:
    json.dump(all_out, f, default=str, indent=2)
print(f"\nSaved per-setup results to {OUT}")
