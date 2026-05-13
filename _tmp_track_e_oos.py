"""
Track E — OOS validation on top candidate filters.

For each finalist candidate, split chronologically and report:
- In-sample lift (first half)
- Out-of-sample lift (second half)
- Lift consistency
- Per-month dollar value
"""
import json
import numpy as np
from datetime import datetime

FEATURES = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json"
OUT = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_oos_results.json"

np.random.seed(0)

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


def bootstrap_diff_ci(pos, neg, n_boot=2000):
    if not pos or not neg or len(pos) < 5 or len(neg) < 5:
        return (0.0, 0.0, 0.0, False)
    p = np.array(pos)
    n = np.array(neg)
    diffs = []
    for _ in range(n_boot):
        a = p[np.random.randint(0, len(p), len(p))]
        b = n[np.random.randint(0, len(n), len(n))]
        diffs.append(a.mean() - b.mean())
    lo = float(np.percentile(diffs, 2.5))
    hi = float(np.percentile(diffs, 97.5))
    return (float(np.mean(diffs)), lo, hi, lo > 0 or hi < 0)


def test_split(records, fn, label):
    """Split chronologically 50/50, compute lift in each half."""
    sorted_r = sorted(records, key=lambda r: r["ts"])
    if not sorted_r:
        return None
    half = len(sorted_r) // 2
    is_r = sorted_r[:half]
    oos_r = sorted_r[half:]

    def stats(rs):
        pos = [r["pnl"] for r in rs if fn(r)]
        neg = [r["pnl"] for r in rs if not fn(r)]
        if not pos or not neg:
            return None
        diff, lo, hi, sig = bootstrap_diff_ci(pos, neg, n_boot=1500)
        return {
            "n_pos": len(pos), "n_neg": len(neg),
            "pos_mean": float(np.mean(pos)), "pos_total": float(np.sum(pos)),
            "pos_wr": sum(1 for x in pos if x > 0) / len(pos),
            "neg_mean": float(np.mean(neg)),
            "diff": diff, "ci_lo": lo, "ci_hi": hi, "sig": sig,
        }

    return {
        "label": label,
        "is_period": (sorted_r[0]["ts"][:10], is_r[-1]["ts"][:10]) if is_r else None,
        "oos_period": (oos_r[0]["ts"][:10], oos_r[-1]["ts"][:10]) if oos_r else None,
        "full": stats(sorted_r),
        "is": stats(is_r),
        "oos": stats(oos_r),
    }


# Top candidate definitions
CANDIDATES = [
    {
        "id": "C1_SC_short_charm_neutral",
        "setup": "Skew Charm",
        "direction": "short",
        "fn": lambda r: abs(r.get("charm_at_spot", 0)) < 25e6,
        "polarity": "include",  # signal to TAKE the trade
        "mechanism": "Skew Charm shorts only when charm at spot strike is near zero (<25M absolute). Indicates breakout-zone setup where dealers aren't pinning the strike.",
    },
    {
        "id": "C2_SC_short_no_against_wall",
        "setup": "Skew Charm",
        "direction": "short",
        "fn": lambda r: r.get("wall_against_dir_dist") is None or r["wall_against_dir_dist"] > 30,
        "polarity": "include",
        "mechanism": "Skew Charm shorts only when there is no charm support wall within 30 pts below spot. Avoids shorts into nearby support that would reverse.",
    },
    {
        "id": "C3_SC_short_against_wall_block",
        "setup": "Skew Charm",
        "direction": "short",
        "fn": lambda r: r.get("wall_against_dir_dist") is not None and r["wall_against_dir_dist"] <= 5,
        "polarity": "exclude",  # signal to AVOID the trade
        "mechanism": "BLOCK Skew Charm shorts when a charm support wall sits within 5 pts below spot. Dealers will buy at that level, reversing the short.",
    },
    {
        "id": "C4_DD_short_charm_neutral",
        "setup": "DD Exhaustion",
        "direction": "short",
        "fn": lambda r: abs(r.get("charm_at_spot", 0)) < 25e6,
        "polarity": "include",
        "mechanism": "DD Exhaustion shorts only when charm at spot near zero. Aligns with contrarian setup design: spot is in a charm-neutral pivot zone.",
    },
    {
        "id": "C5_SC_LONG_gradient_block",
        "setup": "Skew Charm",
        "direction": "long",
        "fn": lambda r: r.get("charm_gradient_15", 0) < -1e6,
        "polarity": "exclude",
        "mechanism": "BLOCK Skew Charm longs when charm gradient is steeply negative across strikes near spot. Counter-intuitively, when charm 'aligns' with long via steep negative slope, the trades fail (anti-predictive in our data).",
    },
    {
        "id": "C6_SC_short_after_14_block",
        "setup": "Skew Charm",
        "direction": "short",
        "fn": lambda r: r.get("et_hour", 12) >= 14.0,
        "polarity": "exclude",
        "mechanism": "BLOCK Skew Charm shorts after 14:00 ET. Contradicts Volland's 'dealer o'clock' wisdom for this specific setup — SC shorts post-2pm have WR 51% vs 65% pre-2pm.",
    },
    {
        "id": "C7_ES_short_charm_neutral",
        "setup": "ES Absorption",
        "direction": "bearish",
        "fn": lambda r: abs(r.get("charm_at_spot", 0)) < 25e6,
        "polarity": "include",
        "mechanism": "ES Absorption shorts only when charm-neutral at spot. Marginal but significant.",
    },
]


results = []
for cand in CANDIDATES:
    # Find direction-specific subset
    if cand["direction"] in ("long", "bullish"):
        sub = [r for r in recs if r["setup"] == cand["setup"] and r["is_long"]]
    else:
        sub = [r for r in recs if r["setup"] == cand["setup"] and r["is_short"]]
    if len(sub) < 50:
        print(f"SKIP {cand['id']}: only {len(sub)} trades")
        continue

    out = test_split(sub, cand["fn"], cand["id"])
    out["polarity"] = cand["polarity"]
    out["mechanism"] = cand["mechanism"]
    out["setup"] = cand["setup"]
    out["direction"] = cand["direction"]
    results.append(out)

    print(f"\n{'=' * 70}")
    print(f"  {cand['id']}  ({cand['setup']} {cand['direction']})  polarity={cand['polarity']}")
    print(f"{'=' * 70}")
    print(f"  {cand['mechanism']}")

    def fmt(stats):
        if not stats:
            return "  (no data)"
        return (f"    n+={stats['n_pos']:4d} n-={stats['n_neg']:4d}  "
                f"WR+={stats['pos_wr']*100:5.1f}%  "
                f"PnL/+={stats['pos_mean']:+6.2f}  Tot+={stats['pos_total']:+7.1f}  "
                f"diff={stats['diff']:+6.2f}  CI=[{stats['ci_lo']:+5.2f},{stats['ci_hi']:+5.2f}]  "
                f"{'*** SIG' if stats['sig'] else '       '}")

    print(f"\n  FULL  ({out['is_period'][0]} -> {out['oos_period'][1]}):")
    print(fmt(out["full"]))
    print(f"  IS   ({out['is_period'][0]} -> {out['is_period'][1]}):")
    print(fmt(out["is"]))
    print(f"  OOS  ({out['oos_period'][0]} -> {out['oos_period'][1]}):")
    print(fmt(out["oos"]))

    # Compute monthly $/MES
    # Period span: Mar 1 to May 13 = ~2.4 months
    if cand["polarity"] == "include":
        # Filter applied = take these trades
        kept_pnl = out["full"]["pos_total"]
        n = out["full"]["n_pos"]
        # Compare to status-quo where ALL trades taken
        sq_pnl = out["full"]["pos_total"] + out["full"]["n_neg"] * out["full"]["neg_mean"]
        delta = kept_pnl - sq_pnl  # change in total PnL by applying include-filter
        monthly_delta = delta * 5 / 2.4  # $ per month at 1 MES
        print(f"\n  Action: TAKE only when filter true (drop {out['full']['n_neg']} trades).")
        print(f"  Status-quo PnL: {sq_pnl:+.1f} pts | Filtered PnL: {kept_pnl:+.1f} pts | Delta: {delta:+.1f} pts")
        print(f"  Monthly $ at 1 MES: ${monthly_delta:+.0f}/mo (delta vs status quo)")
    else:
        # Exclude filter: drop the matches
        dropped_pnl = out["full"]["pos_total"]
        kept_pnl = out["full"]["n_neg"] * out["full"]["neg_mean"]
        sq_pnl = dropped_pnl + kept_pnl
        delta = -dropped_pnl  # we save the dropped PnL (which is negative if dropped trades lose money)
        monthly_delta = delta * 5 / 2.4
        print(f"\n  Action: BLOCK when filter true (drop {out['full']['n_pos']} trades).")
        print(f"  Status-quo PnL: {sq_pnl:+.1f} pts | Filtered PnL: {kept_pnl:+.1f} pts | Delta: {delta:+.1f} pts")
        print(f"  Monthly $ at 1 MES: ${monthly_delta:+.0f}/mo (delta vs status quo)")

    out["monthly_dollars"] = monthly_delta

# Save
with open(OUT, "w") as f:
    json.dump(results, f, default=str, indent=2)
print(f"\nSaved OOS results to {OUT}")

# Rank by $/mo with positive sign and IS+OOS both positive (consistent direction)
print(f"\n{'=' * 70}")
print(f"  FINAL RANKING")
print(f"{'=' * 70}")

scored = []
for r in results:
    is_diff = r["is"]["diff"] if r.get("is") else 0
    oos_diff = r["oos"]["diff"] if r.get("oos") else 0
    full_sig = r["full"]["sig"] if r.get("full") else False
    same_sign = (is_diff > 0) == (oos_diff > 0) if r.get("is") and r.get("oos") else False
    consistency = "HIGH" if full_sig and same_sign and abs(is_diff - oos_diff) < 5 else \
                  "MED" if same_sign else "LOW"
    scored.append((r["label"], r["monthly_dollars"], consistency, is_diff, oos_diff, full_sig))

scored.sort(key=lambda x: -x[1])
for label, mo, cons, is_d, oos_d, sig in scored:
    print(f"  {label:40s}  ${mo:+6.0f}/mo  consistency={cons:4s}  IS={is_d:+6.2f}  OOS={oos_d:+6.2f}  full_sig={sig}")
