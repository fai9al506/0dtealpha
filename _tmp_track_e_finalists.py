"""Final refined finalists. OOS check with tighter thresholds + monthly $ at 1 MES."""
import json
import numpy as np

FEATURES = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json"
with open(FEATURES) as f:
    raw = json.load(f)
recs = []
for r in raw:
    flat = {**{k: r[k] for k in ["trade_id","ts","setup","direction","grade","paradigm","alignment","pnl","vix"]}, **r["charm_features"]}
    flat["is_long"] = r["direction"].lower() in ("long","bullish")
    flat["is_short"] = r["direction"].lower() in ("short","bearish")
    recs.append(flat)

np.random.seed(0)

def bootstrap_ci(values, n_boot=2000):
    if not values:
        return (0.0, 0.0, 0.0)
    arr = np.array(values)
    if len(arr) < 5:
        return (float(arr.mean()), float(arr.mean()), float(arr.mean()))
    boots = np.array([arr[np.random.randint(0, len(arr), len(arr))].mean() for _ in range(n_boot)])
    return (float(arr.mean()), float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5)))

# 5 refined candidates
CANDIDATES = [
    {
        "id": "F1_SC_SHORT_BLOCK_support_within_10",
        "subset": lambda r: r["setup"] == "Skew Charm" and r["is_short"],
        "match_fn": lambda r: r.get("wall_against_dir_dist") is not None and r["wall_against_dir_dist"] <= 10,
        "polarity": "exclude",
        "mechanism": (
            "BLOCK Skew Charm shorts when a charm SUPPORT wall (charm sign-change with |value|>=50M) "
            "sits within 10 pts below spot. Per Volland white paper: dealers reverse hedging at charm "
            "sign-change strikes. A support wall close below traps shorts — dealers buy underlying as price "
            "drops toward it, reversing the move. Within 10 pts is the actionable zone for 0DTE charm hedging."
        ),
    },
    {
        "id": "F2_SC_SHORT_INCL_charm_neutral_lt_25M",
        "subset": lambda r: r["setup"] == "Skew Charm" and r["is_short"],
        "match_fn": lambda r: abs(r.get("charm_at_spot", 0)) < 25e6,
        "polarity": "include",
        "mechanism": (
            "Skew Charm shorts only when |charm at spot strike| < 25M. Charm-neutral entry indicates a "
            "breakout zone: dealers aren't actively hedging at this strike, so directional momentum can "
            "manifest. Volland white paper: 'If an outside party trades strongly in one direction or the "
            "other, the charm bars will flip their sign and price can begin to trend.'"
        ),
    },
    {
        "id": "F3_DD_SHORT_INCL_charm_neutral_lt_25M",
        "subset": lambda r: r["setup"] == "DD Exhaustion" and r["is_short"],
        "match_fn": lambda r: abs(r.get("charm_at_spot", 0)) < 25e6,
        "polarity": "include",
        "mechanism": (
            "DD Exhaustion shorts only when charm at spot < 25M. Aligns with the contrarian "
            "premise: DD Exhaustion fires when DD-charm divergence indicates dealer over-positioning. "
            "If charm at the entry strike is large, dealers are actively pinning — bad time to fade. "
            "Charm-neutral confirms the pivot is genuine."
        ),
    },
    {
        "id": "F4_ES_SHORT_INCL_charm_neutral_lt_25M",
        "subset": lambda r: r["setup"] == "ES Absorption" and r["is_short"],
        "match_fn": lambda r: abs(r.get("charm_at_spot", 0)) < 25e6,
        "polarity": "include",
        "mechanism": (
            "ES Absorption shorts only when charm-neutral at spot. ES absorption is a CVD divergence "
            "setup; charm-neutral entry confirms there's no dealer pin causing the divergence."
        ),
    },
    {
        "id": "F5_SC_SHORT_BLOCK_against_5_10_combined",
        "subset": lambda r: r["setup"] == "Skew Charm" and r["is_short"],
        "match_fn": lambda r: (
            (r.get("wall_against_dir_dist") is not None and r["wall_against_dir_dist"] <= 10)
            or (abs(r.get("charm_at_spot", 0)) >= 25e6)
        ),
        "polarity": "exclude",
        "mechanism": (
            "Combined F1+F2: BLOCK Skew Charm shorts when EITHER (a) charm support wall within 10 pts "
            "below, OR (b) high charm at spot (>=25M). Both indicate dealer activity that traps the trade."
        ),
    },
]

results = []
for cand in CANDIDATES:
    subset = [r for r in recs if cand["subset"](r)]
    sorted_subset = sorted(subset, key=lambda r: r["ts"])
    half = len(sorted_subset) // 2
    is_r = sorted_subset[:half]
    oos_r = sorted_subset[half:]

    def analyze(rs):
        if cand["polarity"] == "include":
            kept = [r for r in rs if cand["match_fn"](r)]
            dropped = [r for r in rs if not cand["match_fn"](r)]
        else:  # exclude
            kept = [r for r in rs if not cand["match_fn"](r)]
            dropped = [r for r in rs if cand["match_fn"](r)]

        all_pnl = sum(r["pnl"] for r in rs)
        kept_pnl = sum(r["pnl"] for r in kept)
        dropped_pnl = sum(r["pnl"] for r in dropped)
        # Delta = kept - status_quo (all)
        delta = kept_pnl - all_pnl  # positive = filter helps
        kept_wr = (sum(1 for r in kept if r["pnl"] > 0) / len(kept)) if kept else 0
        all_wr = (sum(1 for r in rs if r["pnl"] > 0) / len(rs)) if rs else 0
        # CI on dropped-mean (the trades we're affecting)
        d_mean, d_lo, d_hi = bootstrap_ci([r["pnl"] for r in dropped]) if dropped else (0,0,0)
        return {
            "n_all": len(rs), "n_kept": len(kept), "n_dropped": len(dropped),
            "all_pnl": all_pnl, "kept_pnl": kept_pnl, "dropped_pnl": dropped_pnl,
            "kept_wr": kept_wr, "all_wr": all_wr,
            "delta": delta,
            "dropped_mean": d_mean,
            "dropped_ci": (d_lo, d_hi),
        }

    full = analyze(sorted_subset)
    is_stats = analyze(is_r) if is_r else None
    oos_stats = analyze(oos_r) if oos_r else None

    # Monthly $ at 1 MES (2.4 month span, 1 MES = $5/pt)
    monthly_dollars = full["delta"] * 5 / 2.4

    print(f"\n{'=' * 80}")
    print(f"  {cand['id']}  (polarity={cand['polarity']})")
    print(f"{'=' * 80}")
    print(f"  Mechanism: {cand['mechanism']}")
    print(f"\n  FULL:")
    print(f"    All trades: {full['n_all']:4d}  total PnL: {full['all_pnl']:+8.1f}  WR: {full['all_wr']*100:5.1f}%")
    print(f"    Kept (filter+):  {full['n_kept']:4d}  total: {full['kept_pnl']:+8.1f}  WR: {full['kept_wr']*100:5.1f}%")
    print(f"    Dropped:  {full['n_dropped']:4d}  total: {full['dropped_pnl']:+8.1f}  mean/trade: {full['dropped_mean']:+5.2f}  CI=[{full['dropped_ci'][0]:+5.2f}, {full['dropped_ci'][1]:+5.2f}]")
    print(f"    Delta vs status quo: {full['delta']:+.1f} pts  =  ${monthly_dollars:+.0f}/mo at 1 MES")
    print(f"\n  IS half ({is_r[0]['ts'][:10]} -> {is_r[-1]['ts'][:10]}, n={len(is_r)}):")
    if is_stats:
        is_mo = is_stats["delta"] * 5 / 1.2  # 1.2 months
        print(f"    Delta: {is_stats['delta']:+.1f} pts  =  ${is_mo:+.0f}/mo (extrapolated)")
        print(f"    Dropped: n={is_stats['n_dropped']:3d} mean={is_stats['dropped_mean']:+5.2f}")
    if oos_stats:
        oos_mo = oos_stats["delta"] * 5 / 1.2
        print(f"  OOS half ({oos_r[0]['ts'][:10]} -> {oos_r[-1]['ts'][:10]}, n={len(oos_r)}):")
        print(f"    Delta: {oos_stats['delta']:+.1f} pts  =  ${oos_mo:+.0f}/mo (extrapolated)")
        print(f"    Dropped: n={oos_stats['n_dropped']:3d} mean={oos_stats['dropped_mean']:+5.2f}")

    same_sign = is_stats and oos_stats and (is_stats["delta"] > 0) == (oos_stats["delta"] > 0)
    print(f"  OOS consistency: {'PASS' if same_sign else 'FAIL'} (same sign IS+OOS)")

    results.append({
        "id": cand["id"],
        "mechanism": cand["mechanism"],
        "polarity": cand["polarity"],
        "full": full,
        "is": is_stats,
        "oos": oos_stats,
        "monthly_dollars": monthly_dollars,
        "oos_consistent": same_sign,
    })

with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_finalists.json", "w") as f:
    json.dump(results, f, default=str, indent=2)

print(f"\n{'=' * 80}\n  FINAL RANKING\n{'=' * 80}")
results.sort(key=lambda r: -r["monthly_dollars"])
for r in results:
    flag = "OOS-OK" if r["oos_consistent"] else "OOS-FAIL"
    print(f"  {r['id']:50s}  ${r['monthly_dollars']:+6.0f}/mo  {flag}")
