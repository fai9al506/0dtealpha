# -*- coding: utf-8 -*-
"""V18 step 2 — filter-building ENGINE with strict out-of-sample discipline.

Everything here obeys one rule: a condition may only be SELECTED using months the
evaluation never sees. Leave-one-month-out over 6 folds; the reported number is the sum
of six held-out months, each scored by a filter chosen without it.
"""
import collections, statistics, itertools
from _tmp_v18_data import load, enrich, REAL_SETUPS, wr, tot, ppt, ET

rows, gaps, daily = load()
ALLR = [r for r in enrich(rows, gaps, daily)
        if r["pts"] is not None and r["date"].isoformat() >= "2026-03-01"]
MONTHS = sorted({r["month"] for r in ALLR})

# ── candidate condition space ───────────────────────────────────────────────
NUMERIC = ["greek_alignment", "vix", "overvix", "score", "mins", "abs_vol_ratio",
           "v13_gex_above", "lis_abs", "tgt_dist", "from_open", "rr_ratio",
           "spot_vol_beta", "vanna_all", "gap", "dow", "hour"]
CATEG = ["grade", "paradigm", "vanna_cliff_side", "vanna_peak_side", "vanna_regime"]
QUANTILES = (0.2, 0.35, 0.5, 0.65, 0.8)


def gen_conditions(train):
    """All candidate binary conditions, with thresholds derived from TRAIN data only."""
    out = []
    for k in NUMERIC:
        v = sorted(x[k] for x in train if x.get(k) is not None)
        if len(v) < 50:
            continue
        seen = set()
        for q in QUANTILES:
            thr = v[int(len(v) * q)]
            if thr in seen:
                continue
            seen.add(thr)
            out.append((f"{k}>={thr:g}", lambda x, k=k, t=thr: x.get(k) is not None and x[k] >= t))
            out.append((f"{k}<{thr:g}", lambda x, k=k, t=thr: x.get(k) is not None and x[k] < t))
    for k in CATEG:
        vals = collections.Counter(x.get(k) for x in train)
        for val, n in vals.items():
            if n < 20 or n > len(train) - 20:
                continue
            out.append((f"{k}!={val}", lambda x, k=k, v=val: x.get(k) != v))
    return out


def select(train, max_rules=4, min_keep=0.45, min_gain_pts=12.0):
    """Greedy: add the condition that most increases TRAIN total points, while keeping at
    least min_keep of the trades. Selection sees only `train`."""
    conds = gen_conditions(train)
    chosen, cur = [], list(train)
    base_n = len(train)
    for _ in range(max_rules):
        best = None
        for name, fn in conds:
            if any(name == c[0] for c in chosen):
                continue
            kept = [x for x in cur if fn(x)]
            if len(kept) < base_n * min_keep or len(kept) < 25:
                continue
            gain = tot(kept) - tot(cur)
            if best is None or gain > best[0]:
                best = (gain, name, fn, kept)
        if best is None or best[0] < min_gain_pts:
            break
        chosen.append((best[1], best[2]))
        cur = best[3]
    return chosen


def apply_rules(rs, rules):
    return [x for x in rs if all(fn(x) for _, fn in rules)]


# ── the LOMO evaluation ─────────────────────────────────────────────────────
def lomo(setups, max_rules=4, min_keep=0.45, verbose=True):
    """Returns (oos_rows, per_fold_detail). Each held-out month is filtered by rules chosen
    from the other five months, per setup x direction."""
    oos, detail = [], []
    for m in MONTHS:
        train_all = [r for r in ALLR if r["month"] != m]
        test_all = [r for r in ALLR if r["month"] == m]
        picked = {}
        for sn in setups:
            for isl in (True, False):
                tr = [r for r in train_all if r["setup_name"] == sn and r["is_long"] == isl]
                te = [r for r in test_all if r["setup_name"] == sn and r["is_long"] == isl]
                if len(tr) < 60:
                    # too little history to fit anything -> take the bucket unfiltered
                    oos.extend(te)
                    continue
                rules = select(tr, max_rules, min_keep)
                picked[(sn, isl)] = [r[0] for r in rules]
                oos.extend(apply_rules(te, rules))
        detail.append((m, picked))
    return oos, detail


def raw_stats(rs, lab):
    v = [x for x in rs if x["pts"] is not None]
    return f"  {lab:<40}{len(v):>5}t  WR {wr(v):>3.0f}%  {tot(v):>+9.1f} pts  {ppt(v):>+5.2f}/t"


if __name__ == "__main__":
    print("### V18 engine — out-of-sample test of PER-SETUP filter fitting")
    print("    (raw points, before portfolio caps — this isolates the SELECTION question)\n")
    base = [r for r in ALLR if r["setup_name"] in REAL_SETUPS]
    print(raw_stats(base, "no filter at all (the 6 real setups)"))

    from _tmp_s233_rules import passes as v16pass
    v16 = [r for r in base if v16pass(r, gaps)[0]]
    print(raw_stats(v16, "V16 (today's filter)"))

    print("\n  fitted filters, scored OUT OF SAMPLE (leave-one-month-out):")
    for mr, mk in ((2, 0.60), (3, 0.50), (4, 0.45), (6, 0.35)):
        oos, _ = lomo(REAL_SETUPS, mr, mk, verbose=False)
        print(raw_stats(oos, f"V18 fitted: <={mr} rules, keep >={mk:.0%}"))

    print("\n  the SAME fitting, scored IN SAMPLE (what a naive study would report):")
    for mr, mk in ((2, 0.60), (4, 0.45), (6, 0.35)):
        ins = []
        for sn in REAL_SETUPS:
            for isl in (True, False):
                sub = [r for r in ALLR if r["setup_name"] == sn and r["is_long"] == isl]
                if len(sub) < 60:
                    ins.extend(sub); continue
                ins.extend(apply_rules(sub, select(sub, mr, mk)))
        print(raw_stats(ins, f"IN-SAMPLE: <={mr} rules, keep >={mk:.0%}"))

    print("\n  stability of what gets picked (which rules recur across the 6 folds):")
    _, det = lomo(REAL_SETUPS, 3, 0.50)
    cnt = collections.defaultdict(collections.Counter)
    for m, picked in det:
        for k, rules in picked.items():
            for r in rules:
                cnt[k][r] += 1
    for k in sorted(cnt, key=lambda x: (x[0], not x[1])):
        top = cnt[k].most_common(4)
        lab = f"{k[0]} {'LONG' if k[1] else 'SHORT'}"
        print(f"    {lab:<28}" + "  ".join(f"{r}({n}/6)" for r, n in top))
