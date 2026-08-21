"""S244 — validation of the zero-gamma / DEX factors.

Runs the checks that have killed previous candidates in this project:
  leave-one-month-out, era split, VIX regime, within-day control,
  day-concentration, and incremental value on top of the live V16 filter.
"""
import pickle
from collections import defaultdict
from datetime import date
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
S217 = date(2026, 6, 13)


def load():
    with open("_tmp_s244_trades.pkl", "rb") as fh:
        t = pickle.load(fh)
    for x in t:
        e = x["ts"].astimezone(ET)
        x["et"] = e; x["day"] = e.date(); x["month"] = e.strftime("%Y-%m")
        x["post217"] = x["day"] >= S217
        # signed regime position: + means price is on the calm/positive-gamma side
        x["zgd"] = x["zg_dist_resolved"]
        # "with the regime" = long above the flip, short below it
        x["regime_with"] = (x["zgd"] > 0) == x["is_long"]
    return t


def stat(rows, key="pnl"):
    vals = [r[key] for r in rows if r.get(key) is not None]
    if not vals:
        return dict(n=0, wr=0.0, tot=0.0, mean=0.0)
    w = sum(1 for v in vals if v > 0)
    return dict(n=len(vals), wr=100.0 * w / len(vals), tot=sum(vals), mean=sum(vals) / len(vals))


def line(name, rows, key="pnl"):
    s = stat(rows, key)
    print(f"  {name:<28} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f}")


def hdr(t):
    print(f"\n{t}")
    print(f"  {'bucket':<28} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")


def band(v, edges, labels):
    if v is None:
        return None
    for e, lab in zip(edges, labels):
        if v < e:
            return lab
    return labels[-1]


def concentration(rows, key="pnl"):
    byday = defaultdict(float)
    for r in rows:
        if r.get(key) is not None:
            byday[r["day"]] += r[key]
    tot = sum(byday.values())
    top = sorted(byday.values(), reverse=True)
    if not top or tot == 0:
        return "n/a"
    t1 = 100 * top[0] / tot
    t3 = 100 * sum(top[:3]) / tot
    ex3 = tot - sum(top[:3])
    return f"top1 {t1:.0f}% | top3 {t3:.0f}% | ex-top3 {ex3:+.0f}pt over {len(byday)} days"


def lomo(rows, rule, label):
    """Leave-one-month-out: does the rule help in EVERY held-out month?"""
    months = sorted({r["month"] for r in rows})
    print(f"\n  LEAVE-ONE-MONTH-OUT — {label}")
    print(f"    {'month':<9} {'kept n':>7} {'kept tot':>9} {'blkd n':>7} {'blkd tot':>9} {'verdict':>9}")
    good = 0
    for m in months:
        sub = [r for r in rows if r["month"] == m]
        keep = [r for r in sub if rule(r)]
        blk = [r for r in sub if not rule(r)]
        ks, bs = stat(keep), stat(blk)
        v = "HELPS" if bs["tot"] < 0 else ("hurts" if bs["tot"] > 0 else "flat")
        if bs["tot"] < 0:
            good += 1
        print(f"    {m:<9} {ks['n']:>7} {ks['tot']:>9.1f} {bs['n']:>7} {bs['tot']:>9.1f} {v:>9}")
    print(f"    -> rule removes a LOSING bucket in {good}/{len(months)} months")


def main():
    T = load()
    longs = [x for x in T if x["is_long"]]
    shorts = [x for x in T if not x["is_long"]]

    print("=" * 84)
    print("A. ZERO-GAMMA POSITION — now resolved for 100% of trades")
    print("=" * 84)
    edges = [-60, -30, -12, 12, 30, 60]
    labs = ["a:<-60", "b:-60..-30", "c:-30..-12", "d:-12..+12", "e:+12..+30", "f:+30..+60", "g:>+60"]
    hdr("[LONGS] spot - zero_gamma  (negative = below the flip)")
    for L in labs:
        line(L, [r for r in longs if band(r["zgd"], edges, labs) == L])
    hdr("[SHORTS] spot - zero_gamma")
    for L in labs:
        line(L, [r for r in shorts if band(r["zgd"], edges, labs) == L])

    hdr("[ALL] trading WITH the gamma regime (long above flip / short below)")
    line("with regime", [r for r in T if r["regime_with"]])
    line("against regime", [r for r in T if not r["regime_with"]])
    print(f"    with-regime concentration:    {concentration([r for r in T if r['regime_with']])}")
    print(f"    against-regime concentration: {concentration([r for r in T if not r['regime_with']])}")

    print("\n" + "=" * 84)
    print("B. IS IT JUST 'BUYING THE TOP'?  intraday range position at entry")
    print("=" * 84)
    redges = [0.2, 0.4, 0.6, 0.8]
    rlabs = ["0-20% (lows)", "20-40%", "40-60%", "60-80%", "80-100% (highs)"]
    hdr("[LONGS] where in today's range so far")
    for L in rlabs:
        line(L, [r for r in longs if band(r["range_pos"], redges, rlabs) == L])
    hdr("[GEX Long only] where in today's range so far")
    gl = [r for r in T if r["name"] == "GEX Long"]
    for L in rlabs:
        line(L, [r for r in gl if band(r["range_pos"], redges, rlabs) == L])
    tops = [r for r in gl if r["range_pos"] >= 0.8]
    print(f"    GEX Long fired in top 20% of range: {len(tops)}/{len(gl)} = {100*len(tops)/len(gl):.0f}% of its fires")
    hdr("[LONGS all setups] top-20%-of-range x zero-gamma side")
    line("top20 & above flip", [r for r in longs if r["range_pos"] >= 0.8 and r["zgd"] > 0])
    line("top20 & below flip", [r for r in longs if r["range_pos"] >= 0.8 and r["zgd"] <= 0])
    line("not top20 & above flip", [r for r in longs if r["range_pos"] < 0.8 and r["zgd"] > 0])
    line("not top20 & below flip", [r for r in longs if r["range_pos"] < 0.8 and r["zgd"] <= 0])

    print("\n" + "=" * 84)
    print("C. WITHIN-DAY CONTROL — does the flip separate trades on the SAME day?")
    print("=" * 84)
    byday = defaultdict(list)
    for r in T:
        byday[r["day"]].append(r)
    w_tot = a_tot = 0.0; w_n = a_n = 0; days_used = 0
    for d, rows in byday.items():
        w = [r for r in rows if r["regime_with"]]
        a = [r for r in rows if not r["regime_with"]]
        if not w or not a:
            continue                      # need both on the same day
        days_used += 1
        w_tot += sum(r["pnl"] for r in w); w_n += len(w)
        a_tot += sum(r["pnl"] for r in a); a_n += len(a)
    print(f"  days containing BOTH kinds: {days_used}")
    print(f"    with-regime     n={w_n:<5} total {w_tot:>8.1f}  mean {w_tot/max(1,w_n):>6.2f}")
    print(f"    against-regime  n={a_n:<5} total {a_tot:>8.1f}  mean {a_tot/max(1,a_n):>6.2f}")
    print("  (if the gap survives here it is NOT merely a day-direction proxy)")

    print("\n" + "=" * 84)
    print("D. ERA + VIX ROBUSTNESS of 'with the gamma regime'")
    print("=" * 84)
    for nm, pop in (("pre-S217", [r for r in T if not r["post217"]]),
                    ("post-S217 (chain valid)", [r for r in T if r["post217"]])):
        hdr(f"[{nm}]")
        line("with regime", [r for r in pop if r["regime_with"]])
        line("against regime", [r for r in pop if not r["regime_with"]])
    for nm, lo, hi in (("VIX<17", 0, 17), ("VIX 17-21", 17, 21), ("VIX>=21", 21, 999)):
        pop = [r for r in T if r["vix"] is not None and lo <= r["vix"] < hi]
        hdr(f"[{nm}]")
        line("with regime", [r for r in pop if r["regime_with"]])
        line("against regime", [r for r in pop if not r["regime_with"]])

    print("\n" + "=" * 84)
    print("E. LEAVE-ONE-MONTH-OUT")
    print("=" * 84)
    lomo(T, lambda r: r["regime_with"], "block trades against the gamma regime (ALL setups)")
    lomo(longs, lambda r: r["zgd"] > 0, "block LONGS below the flip")
    lomo(shorts, lambda r: r["zgd"] < 0, "block SHORTS above the flip")

    print("\n" + "=" * 84)
    print("F. INCREMENTAL VALUE ON TOP OF THE LIVE V16 FILTER (live_pass=true)")
    print("=" * 84)
    v16 = [r for r in T if r["live_pass"]]
    hdr("[V16 book] gamma-regime split")
    line("V16 all", v16)
    line("V16 & with regime", [r for r in v16 if r["regime_with"]])
    line("V16 & against regime", [r for r in v16 if not r["regime_with"]])
    print(f"    kept concentration:  {concentration([r for r in v16 if r['regime_with']])}")
    v16post = [r for r in v16 if r["post217"]]
    hdr("[V16 book, post-S217 only]")
    line("all", v16post)
    line("with regime", [r for r in v16post if r["regime_with"]])
    line("against regime", [r for r in v16post if not r["regime_with"]])
    lomo(v16, lambda r: r["regime_with"], "V16 + gamma-regime gate")

    print("\n" + "=" * 84)
    print("G. PER-SETUP effect of the gamma-regime gate (V16 book)")
    print("=" * 84)
    hdr("setup: kept vs blocked")
    names = sorted({r["name"] for r in v16})
    for nm in names:
        sub = [r for r in v16 if r["name"] == nm]
        k = stat([r for r in sub if r["regime_with"]])
        b = stat([r for r in sub if not r["regime_with"]])
        print(f"  {nm:<22} keep n={k['n']:<4} {k['tot']:>8.1f} ({k['wr']:.0f}%)   "
              f"block n={b['n']:<4} {b['tot']:>8.1f} ({b['wr']:.0f}%)")


if __name__ == "__main__":
    main()
