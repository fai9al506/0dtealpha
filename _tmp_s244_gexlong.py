"""S244 — GEX Long deep dive + book-wide SIZING overlay test."""
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
        x["zgd"] = x["zg_dist_resolved"]
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
    print(f"  {name:<32} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f}")


def hdr(t):
    print(f"\n{t}")
    print(f"  {'bucket':<32} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")


def maxdd_daily(rows, weight=lambda r: 1.0, key="pnl"):
    """Max peak-to-trough drawdown of the daily-summed equity curve, in points."""
    byday = defaultdict(float)
    for r in rows:
        if r.get(key) is not None:
            byday[r["day"]] += r[key] * weight(r)
    eq, peak, dd = 0.0, 0.0, 0.0
    for d in sorted(byday):
        eq += byday[d]
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    return dd, eq


def main():
    T = load()
    gl = [r for r in T if r["name"] == "GEX Long"]
    glv = [r for r in gl if r["live_pass"]]

    print("=" * 86)
    print("1. GEX LONG — full diagnosis")
    print("=" * 86)
    hdr("population")
    line("all fired", gl)
    line("V16 (live_pass)", glv)

    hdr("[GEX Long ALL] by zero-gamma side")
    line("spot ABOVE flip", [r for r in gl if r["zgd"] > 0])
    line("spot BELOW flip", [r for r in gl if r["zgd"] <= 0])
    hdr("[GEX Long V16] by zero-gamma side")
    line("spot ABOVE flip", [r for r in glv if r["zgd"] > 0])
    line("spot BELOW flip", [r for r in glv if r["zgd"] <= 0])

    hdr("[GEX Long ALL] zero-gamma side x net DEX sign")
    for zs, zl in ((True, "above flip"), (False, "below flip")):
        for ds, dl in ((True, "DEX+"), (False, "DEX-")):
            line(f"{zl} & {dl}", [r for r in gl if (r["zgd"] > 0) == zs and (r["net_dex"] > 0) == ds])

    print("\n  --- month by month, GEX Long ALL: above-flip vs below-flip ---")
    print(f"    {'month':<9} {'above n':>8} {'above tot':>10} {'below n':>8} {'below tot':>10}")
    okm = 0; months = sorted({r['month'] for r in gl})
    for m in months:
        a = stat([r for r in gl if r["month"] == m and r["zgd"] > 0])
        b = stat([r for r in gl if r["month"] == m and r["zgd"] <= 0])
        if b["tot"] < 0:
            okm += 1
        print(f"    {m:<9} {a['n']:>8} {a['tot']:>10.1f} {b['n']:>8} {b['tot']:>10.1f}")
    print(f"    -> below-flip bucket is a LOSER in {okm}/{len(months)} months")

    print("\n  --- era split ---")
    hdr("[GEX Long ALL] pre-S217")
    line("above flip", [r for r in gl if not r["post217"] and r["zgd"] > 0])
    line("below flip", [r for r in gl if not r["post217"] and r["zgd"] <= 0])
    hdr("[GEX Long ALL] post-S217 (chain metric valid)")
    line("above flip", [r for r in gl if r["post217"] and r["zgd"] > 0])
    line("below flip", [r for r in gl if r["post217"] and r["zgd"] <= 0])

    # day concentration of the kept bucket
    keep = [r for r in gl if r["zgd"] > 0]
    byday = defaultdict(float)
    for r in keep:
        byday[r["day"]] += r["pnl"]
    tot = sum(byday.values()); top = sorted(byday.values(), reverse=True)
    print(f"\n  kept-bucket concentration: total {tot:+.1f}pt over {len(byday)} days | "
          f"top1 {100*top[0]/tot:.0f}% | top3 {100*sum(top[:3])/tot:.0f}% | ex-top3 {tot-sum(top[:3]):+.1f}pt")

    print("\n" + "=" * 86)
    print("2. BOOK-WIDE SIZING OVERLAY  (2x with-regime / 1x against)  vs blocking")
    print("=" * 86)
    for nm, pop in (("ALL fired", T), ("V16 book", [r for r in T if r["live_pass"]]),
                    ("V16 post-S217", [r for r in T if r["live_pass"] and r["post217"]])):
        base_dd, base_eq = maxdd_daily(pop)
        blk = [r for r in pop if r["regime_with"]]
        blk_dd, blk_eq = maxdd_daily(blk)
        siz_dd, siz_eq = maxdd_daily(pop, weight=lambda r: 2.0 if r["regime_with"] else 1.0)
        half_dd, half_eq = maxdd_daily(pop, weight=lambda r: 1.0 if r["regime_with"] else 0.5)
        print(f"\n  [{nm}]  n={len(pop)}")
        print(f"    {'scheme':<26} {'total pt':>10} {'maxDD pt':>10} {'pt per unit risk':>18}")
        print(f"    {'1x flat (baseline)':<26} {base_eq:>10.1f} {base_dd:>10.1f} {base_eq/abs(base_dd or 1):>18.2f}")
        print(f"    {'block against-regime':<26} {blk_eq:>10.1f} {blk_dd:>10.1f} {blk_eq/abs(blk_dd or 1):>18.2f}")
        print(f"    {'2x with / 1x against':<26} {siz_eq:>10.1f} {siz_dd:>10.1f} {siz_eq/abs(siz_dd or 1):>18.2f}")
        print(f"    {'1x with / 0.5x against':<26} {half_eq:>10.1f} {half_dd:>10.1f} {half_eq/abs(half_dd or 1):>18.2f}")

    print("\n" + "=" * 86)
    print("3. THE GUIDE'S 'QUIET RELIABLE' STATES — could they be a NEW setup?")
    print("=" * 86)

    def state_of(r, zband=8.0):
        cw, pw, zg = r["call_wall"], r["put_wall"], r["zero_gamma"]
        ng, nd = r["net_gex"], r["net_dex"]
        if cw is None or pw is None:
            return None
        if zg is not None and abs(r["spot"] - zg) < zband:
            return "HIGH_VOL"
        if ng > 0:
            if r["spot"] > cw:
                return "BREAKOUT_TEST" if nd > 0 else "RESISTANCE"
            if r["spot"] < pw:
                return "BREAKDOWN_TEST" if nd < 0 else "SUPPORT"
            return "MEAN_REV"
        if r["spot"] > cw:
            return "SQUEEZE" if nd > 0 else "FAILED_SQUEEZE"
        if r["spot"] < pw:
            return "SHORT_COVER" if nd > 0 else "ACCELERATION"
        return "CHOPPY"

    for r in T:
        r["state"] = state_of(r)
    hdr("[LONGS] guide-aligned states (guide calls these the most reliable)")
    line("SUPPORT (long)", [r for r in T if r["is_long"] and r["state"] == "SUPPORT"])
    line("MEAN_REV (long)", [r for r in T if r["is_long"] and r["state"] == "MEAN_REV"])
    line("BREAKOUT_TEST (long)", [r for r in T if r["is_long"] and r["state"] == "BREAKOUT_TEST"])
    hdr("[SHORTS] guide-aligned states")
    line("ACCELERATION (short)", [r for r in T if not r["is_long"] and r["state"] == "ACCELERATION"])
    line("FAILED_SQUEEZE (short)", [r for r in T if not r["is_long"] and r["state"] == "FAILED_SQUEEZE"])
    line("RESISTANCE (short)", [r for r in T if not r["is_long"] and r["state"] == "RESISTANCE"])

    print("\n  --- SUPPORT-long month by month (is it stable?) ---")
    sl = [r for r in T if r["is_long"] and r["state"] == "SUPPORT"]
    for m in sorted({r["month"] for r in sl}):
        s = stat([r for r in sl if r["month"] == m])
        print(f"    {m}  n={s['n']:<4} WR {s['wr']:>5.1f}%  tot {s['tot']:>8.1f}")
    print("\n  --- which setups produce the SUPPORT-long trades? ---")
    c = defaultdict(list)
    for r in sl:
        c[r["name"]].append(r)
    for nm, rows in sorted(c.items(), key=lambda x: -len(x[1])):
        s = stat(rows)
        print(f"    {nm:<22} n={s['n']:<4} WR {s['wr']:>5.1f}%  tot {s['tot']:>8.1f}")


if __name__ == "__main__":
    main()
