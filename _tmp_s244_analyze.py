"""S244 — factor tests of the Exelza GEX framework against our fired setups."""
import pickle, sys
from collections import defaultdict
from datetime import date
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
S217 = date(2026, 6, 13)


def load():
    with open("_tmp_s244_trades.pkl", "rb") as fh:
        t = pickle.load(fh)
    for x in t:
        x["et"] = x["ts"].astimezone(ET)
        x["day"] = x["et"].date()
        x["month"] = x["et"].strftime("%Y-%m")
        x["post217"] = x["day"] >= S217
    return t


def stat(rows, key="pnl"):
    if not rows:
        return dict(n=0, wr=0.0, tot=0.0, mean=0.0)
    vals = [r[key] for r in rows if r.get(key) is not None]
    if not vals:
        return dict(n=0, wr=0.0, tot=0.0, mean=0.0)
    w = sum(1 for v in vals if v > 0)
    return dict(n=len(vals), wr=100.0 * w / len(vals), tot=sum(vals), mean=sum(vals) / len(vals))


def table(title, buckets, key="pnl", minn=1):
    print(f"\n{title}")
    print(f"  {'bucket':<26} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")
    for name, rows in buckets:
        s = stat(rows, key)
        if s["n"] < minn:
            continue
        print(f"  {name:<26} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f}")


def bucketize(rows, fn, order=None):
    d = defaultdict(list)
    for r in rows:
        k = fn(r)
        if k is None:
            continue
        d[k].append(r)
    keys = order if order else sorted(d.keys(), key=lambda x: str(x))
    return [(str(k), d[k]) for k in keys if k in d]


def band(v, edges, labels):
    if v is None:
        return None
    for e, lab in zip(edges, labels):
        if v < e:
            return lab
    return labels[-1]


def main():
    T = load()
    longs = [x for x in T if x["is_long"]]
    shorts = [x for x in T if not x["is_long"]]
    print(f"TOTAL joined {len(T)}  longs {len(longs)}  shorts {len(shorts)}")
    print(f"date range {min(x['day'] for x in T)} .. {max(x['day'] for x in T)}")

    # ---------- 0. SANITY on the derived levels ----------
    print("\n" + "=" * 78)
    print("0. SANITY CHECKS")
    print("=" * 78)
    zin = [x for x in T if x["zg_in_window"]]
    print(f"zero-gamma inside window: {len(zin)}/{len(T)} ({100*len(zin)/len(T):.0f}%)")
    agree = sum(1 for x in zin if (x["spot"] > x["zero_gamma"]) == (x["net_gex"] > 0))
    print(f"  spot>ZG agrees with net_gex>0: {agree}/{len(zin)} ({100*agree/max(1,len(zin)):.0f}%)"
          "   <- should be high if ZG is meaningful")
    hd = [x["head_cw"] for x in T if x["head_cw"] is not None]
    dp = [x["drop_pw"] for x in T if x["drop_pw"] is not None]
    hd.sort(); dp.sort()
    def q(a, p): return a[int(p * (len(a) - 1))]
    print(f"  headroom to call wall  (pts): p10 {q(hd,.1):.0f}  med {q(hd,.5):.0f}  p90 {q(hd,.9):.0f}")
    print(f"  drop to put wall       (pts): p10 {q(dp,.1):.0f}  med {q(dp,.5):.0f}  p90 {q(dp,.9):.0f}")
    dz = sorted(abs(x["dist_zg"]) for x in zin)
    print(f"  |spot - zero gamma|    (pts): p10 {q(dz,.1):.0f}  med {q(dz,.5):.0f}  p90 {q(dz,.9):.0f}")
    print(f"  (SPX 1% ~ 75pt -> the guide's '1% of zero gamma' band is far too wide here)")

    # ---------- 1. NET DEX — the genuinely new factor ----------
    print("\n" + "=" * 78)
    print("1. NET DEX (delta exposure) — never tested before in this project")
    print("=" * 78)
    for label, pop in (("ALL", T), ("LONGS", longs), ("SHORTS", shorts)):
        table(f"[{label}] DEX agrees with trade direction?",
              bucketize(pop, lambda r: "agree" if r["dex_agree"] else "disagree"))
    # magnitude bands, normalised per snapshot scale
    allmag = sorted(abs(x["net_dex"]) for x in T)
    m33, m67 = q(allmag, .33), q(allmag, .67)
    def dexband(r):
        s = "+" if r["net_dex"] > 0 else "-"
        a = abs(r["net_dex"])
        z = "sml" if a < m33 else ("mid" if a < m67 else "big")
        return f"{s}{z}"
    table("[LONGS] net DEX sign x magnitude", bucketize(longs, dexband))
    table("[SHORTS] net DEX sign x magnitude", bucketize(shorts, dexband))

    # ---------- 2. NET GEX regime ----------
    print("\n" + "=" * 78)
    print("2. NET GEX regime (positive = dampening, negative = amplifying)")
    print("=" * 78)
    table("[LONGS] net_gex sign", bucketize(longs, lambda r: "GEX+" if r["net_gex"] > 0 else "GEX-"))
    table("[SHORTS] net_gex sign", bucketize(shorts, lambda r: "GEX+" if r["net_gex"] > 0 else "GEX-"))

    # ---------- 3. HEADROOM (the guide's core "room to the wall" idea) ----------
    print("\n" + "=" * 78)
    print("3. ROOM TO THE NEXT WALL  (guide: a wall <1% away = tiny profit space)")
    print("=" * 78)
    edges = [5, 10, 15, 20, 30, 45]
    labs = ["a:<5", "b:5-10", "c:10-15", "d:15-20", "e:20-30", "f:30-45", "g:45+"]
    table("[LONGS] pts of headroom to CALL WALL",
          bucketize(longs, lambda r: band(r["head_cw"], edges, labs), order=labs))
    table("[SHORTS] pts of room down to PUT WALL",
          bucketize(shorts, lambda r: band(r["drop_pw"], edges, labs), order=labs))
    table("[LONGS] pts to nearest +GEX ceiling (<=60pt) - prior S-study factor",
          bucketize(longs, lambda r: band(r["head_ceil"], edges, labs) if r["head_ceil"] is not None
                    else "z:none(void)", order=labs + ["z:none(void)"]))

    # ---------- 4. DISTANCE TO ZERO GAMMA ----------
    print("\n" + "=" * 78)
    print("4. DISTANCE TO ZERO GAMMA (guide's most important level)")
    print("=" * 78)
    zedges = [-40, -20, -8, 8, 20, 40]
    zlabs = ["a:<-40", "b:-40..-20", "c:-20..-8", "d:-8..+8", "e:+8..+20", "f:+20..+40", "g:>+40"]
    table("[LONGS] spot - zero_gamma (neg = below ZG)",
          bucketize(longs, lambda r: band(r["dist_zg"], zedges, zlabs) if r["zg_in_window"] else "h:ZG outside",
                    order=zlabs + ["h:ZG outside"]))
    table("[SHORTS] spot - zero_gamma",
          bucketize(shorts, lambda r: band(r["dist_zg"], zedges, zlabs) if r["zg_in_window"] else "h:ZG outside",
                    order=zlabs + ["h:ZG outside"]))

    # ---------- 5. GEX LONG specifically ----------
    print("\n" + "=" * 78)
    print("5. GEX LONG setup only  (the user's problem child)")
    print("=" * 78)
    gl = [x for x in T if x["name"] == "GEX Long"]
    glv = [x for x in gl if x["live_pass"]]
    print(f"all GEX Long fired: {len(gl)}   live_pass(V16): {len(glv)}")
    print("  all:", stat(gl), "\n  V16:", stat(glv))
    table("[GEX Long all] DEX agrees", bucketize(gl, lambda r: "agree" if r["dex_agree"] else "disagree"))
    table("[GEX Long all] headroom to call wall",
          bucketize(gl, lambda r: band(r["head_cw"], edges, labs), order=labs))
    table("[GEX Long all] net_gex sign", bucketize(gl, lambda r: "GEX+" if r["net_gex"] > 0 else "GEX-"))
    table("[GEX Long all] dist to zero gamma",
          bucketize(gl, lambda r: band(r["dist_zg"], zedges, zlabs) if r["zg_in_window"] else "h:ZG outside",
                    order=zlabs + ["h:ZG outside"]))

    # ---------- 6. ERA / REGIME SPLITS of the best factor ----------
    print("\n" + "=" * 78)
    print("6. DEX AGREEMENT split by era and VIX regime")
    print("=" * 78)
    for label, pop in (("LONGS", longs), ("SHORTS", shorts)):
        pre = [x for x in pop if not x["post217"]]
        post = [x for x in pop if x["post217"]]
        table(f"[{label}] pre-S217 (chain metric, indicative)",
              bucketize(pre, lambda r: "agree" if r["dex_agree"] else "disagree"))
        table(f"[{label}] post-S217 2026-06-13+ (chain = valid metric)",
              bucketize(post, lambda r: "agree" if r["dex_agree"] else "disagree"))
    lowvix = [x for x in T if x["vix"] is not None and x["vix"] < 19]
    hivix = [x for x in T if x["vix"] is not None and x["vix"] >= 19]
    table("[ALL, VIX<19] DEX agree", bucketize(lowvix, lambda r: "agree" if r["dex_agree"] else "disagree"))
    table("[ALL, VIX>=19] DEX agree", bucketize(hivix, lambda r: "agree" if r["dex_agree"] else "disagree"))

    # ---------- 7. THE 11-STATE TAXONOMY ----------
    print("\n" + "=" * 78)
    print("7. EXELZA STATE LABEL (recomputed at trade spot, SPX-scaled ZG band = 8pt)")
    print("=" * 78)

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
        else:
            if r["spot"] > cw:
                return "SQUEEZE" if nd > 0 else "FAILED_SQUEEZE"
            if r["spot"] < pw:
                return "SHORT_COVER" if nd > 0 else "ACCELERATION"
            return "CHOPPY"

    for r in T:
        r["state"] = state_of(r)
    table("[LONGS] by state", bucketize(longs, lambda r: r["state"]), minn=8)
    table("[SHORTS] by state", bucketize(shorts, lambda r: r["state"]), minn=8)
    # does the state's own bias agree with our trade?
    def bias_of(r):
        s = r["state"]
        if s in ("BREAKOUT_TEST", "SUPPORT", "SQUEEZE", "SHORT_COVER"):
            return True
        if s in ("RESISTANCE", "BREAKDOWN_TEST", "FAILED_SQUEEZE", "ACCELERATION"):
            return False
        return None
    table("[ALL] state bias agrees with our direction?",
          bucketize(T, lambda r: None if bias_of(r) is None else
                    ("state agrees" if bias_of(r) == r["is_long"] else "state opposes")))


if __name__ == "__main__":
    main()
