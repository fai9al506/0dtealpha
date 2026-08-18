"""S244 — robustness sweep + blind walk-forward for the SUPPORT-state long rule.

The rule comes from an external document (Exelza guide), not from fitting our data,
so the honest test is: does it hold under reasonable variations of the free choices
I had to make (ZG band width, how 'put wall' is defined, the proximity threshold),
and does it hold on data I never looked at while forming it?
"""
import pickle
from collections import defaultdict
from datetime import date
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
S217 = date(2026, 6, 13)


def load():
    with open("_tmp_s244_trades_prior.pkl", "rb") as fh:
        t = pickle.load(fh)
    for x in t:
        e = x["ts"].astimezone(ET)
        x["et"] = e; x["day"] = e.date(); x["month"] = e.strftime("%Y-%m")
        x["post217"] = x["day"] >= S217
    return t


def stat(rows, key="pnl"):
    vals = [r[key] for r in rows if r.get(key) is not None]
    if not vals:
        return dict(n=0, wr=0.0, tot=0.0, mean=0.0)
    w = sum(1 for v in vals if v > 0)
    return dict(n=len(vals), wr=100.0 * w / len(vals), tot=sum(vals), mean=sum(vals) / len(vals))


def support(r, zband=8.0, wall="gex", thr=0.0, need_dex=True, need_gex=True):
    pw = r["put_wall"] if wall == "gex" else r["put_wall_oi"]
    if pw is None:
        return False
    if need_gex and not (r["net_gex"] > 0):
        return False
    if need_dex and not (r["net_dex"] >= 0):
        return False
    if not ((pw - r["spot"]) > thr):
        return False
    if zband > 0 and r["zero_gamma"] is not None and abs(r["spot"] - r["zero_gamma"]) < zband:
        return False
    return True


def main():
    T = load()
    longs = [r for r in T if r["is_long"]]
    gl = [r for r in T if r["name"] == "GEX Long"]

    print("=" * 96)
    print("1. ROBUSTNESS SWEEP — ALL LONGS.  Is the edge a knife-edge or a plateau?")
    print("=" * 96)
    print(f"  baseline all longs: n={len(longs)} WR {stat(longs)['wr']:.1f}% "
          f"tot {stat(longs)['tot']:.1f} mean {stat(longs)['mean']:.2f}\n")
    print(f"  {'zband':>6} {'wall':>5} {'thr':>5} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")
    for wall in ("gex", "oi"):
        for zband in (0, 5, 8, 12, 20):
            for thr in (0.0, 2.5, 5.0):
                sub = [r for r in longs if support(r, zband, wall, thr)]
                s = stat(sub)
                print(f"  {zband:>6} {wall:>5} {thr:>5.1f} {s['n']:>5} {s['wr']:>6.1f} "
                      f"{s['tot']:>9.1f} {s['mean']:>7.2f}")

    print("\n" + "=" * 96)
    print("2. WHICH CONDITIONS ARE LOAD-BEARING?  (all longs, zband=8, wall=gex, thr=0)")
    print("=" * 96)
    print(f"  {'variant':<34} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")
    for nm, kw in (("full SUPPORT", {}),
                   ("no ZG band", dict(zband=0)),
                   ("no DEX condition", dict(need_dex=False)),
                   ("no GEX condition", dict(need_gex=False)),
                   ("ONLY spot<put_wall", dict(zband=0, need_dex=False, need_gex=False))):
        sub = [r for r in longs if support(r, **kw)]
        s = stat(sub)
        print(f"  {nm:<34} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f}")

    print("\n" + "=" * 96)
    print("3. BLIND WALK-FORWARD — form on Feb-May, test on Jun 13 - Aug 11 (never tuned on)")
    print("=" * 96)
    tr = [r for r in longs if r["day"] < date(2026, 6, 1)]
    te = [r for r in longs if r["day"] >= S217]
    for nm, pop in (("TRAIN Feb-May", tr), ("TEST Jun13-Aug11", te)):
        s_in = stat([r for r in pop if support(r)])
        s_out = stat([r for r in pop if not support(r)])
        print(f"\n  [{nm}]  n={len(pop)}")
        print(f"    {'SUPPORT longs':<24} n={s_in['n']:<5} WR {s_in['wr']:>5.1f}%  tot {s_in['tot']:>8.1f}  mean {s_in['mean']:>6.2f}")
        print(f"    {'other longs':<24} n={s_out['n']:<5} WR {s_out['wr']:>5.1f}%  tot {s_out['tot']:>8.1f}  mean {s_out['mean']:>6.2f}")

    print("\n" + "=" * 96)
    print("4. IS 'spot < put_wall' JUST A PULLBACK PROXY?  cross-tab with range position")
    print("=" * 96)
    print(f"  {'bucket':<34} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")
    for rlo, rhi, rn in ((0, .4, "low 0-40% of range"), (.4, .8, "mid 40-80%"), (.8, 1.01, "high 80-100%")):
        for sup in (True, False):
            sub = [r for r in longs if (rlo <= r["range_pos"] < rhi) and support(r) == sup]
            s = stat(sub)
            tag = "SUPPORT" if sup else "other  "
            print(f"  {rn+' / '+tag:<34} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f}")

    print("\n" + "=" * 96)
    print("5. GEX LONG REBUILD — what would the setup look like with a SUPPORT gate?")
    print("=" * 96)
    print(f"  {'variant':<40} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7} {'per month':>10}")
    months = len({r["month"] for r in gl})
    for nm, sel in (
        ("GEX Long as-is (all fired)", lambda r: True),
        ("GEX Long, V16 only", lambda r: r["live_pass"]),
        ("GEX Long + SUPPORT gate", lambda r: support(r)),
        ("GEX Long + SUPPORT, 10:00-14:00 only", lambda r: support(r) and 10 <= r["et"].hour < 14),
        ("GEX Long + spot<put_wall only", lambda r: support(r, zband=0, need_dex=False, need_gex=False)),
        ("GEX Long + net_gex>0 only", lambda r: r["net_gex"] > 0),
    ):
        sub = [r for r in gl if sel(r)]
        s = stat(sub)
        print(f"  {nm:<40} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f} {s['tot']/months:>10.1f}")

    print("\n  GEX Long + SUPPORT, month by month (n / WR / total):")
    for m in sorted({r["month"] for r in gl}):
        s = stat([r for r in gl if r["month"] == m and support(r)])
        print(f"    {m}  n={s['n']:<3} WR {s['wr']:>5.1f}%  tot {s['tot']:>7.1f}")

    print("\n" + "=" * 96)
    print("6. OVERLAP — do SUPPORT longs just duplicate trades we already take?")
    print("=" * 96)
    sl = [r for r in longs if support(r)]
    # a SUPPORT long is 'covered' if another V16 trade fired same day within 15 min, same direction
    v16 = [r for r in T if r["live_pass"]]
    byday = defaultdict(list)
    for r in v16:
        byday[r["day"]].append(r)
    covered = 0
    for r in sl:
        for o in byday.get(r["day"], ()):
            if o["lid"] == r["lid"]:
                continue
            if o["is_long"] == r["is_long"] and abs((o["et"] - r["et"]).total_seconds()) <= 900:
                covered += 1
                break
    print(f"  SUPPORT longs: {len(sl)}")
    print(f"  already covered by a same-direction V16 trade within 15 min: {covered} "
          f"({100*covered/max(1,len(sl)):.0f}%)")
    print(f"  genuinely additional: {len(sl)-covered}")


if __name__ == "__main__":
    main()
