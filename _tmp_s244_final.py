"""S244 — final checks on the no-lookahead dataset: concentration, within-day control, $ value."""
import pickle
from collections import defaultdict
from datetime import date
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
S217 = date(2026, 6, 13)
V6 = date(2026, 6, 8)          # GEX Long v6 detector shipped
MES_PT = 5.0                   # $ per SPX point on 1 MES
SAR = 3.75


def load():
    with open("_tmp_s244_trades_prior.pkl", "rb") as fh:
        t = pickle.load(fh)
    for x in t:
        e = x["ts"].astimezone(ET)
        x["et"] = e; x["day"] = e.date(); x["month"] = e.strftime("%Y-%m")
        x["post217"] = x["day"] >= S217
        x["zgd"] = x["zg_dist_resolved"]
        x["regime_with"] = (x["zgd"] > 0) == x["is_long"]
    return t


def support(r, zband=8.0, thr=0.0):
    pw = r["put_wall"]
    if pw is None or not (r["net_gex"] > 0) or not (r["net_dex"] >= 0):
        return False
    if not ((pw - r["spot"]) > thr):
        return False
    if zband > 0 and r["zero_gamma"] is not None and abs(r["spot"] - r["zero_gamma"]) < zband:
        return False
    return True


def stat(rows, key="pnl"):
    vals = [r[key] for r in rows if r.get(key) is not None]
    if not vals:
        return dict(n=0, wr=0.0, tot=0.0, mean=0.0)
    w = sum(1 for v in vals if v > 0)
    return dict(n=len(vals), wr=100.0 * w / len(vals), tot=sum(vals), mean=sum(vals) / len(vals))


def conc(rows):
    byday = defaultdict(float)
    for r in rows:
        byday[r["day"]] += r["pnl"]
    tot = sum(byday.values()); top = sorted(byday.values(), reverse=True)
    if not top or tot == 0:
        return dict(tot=0, days=0, t1=0, t3=0, ex3=0)
    return dict(tot=tot, days=len(byday), t1=100*top[0]/tot, t3=100*sum(top[:3])/tot,
                ex3=tot-sum(top[:3]))


def maxdd(rows, w=lambda r: 1.0):
    byday = defaultdict(float)
    for r in rows:
        byday[r["day"]] += r["pnl"] * w(r)
    eq = peak = dd = 0.0
    for d in sorted(byday):
        eq += byday[d]; peak = max(peak, eq); dd = min(dd, eq - peak)
    return dd, eq


def main():
    T = load()
    longs = [r for r in T if r["is_long"]]
    gl = [r for r in T if r["name"] == "GEX Long"]
    glv6 = [r for r in gl if r["day"] >= V6]

    print("=" * 94)
    print("1. CONCENTRATION — is any candidate just a few lucky days?")
    print("=" * 94)
    print(f"  {'candidate':<38} {'n':>4} {'WR%':>6} {'total':>8} {'days':>5} {'top1%':>6} {'top3%':>6} {'ex-top3':>8}")
    cands = [
        ("all longs (baseline)", longs),
        ("SUPPORT longs (whole book)", [r for r in longs if support(r)]),
        ("SUPPORT longs, 10:00-14:00", [r for r in longs if support(r) and 10 <= r["et"].hour < 14]),
        ("GEX Long as-is", gl),
        ("GEX Long + SUPPORT", [r for r in gl if support(r)]),
        ("GEX Long + SUPPORT 10-14", [r for r in gl if support(r) and 10 <= r["et"].hour < 14]),
        ("GEX Long + spot<put_wall only", [r for r in gl if r["put_wall"] and r["put_wall"] > r["spot"]]),
    ]
    for nm, rows in cands:
        s = stat(rows); c = conc(rows)
        print(f"  {nm:<38} {s['n']:>4} {s['wr']:>6.1f} {s['tot']:>8.1f} {c['days']:>5} "
              f"{c['t1']:>6.0f} {c['t3']:>6.0f} {c['ex3']:>+8.1f}")

    print("\n" + "=" * 94)
    print("2. WITHIN-DAY CONTROL for SUPPORT (same day, SUPPORT longs vs other longs)")
    print("=" * 94)
    byday = defaultdict(list)
    for r in longs:
        byday[r["day"]].append(r)
    sn = so = 0; st_ = ot = 0.0; days = 0
    for d, rows in byday.items():
        a = [r for r in rows if support(r)]; b = [r for r in rows if not support(r)]
        if not a or not b:
            continue
        days += 1
        sn += len(a); st_ += sum(r["pnl"] for r in a)
        so += len(b); ot += sum(r["pnl"] for r in b)
    print(f"  days with both kinds: {days}")
    print(f"    SUPPORT longs   n={sn:<5} total {st_:>8.1f}  mean {st_/max(1,sn):>6.2f}")
    print(f"    other longs     n={so:<5} total {ot:>8.1f}  mean {ot/max(1,so):>6.2f}")

    print("\n" + "=" * 94)
    print("3. GEX LONG, v6-DETECTOR ERA ONLY (2026-06-08+) — matches today's live definition")
    print("=" * 94)
    print(f"  {'variant':<38} {'n':>4} {'WR%':>6} {'total':>8} {'mean':>7} {'$/1MES':>9} {'SAR':>9}")
    mo = 2.2  # Jun 8 -> Aug 11
    for nm, rows in (("v6 era, as-is", glv6),
                     ("v6 era + SUPPORT", [r for r in glv6 if support(r)]),
                     ("v6 era + SUPPORT 10-14", [r for r in glv6 if support(r) and 10 <= r["et"].hour < 14]),
                     ("v6 era + spot<put_wall", [r for r in glv6 if r["put_wall"] and r["put_wall"] > r["spot"]])):
        s = stat(rows)
        print(f"  {nm:<38} {s['n']:>4} {s['wr']:>6.1f} {s['tot']:>8.1f} {s['mean']:>7.2f} "
              f"{s['tot']*MES_PT/mo:>9.0f} {s['tot']*MES_PT*SAR/mo:>9.0f}")
    print(f"  ($ and SAR columns are PER MONTH at 1 MES, over the {mo:.1f}-month v6 era)")

    print("\n" + "=" * 94)
    print("4. SUPPORT AS AN ADD-ON TO THE LIVE V16 BOOK")
    print("=" * 94)
    v16 = [r for r in T if r["live_pass"]]
    v16l = [r for r in v16 if r["is_long"]]
    add = [r for r in longs if support(r) and not r["live_pass"]]
    print(f"  V16 book today            n={len(v16):<5} total {stat(v16)['tot']:>8.1f}  WR {stat(v16)['wr']:.1f}%")
    print(f"  V16 longs                 n={len(v16l):<5} total {stat(v16l)['tot']:>8.1f}  WR {stat(v16l)['wr']:.1f}%")
    print(f"  SUPPORT longs V16 REJECTS n={len(add):<5} total {stat(add)['tot']:>8.1f}  WR {stat(add)['wr']:.1f}%")
    ca = conc(add)
    print(f"    -> concentration of that add-on: {ca['days']} days, top3 {ca['t3']:.0f}%, ex-top3 {ca['ex3']:+.1f}pt")
    print("    NOTE: V16-rejected trades were never placed at a broker; treat as indicative only.")

    print("\n" + "=" * 94)
    print("5. SIZING OVERLAY on the V16 book, post-S217 (gamma-regime, not SUPPORT)")
    print("=" * 94)
    pop = [r for r in v16 if r["post217"]]
    for nm, w in (("flat 1x", lambda r: 1.0),
                  ("2x with-regime / 1x against", lambda r: 2.0 if r["regime_with"] else 1.0),
                  ("1x with / 0.5x against", lambda r: 1.0 if r["regime_with"] else 0.5),
                  ("block against-regime", lambda r: 1.0 if r["regime_with"] else 0.0)):
        dd, eq = maxdd(pop, w)
        print(f"  {nm:<30} total {eq:>8.1f}pt  maxDD {dd:>8.1f}pt  return/DD {eq/abs(dd or 1):>6.2f}"
              f"   ${eq*MES_PT:>7.0f}")

    print("\n" + "=" * 94)
    print("6. HOW OFTEN WOULD A SUPPORT-GATED GEX LONG ACTUALLY FIRE?")
    print("=" * 94)
    for m in sorted({r["month"] for r in glv6}):
        a = stat([r for r in glv6 if r["month"] == m])
        b = stat([r for r in glv6 if r["month"] == m and support(r)])
        print(f"    {m}   fires now {a['n']:>3} ({a['tot']:>7.1f}pt)   with SUPPORT gate {b['n']:>3} "
              f"({b['tot']:>7.1f}pt, WR {b['wr']:.0f}%)")


if __name__ == "__main__":
    main()
