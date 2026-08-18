"""S244 — verify the SUPPORT-state finding: decompose it, stress it, and check it is not an artifact."""
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
        x["pw_rel"] = (x["put_wall"] - x["spot"]) if x["put_wall"] is not None else None
        x["cw_rel"] = (x["call_wall"] - x["spot"]) if x["call_wall"] is not None else None
    return t


def stat(rows, key="pnl"):
    vals = [r[key] for r in rows if r.get(key) is not None]
    if not vals:
        return dict(n=0, wr=0.0, tot=0.0, mean=0.0)
    w = sum(1 for v in vals if v > 0)
    return dict(n=len(vals), wr=100.0 * w / len(vals), tot=sum(vals), mean=sum(vals) / len(vals))


def line(name, rows, key="pnl"):
    s = stat(rows, key)
    print(f"  {name:<40} {s['n']:>5} {s['wr']:>6.1f} {s['tot']:>9.1f} {s['mean']:>7.2f}")


def hdr(t):
    print(f"\n{t}")
    print(f"  {'bucket':<40} {'n':>5} {'WR%':>6} {'total':>9} {'mean':>7}")


def conc(rows, key="pnl"):
    byday = defaultdict(float)
    for r in rows:
        byday[r["day"]] += r[key]
    tot = sum(byday.values()); top = sorted(byday.values(), reverse=True)
    if not top or tot == 0:
        return "n/a"
    return (f"{tot:+.1f}pt / {len(byday)} days | top1 {100*top[0]/tot:.0f}% | "
            f"top3 {100*sum(top[:3])/tot:.0f}% | ex-top3 {tot-sum(top[:3]):+.1f}pt")


def is_support(r, zband=8.0):
    if r["put_wall"] is None or r["call_wall"] is None:
        return False
    if r["zero_gamma"] is not None and abs(r["spot"] - r["zero_gamma"]) < zband:
        return False
    return r["net_gex"] > 0 and r["spot"] < r["put_wall"] and r["net_dex"] >= 0


def main():
    T = load()
    longs = [r for r in T if r["is_long"]]
    gl = [r for r in T if r["name"] == "GEX Long"]

    print("=" * 92)
    print("1. WHAT DOES 'SUPPORT' ACTUALLY MEAN IN OUR CHAIN?  (sanity on put_wall)")
    print("=" * 92)
    pw = sorted(r["pw_rel"] for r in T if r["pw_rel"] is not None)
    def q(a, p): return a[int(p * (len(a) - 1))]
    print(f"  put_wall - spot (pts): p05 {q(pw,.05):.0f}  p25 {q(pw,.25):.0f}  med {q(pw,.5):.0f}  "
          f"p75 {q(pw,.75):.0f}  p95 {q(pw,.95):.0f}")
    above = sum(1 for v in pw if v > 0)
    print(f"  put wall sits ABOVE spot in {above}/{len(pw)} = {100*above/len(pw):.0f}% of snapshots")
    sup = [r for r in T if is_support(r)]
    spw = sorted(r["pw_rel"] for r in sup)
    print(f"  within SUPPORT state, put_wall - spot: med {q(spw,.5):.0f}pt  p95 {q(spw,.95):.0f}pt")
    print("  -> for 0DTE SPX the put-gamma peak sits near ATM, so 'spot < put wall' reads as")
    print("     'price is just under the ATM put-gamma concentration', not the guide's deep-OTM wall.")

    print("\n" + "=" * 92)
    print("2. DECOMPOSE — which component of SUPPORT carries the edge?  [LONGS only]")
    print("=" * 92)
    hdr("single conditions on the long book")
    line("baseline: all longs", longs)
    line("net_gex > 0", [r for r in longs if r["net_gex"] > 0])
    line("net_dex >= 0", [r for r in longs if r["net_dex"] >= 0])
    line("spot < put_wall", [r for r in longs if r["pw_rel"] is not None and r["pw_rel"] > 0])
    line("|spot-ZG| >= 8", [r for r in longs if r["zero_gamma"] is None or abs(r["spot"]-r["zero_gamma"]) >= 8])
    hdr("cumulative build-up")
    a = [r for r in longs if r["net_gex"] > 0]
    line("+ net_gex>0", a)
    b = [r for r in a if r["net_dex"] >= 0]
    line("+ net_dex>=0", b)
    c = [r for r in b if r["pw_rel"] is not None and r["pw_rel"] > 0]
    line("+ spot<put_wall", c)
    d = [r for r in c if r["zero_gamma"] is None or abs(r["spot"]-r["zero_gamma"]) >= 8]
    line("+ |spot-ZG|>=8  (= SUPPORT)", d)
    hdr("leave-one-condition-out (from full SUPPORT)")
    line("drop net_gex>0", [r for r in longs if r["net_dex"] >= 0 and r["pw_rel"] and r["pw_rel"] > 0
                            and (r["zero_gamma"] is None or abs(r["spot"]-r["zero_gamma"]) >= 8)])
    line("drop net_dex>=0", [r for r in longs if r["net_gex"] > 0 and r["pw_rel"] and r["pw_rel"] > 0
                             and (r["zero_gamma"] is None or abs(r["spot"]-r["zero_gamma"]) >= 8)])
    line("drop spot<put_wall", [r for r in longs if r["net_gex"] > 0 and r["net_dex"] >= 0
                                and (r["zero_gamma"] is None or abs(r["spot"]-r["zero_gamma"]) >= 8)])
    line("drop ZG band", [r for r in longs if r["net_gex"] > 0 and r["net_dex"] >= 0
                          and r["pw_rel"] is not None and r["pw_rel"] > 0])

    print("\n" + "=" * 92)
    print("3. GEX LONG x SUPPORT — the headline claim, stressed")
    print("=" * 92)
    gs = [r for r in gl if is_support(r)]
    gn = [r for r in gl if not is_support(r)]
    hdr("GEX Long split")
    line("GEX Long, SUPPORT state", gs)
    line("GEX Long, everything else", gn)
    print(f"    SUPPORT concentration:   {conc(gs)}")
    print(f"    non-SUPPORT concentration: {conc(gn)}")
    print("\n  month by month")
    print(f"    {'month':<9} {'sup n':>6} {'sup WR':>7} {'sup tot':>9} {'oth n':>6} {'oth tot':>9}")
    for m in sorted({r["month"] for r in gl}):
        s = stat([r for r in gs if r["month"] == m]); o = stat([r for r in gn if r["month"] == m])
        print(f"    {m:<9} {s['n']:>6} {s['wr']:>6.0f}% {s['tot']:>9.1f} {o['n']:>6} {o['tot']:>9.1f}")
    hdr("era split")
    line("SUPPORT pre-S217", [r for r in gs if not r["post217"]])
    line("SUPPORT post-S217", [r for r in gs if r["post217"]])
    line("other pre-S217", [r for r in gn if not r["post217"]])
    line("other post-S217", [r for r in gn if r["post217"]])
    hdr("V16 overlap")
    line("GEX Long SUPPORT & V16", [r for r in gs if r["live_pass"]])
    line("GEX Long SUPPORT & NOT V16", [r for r in gs if not r["live_pass"]])
    hdr("MES-sim cross-check (where populated)")
    line("SUPPORT (mes_sim)", [r for r in gs if r["mes_pnl"] is not None], key="mes_pnl")
    line("other (mes_sim)", [r for r in gn if r["mes_pnl"] is not None], key="mes_pnl")
    print(f"    mes coverage: SUPPORT {sum(1 for r in gs if r['mes_pnl'] is not None)}/{len(gs)}"
          f"  other {sum(1 for r in gn if r['mes_pnl'] is not None)}/{len(gn)}")

    print("\n" + "=" * 92)
    print("4. SUPPORT-LONG ACROSS THE WHOLE BOOK — stability + concentration")
    print("=" * 92)
    sl = [r for r in longs if is_support(r)]
    nl = [r for r in longs if not is_support(r)]
    hdr("all longs")
    line("SUPPORT longs", sl)
    line("non-SUPPORT longs", nl)
    print(f"    SUPPORT concentration: {conc(sl)}")
    hdr("SUPPORT longs by era / vix")
    line("pre-S217", [r for r in sl if not r["post217"]])
    line("post-S217", [r for r in sl if r["post217"]])
    line("VIX < 19", [r for r in sl if r["vix"] is not None and r["vix"] < 19])
    line("VIX >= 19", [r for r in sl if r["vix"] is not None and r["vix"] >= 19])
    hdr("SUPPORT longs by hour ET")
    for h in range(9, 16):
        line(f"{h:02d}:00", [r for r in sl if r["et"].hour == h])


if __name__ == "__main__":
    main()
