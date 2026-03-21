"""
Study: Block GEX-LIS only vs block ALL GEX paradigms on SC/DD shorts.
GEX is a long paradigm — why are we shorting into it?

GEX subtypes: GEX-LIS, GEX-PURE, GEX-TARGET, GEX-MESSY
"""
import sqlalchemy as sa
import os
from collections import defaultdict

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date,
               outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    def passes_v9sc(r):
        if r.setup_name == "VIX Compression": return False
        al = r.greek_alignment or 0
        is_long = r.direction in ("long", "bullish")
        if is_long:
            if al < 2: return False
            if r.setup_name == "Skew Charm": return True
            if r.vix is not None and r.vix > 22:
                ov = r.overvix if r.overvix is not None else -99
                if ov < 2: return False
            return True
        else:
            if r.setup_name in ("Skew Charm", "AG Short"): return True
            if r.setup_name == "DD Exhaustion" and al != 0: return True
            return False

    live = [r for r in rows if passes_v9sc(r)]

    # SC/DD shorts only
    sc_dd_shorts = [r for r in live if r.setup_name in ("Skew Charm", "DD Exhaustion")
                    and r.direction in ("short", "bearish")]
    other_live = [r for r in live if r not in sc_dd_shorts]
    other_pnl = sum(r.outcome_pnl or 0 for r in other_live)

    def stats(trades, label=""):
        if not trades:
            print(f"  {label}: 0 trades")
            return
        w = sum(1 for t in trades if t.outcome_result == 'WIN')
        lo = sum(1 for t in trades if t.outcome_result == 'LOSS')
        ex = sum(1 for t in trades if t.outcome_result == 'EXPIRED')
        pnl = round(sum(t.outcome_pnl or 0 for t in trades), 1)
        wr = round(w / (w + lo) * 100, 1) if (w + lo) > 0 else 0
        # Max drawdown
        running = peak = max_dd = 0
        for t in sorted(trades, key=lambda x: x.ts):
            running += t.outcome_pnl or 0
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd
        print(f"  {label}: {len(trades)}t, {w}W/{lo}L/{ex}E, WR={wr}%, PnL={pnl:+.1f}, MaxDD={max_dd:.1f}")
        return pnl

    print("="*80)
    print("SC/DD SHORTS: PARADIGM DETAIL")
    print("="*80)

    # All GEX subtypes
    gex_types = ["GEX-LIS", "GEX-PURE", "GEX-TARGET", "GEX-MESSY"]
    for par in gex_types:
        trades = [r for r in sc_dd_shorts if r.paradigm == par]
        if trades:
            stats(trades, par)
            # SC vs DD split
            sc = [r for r in trades if r.setup_name == "Skew Charm"]
            dd = [r for r in trades if r.setup_name == "DD Exhaustion"]
            if sc: stats(sc, f"  SC in {par}")
            if dd: stats(dd, f"  DD in {par}")

    print(f"\n  All GEX combined:")
    all_gex = [r for r in sc_dd_shorts if r.paradigm and r.paradigm.startswith("GEX-")]
    stats(all_gex, "ALL GEX shorts")

    print(f"\n  Non-GEX paradigms:")
    non_gex = [r for r in sc_dd_shorts if not r.paradigm or not r.paradigm.startswith("GEX-")]
    stats(non_gex, "NON-GEX shorts")

    # ---- FILTER COMPARISON ----
    print("\n" + "="*80)
    print("FILTER COMPARISON: SC/DD SHORTS")
    print("="*80)

    filters = {
        "Baseline (V9-SC)": lambda r: True,
        "Block GEX-LIS only": lambda r: r.paradigm != "GEX-LIS",
        "Block GEX-LIS + GEX-TARGET": lambda r: r.paradigm not in ("GEX-LIS", "GEX-TARGET"),
        "Block ALL GEX": lambda r: not r.paradigm or not r.paradigm.startswith("GEX-"),
    }

    for fname, fn in filters.items():
        kept = [r for r in sc_dd_shorts if fn(r)]
        blocked = [r for r in sc_dd_shorts if not fn(r)]
        print(f"\n--- {fname} ---")
        stats(kept, "KEPT")
        if blocked:
            stats(blocked, "BLOCKED")

    # ---- FULL SYSTEM (all setups) ----
    print("\n" + "="*80)
    print("FULL V9-SC SYSTEM (all setups included)")
    print("="*80)

    for fname, fn in filters.items():
        kept = [r for r in sc_dd_shorts if fn(r)]
        kept_pnl = sum(r.outcome_pnl or 0 for r in kept)
        total_pnl = kept_pnl + other_pnl
        # compute MaxDD on full system
        full_kept = other_live + kept
        full_kept.sort(key=lambda x: x.ts)
        running = peak = max_dd = 0
        for t in full_kept:
            running += t.outcome_pnl or 0
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd
        w = sum(1 for t in full_kept if t.outcome_result == 'WIN')
        lo = sum(1 for t in full_kept if t.outcome_result == 'LOSS')
        wr = round(w/(w+lo)*100,1) if w+lo > 0 else 0
        print(f"  {fname:35s}: {len(full_kept)}t, WR={wr}%, PnL={total_pnl:+.1f}, MaxDD={max_dd:.1f}")

    # ---- DAILY COMPARISON (March) ----
    print("\n" + "="*80)
    print("MARCH DAILY: Baseline vs !GEX-LIS vs !ALL-GEX")
    print("="*80)

    daily = defaultdict(lambda: {"base": 0, "no_gl": 0, "no_gex": 0,
                                  "n_base": 0, "n_no_gl": 0, "n_no_gex": 0})
    for r in live:
        if r.trade_date.month != 3: continue
        d = r.trade_date
        p = r.outcome_pnl or 0
        daily[d]["base"] += p
        daily[d]["n_base"] += 1

        is_sc_dd_short = (r.setup_name in ("Skew Charm", "DD Exhaustion") and
                          r.direction in ("short", "bearish"))

        # No GEX-LIS
        if not (is_sc_dd_short and r.paradigm == "GEX-LIS"):
            daily[d]["no_gl"] += p
            daily[d]["n_no_gl"] += 1

        # No ALL GEX
        if not (is_sc_dd_short and r.paradigm and r.paradigm.startswith("GEX-")):
            daily[d]["no_gex"] += p
            daily[d]["n_no_gex"] += 1

    print(f"{'Date':>12s} | {'Base':>8s} {'#':>3s} | {'!GEX-LIS':>8s} {'#':>3s} | {'!ALL-GEX':>8s} {'#':>3s} | {'GL diff':>7s} | {'GX diff':>7s}")
    print("-" * 90)

    run_b = run_gl = run_gx = 0
    for d in sorted(daily.keys()):
        dd = daily[d]
        run_b += dd["base"]
        run_gl += dd["no_gl"]
        run_gx += dd["no_gex"]
        gl_diff = dd["no_gl"] - dd["base"]
        gx_diff = dd["no_gex"] - dd["base"]
        flag = ""
        if abs(gl_diff) > 10 or abs(gx_diff) > 10:
            flag = " ***"
        print(f"  {d} | {dd['base']:+8.1f} {dd['n_base']:3d} | {dd['no_gl']:+8.1f} {dd['n_no_gl']:3d} | {dd['no_gex']:+8.1f} {dd['n_no_gex']:3d} | {gl_diff:+7.1f} | {gx_diff:+7.1f}{flag}")

    print("-" * 90)
    print(f"  {'TOTAL':>10s} | {run_b:+8.1f}     | {run_gl:+8.1f}     | {run_gx:+8.1f}     | {run_gl-run_b:+7.1f} | {run_gx-run_b:+7.1f}")

    # ---- GEX-PURE deep dive (it's the one we'd sacrifice) ----
    print("\n" + "="*80)
    print("GEX-PURE DEEP DIVE (would be sacrificed by ALL-GEX block)")
    print("="*80)
    gex_pure = [r for r in sc_dd_shorts if r.paradigm == "GEX-PURE"]
    if gex_pure:
        stats(gex_pure, "GEX-PURE all")
        print(f"\n  Per-trade detail:")
        for r in sorted(gex_pure, key=lambda x: x.ts):
            p = r.outcome_pnl or 0
            mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
            print(f"    #{r.id} {r.trade_date} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f} mxP={mxp} vix={r.vix} al={r.greek_alignment or 0:+d}")

    # ---- GEX-LIS deep dive ----
    print("\n" + "="*80)
    print("GEX-LIS DEEP DIVE (always blocked)")
    print("="*80)
    gex_lis = [r for r in sc_dd_shorts if r.paradigm == "GEX-LIS"]
    if gex_lis:
        stats(gex_lis, "GEX-LIS all")
        print(f"\n  Per-trade detail:")
        for r in sorted(gex_lis, key=lambda x: x.ts):
            p = r.outcome_pnl or 0
            mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
            print(f"    #{r.id} {r.trade_date} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f} mxP={mxp} vix={r.vix} al={r.greek_alignment or 0:+d}")
