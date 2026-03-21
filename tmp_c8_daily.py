"""Daily PnL comparison: baseline vs top 3 filters"""
import sqlalchemy as sa
import os
from collections import defaultdict

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date
        FROM setup_log
        WHERE setup_name IN ('Skew Charm', 'DD Exhaustion')
          AND direction IN ('short', 'bearish')
          AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    def passes_v9sc(name, direction, align, vix_val, overvix_val):
        al = align or 0
        is_long = direction in ("long", "bullish")
        if is_long:
            if al < 2: return False
            if name == "Skew Charm": return True
            if vix_val is not None and vix_val > 22:
                ov = overvix_val if overvix_val is not None else -99
                if ov < 2: return False
            return True
        else:
            if name in ("Skew Charm", "AG Short"): return True
            if name == "DD Exhaustion" and al != 0: return True
            return False

    live = [r for r in rows if passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix)]

    # Daily PnL for baseline, VIX<25, C8, VIX<25+!BOFA-PURE
    daily = defaultdict(lambda: {"base": 0, "vix25": 0, "c8": 0, "c6": 0, "count_base": 0,
                                  "count_vix25": 0, "count_c8": 0, "count_c6": 0})
    for r in live:
        d = r.trade_date
        p = r.outcome_pnl or 0
        daily[d]["base"] += p
        daily[d]["count_base"] += 1
        if r.vix is None or r.vix < 25:
            daily[d]["vix25"] += p
            daily[d]["count_vix25"] += 1
        if r.paradigm not in ("GEX-LIS", "BOFA-PURE"):
            daily[d]["c8"] += p
            daily[d]["count_c8"] += 1
        if (r.vix is None or r.vix < 25) and r.paradigm != "BOFA-PURE":
            daily[d]["c6"] += p
            daily[d]["count_c6"] += 1

    print(f"{'Date':>12s} | {'Base':>8s} {'#':>3s} | {'VIX<25':>8s} {'#':>3s} | {'C8(!GL/BP)':>10s} {'#':>3s} | {'C6(V+!BP)':>10s} {'#':>3s}")
    print("-" * 80)
    totals = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}
    lose_base = lose_vix25 = lose_c8 = lose_c6 = 0
    max_dd = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}
    peaks = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}
    for d in sorted(daily.keys()):
        dd = daily[d]
        for k in ["base", "vix25", "c8", "c6"]:
            totals[k] += dd[k]
            if totals[k] > peaks[k]:
                peaks[k] = totals[k]
            ddv = peaks[k] - totals[k]
            if ddv > max_dd[k]:
                max_dd[k] = ddv
        flag = ""
        if abs(dd["vix25"] - dd["base"]) > 15 or abs(dd["c8"] - dd["base"]) > 15:
            flag = " ***"
        print(f"  {d} | {dd['base']:+8.1f} {dd['count_base']:3d} | {dd['vix25']:+8.1f} {dd['count_vix25']:3d} | {dd['c8']:+10.1f} {dd['count_c8']:3d} | {dd['c6']:+10.1f} {dd['count_c6']:3d}{flag}")
        if dd["base"] < 0: lose_base += 1
        if dd["vix25"] < 0: lose_vix25 += 1
        if dd["c8"] < 0: lose_c8 += 1
        if dd["c6"] < 0: lose_c6 += 1

    print("-" * 80)
    print(f"  {'TOTAL':>10s} | {totals['base']:+8.1f}     | {totals['vix25']:+8.1f}     | {totals['c8']:+10.1f}     | {totals['c6']:+10.1f}")
    print(f"  {'MaxDD':>10s} | {max_dd['base']:8.1f}     | {max_dd['vix25']:8.1f}     | {max_dd['c8']:10.1f}     | {max_dd['c6']:10.1f}")
    print(f"  {'Lose days':>10s} | {lose_base:8d}     | {lose_vix25:8d}     | {lose_c8:10d}     | {lose_c6:10d}")

    # Also get FULL V9-SC PnL (including AG, longs, etc)
    print("\n" + "="*70)
    print("FULL V9-SC (ALL setups) - what would change if we add SC/DD short paradigm filter")
    print("="*70)

    all_rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    full_base = 0
    full_c8 = 0
    for r in all_rows:
        if not passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix):
            continue
        p = r.outcome_pnl or 0
        full_base += p
        # C8 filter: for SC/DD shorts only, block GEX-LIS and BOFA-PURE paradigms
        is_sc_dd_short = (r.setup_name in ("Skew Charm", "DD Exhaustion") and
                          r.direction in ("short", "bearish"))
        if is_sc_dd_short and r.paradigm in ("GEX-LIS", "BOFA-PURE"):
            continue  # blocked
        full_c8 += p

    print(f"  V9-SC baseline (all setups): {full_base:+.1f} pts")
    print(f"  V9-SC + C8 paradigm block:   {full_c8:+.1f} pts")
    print(f"  Improvement:                 {full_c8 - full_base:+.1f} pts")
