"""
Deep March daily comparison: Baseline vs VIX<25 vs C8 vs C6
Show per-trade detail on days where filters diverge significantly.
"""
import sqlalchemy as sa
import os
from collections import defaultdict

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    # Get ALL V9-SC trades (not just SC/DD shorts) for full picture
    all_rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date,
               outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
          AND ts::date >= '2026-03-01'
        ORDER BY ts
    """)).fetchall()

    def passes_v9sc(name, direction, align, vix_val, overvix_val):
        if name == "VIX Compression": return False
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

    # C8 filter: V9-SC + block GEX-LIS/BOFA-PURE on SC/DD shorts
    def passes_c8(r):
        if not passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix):
            return False
        is_sc_dd_short = (r.setup_name in ("Skew Charm", "DD Exhaustion") and
                          r.direction in ("short", "bearish"))
        if is_sc_dd_short and r.paradigm in ("GEX-LIS", "BOFA-PURE"):
            return False
        return True

    # VIX<25 filter on shorts
    def passes_vix25(r):
        if not passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix):
            return False
        is_short = r.direction in ("short", "bearish")
        if is_short and r.setup_name in ("Skew Charm", "DD Exhaustion"):
            if r.vix is not None and r.vix >= 25:
                return False
        return True

    # C6: VIX<25 + not BOFA-PURE on SC/DD shorts
    def passes_c6(r):
        if not passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix):
            return False
        is_sc_dd_short = (r.setup_name in ("Skew Charm", "DD Exhaustion") and
                          r.direction in ("short", "bearish"))
        if is_sc_dd_short:
            if r.vix is not None and r.vix >= 25:
                return False
            if r.paradigm == "BOFA-PURE":
                return False
        return True

    live = [r for r in all_rows if passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix)]

    # Group by date
    by_date = defaultdict(list)
    for r in live:
        by_date[r.trade_date].append(r)

    print("="*120)
    print("MARCH 2026: DAY-BY-DAY FULL V9-SC ANALYSIS")
    print("="*120)

    grand = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}
    running = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}
    peaks = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}
    max_dd = {"base": 0, "vix25": 0, "c8": 0, "c6": 0}

    for d in sorted(by_date.keys()):
        day_trades = by_date[d]
        day_base = sum(r.outcome_pnl or 0 for r in day_trades)
        day_vix25 = sum(r.outcome_pnl or 0 for r in day_trades if passes_vix25(r))
        day_c8 = sum(r.outcome_pnl or 0 for r in day_trades if passes_c8(r))
        day_c6 = sum(r.outcome_pnl or 0 for r in day_trades if passes_c6(r))

        n_base = len(day_trades)
        n_vix25 = sum(1 for r in day_trades if passes_vix25(r))
        n_c8 = sum(1 for r in day_trades if passes_c8(r))
        n_c6 = sum(1 for r in day_trades if passes_c6(r))

        for k, v in [("base", day_base), ("vix25", day_vix25), ("c8", day_c8), ("c6", day_c6)]:
            grand[k] += v
            running[k] += v
            if running[k] > peaks[k]: peaks[k] = running[k]
            dd = peaks[k] - running[k]
            if dd > max_dd[k]: max_dd[k] = dd

        # Check if any filter diverges significantly
        diverges = (abs(day_vix25 - day_base) > 10 or abs(day_c8 - day_base) > 10 or abs(day_c6 - day_base) > 10)

        print(f"\n{'='*120}")
        flag = " *** DIVERGENCE ***" if diverges else ""
        print(f"  {d}  |  Base: {day_base:+.1f} ({n_base}t)  |  VIX<25: {day_vix25:+.1f} ({n_vix25}t)  |  C8: {day_c8:+.1f} ({n_c8}t)  |  C6: {day_c6:+.1f} ({n_c6}t){flag}")
        print(f"  Running: Base={running['base']:+.1f}  VIX<25={running['vix25']:+.1f}  C8={running['c8']:+.1f}  C6={running['c6']:+.1f}")

        # Show individual SC/DD short trades that differ between filters
        sc_dd_shorts = [r for r in day_trades if r.setup_name in ("Skew Charm", "DD Exhaustion")
                        and r.direction in ("short", "bearish")]
        other_trades = [r for r in day_trades if r not in sc_dd_shorts]

        if other_trades:
            other_pnl = sum(r.outcome_pnl or 0 for r in other_trades)
            other_w = sum(1 for r in other_trades if r.outcome_result == 'WIN')
            other_l = sum(1 for r in other_trades if r.outcome_result == 'LOSS')
            other_names = defaultdict(lambda: {"n": 0, "pnl": 0})
            for r in other_trades:
                other_names[r.setup_name]["n"] += 1
                other_names[r.setup_name]["pnl"] += r.outcome_pnl or 0
            other_str = ", ".join(f"{k}({v['n']}t,{v['pnl']:+.1f})" for k,v in other_names.items())
            print(f"  Other setups: {other_pnl:+.1f} [{other_str}]")

        if sc_dd_shorts:
            print(f"  SC/DD shorts ({len(sc_dd_shorts)}):")
            for r in sc_dd_shorts:
                p = r.outcome_pnl or 0
                in_vix25 = "PASS" if passes_vix25(r) else "BLOCK"
                in_c8 = "PASS" if passes_c8(r) else "BLOCK"
                in_c6 = "PASS" if passes_c6(r) else "BLOCK"
                mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
                print(f"    #{r.id} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f}pts | vix={r.vix or 0:.1f} par={r.paradigm:12s} mxP={mxp} | VIX<25={in_vix25:5s} C8={in_c8:5s} C6={in_c6:5s}")

    print(f"\n{'='*120}")
    print(f"MARCH TOTALS")
    print(f"{'='*120}")
    print(f"  Base:   {grand['base']:+.1f} pts  MaxDD={max_dd['base']:.1f}")
    print(f"  VIX<25: {grand['vix25']:+.1f} pts  MaxDD={max_dd['vix25']:.1f}  (diff: {grand['vix25']-grand['base']:+.1f})")
    print(f"  C8:     {grand['c8']:+.1f} pts  MaxDD={max_dd['c8']:.1f}  (diff: {grand['c8']-grand['base']:+.1f})")
    print(f"  C6:     {grand['c6']:+.1f} pts  MaxDD={max_dd['c6']:.1f}  (diff: {grand['c6']-grand['base']:+.1f})")

    # Which paradigms are blocked by C8?
    print(f"\n{'='*120}")
    print(f"SC/DD SHORT PARADIGM BREAKDOWN (March only)")
    print(f"{'='*120}")
    par_stats = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "e": 0, "pnl": 0})
    for r in live:
        if r.setup_name in ("Skew Charm", "DD Exhaustion") and r.direction in ("short", "bearish"):
            k = r.paradigm or "None"
            par_stats[k]["n"] += 1
            par_stats[k]["pnl"] += r.outcome_pnl or 0
            if r.outcome_result == "WIN": par_stats[k]["w"] += 1
            elif r.outcome_result == "LOSS": par_stats[k]["l"] += 1
            else: par_stats[k]["e"] += 1
    for p in sorted(par_stats.keys(), key=lambda x: par_stats[x]["pnl"]):
        s = par_stats[p]
        wr = round(s["w"]/(s["w"]+s["l"])*100,1) if s["w"]+s["l"]>0 else 0
        blocked = "BLOCKED" if p in ("GEX-LIS","BOFA-PURE") else "kept"
        print(f"  {p:15s}: {s['n']:3d}t  {s['w']}W/{s['l']}L/{s['e']}E  WR={wr:5.1f}%  PnL={s['pnl']:+7.1f}  [{blocked}]")
