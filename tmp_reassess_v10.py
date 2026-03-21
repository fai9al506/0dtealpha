"""Reassess V10 vs V9-SC with today's full data (including late GEX-LIS winners)."""
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

    def passes_v9sc(r):
        al = r.greek_alignment or 0
        if r.setup_name in ("Skew Charm", "AG Short"): return True
        if r.setup_name == "DD Exhaustion" and al != 0: return True
        return False

    live = [r for r in rows if passes_v9sc(r)]

    # GEX-LIS stats updated with today's late winners
    gex_lis = [r for r in live if r.paradigm == "GEX-LIS"]
    non_gex_lis = [r for r in live if r.paradigm != "GEX-LIS"]

    def stats(trades, label):
        w = sum(1 for t in trades if t.outcome_result == 'WIN')
        lo = sum(1 for t in trades if t.outcome_result == 'LOSS')
        ex = sum(1 for t in trades if t.outcome_result == 'EXPIRED')
        pnl = round(sum(t.outcome_pnl or 0 for t in trades), 1)
        wr = round(w/(w+lo)*100,1) if w+lo > 0 else 0
        running = peak = max_dd = 0
        for t in sorted(trades, key=lambda x: x.ts):
            running += t.outcome_pnl or 0
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd
        print(f"  {label}: {len(trades)}t, {w}W/{lo}L/{ex}E, WR={wr}%, PnL={pnl:+.1f}, MaxDD={max_dd:.1f}")
        return pnl

    print("="*80)
    print("REASSESSMENT: GEX-LIS after today's late winners")
    print("="*80)
    stats(gex_lis, "GEX-LIS (ALL TIME)")
    stats(non_gex_lis, "Non-GEX-LIS")

    # Today's GEX-LIS detail
    today_gl = [r for r in gex_lis if r.trade_date.month == 3 and r.trade_date.day == 20]
    print(f"\n  Today's GEX-LIS: {len(today_gl)} trades")
    t_pnl = 0
    for r in today_gl:
        p = r.outcome_pnl or 0
        t_pnl += p
        print(f"    #{r.id} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f} t={str(r.ts)[11:16]}")
    print(f"  Today GEX-LIS total: {t_pnl:+.1f}")

    # Full system comparison
    print(f"\n{'='*80}")
    print("FULL SYSTEM: V9-SC vs V10 (all setups, updated)")
    print("="*80)

    all_rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    def full_v9sc(r):
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

    def full_v10(r):
        if not full_v9sc(r): return False
        is_short = r.direction in ("short", "bearish")
        if is_short and r.setup_name in ("Skew Charm", "DD Exhaustion") and r.paradigm == "GEX-LIS":
            return False
        return True

    v9_all = [r for r in all_rows if full_v9sc(r)]
    v10_all = [r for r in all_rows if full_v10(r)]

    v9_pnl = sum(r.outcome_pnl or 0 for r in v9_all)
    v10_pnl = sum(r.outcome_pnl or 0 for r in v10_all)

    v9_w = sum(1 for r in v9_all if r.outcome_result == 'WIN')
    v9_l = sum(1 for r in v9_all if r.outcome_result == 'LOSS')
    v10_w = sum(1 for r in v10_all if r.outcome_result == 'WIN')
    v10_l = sum(1 for r in v10_all if r.outcome_result == 'LOSS')

    print(f"  V9-SC: {len(v9_all)}t, {v9_w}W/{v9_l}L, WR={round(v9_w/(v9_w+v9_l)*100,1)}%, PnL={v9_pnl:+.1f}")
    print(f"  V10:   {len(v10_all)}t, {v10_w}W/{v10_l}L, WR={round(v10_w/(v10_w+v10_l)*100,1)}%, PnL={v10_pnl:+.1f}")
    print(f"  Diff:  {v10_pnl - v9_pnl:+.1f} pts")

    # Daily comparison March
    print(f"\n{'='*80}")
    print("MARCH DAILY: V9-SC vs V10 (updated with today's late trades)")
    print("="*80)
    daily_v9 = defaultdict(float)
    daily_v10 = defaultdict(float)
    for r in v9_all:
        if r.trade_date.month == 3:
            daily_v9[r.trade_date] += r.outcome_pnl or 0
    for r in v10_all:
        if r.trade_date.month == 3:
            daily_v10[r.trade_date] += r.outcome_pnl or 0

    run_v9 = run_v10 = 0
    for d in sorted(set(list(daily_v9.keys()) + list(daily_v10.keys()))):
        v9d = daily_v9[d]
        v10d = daily_v10[d]
        run_v9 += v9d
        run_v10 += v10d
        diff = v10d - v9d
        flag = " ***" if abs(diff) > 10 else ""
        print(f"  {d} | V9={v9d:+7.1f} V10={v10d:+7.1f} diff={diff:+7.1f} | run V9={run_v9:+8.1f} V10={run_v10:+8.1f}{flag}")

    print(f"\n  March total: V9-SC={run_v9:+.1f}  V10={run_v10:+.1f}  diff={run_v10-run_v9:+.1f}")
