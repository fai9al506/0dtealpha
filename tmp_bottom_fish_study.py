"""
Study: Why did V9-SC lose today? What filter could prevent "shorting the bottom"?

Today's problem: After 45pt selloff, SC and DD kept firing shorts at the bottom.
All 5 resolved short trades lost. The bounce killed them.

Hypotheses to test:
1. Paradigm: GEX-LIS shorts perform worse?
2. VIX level: shorts at VIX > 24-25 perform worse?
3. Intraday drawdown: shorts after a big intraday drop perform worse?
4. Consecutive same-direction signals: later shorts in a cluster perform worse?
5. DD hedging alignment (Analysis #20 revisit)
6. Combined filters
"""
import sqlalchemy as sa
import os
import json
from collections import defaultdict
from datetime import datetime, timedelta

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    # Get ALL SC and DD short trades (the two setups that passed V9-SC today)
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, score, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, charm_limit_entry,
               outcome_max_profit, outcome_max_loss,
               lis, target, ts::date as trade_date
        FROM setup_log
        WHERE setup_name IN ('Skew Charm', 'DD Exhaustion')
          AND direction IN ('short', 'bearish')
          AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    print(f"Total resolved SC/DD short trades: {len(rows)}")

    # Apply V9-SC filter
    def passes_v9sc(name, direction, align, vix_val, overvix_val):
        is_long = direction in ("long", "bullish")
        al = align or 0
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
    print(f"V9-SC live shorts: {len(live)}")

    # ---- BASELINE STATS ----
    def stats(trades, label=""):
        if not trades:
            return {"n": 0, "w": 0, "l": 0, "pnl": 0, "wr": 0}
        w = sum(1 for t in trades if t.outcome_result == 'WIN')
        lo = sum(1 for t in trades if t.outcome_result == 'LOSS')
        ex = sum(1 for t in trades if t.outcome_result == 'EXPIRED')
        pnl = sum(t.outcome_pnl or 0 for t in trades)
        wr = round(w / (w + lo) * 100, 1) if (w + lo) > 0 else 0
        if label:
            print(f"  {label}: {w+lo+ex}t, {w}W/{lo}L/{ex}E, WR={wr}%, PnL={pnl:+.1f}")
        return {"n": w + lo + ex, "w": w, "l": lo, "pnl": pnl, "wr": wr}

    print("\n" + "="*60)
    print("BASELINE (all V9-SC SC/DD shorts)")
    print("="*60)
    stats(live, "ALL")
    sc = [r for r in live if r.setup_name == "Skew Charm"]
    dd = [r for r in live if r.setup_name == "DD Exhaustion"]
    stats(sc, "SC only")
    stats(dd, "DD only")

    # ---- HYPOTHESIS 1: PARADIGM ----
    print("\n" + "="*60)
    print("H1: PARADIGM BREAKDOWN (SC/DD shorts)")
    print("="*60)
    by_par = defaultdict(list)
    for r in live:
        by_par[r.paradigm or "None"].append(r)
    for par in sorted(by_par.keys()):
        stats(by_par[par], par)

    # ---- HYPOTHESIS 2: VIX LEVEL ----
    print("\n" + "="*60)
    print("H2: VIX LEVEL BREAKDOWN (SC/DD shorts)")
    print("="*60)
    vix_buckets = [(0, 16, "<16"), (16, 18, "16-18"), (18, 20, "18-20"),
                   (20, 22, "20-22"), (22, 24, "22-24"), (24, 26, "24-26"), (26, 99, ">26")]
    for lo_v, hi_v, label in vix_buckets:
        bucket = [r for r in live if r.vix is not None and lo_v <= r.vix < hi_v]
        if bucket:
            stats(bucket, f"VIX {label}")

    # ---- HYPOTHESIS 3: INTRADAY DRAWDOWN (distance from day's high) ----
    print("\n" + "="*60)
    print("H3: INTRADAY SPOT MOVE (shorts after big drop)")
    print("="*60)
    # Group trades by date, compute running session high
    by_date = defaultdict(list)
    for r in live:
        by_date[r.trade_date].append(r)

    # Also get all setup_log entries per date for session high tracking
    all_spots = c.execute(sa.text("""
        SELECT ts::date as d, ts, spot FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts
    """)).fetchall()
    date_spots = defaultdict(list)
    for s in all_spots:
        date_spots[s.d].append((s.ts, float(s.spot)))

    # For each trade, compute: spot relative to session high up to that point
    trades_with_context = []
    for r in live:
        d = r.trade_date
        spots_before = [(ts, sp) for ts, sp in date_spots[d] if ts <= r.ts]
        if spots_before:
            session_high = max(sp for _, sp in spots_before)
            session_low = min(sp for _, sp in spots_before)
            drop_from_high = session_high - float(r.spot)  # positive = price dropped
            range_pct = (float(r.spot) - session_low) / (session_high - session_low) * 100 if session_high != session_low else 50
        else:
            drop_from_high = 0
            range_pct = 50
        trades_with_context.append((r, drop_from_high, range_pct))

    # Bucket by drop from high
    drop_buckets = [(0, 10, "<10pts"), (10, 20, "10-20pts"), (20, 30, "20-30pts"),
                    (30, 40, "30-40pts"), (40, 999, ">40pts")]
    for lo_d, hi_d, label in drop_buckets:
        bucket = [r for r, drop, _ in trades_with_context if lo_d <= drop < hi_d]
        if bucket:
            stats(bucket, f"Drop {label}")

    # Bucket by position in day range
    print("\n  Position in day range (0%=low, 100%=high):")
    range_buckets = [(0, 20, "0-20% (near low)"), (20, 40, "20-40%"), (40, 60, "40-60% (middle)"),
                     (60, 80, "60-80%"), (80, 100.1, "80-100% (near high)")]
    for lo_r, hi_r, label in range_buckets:
        bucket = [r for r, _, rng in trades_with_context if lo_r <= rng < hi_r]
        if bucket:
            stats(bucket, label)

    # ---- HYPOTHESIS 4: CONSECUTIVE SHORTS ----
    print("\n" + "="*60)
    print("H4: NTH SHORT ON SAME DAY (cluster fatigue)")
    print("="*60)
    for d, day_trades in sorted(by_date.items()):
        day_trades.sort(key=lambda r: r.ts)
    nth_bucket = defaultdict(list)
    for d, day_trades in by_date.items():
        for i, r in enumerate(day_trades):
            nth = i + 1
            nth_bucket[min(nth, 5)].append(r)  # cap at 5+
    for nth in sorted(nth_bucket.keys()):
        label = f"#{nth}" if nth < 5 else f"#{nth}+"
        stats(nth_bucket[nth], f"Trade {label} of day")

    # ---- HYPOTHESIS 5: MFE analysis (could tighter management help?) ----
    print("\n" + "="*60)
    print("H5: MFE ON LOSING TRADES (management opportunity)")
    print("="*60)
    losses = [r for r in live if r.outcome_result == 'LOSS']
    mfe_buckets = [(0, 3, "MFE <3"), (3, 6, "MFE 3-6"), (6, 10, "MFE 6-10"), (10, 999, "MFE 10+")]
    for lo_m, hi_m, label in mfe_buckets:
        bucket = [r for r in losses if r.outcome_max_profit is not None and lo_m <= r.outcome_max_profit < hi_m]
        if bucket:
            print(f"  {label}: {len(bucket)} losses (avg pnl {sum(r.outcome_pnl or 0 for r in bucket)/len(bucket):+.1f})")

    # ---- HYPOTHESIS 6: TIME OF DAY ----
    print("\n" + "="*60)
    print("H6: TIME OF DAY (SC/DD shorts)")
    print("="*60)
    time_buckets = [(9, 11, "9:30-11:00"), (11, 13, "11:00-13:00"),
                    (13, 14, "13:00-14:00"), (14, 15, "14:00-15:00"), (15, 16.5, "15:00-16:00+")]
    for lo_h, hi_h, label in time_buckets:
        bucket = [r for r in live if r.ts.hour + r.ts.minute/60 >= lo_h and r.ts.hour + r.ts.minute/60 < hi_h]
        if bucket:
            stats(bucket, label)

    # ---- HYPOTHESIS 7: GRADE ----
    print("\n" + "="*60)
    print("H7: GRADE (SC/DD shorts)")
    print("="*60)
    by_grade = defaultdict(list)
    for r in live:
        by_grade[f"{r.setup_name} {r.grade}"].append(r)
    for g in sorted(by_grade.keys()):
        stats(by_grade[g], g)

    # ---- COMBINED FILTER TESTS ----
    print("\n" + "="*60)
    print("COMBINED FILTER TESTS")
    print("="*60)

    # Filter A: Block shorts when drop from high > 30pts
    fa = [r for r, drop, _ in trades_with_context if drop < 30]
    fb = [r for r, drop, _ in trades_with_context if drop >= 30]
    print("\nFilter A: Block shorts when drop_from_high > 30pts")
    stats(fa, "KEPT (drop<30)")
    stats(fb, "BLOCKED (drop>=30)")

    # Filter B: Block shorts when drop from high > 20pts
    fa2 = [r for r, drop, _ in trades_with_context if drop < 20]
    fb2 = [r for r, drop, _ in trades_with_context if drop >= 20]
    print("\nFilter B: Block shorts when drop_from_high > 20pts")
    stats(fa2, "KEPT (drop<20)")
    stats(fb2, "BLOCKED (drop>=20)")

    # Filter C: Block shorts when in bottom 20% of day range
    fc_keep = [r for r, _, rng in trades_with_context if rng >= 20]
    fc_block = [r for r, _, rng in trades_with_context if rng < 20]
    print("\nFilter C: Block shorts in bottom 20% of day range")
    stats(fc_keep, "KEPT (range>=20%)")
    stats(fc_block, "BLOCKED (range<20%)")

    # Filter D: Block 4th+ short of the day
    fd_keep = []
    fd_block = []
    for d, day_trades in by_date.items():
        day_trades_sorted = sorted(day_trades, key=lambda r: r.ts)
        for i, r in enumerate(day_trades_sorted):
            if i < 3:
                fd_keep.append(r)
            else:
                fd_block.append(r)
    print("\nFilter D: Max 3 shorts per day")
    stats(fd_keep, "KEPT (first 3)")
    stats(fd_block, "BLOCKED (4th+)")

    # Filter E: VIX gate on shorts (VIX < 25)
    fe_keep = [r for r in live if r.vix is None or r.vix < 25]
    fe_block = [r for r in live if r.vix is not None and r.vix >= 25]
    print("\nFilter E: Block shorts when VIX >= 25")
    stats(fe_keep, "KEPT (VIX<25)")
    stats(fe_block, "BLOCKED (VIX>=25)")

    # Filter F: VIX gate on shorts (VIX < 24)
    ff_keep = [r for r in live if r.vix is None or r.vix < 24]
    ff_block = [r for r in live if r.vix is not None and r.vix >= 24]
    print("\nFilter F: Block shorts when VIX >= 24")
    stats(ff_keep, "KEPT (VIX<24)")
    stats(ff_block, "BLOCKED (VIX>=24)")

    # Filter G: Block GEX-LIS paradigm shorts
    fg_keep = [r for r in live if r.paradigm != 'GEX-LIS']
    fg_block = [r for r in live if r.paradigm == 'GEX-LIS']
    print("\nFilter G: Block GEX-LIS paradigm shorts")
    stats(fg_keep, "KEPT (non GEX-LIS)")
    stats(fg_block, "BLOCKED (GEX-LIS)")

    # ---- TODAY'S TRADES with context ----
    print("\n" + "="*60)
    print("TODAY'S TRADES WITH CONTEXT")
    print("="*60)
    today_trades = [(r, drop, rng) for r, drop, rng in trades_with_context
                    if r.trade_date == datetime(2026, 3, 20).date()]
    for r, drop, rng in today_trades:
        res = r.outcome_result or 'OPEN'
        p = r.outcome_pnl or 0
        print(f"#{r.id} {r.setup_name:15s} {res:8s} {p:+.1f}pts | drop={drop:.0f} range={rng:.0f}% | vix={r.vix} | par={r.paradigm} | t={str(r.ts)[11:16]}")
