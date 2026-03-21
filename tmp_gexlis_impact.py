"""
Impact of blocking GEX-LIS on SC/DD shorts:
- Today (Mar 20) trade-by-trade
- Last 2 weeks (Mar 9-20) day-by-day with running PnL
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
               outcome_max_profit, outcome_max_loss, charm_limit_entry
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
          AND ts::date >= '2026-03-09'
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

    def is_blocked_by_gexlis(r):
        """Would this trade be blocked by GEX-LIS filter?"""
        is_sc_dd_short = (r.setup_name in ("Skew Charm", "DD Exhaustion") and
                          r.direction in ("short", "bearish"))
        return is_sc_dd_short and r.paradigm == "GEX-LIS"

    live = [r for r in rows if passes_v9sc(r)]

    # ---- TODAY DETAIL ----
    print("="*100)
    print("TODAY (Mar 20) — EVERY V9-SC TRADE")
    print("="*100)
    today = [r for r in live if r.trade_date.day == 20 and r.trade_date.month == 3]
    base_pnl = 0
    filt_pnl = 0
    for r in today:
        p = r.outcome_pnl or 0
        blocked = is_blocked_by_gexlis(r)
        base_pnl += p
        if not blocked:
            filt_pnl += p
        mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
        status = "BLOCKED" if blocked else "PASS"
        print(f"  #{r.id} {r.setup_name:20s} {r.direction:7s} {r.outcome_result:8s} {p:+6.1f}pts mxP={mxp:>6s} vix={r.vix:.1f} par={r.paradigm:12s} [{status}]")
    print(f"\n  Today baseline:  {base_pnl:+.1f} pts")
    print(f"  Today filtered:  {filt_pnl:+.1f} pts")
    print(f"  Improvement:     {filt_pnl - base_pnl:+.1f} pts")

    # ---- LAST 2 WEEKS DAY-BY-DAY ----
    print(f"\n{'='*100}")
    print("LAST 2 WEEKS (Mar 9-20) — DAY BY DAY")
    print("="*100)

    daily = defaultdict(lambda: {"base_trades": [], "filt_trades": [], "blocked_trades": []})
    for r in live:
        d = r.trade_date
        daily[d]["base_trades"].append(r)
        if is_blocked_by_gexlis(r):
            daily[d]["blocked_trades"].append(r)
        else:
            daily[d]["filt_trades"].append(r)

    run_base = 0
    run_filt = 0
    print(f"\n{'Date':>12s} | {'Base':>7s} {'#':>3s} | {'!GL':>7s} {'#':>3s} | {'Diff':>6s} | {'Run Base':>9s} | {'Run !GL':>9s} | Blocked detail")
    print("-" * 120)

    for d in sorted(daily.keys()):
        dd = daily[d]
        b_pnl = sum(r.outcome_pnl or 0 for r in dd["base_trades"])
        f_pnl = sum(r.outcome_pnl or 0 for r in dd["filt_trades"])
        run_base += b_pnl
        run_filt += f_pnl
        diff = f_pnl - b_pnl

        # Blocked detail
        blocked_detail = ""
        if dd["blocked_trades"]:
            parts = []
            for r in dd["blocked_trades"]:
                p = r.outcome_pnl or 0
                parts.append(f"{r.setup_name[:2]}#{r.id}={p:+.1f}")
            blocked_detail = ", ".join(parts)

        flag = " ***" if abs(diff) > 5 else ""
        print(f"  {d} | {b_pnl:+7.1f} {len(dd['base_trades']):3d} | {f_pnl:+7.1f} {len(dd['filt_trades']):3d} | {diff:+6.1f} | {run_base:+9.1f} | {run_filt:+9.1f} | {blocked_detail}{flag}")

    print("-" * 120)
    print(f"  {'TOTAL':>10s} | {run_base:+7.1f}     | {run_filt:+7.1f}     | {run_filt-run_base:+6.1f}")

    # ---- SUMMARY STATS ----
    print(f"\n{'='*100}")
    print("SUMMARY: Mar 9-20 (last 2 weeks)")
    print("="*100)

    base_all = [r for r in live]
    filt_all = [r for r in live if not is_blocked_by_gexlis(r)]
    blocked_all = [r for r in live if is_blocked_by_gexlis(r)]

    def full_stats(trades, label):
        w = sum(1 for t in trades if t.outcome_result == 'WIN')
        lo = sum(1 for t in trades if t.outcome_result == 'LOSS')
        ex = sum(1 for t in trades if t.outcome_result == 'EXPIRED')
        pnl = round(sum(t.outcome_pnl or 0 for t in trades), 1)
        wr = round(w/(w+lo)*100,1) if w+lo > 0 else 0
        # MaxDD
        running = peak = max_dd = 0
        for t in sorted(trades, key=lambda x: x.ts):
            running += t.outcome_pnl or 0
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd
        # Losing days
        by_d = defaultdict(float)
        for t in trades:
            by_d[t.trade_date] += t.outcome_pnl or 0
        lose_days = sum(1 for v in by_d.values() if v < 0)
        total_days = len(by_d)
        print(f"  {label}:")
        print(f"    Trades: {len(trades)} ({w}W/{lo}L/{ex}E)")
        print(f"    WR: {wr}%")
        print(f"    PnL: {pnl:+.1f} pts")
        print(f"    MaxDD: {max_dd:.1f} pts")
        print(f"    Losing days: {lose_days}/{total_days}")
        print(f"    Avg daily: {pnl/total_days:+.1f} pts/day")

    full_stats(base_all, "V9-SC (current)")
    print()
    full_stats(filt_all, "V9-SC + !GEX-LIS (proposed)")
    print()
    full_stats(blocked_all, "BLOCKED trades (GEX-LIS shorts)")
