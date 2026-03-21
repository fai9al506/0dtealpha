"""
V10 all setups projection: 1 MES, 1 ES, 2 ES.
Include FULL risk profile: MaxDD, worst day, worst streak, PnL/MaxDD ratio.
Use March data (15 trading days) extrapolated to 21-day month.
"""
import sqlalchemy as sa
import os
from collections import defaultdict

MES_PT = 5.0    # $5/pt per MES
ES_PT = 50.0    # $50/pt per ES
CAP = 2

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
          AND ts::date >= '2026-03-01'
        ORDER BY ts
    """)).fetchall()

    def passes_v10(r):
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
            if r.setup_name in ("Skew Charm", "DD Exhaustion"):
                if r.paradigm == "GEX-LIS": return False
            if r.setup_name in ("Skew Charm", "AG Short"): return True
            if r.setup_name == "DD Exhaustion" and al != 0: return True
            return False

    live = [r for r in rows if passes_v10(r)]

    # Simulate with cap=2
    open_longs = []
    open_shorts = []
    daily = defaultdict(lambda: {"trades": [], "pnl_pts": 0})

    for r in live:
        is_long = r.direction in ("long", "bullish")
        if is_long:
            if len(open_longs) >= CAP: continue
            open_longs.append(r)
        else:
            if len(open_shorts) >= CAP: continue
            open_shorts.append(r)

        p = r.outcome_pnl or 0
        daily[r.trade_date]["trades"].append(r)
        daily[r.trade_date]["pnl_pts"] += p

        if is_long:
            open_longs = [x for x in open_longs if x.id != r.id]
        else:
            open_shorts = [x for x in open_shorts if x.id != r.id]

    # Compute stats
    all_trades = []
    for d in sorted(daily.keys()):
        for r in daily[d]["trades"]:
            all_trades.append(r)

    total_pts = sum(r.outcome_pnl or 0 for r in all_trades)
    w = sum(1 for r in all_trades if r.outcome_result == 'WIN')
    lo = sum(1 for r in all_trades if r.outcome_result == 'LOSS')
    ex = sum(1 for r in all_trades if r.outcome_result == 'EXPIRED')
    wr = round(w/(w+lo)*100,1) if w+lo > 0 else 0
    trading_days = len(daily)

    # Daily PnL list for risk metrics
    daily_pnls = []
    for d in sorted(daily.keys()):
        daily_pnls.append({"date": d, "pnl": daily[d]["pnl_pts"]})

    # MaxDD (intraday, trade-by-trade)
    running = peak = max_dd_pts = 0
    for r in all_trades:
        running += r.outcome_pnl or 0
        if running > peak: peak = running
        dd = peak - running
        if dd > max_dd_pts: max_dd_pts = dd

    # MaxDD (daily)
    running_d = peak_d = max_dd_daily = 0
    for dp in daily_pnls:
        running_d += dp["pnl"]
        if running_d > peak_d: peak_d = running_d
        dd = peak_d - running_d
        if dd > max_dd_daily: max_dd_daily = dd

    # Worst day
    worst_day = min(daily_pnls, key=lambda x: x["pnl"])
    best_day = max(daily_pnls, key=lambda x: x["pnl"])

    # Worst consecutive losing streak (days)
    max_lose_streak = 0
    current_streak = 0
    for dp in daily_pnls:
        if dp["pnl"] < 0:
            current_streak += 1
            if current_streak > max_lose_streak:
                max_lose_streak = current_streak
        else:
            current_streak = 0

    # Worst consecutive losing trades
    max_trade_streak = 0
    current_t_streak = 0
    for r in all_trades:
        if r.outcome_result == 'LOSS':
            current_t_streak += 1
            if current_t_streak > max_trade_streak:
                max_trade_streak = current_t_streak
        else:
            current_t_streak = 0

    # Winning/losing days
    win_days = sum(1 for dp in daily_pnls if dp["pnl"] > 0)
    lose_days = sum(1 for dp in daily_pnls if dp["pnl"] < 0)
    flat_days = sum(1 for dp in daily_pnls if dp["pnl"] == 0)

    avg_daily = total_pts / trading_days
    avg_win_day = sum(dp["pnl"] for dp in daily_pnls if dp["pnl"] > 0) / win_days if win_days > 0 else 0
    avg_lose_day = sum(dp["pnl"] for dp in daily_pnls if dp["pnl"] < 0) / lose_days if lose_days > 0 else 0

    # PF
    gross_win = sum(r.outcome_pnl for r in all_trades if (r.outcome_pnl or 0) > 0)
    gross_loss = abs(sum(r.outcome_pnl for r in all_trades if (r.outcome_pnl or 0) < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')

    # ---- DISPLAY ----
    print("="*90)
    print("V10 ALL SETUPS — MARCH 2026 PERFORMANCE + RISK PROFILE")
    print("="*90)

    print(f"\n  PERFORMANCE")
    print(f"  {'Trades':25s}: {len(all_trades)} ({w}W/{lo}L/{ex}E)")
    print(f"  {'Win Rate':25s}: {wr}%")
    print(f"  {'Total PnL':25s}: {total_pts:+.1f} pts")
    print(f"  {'Profit Factor':25s}: {pf:.2f}x")
    print(f"  {'Avg Daily PnL':25s}: {avg_daily:+.1f} pts")

    print(f"\n  RISK")
    print(f"  {'Max DD (trade-by-trade)':25s}: {max_dd_pts:.1f} pts")
    print(f"  {'Max DD (daily)':25s}: {max_dd_daily:.1f} pts")
    print(f"  {'PnL/MaxDD Ratio':25s}: {total_pts/max_dd_pts:.2f}x" if max_dd_pts > 0 else "")
    print(f"  {'Worst Day':25s}: {worst_day['date']} = {worst_day['pnl']:+.1f} pts")
    print(f"  {'Best Day':25s}: {best_day['date']} = {best_day['pnl']:+.1f} pts")
    print(f"  {'Avg Winning Day':25s}: {avg_win_day:+.1f} pts")
    print(f"  {'Avg Losing Day':25s}: {avg_lose_day:+.1f} pts")
    print(f"  {'Max Losing Streak (days)':25s}: {max_lose_streak}")
    print(f"  {'Max Losing Streak (trades)':25s}: {max_trade_streak}")
    print(f"  {'Win/Lose Days':25s}: {win_days}W / {lose_days}L / {flat_days}F")

    print(f"\n  DAILY BREAKDOWN")
    print(f"  {'Date':>12s} | {'PnL pts':>8s} | {'Trades':>6s} | {'W/L':>5s}")
    print(f"  {'-'*45}")
    for dp in daily_pnls:
        d = dp["date"]
        day = daily[d]
        dw = sum(1 for r in day["trades"] if r.outcome_result == 'WIN')
        dl = sum(1 for r in day["trades"] if r.outcome_result == 'LOSS')
        print(f"  {d} | {dp['pnl']:+8.1f} | {len(day['trades']):6d} | {dw}W/{dl}L")

    # ---- PROJECTIONS ----
    scales = [
        ("1 MES", MES_PT, 1, 2737),
        ("1 ES", ES_PT, 1, 15000),
        ("2 ES", ES_PT, 2, 30000),
    ]

    print(f"\n{'='*90}")
    print(f"MONTHLY PROJECTIONS (21 trading days)")
    print(f"{'='*90}")

    monthly_pts = avg_daily * 21

    print(f"\n  {'':20s} | {'1 MES':>12s} | {'1 ES':>12s} | {'2 ES':>12s}")
    print(f"  {'-'*20}-+-{'-'*12}-+-{'-'*12}-+-{'-'*12}")

    # Monthly PnL
    vals = []
    for name, pt_val, qty, cap_req in scales:
        monthly_usd = monthly_pts * pt_val * qty
        vals.append(f"${monthly_usd:+,.0f}")
    print(f"  {'Monthly PnL':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # Max DD
    vals = []
    for name, pt_val, qty, cap_req in scales:
        dd_usd = max_dd_pts * pt_val * qty
        vals.append(f"${dd_usd:,.0f}")
    print(f"  {'Max Drawdown':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # Worst Day
    vals = []
    for name, pt_val, qty, cap_req in scales:
        wd_usd = worst_day["pnl"] * pt_val * qty
        vals.append(f"${wd_usd:+,.0f}")
    print(f"  {'Worst Day':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # Capital required (margin)
    vals = []
    for name, pt_val, qty, cap_req in scales:
        vals.append(f"${cap_req * qty:,}")
    print(f"  {'Margin Required':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # Monthly ROI
    vals = []
    for name, pt_val, qty, cap_req in scales:
        monthly_usd = monthly_pts * pt_val * qty
        roi = monthly_usd / (cap_req * qty) * 100
        vals.append(f"{roi:+.0f}%")
    print(f"  {'Monthly ROI':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # PnL/MaxDD ratio
    vals = []
    for name, pt_val, qty, cap_req in scales:
        monthly_usd = monthly_pts * pt_val * qty
        dd_usd = max_dd_pts * pt_val * qty
        ratio = monthly_usd / dd_usd if dd_usd > 0 else 0
        vals.append(f"{ratio:.1f}x")
    print(f"  {'PnL/MaxDD':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # DD as % of capital
    vals = []
    for name, pt_val, qty, cap_req in scales:
        dd_usd = max_dd_pts * pt_val * qty
        dd_pct = dd_usd / (cap_req * qty) * 100
        vals.append(f"{dd_pct:.0f}%")
    print(f"  {'DD % of Capital':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # Worst day as % of capital
    vals = []
    for name, pt_val, qty, cap_req in scales:
        wd_usd = abs(worst_day["pnl"]) * pt_val * qty
        wd_pct = wd_usd / (cap_req * qty) * 100
        vals.append(f"{wd_pct:.0f}%")
    print(f"  {'Worst Day % Capital':20s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    # By setup
    print(f"\n{'='*90}")
    print(f"BY SETUP (March, V10, cap=2)")
    print(f"{'='*90}")
    by_setup = defaultdict(lambda: {"n": 0, "pnl": 0, "w": 0, "l": 0, "e": 0})
    for r in all_trades:
        s = by_setup[r.setup_name]
        s["n"] += 1
        s["pnl"] += r.outcome_pnl or 0
        if r.outcome_result == "WIN": s["w"] += 1
        elif r.outcome_result == "LOSS": s["l"] += 1
        else: s["e"] += 1
    for sn in sorted(by_setup.keys(), key=lambda x: by_setup[x]["pnl"], reverse=True):
        s = by_setup[sn]
        swr = round(s["w"]/(s["w"]+s["l"])*100,1) if s["w"]+s["l"]>0 else 0
        print(f"  {sn:20s}: {s['n']:3d}t  {s['w']}W/{s['l']}L/{s['e']}E  WR={swr:5.1f}%  {s['pnl']:+7.1f}pts  ${s['pnl']*MES_PT:+,.0f}(MES)  ${s['pnl']*ES_PT:+,.0f}(ES)")
