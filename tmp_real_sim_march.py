"""
Simulate REAL MONEY trader for March 2026.
Settings: SC-only, 1 MES ($5/pt), cap=2 per direction, V10 filter, SL=14, BE@10, trail act=10 gap=8.
Direction routing: longs → account A, shorts → account B.
"""
import sqlalchemy as sa
import os
from collections import defaultdict
from datetime import datetime

MES_PER_PT = 5.0  # $5 per point per MES contract
QTY = 1  # 1 MES
CAP = 2  # max 2 simultaneous positions per direction

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    # Get ALL SC trades in March with V10 filter
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
               charm_limit_entry
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
          AND ts::date >= '2026-03-01'
        ORDER BY ts
    """)).fetchall()

    def passes_v10(r):
        al = r.greek_alignment or 0
        is_long = r.direction in ("long", "bullish")
        if is_long:
            if al < 2: return False
            return True  # SC longs exempt from VIX gate
        else:
            if r.paradigm == "GEX-LIS": return False  # V10 block
            return True  # SC shorts always pass V9-SC

    live = [r for r in rows if passes_v10(r)]
    print(f"March SC trades passing V10: {len(live)}")

    # Simulate with cap=2 per direction
    # Track open positions per direction
    open_longs = []  # list of trade dicts
    open_shorts = []
    daily_results = defaultdict(lambda: {
        "trades": 0, "wins": 0, "losses": 0, "expired": 0,
        "pnl_pts": 0, "pnl_usd": 0, "skipped_cap": 0,
        "details": []
    })

    total_trades = 0
    total_skipped_cap = 0
    all_trades = []

    for r in live:
        d = r.trade_date
        day = daily_results[d]
        is_long = r.direction in ("long", "bullish")

        # Check cap
        if is_long:
            if len(open_longs) >= CAP:
                day["skipped_cap"] += 1
                total_skipped_cap += 1
                continue
            open_longs.append(r)
        else:
            if len(open_shorts) >= CAP:
                day["skipped_cap"] += 1
                total_skipped_cap += 1
                continue
            open_shorts.append(r)

        p = r.outcome_pnl or 0
        usd = p * MES_PER_PT * QTY
        day["trades"] += 1
        day["pnl_pts"] += p
        day["pnl_usd"] += usd
        total_trades += 1

        if r.outcome_result == "WIN":
            day["wins"] += 1
        elif r.outcome_result == "LOSS":
            day["losses"] += 1
        else:
            day["expired"] += 1

        charm_tag = " [CHARM]" if r.charm_limit_entry else ""
        day["details"].append(f"#{r.id} {r.direction:5s} {r.outcome_result:8s} {p:+.1f}pts (${usd:+.0f}) par={r.paradigm}{charm_tag}")
        all_trades.append({"r": r, "pnl_pts": p, "pnl_usd": usd})

        # Remove from open on completion (simplified — assume instant resolution)
        if is_long:
            open_longs = [x for x in open_longs if x.id != r.id]
        else:
            open_shorts = [x for x in open_shorts if x.id != r.id]

    # Print daily results
    print(f"\n{'='*100}")
    print(f"MARCH 2026: REAL TRADER SIMULATION — SC only, 1 MES, cap=2, V10")
    print(f"{'='*100}")

    running_pts = 0
    running_usd = 0
    peak_usd = 0
    max_dd_usd = 0
    losing_days = 0
    winning_days = 0

    print(f"\n{'Date':>12s} | {'Trades':>6s} | {'W/L/E':>7s} | {'PnL pts':>8s} | {'PnL $':>8s} | {'Run pts':>8s} | {'Run $':>8s} | {'Cap skip':>8s} | Details")
    print("-" * 150)

    for d in sorted(daily_results.keys()):
        day = daily_results[d]
        if day["trades"] == 0 and day["skipped_cap"] == 0:
            continue
        running_pts += day["pnl_pts"]
        running_usd += day["pnl_usd"]
        if running_usd > peak_usd:
            peak_usd = running_usd
        dd = peak_usd - running_usd
        if dd > max_dd_usd:
            max_dd_usd = dd

        if day["pnl_usd"] < 0:
            losing_days += 1
        elif day["pnl_usd"] > 0:
            winning_days += 1

        wle = f"{day['wins']}W/{day['losses']}L/{day['expired']}E"
        details = " | ".join(day["details"][:3])
        if len(day["details"]) > 3:
            details += f" +{len(day['details'])-3} more"
        cap_str = f"{day['skipped_cap']}" if day["skipped_cap"] > 0 else ""
        print(f"  {d} | {day['trades']:6d} | {wle:>7s} | {day['pnl_pts']:+8.1f} | {day['pnl_usd']:+8.0f} | {running_pts:+8.1f} | {running_usd:+8.0f} | {cap_str:>8s} | {details}")

    print("-" * 150)

    # Summary
    w = sum(1 for t in all_trades if t["r"].outcome_result == "WIN")
    lo = sum(1 for t in all_trades if t["r"].outcome_result == "LOSS")
    ex = sum(1 for t in all_trades if t["r"].outcome_result == "EXPIRED")
    wr = round(w / (w + lo) * 100, 1) if (w + lo) > 0 else 0
    total_days = len([d for d in daily_results if daily_results[d]["trades"] > 0])

    print(f"\n{'='*100}")
    print(f"SUMMARY — March 2026")
    print(f"{'='*100}")
    print(f"  Total trades:     {total_trades} ({w}W/{lo}L/{ex}E)")
    print(f"  Win rate:         {wr}%")
    print(f"  Total PnL:        {running_pts:+.1f} pts  /  ${running_usd:+,.0f}")
    print(f"  Max drawdown:     ${max_dd_usd:,.0f}")
    print(f"  Trading days:     {total_days}")
    print(f"  Winning days:     {winning_days}")
    print(f"  Losing days:      {losing_days}")
    print(f"  Avg daily PnL:    {running_pts/total_days:+.1f} pts  /  ${running_usd/total_days:+,.0f}")
    print(f"  Skipped (cap):    {total_skipped_cap}")
    print(f"  Profit factor:    {abs(sum(t['pnl_usd'] for t in all_trades if t['pnl_usd'] > 0)) / abs(sum(t['pnl_usd'] for t in all_trades if t['pnl_usd'] < 0)):.2f}x" if sum(t['pnl_usd'] for t in all_trades if t['pnl_usd'] < 0) != 0 else "  Profit factor:    inf")

    # Monthly projection
    print(f"\n  --- Projection ---")
    print(f"  Per month (~21 days): ${running_usd/total_days * 21:+,.0f}")
    print(f"  At 2 MES:            ${running_usd/total_days * 21 * 2:+,.0f}")
    print(f"  At 4 MES:            ${running_usd/total_days * 21 * 4:+,.0f}")

    # Worst/best days
    print(f"\n  --- Extremes ---")
    best_day = max(daily_results.items(), key=lambda x: x[1]["pnl_usd"])
    worst_day = min(daily_results.items(), key=lambda x: x[1]["pnl_usd"])
    print(f"  Best day:   {best_day[0]} = ${best_day[1]['pnl_usd']:+,.0f} ({best_day[1]['trades']}t)")
    print(f"  Worst day:  {worst_day[0]} = ${worst_day[1]['pnl_usd']:+,.0f} ({worst_day[1]['trades']}t)")

    # Compare V9-SC vs V10
    print(f"\n{'='*100}")
    print(f"COMPARISON: V9-SC vs V10 (same sim, SC-only, 1 MES, cap=2)")
    print(f"{'='*100}")

    # Re-run with V9-SC (no GEX-LIS block)
    def passes_v9sc(r):
        al = r.greek_alignment or 0
        is_long = r.direction in ("long", "bullish")
        if is_long:
            if al < 2: return False
            return True
        else:
            return True  # SC shorts always pass V9-SC (no GEX-LIS block)

    live_v9 = [r for r in rows if passes_v9sc(r)]
    v9_trades = []
    v9_open_longs = []
    v9_open_shorts = []
    for r in live_v9:
        is_long = r.direction in ("long", "bullish")
        if is_long:
            if len(v9_open_longs) >= CAP: continue
            v9_open_longs.append(r)
        else:
            if len(v9_open_shorts) >= CAP: continue
            v9_open_shorts.append(r)
        p = r.outcome_pnl or 0
        v9_trades.append({"pnl_usd": p * MES_PER_PT * QTY, "r": r})
        if is_long:
            v9_open_longs = [x for x in v9_open_longs if x.id != r.id]
        else:
            v9_open_shorts = [x for x in v9_open_shorts if x.id != r.id]

    v9_pnl = sum(t["pnl_usd"] for t in v9_trades)
    v10_pnl = running_usd
    v9_w = sum(1 for t in v9_trades if t["r"].outcome_result == "WIN")
    v9_l = sum(1 for t in v9_trades if t["r"].outcome_result == "LOSS")
    v9_wr = round(v9_w / (v9_w + v9_l) * 100, 1) if (v9_w + v9_l) > 0 else 0

    print(f"  V9-SC:  {len(v9_trades)} trades, {v9_wr}% WR, ${v9_pnl:+,.0f}")
    print(f"  V10:    {total_trades} trades, {wr}% WR, ${v10_pnl:+,.0f}")
    print(f"  Improvement: ${v10_pnl - v9_pnl:+,.0f}")
