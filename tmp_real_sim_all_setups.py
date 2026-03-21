"""
Simulate REAL MONEY trader for March 2026: SC-only vs ALL setups.
Settings: 1 MES ($5/pt), cap=2 per direction, V10 filter, SL per setup.
Direction routing: longs → account A, shorts → account B.
"""
import sqlalchemy as sa
import os
from collections import defaultdict

MES_PER_PT = 5.0
QTY = 1
CAP = 2

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
               charm_limit_entry
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
          AND ts::date >= '2026-03-01'
        ORDER BY ts
    """)).fetchall()

    def passes_v10(r):
        if r.setup_name == "VIX Compression":
            return False
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
            if r.setup_name in ("Skew Charm", "AG Short"):
                return True
            if r.setup_name == "DD Exhaustion" and al != 0:
                return True
            return False

    live_all = [r for r in rows if passes_v10(r)]
    live_sc = [r for r in live_all if r.setup_name == "Skew Charm"]

    def simulate(trades, label):
        open_longs = []
        open_shorts = []
        daily = defaultdict(lambda: {
            "trades": 0, "wins": 0, "losses": 0, "expired": 0,
            "pnl_pts": 0, "pnl_usd": 0, "skipped_cap": 0, "details": [],
            "by_setup": defaultdict(lambda: {"n": 0, "pnl": 0, "w": 0, "l": 0})
        })
        all_trades = []
        total_skipped = 0

        for r in trades:
            d = r.trade_date
            day = daily[d]
            is_long = r.direction in ("long", "bullish")

            if is_long:
                if len(open_longs) >= CAP:
                    day["skipped_cap"] += 1
                    total_skipped += 1
                    continue
                open_longs.append(r)
            else:
                if len(open_shorts) >= CAP:
                    day["skipped_cap"] += 1
                    total_skipped += 1
                    continue
                open_shorts.append(r)

            p = r.outcome_pnl or 0
            usd = p * MES_PER_PT * QTY
            day["trades"] += 1
            day["pnl_pts"] += p
            day["pnl_usd"] += usd

            s = day["by_setup"][r.setup_name]
            s["n"] += 1
            s["pnl"] += usd

            if r.outcome_result == "WIN":
                day["wins"] += 1
                s["w"] += 1
            elif r.outcome_result == "LOSS":
                day["losses"] += 1
                s["l"] += 1
            else:
                day["expired"] += 1

            all_trades.append({"r": r, "pnl_pts": p, "pnl_usd": usd})

            if is_long:
                open_longs = [x for x in open_longs if x.id != r.id]
            else:
                open_shorts = [x for x in open_shorts if x.id != r.id]

        # Print
        print(f"\n{'='*120}")
        print(f"{label}")
        print(f"{'='*120}")

        running_usd = 0
        peak_usd = 0
        max_dd = 0
        losing_days = 0
        winning_days = 0

        print(f"\n{'Date':>12s} | {'#':>3s} | {'W/L/E':>7s} | {'PnL $':>8s} | {'Run $':>8s} | {'Skip':>4s} | Setup breakdown")
        print("-" * 120)

        for d in sorted(daily.keys()):
            day = daily[d]
            if day["trades"] == 0 and day["skipped_cap"] == 0:
                continue
            running_usd += day["pnl_usd"]
            if running_usd > peak_usd:
                peak_usd = running_usd
            dd = peak_usd - running_usd
            if dd > max_dd:
                max_dd = dd
            if day["pnl_usd"] < 0: losing_days += 1
            elif day["pnl_usd"] > 0: winning_days += 1

            wle = f"{day['wins']}W/{day['losses']}L/{day['expired']}E"
            skip = str(day["skipped_cap"]) if day["skipped_cap"] > 0 else ""
            # Setup breakdown
            parts = []
            for sn in sorted(day["by_setup"].keys()):
                s = day["by_setup"][sn]
                short_name = sn[:2] if sn != "Paradigm Reversal" else "PR"
                if sn == "ES Absorption": short_name = "EA"
                parts.append(f"{short_name}({s['n']}t,${s['pnl']:+.0f})")
            setup_str = " ".join(parts)
            print(f"  {d} | {day['trades']:3d} | {wle:>7s} | {day['pnl_usd']:+8.0f} | {running_usd:+8.0f} | {skip:>4s} | {setup_str}")

        print("-" * 120)

        w = sum(1 for t in all_trades if t["r"].outcome_result == "WIN")
        lo = sum(1 for t in all_trades if t["r"].outcome_result == "LOSS")
        ex = sum(1 for t in all_trades if t["r"].outcome_result == "EXPIRED")
        wr = round(w / (w + lo) * 100, 1) if (w + lo) > 0 else 0
        total_days = len([d for d in daily if daily[d]["trades"] > 0])
        gross_win = sum(t["pnl_usd"] for t in all_trades if t["pnl_usd"] > 0)
        gross_loss = abs(sum(t["pnl_usd"] for t in all_trades if t["pnl_usd"] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else float('inf')

        print(f"\n  Trades:       {len(all_trades)} ({w}W/{lo}L/{ex}E)")
        print(f"  Win rate:     {wr}%")
        print(f"  Total PnL:    {running_usd/MES_PER_PT:+.1f} pts / ${running_usd:+,.0f}")
        print(f"  Max DD:       ${max_dd:,.0f}")
        print(f"  Days:         {total_days} ({winning_days}W/{losing_days}L)")
        print(f"  Avg daily:    ${running_usd/total_days:+,.0f}")
        print(f"  PF:           {pf:.2f}x")
        print(f"  Skipped(cap): {total_skipped}")

        # Per-setup breakdown
        print(f"\n  --- By Setup ---")
        by_setup = defaultdict(lambda: {"n": 0, "pnl": 0, "w": 0, "l": 0, "e": 0})
        for t in all_trades:
            s = by_setup[t["r"].setup_name]
            s["n"] += 1
            s["pnl"] += t["pnl_usd"]
            if t["r"].outcome_result == "WIN": s["w"] += 1
            elif t["r"].outcome_result == "LOSS": s["l"] += 1
            else: s["e"] += 1
        for sn in sorted(by_setup.keys(), key=lambda x: by_setup[x]["pnl"], reverse=True):
            s = by_setup[sn]
            swr = round(s["w"]/(s["w"]+s["l"])*100,1) if s["w"]+s["l"]>0 else 0
            print(f"    {sn:20s}: {s['n']:3d}t  {s['w']}W/{s['l']}L/{s['e']}E  WR={swr:5.1f}%  ${s['pnl']:+,.0f}")

        return {
            "trades": len(all_trades), "wr": wr, "pnl_usd": running_usd,
            "max_dd": max_dd, "avg_daily": running_usd/total_days,
            "pf": pf, "skipped": total_skipped,
            "winning_days": winning_days, "losing_days": losing_days
        }

    # Run both simulations
    r_sc = simulate(live_sc, "SCENARIO A: SC-ONLY (current real trader config)")
    r_all = simulate(live_all, "SCENARIO B: ALL SETUPS (V10 filter)")

    # Side-by-side comparison
    print(f"\n{'='*120}")
    print(f"SIDE-BY-SIDE COMPARISON — March 2026, 1 MES, cap=2, V10")
    print(f"{'='*120}")
    print(f"  {'Metric':25s} | {'SC-Only':>15s} | {'All Setups':>15s} | {'Diff':>15s}")
    print(f"  {'-'*25}-+-{'-'*15}-+-{'-'*15}-+-{'-'*15}")
    print(f"  {'Trades':25s} | {r_sc['trades']:>15d} | {r_all['trades']:>15d} | {r_all['trades']-r_sc['trades']:>+15d}")
    print(f"  {'Win Rate':25s} | {r_sc['wr']:>14.1f}% | {r_all['wr']:>14.1f}% | {r_all['wr']-r_sc['wr']:>+14.1f}%")
    print(f"  {'Total PnL':25s} | ${r_sc['pnl_usd']:>14,.0f} | ${r_all['pnl_usd']:>14,.0f} | ${r_all['pnl_usd']-r_sc['pnl_usd']:>+14,.0f}")
    print(f"  {'Max Drawdown':25s} | ${r_sc['max_dd']:>14,.0f} | ${r_all['max_dd']:>14,.0f} | ${r_all['max_dd']-r_sc['max_dd']:>+14,.0f}")
    print(f"  {'Avg Daily PnL':25s} | ${r_sc['avg_daily']:>14,.0f} | ${r_all['avg_daily']:>14,.0f} | ${r_all['avg_daily']-r_sc['avg_daily']:>+14,.0f}")
    print(f"  {'Profit Factor':25s} | {r_sc['pf']:>14.2f}x | {r_all['pf']:>14.2f}x |")
    print(f"  {'Winning Days':25s} | {r_sc['winning_days']:>15d} | {r_all['winning_days']:>15d} |")
    print(f"  {'Losing Days':25s} | {r_sc['losing_days']:>15d} | {r_all['losing_days']:>15d} |")
    print(f"  {'Skipped (cap)':25s} | {r_sc['skipped']:>15d} | {r_all['skipped']:>15d} |")

    print(f"\n  --- Monthly Projection (21 days) ---")
    print(f"  {'1 MES':25s} | ${r_sc['avg_daily']*21:>14,.0f} | ${r_all['avg_daily']*21:>14,.0f} | ${(r_all['avg_daily']-r_sc['avg_daily'])*21:>+14,.0f}")
    print(f"  {'2 MES':25s} | ${r_sc['avg_daily']*21*2:>14,.0f} | ${r_all['avg_daily']*21*2:>14,.0f} | ${(r_all['avg_daily']-r_sc['avg_daily'])*21*2:>+14,.0f}")
