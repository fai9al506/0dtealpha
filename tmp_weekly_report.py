"""Weekly trading report: Feb 24-28, 2026"""
import sys, os, re, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import psycopg
from psycopg.rows import dict_row
from collections import defaultdict

DATABASE_URL = os.environ["DATABASE_URL"]

def extract_pattern(comments, abs_details):
    """Extract absorption pattern from comments or abs_details"""
    if comments:
        m = re.match(r'Pattern:\s*(.+?)(?:\s*\||$)', comments)
        if m:
            # Clean up tier annotation
            pat = m.group(1).strip()
            pat = re.sub(r'\s*\(T\d\)\s*', '', pat)
            return pat
    if abs_details and isinstance(abs_details, dict):
        return abs_details.get("pattern", "unknown")
    return "unknown"

def main():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)

    # -- 1. All trades for the week --
    rows = conn.execute("""
        SELECT id, setup_name, direction, score, grade, outcome_result, outcome_pnl,
               outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
               abs_es_price, abs_vol_ratio, abs_details,
               ts AT TIME ZONE 'America/New_York' as ts_et,
               ts::date as trade_date,
               comments
        FROM setup_log
        WHERE ts::date >= '2026-02-24' AND ts::date <= '2026-02-28'
        ORDER BY id
    """).fetchall()

    print("=" * 110)
    print(f"  WEEKLY TRADING REPORT: Feb 24-28, 2026  ({len(rows)} total setups fired)")
    print("=" * 110)

    # Resolved trades only for stats
    resolved = [r for r in rows if r["outcome_result"] is not None]
    unresolved = [r for r in rows if r["outcome_result"] is None]
    print(f"\n  Resolved: {len(resolved)}  |  Unresolved/Open: {len(unresolved)}")

    # -- 2. Daily breakdown --
    print("\n" + "=" * 110)
    print("  DAILY BREAKDOWN")
    print("=" * 110)

    by_day = defaultdict(list)
    for r in resolved:
        by_day[str(r["trade_date"])].append(r)

    cumulative_pnl = 0.0
    daily_cum = []
    for day in sorted(by_day.keys()):
        trades = by_day[day]
        wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
        losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
        expired = sum(1 for t in trades if t["outcome_result"] == "EXPIRED")
        pnl = sum(t["outcome_pnl"] or 0 for t in trades)
        cumulative_pnl += pnl
        daily_cum.append((day, pnl, cumulative_pnl))

        best = max(trades, key=lambda t: t["outcome_pnl"] or -9999)
        worst = min(trades, key=lambda t: t["outcome_pnl"] or 9999)
        wr = (wins / len(trades) * 100) if trades else 0

        print(f"\n  {day}  |  Trades: {len(trades)}  |  W: {wins}  L: {losses}  E: {expired}  |  WR: {wr:.0f}%  |  PnL: {pnl:+.1f} pts")
        print(f"    Best:  #{best['id']} {best['setup_name']} {best['outcome_result']} {(best['outcome_pnl'] or 0):+.1f} pts")
        print(f"    Worst: #{worst['id']} {worst['setup_name']} {worst['outcome_result']} {(worst['outcome_pnl'] or 0):+.1f} pts")

        # List all trades for the day
        for t in trades:
            pnl_str = f"{(t['outcome_pnl'] or 0):+.1f}"
            ts_str = t["ts_et"].strftime("%H:%M") if t["ts_et"] else "?"
            pat = ""
            if t["setup_name"] == "ES Absorption":
                pat = f" [{extract_pattern(t['comments'], t['abs_details'])}]"
            print(f"      #{t['id']:>4} {ts_str} {t['setup_name']:<20} {t['direction']:<5} {t['grade']:<5} {t['outcome_result']:<8} {pnl_str:>8} pts  maxP={t['outcome_max_profit'] or 0:.1f}  maxL={t['outcome_max_loss'] or 0:.1f}{pat}")

    # -- 3. Setup-by-setup breakdown --
    print("\n" + "=" * 110)
    print("  SETUP-BY-SETUP BREAKDOWN (Week)")
    print("=" * 110)

    by_setup = defaultdict(list)
    for r in resolved:
        by_setup[r["setup_name"]].append(r)

    for setup in sorted(by_setup.keys()):
        trades = by_setup[setup]
        wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
        losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
        expired = sum(1 for t in trades if t["outcome_result"] == "EXPIRED")
        total_pnl = sum(t["outcome_pnl"] or 0 for t in trades)
        avg_pnl = total_pnl / len(trades) if trades else 0
        wr = (wins / len(trades) * 100) if trades else 0

        gross_profit = sum(t["outcome_pnl"] or 0 for t in trades if (t["outcome_pnl"] or 0) > 0)
        gross_loss = abs(sum(t["outcome_pnl"] or 0 for t in trades if (t["outcome_pnl"] or 0) < 0))
        pf = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')

        best = max(trades, key=lambda t: t["outcome_pnl"] or -9999)
        worst = min(trades, key=lambda t: t["outcome_pnl"] or 9999)

        print(f"\n  {setup}")
        print(f"    Trades: {len(trades)}  |  W: {wins}  L: {losses}  E: {expired}  |  WR: {wr:.1f}%")
        print(f"    Total PnL: {total_pnl:+.1f} pts  |  Avg: {avg_pnl:+.1f} pts/trade  |  PF: {pf:.2f}x")
        print(f"    Gross Profit: {gross_profit:+.1f}  |  Gross Loss: {gross_loss:.1f}")
        print(f"    Best:  #{best['id']} {best['outcome_result']} {(best['outcome_pnl'] or 0):+.1f} pts")
        print(f"    Worst: #{worst['id']} {worst['outcome_result']} {(worst['outcome_pnl'] or 0):+.1f} pts")

    # -- 4. ES Absorption pattern breakdown --
    print("\n" + "=" * 110)
    print("  ES ABSORPTION - PATTERN BREAKDOWN")
    print("=" * 110)

    abs_trades = [r for r in resolved if r["setup_name"] == "ES Absorption"]
    by_pattern = defaultdict(list)
    for t in abs_trades:
        pat = extract_pattern(t["comments"], t["abs_details"])
        by_pattern[pat].append(t)

    if not abs_trades:
        print("\n  No ES Absorption trades this week.")
    else:
        for pat in sorted(by_pattern.keys()):
            trades = by_pattern[pat]
            wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
            losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
            expired = sum(1 for t in trades if t["outcome_result"] == "EXPIRED")
            pnl = sum(t["outcome_pnl"] or 0 for t in trades)
            wr = (wins / len(trades) * 100) if trades else 0
            print(f"\n  {pat}")
            print(f"    Trades: {len(trades)}  |  W: {wins}  L: {losses}  E: {expired}  |  WR: {wr:.0f}%  |  PnL: {pnl:+.1f} pts")
            for t in trades:
                ts_str = t["ts_et"].strftime("%Y-%m-%d %H:%M") if t["ts_et"] else "?"
                print(f"      #{t['id']:>4} {ts_str} {t['direction']:<5} {t['outcome_result']:<8} {(t['outcome_pnl'] or 0):+.1f} pts  vol_ratio={t['abs_vol_ratio'] or 0:.2f}  maxP={t['outcome_max_profit'] or 0:.1f}")

    # -- 5. Direction breakdown per setup --
    print("\n" + "=" * 110)
    print("  DIRECTION BREAKDOWN PER SETUP")
    print("=" * 110)

    for setup in sorted(by_setup.keys()):
        trades = by_setup[setup]
        by_dir = defaultdict(list)
        for t in trades:
            by_dir[t["direction"] or "?"].append(t)

        print(f"\n  {setup}:")
        for direction in sorted(by_dir.keys()):
            dt = by_dir[direction]
            wins = sum(1 for t in dt if t["outcome_result"] == "WIN")
            losses = sum(1 for t in dt if t["outcome_result"] == "LOSS")
            pnl = sum(t["outcome_pnl"] or 0 for t in dt)
            wr = (wins / len(dt) * 100) if dt else 0
            print(f"    {direction:<8}  Trades: {len(dt):>3}  |  W: {wins}  L: {losses}  WR: {wr:.0f}%  |  PnL: {pnl:+.1f} pts")

    # -- 6. Hourly heatmap --
    print("\n" + "=" * 110)
    print("  HOURLY HEATMAP (PnL by hour, ET)")
    print("=" * 110)

    by_hour = defaultdict(list)
    for r in resolved:
        if r["ts_et"]:
            h = r["ts_et"].hour
            by_hour[h].append(r)

    print(f"\n  {'Hour':<8} {'Trades':>7} {'Wins':>6} {'Losses':>7} {'WR':>6} {'PnL':>10} {'Avg':>8}  Visual")
    print("  " + "-" * 70)
    for h in range(9, 17):
        trades = by_hour.get(h, [])
        if not trades:
            print(f"  {h:02d}:00    {0:>7} {0:>6} {0:>7} {'--':>6} {0:>+10.1f} {'--':>8}")
            continue
        wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
        losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
        pnl = sum(t["outcome_pnl"] or 0 for t in trades)
        wr = (wins / len(trades) * 100) if trades else 0
        avg = pnl / len(trades) if trades else 0
        # Visual bar
        if pnl >= 0:
            bar = "+" * min(40, int(pnl / 3))
        else:
            bar = "-" * min(40, int(abs(pnl) / 3))
        print(f"  {h:02d}:00    {len(trades):>7} {wins:>6} {losses:>7} {wr:>5.0f}% {pnl:>+10.1f} {avg:>+7.1f}  {bar}")

    # -- 7. Cumulative PnL progression --
    print("\n" + "=" * 110)
    print("  CUMULATIVE PnL PROGRESSION")
    print("=" * 110)

    for day, daily_pnl, cum_pnl in daily_cum:
        bar_len = int(cum_pnl / 3)
        if bar_len >= 0:
            bar = "|" + "=" * min(60, bar_len)
        else:
            bar = "|" + "X" * min(60, abs(bar_len))
        print(f"  {day}  Daily: {daily_pnl:>+8.1f}  Cumulative: {cum_pnl:>+8.1f}  {bar}")

    week_total = sum(t["outcome_pnl"] or 0 for t in resolved)
    week_wins = sum(1 for t in resolved if t["outcome_result"] == "WIN")
    week_losses = sum(1 for t in resolved if t["outcome_result"] == "LOSS")
    week_expired = sum(1 for t in resolved if t["outcome_result"] == "EXPIRED")
    week_wr = (week_wins / len(resolved) * 100) if resolved else 0

    gp = sum(t["outcome_pnl"] or 0 for t in resolved if (t["outcome_pnl"] or 0) > 0)
    gl = abs(sum(t["outcome_pnl"] or 0 for t in resolved if (t["outcome_pnl"] or 0) < 0))
    wpf = (gp / gl) if gl > 0 else float('inf')

    print(f"\n  WEEK TOTAL: {week_total:+.1f} pts  |  {len(resolved)} trades  |  W: {week_wins}  L: {week_losses}  E: {week_expired}  |  WR: {week_wr:.1f}%  |  PF: {wpf:.2f}x")
    print(f"  Gross Profit: {gp:+.1f}  |  Gross Loss: {gl:.1f}")

    # -- 8. All-time totals --
    print("\n" + "=" * 110)
    print("  ALL-TIME TOTALS")
    print("=" * 110)

    alltime = conn.execute("""
        SELECT setup_name,
               COUNT(*) as total,
               COUNT(*) FILTER (WHERE outcome_result = 'WIN') as wins,
               COUNT(*) FILTER (WHERE outcome_result = 'LOSS') as losses,
               COUNT(*) FILTER (WHERE outcome_result = 'EXPIRED') as expired,
               COALESCE(SUM(outcome_pnl), 0) as total_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name
        ORDER BY total_pnl DESC
    """).fetchall()

    grand_total = 0.0
    grand_trades = 0
    grand_wins = 0
    grand_losses = 0
    grand_expired = 0

    print(f"\n  {'Setup':<24} {'Total':>6} {'W':>4} {'L':>4} {'E':>4} {'WR':>7} {'PnL':>11} {'Avg/Trade':>10}")
    print("  " + "-" * 78)
    for r in alltime:
        wr = (r["wins"] / r["total"] * 100) if r["total"] else 0
        avg = float(r["total_pnl"]) / r["total"] if r["total"] else 0
        print(f"  {r['setup_name']:<24} {r['total']:>6} {r['wins']:>4} {r['losses']:>4} {r['expired']:>4} {wr:>6.1f}% {r['total_pnl']:>+11.1f} {avg:>+10.1f}")
        grand_total += float(r["total_pnl"])
        grand_trades += r["total"]
        grand_wins += r["wins"]
        grand_losses += r["losses"]
        grand_expired += r["expired"]

    grand_wr = (grand_wins / grand_trades * 100) if grand_trades else 0
    grand_avg = grand_total / grand_trades if grand_trades else 0
    print("  " + "-" * 78)
    print(f"  {'GRAND TOTAL':<24} {grand_trades:>6} {grand_wins:>4} {grand_losses:>4} {grand_expired:>4} {grand_wr:>6.1f}% {grand_total:>+11.1f} {grand_avg:>+10.1f}")

    # All-time by date range
    date_range = conn.execute("""
        SELECT MIN(ts::date) as first_trade, MAX(ts::date) as last_trade,
               COUNT(DISTINCT ts::date) as trading_days
        FROM setup_log WHERE outcome_result IS NOT NULL
    """).fetchone()
    if date_range:
        print(f"\n  Date range: {date_range['first_trade']} to {date_range['last_trade']} ({date_range['trading_days']} trading days)")
        print(f"  Avg PnL/day: {grand_total / date_range['trading_days']:+.1f} pts")

    # -- 9. GEX Long vanna filter data --
    print("\n" + "=" * 110)
    print("  GEX LONG - VANNA FILTER DATA (Week)")
    print("=" * 110)

    vanna_rows = conn.execute("""
        SELECT sl.id, sl.ts::date as trade_date, sl.setup_name, sl.outcome_result, sl.outcome_pnl,
               (SELECT SUM(vep.value)
                FROM volland_exposure_points vep
                WHERE vep.greek = 'vanna'
                  AND vep.expiration_option = 'ALL'
                  AND vep.ts_utc = (SELECT MAX(v2.ts_utc) FROM volland_exposure_points v2
                                    WHERE v2.ts_utc <= sl.ts AND v2.greek = 'vanna' AND v2.expiration_option = 'ALL')
               ) as vanna_sum
        FROM setup_log sl
        WHERE sl.ts::date >= '2026-02-24' AND sl.ts::date <= '2026-02-28'
          AND sl.setup_name = 'GEX Long'
          AND sl.outcome_result IS NOT NULL
        ORDER BY sl.id
    """).fetchall()

    if not vanna_rows:
        print("\n  No GEX Long trades this week.")
    else:
        print(f"\n  {'ID':>5} {'Date':<12} {'Result':<8} {'PnL':>8} {'Vanna Sum':>14} {'Sign':>6}")
        print("  " + "-" * 60)
        pos_vanna_pnl = 0.0
        neg_vanna_pnl = 0.0
        pos_count = 0
        neg_count = 0
        pos_wins = 0
        neg_wins = 0
        for r in vanna_rows:
            vs = r["vanna_sum"]
            vanna_sign = "POS" if (vs and vs > 0) else "NEG" if (vs and vs < 0) else "?"
            print(f"  {r['id']:>5} {str(r['trade_date']):<12} {r['outcome_result']:<8} {(r['outcome_pnl'] or 0):>+8.1f} {(vs or 0):>14.1f} {vanna_sign:>6}")
            if vs and vs > 0:
                pos_vanna_pnl += (r["outcome_pnl"] or 0)
                pos_count += 1
                if r["outcome_result"] == "WIN":
                    pos_wins += 1
            elif vs and vs < 0:
                neg_vanna_pnl += (r["outcome_pnl"] or 0)
                neg_count += 1
                if r["outcome_result"] == "WIN":
                    neg_wins += 1

        print(f"\n  Vanna > 0:  {pos_count} trades, {pos_wins} wins ({(pos_wins/pos_count*100) if pos_count else 0:.0f}% WR), PnL: {pos_vanna_pnl:+.1f}")
        print(f"  Vanna < 0:  {neg_count} trades, {neg_wins} wins ({(neg_wins/neg_count*100) if neg_count else 0:.0f}% WR), PnL: {neg_vanna_pnl:+.1f}")
        blocked = neg_vanna_pnl
        print(f"  Filter would save: {abs(blocked):.1f} pts (blocking neg-vanna trades)")

    # -- 10. Top 10 best and worst trades of the week --
    print("\n" + "=" * 110)
    print("  TOP 10 BEST / WORST TRADES OF THE WEEK")
    print("=" * 110)

    sorted_by_pnl = sorted(resolved, key=lambda t: t["outcome_pnl"] or 0, reverse=True)

    print("\n  BEST:")
    for t in sorted_by_pnl[:10]:
        ts_str = t["ts_et"].strftime("%m/%d %H:%M") if t["ts_et"] else "?"
        pat = ""
        if t["setup_name"] == "ES Absorption":
            pat = f" [{extract_pattern(t['comments'], t['abs_details'])}]"
        print(f"    #{t['id']:>4} {ts_str} {t['setup_name']:<20} {t['direction']:<6} {t['grade']:<5} {(t['outcome_pnl'] or 0):>+8.1f} pts  elapsed={t['outcome_elapsed_min'] or 0:.0f}min  maxP={t['outcome_max_profit'] or 0:.1f}{pat}")

    print("\n  WORST:")
    for t in sorted_by_pnl[-10:]:
        ts_str = t["ts_et"].strftime("%m/%d %H:%M") if t["ts_et"] else "?"
        pat = ""
        if t["setup_name"] == "ES Absorption":
            pat = f" [{extract_pattern(t['comments'], t['abs_details'])}]"
        print(f"    #{t['id']:>4} {ts_str} {t['setup_name']:<20} {t['direction']:<6} {t['grade']:<5} {(t['outcome_pnl'] or 0):>+8.1f} pts  elapsed={t['outcome_elapsed_min'] or 0:.0f}min  maxL={t['outcome_max_loss'] or 0:.1f}{pat}")

    conn.close()
    print("\n" + "=" * 110)
    print("  Report complete.")
    print("=" * 110)

if __name__ == "__main__":
    main()
