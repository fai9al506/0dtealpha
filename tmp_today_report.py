"""Feb 27, 2026 (last trading day) — Daily trading report"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import psycopg
from psycopg.rows import dict_row
from collections import defaultdict

DATABASE_URL = os.environ["DATABASE_URL"]

def main():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)

    # ── 1. All trades today ──────────────────────────────────────────
    rows = conn.execute("""
        SELECT id, setup_name, direction, score, grade, outcome_result, outcome_pnl,
               outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
               abs_es_price, abs_vol_ratio, abs_details,
               ts AT TIME ZONE 'America/New_York' as ts_et,
               comments, spot, paradigm, lis, target, vix
        FROM setup_log
        WHERE ts::date = '2026-02-27'
        ORDER BY id
    """).fetchall()

    print("=" * 120)
    print(f"  DAILY TRADING REPORT — Feb 27, 2026 (last trading day)  ({len(rows)} trades)")
    print("=" * 120)

    if not rows:
        print("\nNo trades found for today.")
    else:
        for r in rows:
            print(f"\n--- Trade #{r['id']} ---")
            print(f"  Time (ET):    {r['ts_et']}")
            print(f"  Setup:        {r['setup_name']}")
            print(f"  Direction:    {r['direction']}")
            print(f"  Grade:        {r['grade']}   Score: {r['score']}")
            print(f"  Outcome:      {r['outcome_result']}")
            print(f"  PnL:          {r['outcome_pnl']}")
            print(f"  Elapsed:      {r['outcome_elapsed_min']} min")
            print(f"  Max Profit:   {r['outcome_max_profit']}")
            print(f"  Max Loss:     {r['outcome_max_loss']}")
            if r['spot']:
                print(f"  SPX Spot:     {r['spot']}")
            if r['paradigm']:
                print(f"  Paradigm:     {r['paradigm']}")
            if r['lis']:
                print(f"  LIS:          {r['lis']}")
            if r['target']:
                print(f"  Target:       {r['target']}")
            if r['vix']:
                print(f"  VIX:          {r['vix']}")
            if r['abs_es_price']:
                print(f"  ES Price:     {r['abs_es_price']}")
            if r['abs_vol_ratio']:
                print(f"  Vol Ratio:    {r['abs_vol_ratio']}")
            # Extract pattern from abs_details JSONB
            abs_det = r.get('abs_details') or {}
            if isinstance(abs_det, dict) and abs_det.get('pattern'):
                print(f"  Pattern:      {abs_det['pattern']}")
            if r['comments']:
                cmt = r['comments']
                if len(cmt) > 200:
                    cmt = cmt[:200] + "..."
                print(f"  Comments:     {cmt}")

    # ── 2. Summary by setup ──────────────────────────────────────────
    print("\n" + "=" * 120)
    print("  SUMMARY BY SETUP")
    print("=" * 120)

    by_setup = defaultdict(lambda: {"count": 0, "wins": 0, "losses": 0, "expired": 0, "open": 0, "pnl": 0.0})
    for r in rows:
        s = by_setup[r['setup_name']]
        s['count'] += 1
        res = (r['outcome_result'] or '').upper()
        if 'WIN' in res:
            s['wins'] += 1
        elif 'LOSS' in res:
            s['losses'] += 1
        elif 'EXPIRED' in res:
            s['expired'] += 1
        else:
            s['open'] += 1
        s['pnl'] += float(r['outcome_pnl'] or 0)

    print(f"\n{'Setup':<25} {'Count':>6} {'Wins':>6} {'Losses':>6} {'Expired':>8} {'Open':>6} {'PnL':>10} {'WR':>8}")
    print("-" * 85)
    total_count = total_wins = total_losses = total_expired = total_open = 0
    total_pnl = 0.0
    for name in sorted(by_setup.keys()):
        s = by_setup[name]
        decided = s['wins'] + s['losses']
        wr = f"{s['wins']/decided*100:.1f}%" if decided > 0 else "N/A"
        print(f"{name:<25} {s['count']:>6} {s['wins']:>6} {s['losses']:>6} {s['expired']:>8} {s['open']:>6} {s['pnl']:>+10.1f} {wr:>8}")
        total_count += s['count']
        total_wins += s['wins']
        total_losses += s['losses']
        total_expired += s['expired']
        total_open += s['open']
        total_pnl += s['pnl']
    print("-" * 85)
    total_decided = total_wins + total_losses
    total_wr = f"{total_wins/total_decided*100:.1f}%" if total_decided > 0 else "N/A"
    print(f"{'TOTAL':<25} {total_count:>6} {total_wins:>6} {total_losses:>6} {total_expired:>8} {total_open:>6} {total_pnl:>+10.1f} {total_wr:>8}")

    # ── 3. Hourly breakdown ──────────────────────────────────────────
    print("\n" + "=" * 120)
    print("  HOURLY BREAKDOWN")
    print("=" * 120)

    by_hour = defaultdict(lambda: {"count": 0, "wins": 0, "losses": 0, "pnl": 0.0})
    for r in rows:
        if r['ts_et']:
            h = r['ts_et'].hour
            hh = by_hour[h]
            hh['count'] += 1
            res = (r['outcome_result'] or '').upper()
            if 'WIN' in res:
                hh['wins'] += 1
            elif 'LOSS' in res:
                hh['losses'] += 1
            hh['pnl'] += float(r['outcome_pnl'] or 0)

    print(f"\n{'Hour (ET)':<12} {'Count':>6} {'Wins':>6} {'Losses':>6} {'PnL':>10} {'WR':>8}")
    print("-" * 55)
    for h in sorted(by_hour.keys()):
        hh = by_hour[h]
        decided = hh['wins'] + hh['losses']
        wr = f"{hh['wins']/decided*100:.1f}%" if decided > 0 else "N/A"
        print(f"{h:02d}:00        {hh['count']:>6} {hh['wins']:>6} {hh['losses']:>6} {hh['pnl']:>+10.1f} {wr:>8}")

    # ── 4. Direction breakdown ───────────────────────────────────────
    print("\n" + "=" * 120)
    print("  DIRECTION BREAKDOWN")
    print("=" * 120)

    by_dir = defaultdict(lambda: {"count": 0, "wins": 0, "losses": 0, "expired": 0, "open": 0, "pnl": 0.0})
    for r in rows:
        d = (r['direction'] or 'unknown').upper()
        dd = by_dir[d]
        dd['count'] += 1
        res = (r['outcome_result'] or '').upper()
        if 'WIN' in res:
            dd['wins'] += 1
        elif 'LOSS' in res:
            dd['losses'] += 1
        elif 'EXPIRED' in res:
            dd['expired'] += 1
        else:
            dd['open'] += 1
        dd['pnl'] += float(r['outcome_pnl'] or 0)

    print(f"\n{'Direction':<12} {'Count':>6} {'Wins':>6} {'Losses':>6} {'Expired':>8} {'Open':>6} {'PnL':>10} {'WR':>8}")
    print("-" * 70)
    for d in sorted(by_dir.keys()):
        dd = by_dir[d]
        decided = dd['wins'] + dd['losses']
        wr = f"{dd['wins']/decided*100:.1f}%" if decided > 0 else "N/A"
        print(f"{d:<12} {dd['count']:>6} {dd['wins']:>6} {dd['losses']:>6} {dd['expired']:>8} {dd['open']:>6} {dd['pnl']:>+10.1f} {wr:>8}")

    # ── 5. ES Range Bars context ─────────────────────────────────────
    print("\n" + "=" * 120)
    print("  ES RANGE BARS — Market Context")
    print("=" * 120)

    es = conn.execute("""
        SELECT COUNT(*) as bar_count,
               MIN(bar_open) as day_low_open,
               MAX(bar_high) as day_high,
               MIN(bar_low) as day_low
        FROM es_range_bars
        WHERE trade_date = '2026-02-27' AND source = 'rithmic' AND status = 'closed'
    """).fetchone()

    if es and es['bar_count'] and es['bar_count'] > 0:
        print(f"\n  Bars (Rithmic):  {es['bar_count']}")
        print(f"  Day High:        {es['day_high']}")
        print(f"  Day Low:         {es['day_low']}")
        rng = float(es['day_high']) - float(es['day_low']) if es['day_high'] and es['day_low'] else 0
        print(f"  Range:           {rng:.2f} pts")
    else:
        # Fallback to live source
        es2 = conn.execute("""
            SELECT COUNT(*) as bar_count,
                   MIN(bar_open) as day_low_open,
                   MAX(bar_high) as day_high,
                   MIN(bar_low) as day_low
            FROM es_range_bars
            WHERE trade_date = '2026-02-27' AND source = 'live' AND status = 'closed'
        """).fetchone()
        if es2 and es2['bar_count'] and es2['bar_count'] > 0:
            print(f"\n  Bars (TS live):  {es2['bar_count']}")
            print(f"  Day High:        {es2['day_high']}")
            print(f"  Day Low:         {es2['day_low']}")
            rng = float(es2['day_high']) - float(es2['day_low']) if es2['day_high'] and es2['day_low'] else 0
            print(f"  Range:           {rng:.2f} pts")
        else:
            print("\n  No ES range bar data found for today.")

    # ── 6. Overall daily summary ─────────────────────────────────────
    print("\n" + "=" * 120)
    print("  OVERALL DAILY SUMMARY")
    print("=" * 120)

    print(f"\n  Total Trades:     {total_count}")
    print(f"  Wins:             {total_wins}")
    print(f"  Losses:           {total_losses}")
    print(f"  Expired:          {total_expired}")
    print(f"  Open:             {total_open}")
    print(f"  Net PnL:          {total_pnl:+.1f} pts")
    if total_decided > 0:
        print(f"  Win Rate:         {total_wins/total_decided*100:.1f}%")
        avg_win = sum(float(r['outcome_pnl'] or 0) for r in rows if 'WIN' in (r['outcome_result'] or '').upper()) / total_wins if total_wins > 0 else 0
        avg_loss = sum(float(r['outcome_pnl'] or 0) for r in rows if 'LOSS' in (r['outcome_result'] or '').upper()) / total_losses if total_losses > 0 else 0
        print(f"  Avg Win:          {avg_win:+.1f} pts")
        print(f"  Avg Loss:         {avg_loss:+.1f} pts")
        if avg_loss != 0:
            print(f"  Profit Factor:    {abs(avg_win * total_wins / (avg_loss * total_losses)):.2f}x")

    # ── 7. All-time running total ────────────────────────────────────
    alltime = conn.execute("""
        SELECT COUNT(*) as cnt,
               SUM(CASE WHEN outcome_result ILIKE '%WIN%' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result ILIKE '%LOSS%' THEN 1 ELSE 0 END) as losses,
               SUM(COALESCE(outcome_pnl, 0)) as total_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
    """).fetchone()

    if alltime and alltime['cnt']:
        print(f"\n  --- All-Time (cumulative) ---")
        print(f"  Total Resolved:   {alltime['cnt']}")
        print(f"  All-Time PnL:     {float(alltime['total_pnl'] or 0):+.1f} pts")

    print("\n" + "=" * 120)
    conn.close()

if __name__ == "__main__":
    main()
