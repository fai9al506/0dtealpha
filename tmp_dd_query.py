"""
Query DD Exhaustion trades from setup_log with nearest Volland snapshot context.
Temporary analysis script.
"""
import os
import psycopg2
import psycopg2.extras
import json
from datetime import timedelta

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. Get all DD Exhaustion trades
    cur.execute("""
        SELECT
            id, ts, setup_name, direction, grade, score,
            paradigm, spot, lis, target,
            max_plus_gex, max_minus_gex,
            gap_to_lis, upside, rr_ratio,
            first_hour,
            support_score,      -- DD shift magnitude (0-30)
            upside_score,       -- charm strength (0-25)
            floor_cluster_score, -- time-of-day (0-15)
            target_cluster_score, -- paradigm context (0-15)
            rr_score,           -- direction bonus (0-15)
            bofa_stop_level, bofa_target_level,
            abs_vol_ratio, abs_es_price,
            comments,
            outcome_result, outcome_pnl,
            outcome_target_level, outcome_stop_level,
            outcome_max_profit, outcome_max_loss,
            outcome_first_event, outcome_elapsed_min
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion'
        ORDER BY ts ASC
    """)
    dd_trades = cur.fetchall()
    print(f"\n{'='*120}")
    print(f"DD EXHAUSTION TRADES — Total: {len(dd_trades)}")
    print(f"{'='*120}\n")

    if not dd_trades:
        print("No DD Exhaustion trades found.")
        conn.close()
        return

    # 2. For each DD trade, get nearest volland_snapshots within 5 min before
    all_results = []
    for i, trade in enumerate(dd_trades):
        trade_ts = trade['ts']

        # Get nearest Volland snapshot within 5 minutes before the signal
        cur.execute("""
            SELECT ts, payload
            FROM volland_snapshots
            WHERE ts <= %s
              AND ts >= %s
              AND payload->>'error_event' IS NULL
              AND payload->'statistics' IS NOT NULL
            ORDER BY ts DESC
            LIMIT 1
        """, (trade_ts, trade_ts - timedelta(minutes=5)))

        vol_row = cur.fetchone()
        vol_payload = vol_row['payload'] if vol_row else {}
        vol_ts = vol_row['ts'] if vol_row else None

        if isinstance(vol_payload, str):
            vol_payload = json.loads(vol_payload)

        stats = vol_payload.get('statistics', {}) if vol_payload else {}
        spot_vol = vol_payload.get('spot_vol_beta', {}) if vol_payload else {}
        exp_summary = vol_payload.get('exposure_summary', {}) if vol_payload else {}

        result = {
            # Trade info
            'trade_num': i + 1,
            'id': trade['id'],
            'ts': str(trade['ts']),
            'direction': trade['direction'],
            'grade': trade['grade'],
            'score': trade['score'],
            'spot': trade['spot'],
            'paradigm': trade['paradigm'],

            # Sub-scores (repurposed columns)
            'dd_shift_score': trade['support_score'],       # 0-30
            'charm_score': trade['upside_score'],           # 0-25
            'time_score': trade['floor_cluster_score'],     # 0-15
            'paradigm_score': trade['target_cluster_score'], # 0-15
            'direction_score': trade['rr_score'],           # 0-15

            # Outcome
            'outcome': trade['outcome_result'],
            'pnl': trade['outcome_pnl'],
            'max_profit': trade['outcome_max_profit'],
            'max_loss': trade['outcome_max_loss'],
            'target_level': trade['outcome_target_level'],
            'stop_level': trade['outcome_stop_level'],
            'first_event': trade['outcome_first_event'],
            'elapsed_min': trade['outcome_elapsed_min'],

            # Volland context
            'vol_ts': str(vol_ts) if vol_ts else 'N/A',
            'vol_paradigm': stats.get('paradigm'),
            'vol_target': stats.get('target'),
            'vol_lis': stats.get('lis'),
            'vol_charm': stats.get('aggregatedCharm'),
            'vol_dd_hedging': stats.get('deltadecayHedging') or stats.get('delta_decay_hedging'),
            'vol_dd_aggregated': stats.get('aggregatedDeltaDecay'),
            'vol_opt_volume': stats.get('totalZeroDteOptionVolume'),
            'vol_spot_vol_corr': spot_vol.get('correlation'),
            'vol_current_price': vol_payload.get('current_price'),
            'vol_exposure_pts': vol_payload.get('exposure_points_saved'),
        }
        all_results.append(result)

    # Print detailed results
    for r in all_results:
        print(f"--- Trade #{r['trade_num']} (id={r['id']}) ---")
        print(f"  Time:        {r['ts']}")
        print(f"  Direction:   {r['direction']}")
        print(f"  Grade:       {r['grade']}  Score: {r['score']}")
        print(f"  Entry (SPX): {r['spot']}")
        print(f"  Paradigm:    {r['paradigm']}")
        print()
        print(f"  Sub-scores:  shift={r['dd_shift_score']}  charm={r['charm_score']}  "
              f"time={r['time_score']}  paradigm={r['paradigm_score']}  dir={r['direction_score']}")
        print()
        print(f"  Outcome:     {r['outcome']}")
        print(f"  P&L:         {r['pnl']} pts")
        print(f"  Max Profit:  {r['max_profit']}")
        print(f"  Max Loss:    {r['max_loss']}")
        print(f"  Target Lvl:  {r['target_level']}")
        print(f"  Stop Lvl:    {r['stop_level']}")
        print(f"  First Event: {r['first_event']}")
        print(f"  Elapsed:     {r['elapsed_min']} min")
        print()
        print(f"  -- Volland Context (snapshot {r['vol_ts']}) --")
        print(f"  Vol Paradigm:    {r['vol_paradigm']}")
        print(f"  Vol Target:      {r['vol_target']}")
        print(f"  Vol LIS:         {r['vol_lis']}")
        print(f"  Vol Charm:       {r['vol_charm']}")
        print(f"  Vol DD Hedging:  {r['vol_dd_hedging']}")
        print(f"  Vol DD Aggreg:   {r['vol_dd_aggregated']}")
        print(f"  Vol Opt Volume:  {r['vol_opt_volume']}")
        print(f"  Spot-Vol Corr:   {r['vol_spot_vol_corr']}")
        print(f"  Current Price:   {r['vol_current_price']}")
        print(f"  Exposure Points: {r['vol_exposure_pts']}")
        print()

    # Summary statistics
    print(f"\n{'='*120}")
    print("SUMMARY STATISTICS")
    print(f"{'='*120}")

    wins = [r for r in all_results if r['outcome'] == 'WIN']
    losses = [r for r in all_results if r['outcome'] == 'LOSS']
    expired = [r for r in all_results if r['outcome'] == 'EXPIRED']
    pending = [r for r in all_results if r['outcome'] is None]

    total_pnl = sum(r['pnl'] for r in all_results if r['pnl'] is not None)

    print(f"  Total trades: {len(all_results)}")
    print(f"  Wins:    {len(wins)}  ({100*len(wins)/len(all_results):.1f}%)")
    print(f"  Losses:  {len(losses)}  ({100*len(losses)/len(all_results):.1f}%)")
    print(f"  Expired: {len(expired)}")
    print(f"  Pending: {len(pending)}")
    print(f"  Total P&L: {total_pnl:.1f} pts")
    print()

    # By direction
    longs = [r for r in all_results if r['direction'] == 'long']
    shorts = [r for r in all_results if r['direction'] == 'short']

    for label, subset in [("LONG", longs), ("SHORT", shorts)]:
        if not subset:
            continue
        s_wins = [r for r in subset if r['outcome'] == 'WIN']
        s_pnl = sum(r['pnl'] for r in subset if r['pnl'] is not None)
        print(f"  {label}: {len(subset)} trades, {len(s_wins)} wins ({100*len(s_wins)/len(subset) if subset else 0:.1f}%), P&L={s_pnl:.1f} pts")

    # By grade
    print()
    grades = set(r['grade'] for r in all_results)
    for g in sorted(grades):
        g_trades = [r for r in all_results if r['grade'] == g]
        g_wins = [r for r in g_trades if r['outcome'] == 'WIN']
        g_pnl = sum(r['pnl'] for r in g_trades if r['pnl'] is not None)
        wr = 100*len(g_wins)/len(g_trades) if g_trades else 0
        print(f"  Grade {g}: {len(g_trades)} trades, {len(g_wins)} wins ({wr:.1f}%), P&L={g_pnl:.1f} pts")

    # By paradigm
    print()
    paradigms = set(r['paradigm'] for r in all_results if r['paradigm'])
    for p in sorted(paradigms):
        p_trades = [r for r in all_results if r['paradigm'] == p]
        p_wins = [r for r in p_trades if r['outcome'] == 'WIN']
        p_pnl = sum(r['pnl'] for r in p_trades if r['pnl'] is not None)
        wr = 100*len(p_wins)/len(p_trades) if p_trades else 0
        print(f"  Paradigm {p}: {len(p_trades)} trades, {len(p_wins)} wins ({wr:.1f}%), P&L={p_pnl:.1f} pts")

    # Avg max profit and max loss
    print()
    max_profits = [r['max_profit'] for r in all_results if r['max_profit'] is not None]
    max_losses = [r['max_loss'] for r in all_results if r['max_loss'] is not None]
    if max_profits:
        print(f"  Avg Max Profit: {sum(max_profits)/len(max_profits):.1f} pts (max: {max(max_profits):.1f})")
    if max_losses:
        print(f"  Avg Max Loss:   {sum(max_losses)/len(max_losses):.1f} pts (max: {min(max_losses):.1f})")

    # P&L by sub-score ranges
    print()
    print("  -- P&L by DD Shift Score --")
    for score_val in sorted(set(r['dd_shift_score'] for r in all_results if r['dd_shift_score'] is not None)):
        s_trades = [r for r in all_results if r['dd_shift_score'] == score_val]
        s_pnl = sum(r['pnl'] for r in s_trades if r['pnl'] is not None)
        s_wins = len([r for r in s_trades if r['outcome'] == 'WIN'])
        print(f"    Shift={score_val}: {len(s_trades)} trades, {s_wins} wins, P&L={s_pnl:.1f}")

    print()
    print("  -- P&L by Charm Score --")
    for score_val in sorted(set(r['charm_score'] for r in all_results if r['charm_score'] is not None)):
        s_trades = [r for r in all_results if r['charm_score'] == score_val]
        s_pnl = sum(r['pnl'] for r in s_trades if r['pnl'] is not None)
        s_wins = len([r for r in s_trades if r['outcome'] == 'WIN'])
        print(f"    Charm={score_val}: {len(s_trades)} trades, {s_wins} wins, P&L={s_pnl:.1f}")

    print()
    print("  -- P&L by Time Score --")
    for score_val in sorted(set(r['time_score'] for r in all_results if r['time_score'] is not None)):
        s_trades = [r for r in all_results if r['time_score'] == score_val]
        s_pnl = sum(r['pnl'] for r in s_trades if r['pnl'] is not None)
        s_wins = len([r for r in s_trades if r['outcome'] == 'WIN'])
        print(f"    Time={score_val}: {len(s_trades)} trades, {s_wins} wins, P&L={s_pnl:.1f}")

    # Export to CSV
    import csv
    csv_path = os.path.join(os.path.dirname(__file__), "dd_exhaustion_analysis.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\n  CSV exported to: {csv_path}")

    conn.close()


if __name__ == "__main__":
    main()
