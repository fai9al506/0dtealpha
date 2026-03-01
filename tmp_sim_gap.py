"""Investigate gap between TS SIM account P&L and setup_log theoretical P&L.
   FIXED: uses 'fill_price' not 'entry_price' from state JSONB.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import os
import json
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]
MES_POINT_VALUE = 5.0

def compute_actual_pnl(state):
    """Compute actual MES $ P&L from fill prices in auto_trade_orders state."""
    entry_price = state.get('fill_price')  # KEY FIX: field is 'fill_price' not 'entry_price'
    if not entry_price:
        return 0.0, [], "NO ENTRY FILL"

    direction = (state.get('direction') or '').upper()
    # bullish/long = +1, bearish/short = -1
    sign = 1 if direction in ('BUY', 'LONG', 'BULLISH') else -1

    total_qty = state.get('total_qty', 10)
    fills = []
    actual_pnl = 0.0

    # T1 fill
    t1_fill = state.get('t1_fill_price')
    if t1_fill:
        qty = state.get('t1_qty', 5) or 5
        pts = (t1_fill - entry_price) * sign
        pnl = pts * qty * MES_POINT_VALUE
        actual_pnl += pnl
        fills.append(f"T1: {qty}@{t1_fill} ({pts:+.2f}pts) = ${pnl:+.0f}")

    # T2 fill
    t2_fill = state.get('t2_fill_price')
    if t2_fill:
        qty = state.get('t2_qty', 5) or 5
        pts = (t2_fill - entry_price) * sign
        pnl = pts * qty * MES_POINT_VALUE
        actual_pnl += pnl
        fills.append(f"T2: {qty}@{t2_fill} ({pts:+.2f}pts) = ${pnl:+.0f}")

    # Stop fill
    stop_fill = state.get('stop_fill_price')
    if stop_fill:
        qty = state.get('stop_filled_qty', total_qty) or total_qty
        pts = (stop_fill - entry_price) * sign
        pnl = pts * qty * MES_POINT_VALUE
        actual_pnl += pnl
        fills.append(f"STOP: {qty}@{stop_fill} ({pts:+.2f}pts) = ${pnl:+.0f}")

    # Close fill (reversal or manual close)
    close_fill = state.get('close_fill_price')
    if close_fill:
        close_qty = state.get('close_qty', total_qty) or total_qty
        pts = (close_fill - entry_price) * sign
        pnl = pts * close_qty * MES_POINT_VALUE
        actual_pnl += pnl
        fills.append(f"CLOSE: {close_qty}@{close_fill} ({pts:+.2f}pts) = ${pnl:+.0f}")

    # Check: did we account for all 10 contracts?
    accounted = 0
    if t1_fill:
        accounted += state.get('t1_qty', 5) or 5
    if t2_fill:
        accounted += state.get('t2_qty', 5) or 5
    if stop_fill:
        accounted += state.get('stop_filled_qty', total_qty) or total_qty
    if close_fill:
        accounted += state.get('close_qty', total_qty) or total_qty

    if accounted == 0:
        fills.append(f"WARNING: no exit fills! entry={entry_price} but no T1/T2/stop/close recorded")
        return 0.0, fills, "NO EXIT FILLS"

    if accounted != 10 and accounted != total_qty:
        fills.append(f"WARNING: accounted qty={accounted} vs expected={total_qty}")

    return actual_pnl, fills, "OK"


def main():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        cur = conn.cursor()

        # ===== ALL auto_trade_orders with full P&L calculation =====
        print("=" * 120)
        print("ALL AUTO-TRADE ORDERS: Actual MES $ P&L vs Theoretical SPX P&L")
        print("=" * 120)

        cur.execute("""
            SELECT sl.id, sl.setup_name, sl.direction, sl.outcome_result, sl.outcome_pnl,
                   sl.ts AT TIME ZONE 'America/New_York' as ts_et,
                   sl.ts::date as trade_date,
                   ato.state
            FROM setup_log sl
            JOIN auto_trade_orders ato ON ato.setup_log_id = sl.id
            ORDER BY sl.id
        """)
        rows = cur.fetchall()

        by_date = {}
        grand_theoretical = 0.0
        grand_actual = 0.0
        total_trades = 0
        no_exit_trades = []
        discrepancy_trades = []

        for r in rows:
            state = r['state']
            if isinstance(state, str):
                state = json.loads(state)

            d = str(r['trade_date'])
            if d not in by_date:
                by_date[d] = {'trades': [], 'theo_total': 0.0, 'actual_total': 0.0}

            actual_pnl, fills, status = compute_actual_pnl(state)
            theoretical_pnl = r['outcome_pnl'] or 0.0
            # Convert theoretical SPX pts to MES $ for comparison (10 contracts * $5/pt)
            theo_dollars = theoretical_pnl * 10 * MES_POINT_VALUE

            entry = state.get('fill_price')
            total_trades += 1
            grand_theoretical += theo_dollars
            grand_actual += actual_pnl
            by_date[d]['theo_total'] += theo_dollars
            by_date[d]['actual_total'] += actual_pnl

            trade_info = {
                'id': r['id'],
                'setup': r['setup_name'],
                'dir': state.get('direction', r['direction']),
                'outcome': r['outcome_result'],
                'entry': entry,
                'theo_pts': theoretical_pnl,
                'theo_$': theo_dollars,
                'actual_$': actual_pnl,
                'gap_$': actual_pnl - theo_dollars,
                'fills': fills,
                'status': status,
            }
            by_date[d]['trades'].append(trade_info)

            if status == "NO EXIT FILLS":
                no_exit_trades.append(trade_info)

            # Flag significant discrepancies (>$50 gap)
            gap = abs(actual_pnl - theo_dollars)
            if gap > 50 and status == "OK":
                discrepancy_trades.append(trade_info)

        # Print by date
        for d in sorted(by_date.keys()):
            info = by_date[d]
            print(f"\n{'='*100}")
            print(f"DATE: {d} | Trades: {len(info['trades'])} | "
                  f"Theoretical: ${info['theo_total']:+,.0f} | "
                  f"Actual MES: ${info['actual_total']:+,.0f} | "
                  f"GAP: ${info['actual_total'] - info['theo_total']:+,.0f}")
            print(f"{'='*100}")

            for t in info['trades']:
                marker = " ***" if abs(t['gap_$']) > 50 else ""
                print(f"\n  #{t['id']} {t['setup']:20s} {t['dir']:8s} | "
                      f"outcome={t['outcome'] or 'OPEN':10s} | "
                      f"entry={t['entry']}")
                print(f"    Theo: {t['theo_pts']:+.1f} pts = ${t['theo_$']:+,.0f}")
                print(f"    Actual MES $: ${t['actual_$']:+,.0f}{marker}")
                if t['gap_$'] != 0 and t['status'] == 'OK':
                    print(f"    GAP: ${t['gap_$']:+,.0f}")
                for f in t['fills']:
                    print(f"    {f}")

        # ===== SUMMARY =====
        print(f"\n\n{'='*120}")
        print("GRAND SUMMARY")
        print(f"{'='*120}")
        print(f"Total auto-traded setups: {total_trades}")
        print(f"Theoretical (setup_log SPX pts -> MES $): ${grand_theoretical:+,.0f}")
        print(f"Actual MES $ from fills: ${grand_actual:+,.0f}")
        print(f"GAP (actual - theoretical): ${grand_actual - grand_theoretical:+,.0f}")
        print()

        if no_exit_trades:
            print(f"\nTrades with NO EXIT FILLS ({len(no_exit_trades)}):")
            print("  These had entries placed but no T1/T2/stop/close fill recorded.")
            print("  The position may have been closed by TS but we didn't capture the fill.")
            for t in no_exit_trades:
                print(f"  #{t['id']} {t['setup']} {t['dir']} entry={t['entry']} "
                      f"theo=${t['theo_$']:+,.0f}")

        if discrepancy_trades:
            print(f"\nTrades with SIGNIFICANT DISCREPANCY (>{50} gap, {len(discrepancy_trades)}):")
            for t in discrepancy_trades:
                print(f"  #{t['id']} {t['setup']} {t['dir']} | "
                      f"theo=${t['theo_$']:+,.0f} actual=${t['actual_$']:+,.0f} gap=${t['gap_$']:+,.0f}")

        # ===== Check for trades closed by reversal (close_fill but no stop) =====
        print(f"\n\n{'='*120}")
        print("REVERSAL / CLOSE ANALYSIS")
        print(f"{'='*120}")
        reversal_count = 0
        for r in rows:
            state = r['state']
            if isinstance(state, str):
                state = json.loads(state)
            if state.get('close_fill_price') and not state.get('stop_fill_price'):
                reversal_count += 1
                entry = state.get('fill_price')
                close = state.get('close_fill_price')
                close_qty = state.get('close_qty', 10)
                direction = (state.get('direction') or '').upper()
                sign = 1 if direction in ('BUY', 'LONG', 'BULLISH') else -1
                pts = (close - entry) * sign if entry else 0
                print(f"  #{r['id']} {r['setup_name']:20s} {state.get('direction'):8s} | "
                      f"entry={entry} close={close} qty={close_qty} | "
                      f"pts={pts:+.2f} | outcome={r['outcome_result']}")
        print(f"Total closed by reversal/manual: {reversal_count}")

        # ===== Check for Feb 20 and Feb 23 with entry=None =====
        print(f"\n\n{'='*120}")
        print("LEGACY TRADES (no fill_price - pre-deployment?)")
        print(f"{'='*120}")
        legacy_count = 0
        legacy_theo = 0.0
        for r in rows:
            state = r['state']
            if isinstance(state, str):
                state = json.loads(state)
            if not state.get('fill_price'):
                legacy_count += 1
                theo = (r['outcome_pnl'] or 0) * 10 * MES_POINT_VALUE
                legacy_theo += theo
                print(f"  #{r['id']} {r['setup_name']:20s} | {r['outcome_result']:10s} | "
                      f"theo_pnl={r['outcome_pnl'] or 0:+.1f} = ${theo:+,.0f} | "
                      f"date={r['trade_date']} | state_keys={sorted(state.keys())}")
        print(f"Total legacy (no fill_price): {legacy_count}")
        print(f"Legacy theoretical MES $: ${legacy_theo:+,.0f}")

        # ===== SIM Account Balance Calculation =====
        print(f"\n\n{'='*120}")
        print("SIM ACCOUNT ESTIMATE")
        print(f"{'='*120}")
        print(f"Starting balance: $50,000")
        print(f"Actual fills P&L: ${grand_actual:+,.0f}")
        print(f"Estimated balance from fills: ${50000 + grand_actual:+,.0f}")
        print()
        print("If account shows ~$48,000 (-$2K from start):")
        print(f"  That implies actual P&L = -$2,000")
        print(f"  Our calculated actual: ${grand_actual:+,.0f}")
        print(f"  Unaccounted gap: ${-2000 - grand_actual:+,.0f}")
        print()
        print("Possible causes of gap:")
        print("  1. Legacy trades (no fill_price) had real SIM executions we can't see")
        print("  2. Reversal close prices differ from what setup_log tracked")
        print("  3. Slippage on SIM (market orders)")
        print("  4. TS SIM commissions")
        print("  5. Orphaned positions from crashes/reversals not properly closed")

if __name__ == "__main__":
    main()
