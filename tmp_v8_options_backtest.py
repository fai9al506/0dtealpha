"""V8 Options Backtest — Real option prices from chain_snapshots, Mar 1-13 2026."""
import sys, io, json, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta

# Column indices in chain_snapshots rows array
C_DELTA, C_ASK, C_BID, STRIKE, P_DELTA, P_ASK, P_BID = 4, 7, 5, 10, 16, 12, 14

e = create_engine(os.environ['DATABASE_URL'])

with e.connect() as conn:
    # Get all trades with outcomes, Mar 1-13
    trades = conn.execute(text("""
        SELECT id, setup_name, direction, grade, spot, ts,
               outcome_result, outcome_pnl, outcome_elapsed_min,
               greek_alignment, score, vix, overvix
        FROM setup_log
        WHERE ts >= '2026-03-01' AND ts < '2026-03-14'
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    print(f"Total trades Mar 1-13: {len(trades)}")

    # V8 filter
    def passes_v8(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        vix_val = r['vix']
        ov_val = r['overvix']
        if dirn in ('long', 'bullish'):
            if align is None or align < 2:
                return False
            # V8 VIX gate
            if vix_val is not None and vix_val > 26:
                ov = ov_val if ov_val is not None else -99
                if ov < 2:
                    return False
            return True
        else:  # short
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    # V7+AG filter for comparison
    def passes_v7ag(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        if dirn in ('long', 'bullish'):
            return align is not None and align >= 2
        else:
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    v8_trades = [r for r in trades if passes_v8(r)]
    v7ag_trades = [r for r in trades if passes_v7ag(r)]
    print(f"V8 filtered: {len(v8_trades)}")
    print(f"V7+AG filtered: {len(v7ag_trades)}")

    def get_option_prices(r):
        """Get entry ask and exit bid for ~0.30 delta option."""
        ts_entry = r['ts']
        is_long = r['direction'] in ('long', 'bullish')
        elapsed = r['outcome_elapsed_min'] or 0
        opt_type = 'call' if is_long else 'put'

        # Find closest chain snapshot at entry
        entry_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_entry - timedelta(minutes=2),
            "t2": ts_entry + timedelta(minutes=2),
            "ts": ts_entry}).mappings().first()
        if not entry_snap:
            return None

        strikes = entry_snap['rows']
        if isinstance(strikes, str):
            strikes = json.loads(strikes)

        # Find ~0.30 delta option
        best_strike, best_ask, best_bid, best_delta, best_dd = None, None, None, None, 999
        for sd in strikes:
            if len(sd) < 17:
                continue
            try:
                delta = float(sd[C_DELTA] if opt_type == 'call' else sd[P_DELTA])
                ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
                bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
            except (TypeError, ValueError):
                continue
            if ask <= 0:
                continue
            diff = abs(abs(delta) - 0.30)
            if diff < best_dd:
                best_dd = diff
                best_strike = float(sd[STRIKE])
                best_ask = ask
                best_bid = bid
                best_delta = abs(delta)

        if not best_strike or best_ask is None:
            return None

        # Find closest chain snapshot at exit
        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)
        exit_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_exit - timedelta(minutes=2),
            "t2": ts_exit + timedelta(minutes=2),
            "ts": ts_exit}).mappings().first()
        if not exit_snap:
            return None

        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str):
            exit_strikes = json.loads(exit_strikes)

        for sd in exit_strikes:
            if len(sd) < 17:
                continue
            try:
                if float(sd[STRIKE]) == best_strike:
                    exit_bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
                    exit_ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
                    return {
                        'strike': best_strike,
                        'entry_delta': best_delta,
                        'entry_ask': best_ask,
                        'entry_bid': best_bid,
                        'exit_bid': exit_bid,
                        'exit_ask': exit_ask,
                        'pnl_per_contract': (exit_bid - best_ask) * 100,  # SPX options = $100 multiplier
                        'pnl_pct': (exit_bid - best_ask) / best_ask * 100,
                        'opt_type': opt_type,
                        'elapsed_min': elapsed,
                    }
            except (TypeError, ValueError):
                continue
        return None

    # ============= ENRICH V8 TRADES =============
    print("\nLoading real option prices for V8 trades...", flush=True)
    v8_enriched = []
    v8_no_price = 0
    for r in v8_trades:
        opt = get_option_prices(r)
        if opt is None:
            v8_no_price += 1
            continue
        v8_enriched.append({**dict(r), **opt})

    print(f"V8: {len(v8_enriched)} trades with prices, {v8_no_price} skipped (no snapshot match)")

    # ============= ENRICH V7+AG TRADES =============
    print("Loading real option prices for V7+AG trades...", flush=True)
    v7_enriched = []
    v7_no_price = 0
    for r in v7ag_trades:
        opt = get_option_prices(r)
        if opt is None:
            v7_no_price += 1
            continue
        v7_enriched.append({**dict(r), **opt})

    print(f"V7+AG: {len(v7_enriched)} trades with prices, {v7_no_price} skipped")

    # ============= ANALYSIS =============
    def analyze(name, tlist):
        if not tlist:
            print(f"\n{name}: 0 trades")
            return
        wins = [t for t in tlist if t['pnl_per_contract'] > 0]
        losses = [t for t in tlist if t['pnl_per_contract'] <= 0]
        total_pnl = sum(t['pnl_per_contract'] for t in tlist)
        gross_win = sum(t['pnl_per_contract'] for t in wins)
        gross_loss = abs(sum(t['pnl_per_contract'] for t in losses))
        pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
        wr = len(wins) / len(tlist) * 100
        avg_win = gross_win / len(wins) if wins else 0
        avg_loss = gross_loss / len(losses) if losses else 0
        avg_entry = sum(t['entry_ask'] for t in tlist) / len(tlist)
        avg_pnl_pct = sum(t['pnl_pct'] for t in tlist) / len(tlist)

        # Max drawdown
        cum = 0
        peak = 0
        max_dd = 0
        for t in sorted(tlist, key=lambda x: x['ts']):
            cum += t['pnl_per_contract']
            if cum > peak:
                peak = cum
            dd = peak - cum
            if dd > max_dd:
                max_dd = dd

        days = sorted(set(t['ts'].strftime('%Y-%m-%d') for t in tlist))
        num_days = len(days)

        print(f"\n{'='*70}")
        print(f" {name}")
        print(f"{'='*70}")
        print(f" Trades:     {len(tlist)} ({len(wins)}W / {len(losses)}L)")
        print(f" Win Rate:   {wr:.1f}%")
        print(f" Total P&L:  ${total_pnl:,.0f} (per 1 SPX contract)")
        print(f" Avg P&L:    ${total_pnl/len(tlist):,.0f}/trade")
        print(f" Avg Winner: ${avg_win:,.0f}   Avg Loser: -${avg_loss:,.0f}")
        print(f" PF:         {pf:.2f}")
        print(f" Max DD:     ${max_dd:,.0f}")
        print(f" Days:       {num_days}")
        print(f" P&L/day:    ${total_pnl/num_days:,.0f}")
        print(f" Avg Entry:  ${avg_entry:.2f} (option premium)")
        print(f" Avg P&L %:  {avg_pnl_pct:+.1f}%")

        # By setup
        print(f"\n  By Setup:")
        by_setup = defaultdict(list)
        for t in tlist:
            by_setup[t['setup_name']].append(t)
        for sn in sorted(by_setup.keys()):
            st = by_setup[sn]
            sw = sum(1 for t in st if t['pnl_per_contract'] > 0)
            sp = sum(t['pnl_per_contract'] for t in st)
            swr = sw / len(st) * 100 if st else 0
            print(f"    {sn:<22s} {len(st):>3d}t  {swr:>5.1f}% WR  ${sp:>+8,.0f}")

        # Daily breakdown
        print(f"\n  Daily P&L:")
        daily = defaultdict(list)
        for t in tlist:
            daily[t['ts'].strftime('%Y-%m-%d')].append(t)
        cum_pnl = 0
        for d in sorted(daily.keys()):
            dt = daily[d]
            dp = sum(t['pnl_per_contract'] for t in dt)
            dw = sum(1 for t in dt if t['pnl_per_contract'] > 0)
            cum_pnl += dp
            avg_vix = sum(float(t['vix']) for t in dt if t['vix']) / max(1, sum(1 for t in dt if t['vix']))
            print(f"    {d}  {len(dt):>2d}t  {dw:>2d}W  ${dp:>+8,.0f}  cum=${cum_pnl:>+9,.0f}  VIX={avg_vix:.1f}")

        # Direction breakdown
        print(f"\n  By Direction:")
        for dirn in ('long', 'bullish', 'short', 'bearish'):
            dt = [t for t in tlist if t['direction'] == dirn]
            if not dt:
                continue
            dw = sum(1 for t in dt if t['pnl_per_contract'] > 0)
            dp = sum(t['pnl_per_contract'] for t in dt)
            dwr = dw / len(dt) * 100
            print(f"    {dirn:<10s} {len(dt):>3d}t  {dwr:>5.1f}% WR  ${dp:>+8,.0f}")

        # Top 5 best and worst trades
        print(f"\n  Top 5 Winners:")
        for t in sorted(tlist, key=lambda x: x['pnl_per_contract'], reverse=True)[:5]:
            print(f"    #{t['id']:<5d} {t['setup_name']:<20s} {t['direction']:<6s} entry=${t['entry_ask']:.2f} exit=${t['exit_bid']:.2f}  ${t['pnl_per_contract']:>+8,.0f} ({t['pnl_pct']:>+.0f}%)  {t['elapsed_min']:.0f}min")

        print(f"\n  Top 5 Losers:")
        for t in sorted(tlist, key=lambda x: x['pnl_per_contract'])[:5]:
            print(f"    #{t['id']:<5d} {t['setup_name']:<20s} {t['direction']:<6s} entry=${t['entry_ask']:.2f} exit=${t['exit_bid']:.2f}  ${t['pnl_per_contract']:>+8,.0f} ({t['pnl_pct']:>+.0f}%)  {t['elapsed_min']:.0f}min")

        return total_pnl, num_days

    # Run analysis
    v8_result = analyze("V8 FILTER — Real SPX Option Prices (Mar 1-13)", v8_enriched)
    v7_result = analyze("V7+AG FILTER — Real SPX Option Prices (Mar 1-13)", v7_enriched)

    # ============= COMPARISON =============
    if v8_result and v7_result:
        v8_pnl, v8_days = v8_result
        v7_pnl, v7_days = v7_result
        print(f"\n{'='*70}")
        print(f" V8 vs V7+AG COMPARISON")
        print(f"{'='*70}")
        print(f" V8 total:     ${v8_pnl:>+10,.0f}  ({v8_days} days, ${v8_pnl/v8_days:>+,.0f}/day)")
        print(f" V7+AG total:  ${v7_pnl:>+10,.0f}  ({v7_days} days, ${v7_pnl/v7_days:>+,.0f}/day)")
        print(f" V8 advantage: ${v8_pnl - v7_pnl:>+10,.0f}")

    # ============= INCOME PROJECTIONS =============
    if v8_result:
        v8_pnl, v8_days = v8_result
        daily_avg = v8_pnl / v8_days
        print(f"\n{'='*70}")
        print(f" INCOME PROJECTIONS (based on V8, {v8_days}-day avg)")
        print(f"{'='*70}")
        print(f" Per 1 SPX contract (100x multiplier):")
        print(f"   Daily avg:   ${daily_avg:>+,.0f}")
        print(f"   Weekly:      ${daily_avg * 5:>+,.0f}")
        print(f"   Monthly:     ${daily_avg * 21:>+,.0f}")
        print(f"")
        # SPY option equivalent (10x cheaper than SPX, $100 multiplier)
        # SPY option ~$0.50-1.50 vs SPX ~$5-15
        # But SPY multiplier is also $100
        print(f" SPY equivalent (options ~10x cheaper, same $100 multiplier):")
        spy_factor = 0.1  # SPY is ~1/10 of SPX
        print(f"   1 SPY contract/trade:  ${daily_avg * spy_factor:>+,.0f}/day  ${daily_avg * spy_factor * 21:>+,.0f}/month")
        print(f"   5 SPY contracts/trade: ${daily_avg * spy_factor * 5:>+,.0f}/day  ${daily_avg * spy_factor * 5 * 21:>+,.0f}/month")
        print(f"   10 SPY contracts/trade: ${daily_avg * spy_factor * 10:>+,.0f}/day  ${daily_avg * spy_factor * 10 * 21:>+,.0f}/month")
        print(f"")
        print(f" Capital needed (per contract):")
        avg_entry = sum(t['entry_ask'] for t in v8_enriched) / len(v8_enriched)
        avg_trades_day = len(v8_enriched) / v8_days
        print(f"   Avg SPX option premium: ${avg_entry:.2f} = ${avg_entry * 100:.0f}/contract")
        print(f"   Avg trades/day: {avg_trades_day:.1f}")
        print(f"   Max simultaneous (assume 3): ${avg_entry * 100 * 3:,.0f} capital at risk")
        print(f"   SPY equivalent: ~${avg_entry * 10 * 3:,.0f} capital at risk")

print("\nDone.")
