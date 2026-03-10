import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
import sqlalchemy as sa
from datetime import datetime, timedelta
from decimal import Decimal

engine = sa.create_engine(os.environ['DATABASE_URL'])

# Chain columns (side-by-side):
# CALLS: 0=Vol,1=OI,2=IV,3=Gamma,4=Delta,5=BID,6=BIDQTY,7=ASK,8=ASKQTY,9=LAST
# 10=Strike
# PUTS: 11=LAST,12=ASK,13=ASKQTY,14=BID,15=BIDQTY,16=Delta,17=Gamma,18=IV,19=OI,20=Vol
CALL_BID, CALL_ASK, CALL_DELTA = 5, 7, 4
PUT_BID, PUT_ASK, PUT_DELTA = 14, 12, 16
STRIKE_IDX = 10

def get_atm_option(snapshot_rows, spot, direction):
    """Find ATM option. LONG->call, SHORT->put. Returns (strike, bid, ask, delta)"""
    best = None
    best_dist = 999999
    for row in snapshot_rows:
        strike = row[STRIKE_IDX]
        if strike is None:
            continue
        dist = abs(strike - spot)
        if dist < best_dist:
            if direction == 'long':
                bid = row[CALL_BID] or 0
                ask = row[CALL_ASK] or 0
                delta = abs(row[CALL_DELTA] or 0.5)
            else:
                bid = row[PUT_BID] or 0
                ask = row[PUT_ASK] or 0
                delta = abs(row[PUT_DELTA] or 0.5)
            if bid > 0 and ask > 0:
                best = (strike, bid, ask, delta)
                best_dist = dist
    return best

with engine.connect() as c:
    trades = c.execute(sa.text("""
        SELECT id, setup_name, direction, spot, target,
               outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss, ts, outcome_elapsed_min
        FROM setup_log WHERE outcome_result IN ('WIN','LOSS') ORDER BY ts
    """)).fetchall()

    print(f"Total trades with outcomes: {len(trades)}")

    results = []
    skipped = 0

    for trade in trades:
        t = dict(trade._mapping)
        trade_ts = t['ts']
        spot = float(t['spot'])
        direction = t['direction']
        outcome = t['outcome_result']
        pts = float(t['outcome_pnl']) if t['outcome_pnl'] else 0
        setup = t['setup_name']

        # Find nearest chain snapshot within 5 min
        snap = c.execute(sa.text("""
            SELECT rows FROM chain_snapshots
            WHERE ts BETWEEN :t1 AND :t2
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1
        """), {'t1': trade_ts - timedelta(minutes=5),
               't2': trade_ts + timedelta(minutes=5),
               'ts': trade_ts}).fetchone()

        if not snap:
            skipped += 1
            continue

        rows_data = json.loads(snap[0]) if isinstance(snap[0], str) else snap[0]
        option = get_atm_option(rows_data, spot, direction)
        if not option:
            skipped += 1
            continue

        strike, bid, ask, delta = option
        if ask <= 0.5:  # too cheap / near expiry
            skipped += 1
            continue

        entry_cost = ask  # pay the ask to enter
        spread = ask - bid
        move = abs(pts)

        if outcome == 'WIN':
            # Winner: option gains delta*move, exit at bid (conservative)
            option_exit_value = entry_cost + delta * move - spread * 0.5
            option_pnl = option_exit_value - entry_cost  # = delta*move - half_spread
        else:
            # Loser: option loses delta*move, but capped at premium paid
            raw_loss = delta * move + spread * 0.5
            option_pnl = -min(raw_loss, entry_cost)  # can't lose more than premium

        results.append({
            'id': t['id'], 'setup': setup, 'direction': direction,
            'outcome': outcome, 'spot': spot, 'strike': strike, 'pts': pts,
            'entry_cost': entry_cost, 'delta': delta,
            'futures_pnl_1es': pts * 50,
            'option_pnl_1c': option_pnl * 100,
            'option_capital': entry_cost * 100,
        })

    print(f"Matched: {len(results)}, Skipped: {skipped}")

    wins = [r for r in results if r['outcome'] == 'WIN']
    losses = [r for r in results if r['outcome'] == 'LOSS']

    total_fut = sum(r['futures_pnl_1es'] for r in results)
    total_opt = sum(r['option_pnl_1c'] for r in results)
    total_pts = sum(r['pts'] for r in results)
    avg_entry = sum(r['entry_cost'] for r in results) / len(results)
    avg_capital = avg_entry * 100

    es_margin = 15900  # 1 ES day trade margin
    equiv_contracts = es_margin / avg_capital if avg_capital > 0 else 1

    print()
    print("=" * 70)
    print("  ES FUTURES vs SPX 0DTE OPTIONS — PERFORMANCE COMPARISON")
    print("=" * 70)
    first_ts = trades[0]._mapping['ts']
    last_ts = trades[-1]._mapping['ts']
    trade_days = (last_ts - first_ts).days
    trading_days = max(1, trade_days * 5 / 7)

    print(f"  Period: {first_ts.strftime('%b %d')} - {last_ts.strftime('%b %d, %Y')} ({trading_days:.0f} trading days)")
    print(f"  Trades: {len(results)} matched")
    print()

    print("  --- ES/MES FUTURES ---")
    print(f"  Total SPX points:          {total_pts:+.1f}")
    print(f"  1 ES  ($50/pt):            ${total_fut:+,.0f}")
    print(f"  10 MES ($50/pt equiv):     ${total_fut:+,.0f}")
    print(f"  4 ES:                      ${total_fut*4:+,.0f}")
    print(f"  Capital (1 ES margin):     $15,900")
    print(f"  ROI (1 ES):                {total_fut/es_margin*100:+.1f}%")
    print()

    print("  --- SPX 0DTE ATM OPTIONS (buy call for LONG, put for SHORT) ---")
    print(f"  Avg entry premium:         ${avg_entry:.2f} (${avg_capital:.0f}/contract)")
    print(f"  1 contract total P&L:      ${total_opt:+,.0f}")
    print(f"  Equiv contracts (=$15,900): {equiv_contracts:.1f}")
    print(f"  P&L at equiv sizing:       ${total_opt*equiv_contracts:+,.0f}")
    opt_roi = total_opt / avg_capital * 100 if avg_capital else 0
    print(f"  ROI per contract:          {opt_roi:+.1f}%")
    print()

    # Win/Loss averages
    avg_win_f = sum(r['futures_pnl_1es'] for r in wins) / len(wins) if wins else 0
    avg_loss_f = sum(r['futures_pnl_1es'] for r in losses) / len(losses) if losses else 0
    avg_win_o = sum(r['option_pnl_1c'] for r in wins) / len(wins) if wins else 0
    avg_loss_o = sum(r['option_pnl_1c'] for r in losses) / len(losses) if losses else 0

    print("  --- WIN/LOSS COMPARISON ---")
    print(f"  {'':22} {'Futures(1ES)':>14} {'Options(1c)':>14}")
    print(f"  {'Avg WIN':22} ${avg_win_f:>+11,.0f}   ${avg_win_o:>+11,.0f}")
    print(f"  {'Avg LOSS':22} ${avg_loss_f:>+11,.0f}   ${avg_loss_o:>+11,.0f}")
    wl_f = abs(avg_win_f/avg_loss_f) if avg_loss_f else 0
    wl_o = abs(avg_win_o/avg_loss_o) if avg_loss_o else 0
    print(f"  {'Win:Loss ratio':22} {wl_f:>11.2f}x   {wl_o:>11.2f}x")
    print()

    # Profit factor
    gw_f = sum(r['futures_pnl_1es'] for r in wins)
    gl_f = abs(sum(r['futures_pnl_1es'] for r in losses))
    gw_o = sum(r['option_pnl_1c'] for r in wins)
    gl_o = abs(sum(r['option_pnl_1c'] for r in losses))
    pf_f = gw_f / gl_f if gl_f else 999
    pf_o = gw_o / gl_o if gl_o else 999

    print(f"  {'Profit Factor':22} {pf_f:>11.2f}x   {pf_o:>11.2f}x")
    print(f"  {'Gross Wins':22} ${gw_f:>+11,.0f}   ${gw_o:>+11,.0f}")
    print(f"  {'Gross Losses':22} ${-gl_f:>+11,.0f}   ${-gl_o:>+11,.0f}")
    print()

    # Loss capping analysis
    capped = [r for r in losses if r['delta'] * abs(r['pts']) > r['entry_cost']]
    print(f"  --- OPTIONS LOSS CAPPING ---")
    print(f"  Losses where futures loss > option premium: {len(capped)}/{len(losses)}")
    if capped:
        fut_loss_capped = sum(abs(r['pts']) * 50 for r in capped)
        opt_loss_capped = sum(r['entry_cost'] * 100 for r in capped)
        print(f"  Futures loss on those trades:  ${fut_loss_capped:,.0f}")
        print(f"  Options loss (capped):         ${opt_loss_capped:,.0f}")
        print(f"  Saved by capping:              ${fut_loss_capped - opt_loss_capped:+,.0f}")
    print()

    # Per-setup breakdown
    print("  --- PER-SETUP BREAKDOWN ---")
    print(f"  {'Setup':22} {'#':>4} {'Fut PnL':>12} {'Opt PnL':>12} {'Opt ROI':>10} {'Better':>8}")
    print(f"  {'-'*22} {'-'*4} {'-'*12} {'-'*12} {'-'*10} {'-'*8}")
    setups = sorted(set(r['setup'] for r in results))
    for s in setups:
        sr = [r for r in results if r['setup'] == s]
        fp = sum(r['futures_pnl_1es'] for r in sr)
        op = sum(r['option_pnl_1c'] for r in sr)
        avg_cap = sum(r['option_capital'] for r in sr) / len(sr)
        roi = op / avg_cap * 100 if avg_cap else 0
        better = "OPT" if op > fp else "FUT"
        print(f"  {s:22} {len(sr):>4} ${fp:>+10,.0f} ${op:>+10,.0f} {roi:>+8.1f}% {better:>8}")
    print()

    # Monthly projections
    daily_f = total_fut / trading_days
    daily_o = total_opt / trading_days
    print("  --- MONTHLY PROJECTIONS (21 trading days) ---")
    print(f"  {'':22} {'Futures':>14} {'Options':>14}")
    print(f"  {'1 ES / 1 contract':22} ${daily_f*21:>+12,.0f} ${daily_o*21:>+12,.0f}")
    print(f"  {'4 ES / equiv opts':22} ${daily_f*21*4:>+12,.0f} ${daily_o*21*equiv_contracts:>+12,.0f}")
    print(f"  {'10 MES':22} ${daily_f*21:>+12,.0f}   {'N/A':>12}")
    print()

    # The verdict
    print("  --- CAPITAL EFFICIENCY ---")
    print(f"  Same $15,900 capital:")
    print(f"    Futures: 1 ES  -> ${total_fut:+,.0f} total ({total_fut/es_margin*100:+.1f}% ROI)")
    print(f"    Options: {equiv_contracts:.0f} contracts -> ${total_opt*equiv_contracts:+,.0f} total ({opt_roi*equiv_contracts:+.1f}% ROI)")
    print()

    # CRITICAL: Theta decay analysis
    # How many of our trades are short-duration (< 30 min)?
    short_trades = [t for t in trades if t._mapping.get('outcome_elapsed_min') and float(t._mapping['outcome_elapsed_min']) < 30]
    long_trades = [t for t in trades if t._mapping.get('outcome_elapsed_min') and float(t._mapping['outcome_elapsed_min']) >= 30]
    print(f"  --- THETA DECAY RISK ---")
    print(f"  Trades resolved < 30 min: {len(short_trades)}/{len(trades)} ({len(short_trades)/len(trades)*100:.0f}%)")
    print(f"  Trades resolved >= 30 min: {len(long_trades)}/{len(trades)} ({len(long_trades)/len(trades)*100:.0f}%)")
    print(f"  (Longer holds = more theta decay eating into option profits)")
