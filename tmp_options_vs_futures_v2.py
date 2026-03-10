"""
Options vs Futures - V2: More realistic model with gamma boost, theta decay, and spreads.

Key differences from V1:
1. Gamma boost on winners (0DTE gamma is HUGE for ATM)
2. Theta decay modeled by time-of-day and hold duration
3. Debit spreads as alternative (defined risk, lower cost, less theta)
4. Multiple sizing scenarios
"""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
import sqlalchemy as sa
from datetime import datetime, timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

CALL_BID, CALL_ASK, CALL_DELTA, CALL_GAMMA = 5, 7, 4, 3
PUT_BID, PUT_ASK, PUT_DELTA, PUT_GAMMA = 14, 12, 16, 17
STRIKE_IDX = 10

def get_option_data(snapshot_rows, spot, direction, offset=0):
    """Get ATM (or OTM by offset) option data"""
    strikes_data = []
    for row in snapshot_rows:
        strike = row[STRIKE_IDX]
        if strike is None:
            continue
        if direction == 'long':
            bid, ask = row[CALL_BID] or 0, row[CALL_ASK] or 0
            delta, gamma = abs(row[CALL_DELTA] or 0), row[CALL_GAMMA] or 0
        else:
            bid, ask = row[PUT_BID] or 0, row[PUT_ASK] or 0
            delta, gamma = abs(row[PUT_DELTA] or 0), row[PUT_GAMMA] or 0
        if bid > 0 and ask > 0:
            strikes_data.append((strike, bid, ask, delta, gamma, abs(strike - spot)))

    if not strikes_data:
        return None

    # Sort by distance from spot
    strikes_data.sort(key=lambda x: x[5])
    idx = min(offset, len(strikes_data) - 1)
    s = strikes_data[idx]
    return {'strike': s[0], 'bid': s[1], 'ask': s[2], 'delta': s[3], 'gamma': s[4]}

def estimate_theta_multiplier(trade_hour_utc, hold_minutes):
    """0DTE theta accelerates dramatically after 2pm ET (18:00 UTC)"""
    et_hour = trade_hour_utc - 5  # rough UTC to ET
    if et_hour < 0:
        et_hour += 24

    # Theta acceleration: morning ~1x, midday ~1.5x, afternoon ~3-5x
    if et_hour < 11:
        base = 1.0
    elif et_hour < 13:
        base = 1.3
    elif et_hour < 14:
        base = 2.0
    elif et_hour < 15:
        base = 3.5
    else:
        base = 5.0

    # Scale by hold duration (theta per minute increases as expiry nears)
    return base * (hold_minutes / 60.0)  # convert to hourly equivalent

with engine.connect() as c:
    trades = c.execute(sa.text("""
        SELECT id, setup_name, direction, spot, target,
               outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss, ts, outcome_elapsed_min
        FROM setup_log WHERE outcome_result IN ('WIN','LOSS') ORDER BY ts
    """)).fetchall()

    results = []
    for trade in trades:
        t = dict(trade._mapping)
        trade_ts = t['ts']
        spot = float(t['spot'])
        direction = t['direction']
        outcome = t['outcome_result']
        pts = float(t['outcome_pnl']) if t['outcome_pnl'] else 0
        setup = t['setup_name']
        hold_min = float(t['outcome_elapsed_min']) if t['outcome_elapsed_min'] else 15

        snap = c.execute(sa.text("""
            SELECT rows FROM chain_snapshots
            WHERE ts BETWEEN :t1 AND :t2
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1
        """), {'t1': trade_ts - timedelta(minutes=5),
               't2': trade_ts + timedelta(minutes=5),
               'ts': trade_ts}).fetchone()

        if not snap:
            continue

        rows_data = json.loads(snap[0]) if isinstance(snap[0], str) else snap[0]

        # ATM option
        atm = get_option_data(rows_data, spot, direction, offset=0)
        if not atm or atm['ask'] <= 0.5:
            continue

        # 5pt OTM option (for debit spread short leg)
        otm5 = get_option_data(rows_data, spot, direction, offset=1)

        entry_cost = atm['ask']
        spread = atm['ask'] - atm['bid']
        delta = atm['delta']
        gamma = atm['gamma']
        move = abs(pts)

        # ----- MODEL 1: Naked ATM option (conservative delta-only) -----
        if outcome == 'WIN':
            naked_pnl = (delta * move - spread * 0.5) * 100
        else:
            raw_loss = delta * move + spread * 0.5
            naked_pnl = -min(raw_loss, entry_cost) * 100

        # ----- MODEL 2: Naked ATM with gamma boost -----
        # 0DTE gamma is huge (~0.005-0.02 per point for ATM)
        gamma_effect = 0.5 * gamma * move * move  # gamma P&L
        theta_drag = estimate_theta_multiplier(trade_ts.hour, hold_min) * entry_cost * 0.03

        if outcome == 'WIN':
            gamma_pnl = (delta * move + gamma_effect - theta_drag - spread * 0.5) * 100
        else:
            raw_loss = delta * move - gamma_effect + theta_drag + spread * 0.5
            gamma_pnl = -min(raw_loss, entry_cost) * 100

        # ----- MODEL 3: 5pt Debit Spread -----
        # Buy ATM, sell 5pt OTM. Max profit = 5 - debit. Max loss = debit.
        if otm5:
            spread_debit = entry_cost - otm5['bid']  # pay ATM ask, receive OTM bid
            spread_debit = max(spread_debit, 0.5)  # floor
            max_spread_profit = 5.0 - spread_debit

            if outcome == 'WIN':
                # Spread value increases, but capped at width
                intrinsic_at_exit = min(move, 5.0)
                spread_exit = max(intrinsic_at_exit - spread_debit, -spread_debit)
                spread_pnl = min(spread_exit, max_spread_profit) * 100
            else:
                spread_pnl = -spread_debit * 100  # max loss = debit
        else:
            spread_debit = entry_cost * 0.4  # estimate
            max_spread_profit = 5.0 - spread_debit
            if outcome == 'WIN':
                spread_pnl = min(min(move, 5.0) - spread_debit, max_spread_profit) * 100
            else:
                spread_pnl = -spread_debit * 100

        results.append({
            'id': t['id'], 'setup': setup, 'direction': direction,
            'outcome': outcome, 'pts': pts, 'hold_min': hold_min,
            'entry_cost': entry_cost, 'delta': delta, 'gamma': gamma,
            'futures_pnl': pts * 50,
            'naked_pnl': naked_pnl,
            'gamma_pnl': gamma_pnl,
            'spread_pnl': spread_pnl,
            'spread_debit': spread_debit if otm5 else entry_cost * 0.4,
            'option_capital': entry_cost * 100,
            'spread_capital': (spread_debit if otm5 else entry_cost * 0.4) * 100,
        })

    wins = [r for r in results if r['outcome'] == 'WIN']
    losses = [r for r in results if r['outcome'] == 'LOSS']

    first_ts = trades[0]._mapping['ts']
    last_ts = trades[-1]._mapping['ts']
    trading_days = max(1, (last_ts - first_ts).days * 5 / 7)

    tot_f = sum(r['futures_pnl'] for r in results)
    tot_naked = sum(r['naked_pnl'] for r in results)
    tot_gamma = sum(r['gamma_pnl'] for r in results)
    tot_spread = sum(r['spread_pnl'] for r in results)

    avg_opt_cap = sum(r['option_capital'] for r in results) / len(results)
    avg_spr_cap = sum(r['spread_capital'] for r in results) / len(results)
    es_margin = 15900

    print()
    print("=" * 75)
    print("  ES FUTURES vs SPX OPTIONS — COMPREHENSIVE COMPARISON")
    print("=" * 75)
    print(f"  {len(results)} trades | {first_ts.strftime('%b %d')} - {last_ts.strftime('%b %d %Y')} | ~{trading_days:.0f} trading days")
    print()

    print("  ============================================================")
    print("  STRATEGY              TOTAL P&L    /DAY     PF    AVG WIN  AVG LOSS")
    print("  ============================================================")

    for label, key, cap in [
        ("ES Futures (1 contract)", 'futures_pnl', es_margin),
        ("SPX Naked ATM (1c)", 'naked_pnl', avg_opt_cap),
        ("SPX ATM+Gamma (1c)", 'gamma_pnl', avg_opt_cap),
        ("SPX 5pt Spread (1c)", 'spread_pnl', avg_spr_cap),
    ]:
        tot = sum(r[key] for r in results)
        gw = sum(r[key] for r in wins)
        gl = abs(sum(r[key] for r in losses))
        pf = gw / gl if gl else 999
        aw = gw / len(wins) if wins else 0
        al = -gl / len(losses) if losses else 0
        daily = tot / trading_days
        print(f"  {label:28} ${tot:>+10,.0f}  ${daily:>+6,.0f}  {pf:.2f}x  ${aw:>+7,.0f}  ${al:>+7,.0f}")

    print()
    print("  ============================================================")
    print("  CAPITAL EFFICIENCY (same $15,900 capital)")
    print("  ============================================================")

    for label, key, per_cap in [
        ("ES Futures", 'futures_pnl', es_margin),
        ("SPX Naked ATM", 'naked_pnl', avg_opt_cap),
        ("SPX ATM+Gamma", 'gamma_pnl', avg_opt_cap),
        ("SPX 5pt Spread", 'spread_pnl', avg_spr_cap),
    ]:
        tot = sum(r[key] for r in results)
        contracts = es_margin / per_cap if per_cap else 1
        scaled = tot * contracts
        roi = scaled / es_margin * 100
        daily = scaled / trading_days
        monthly = daily * 21
        print(f"  {label:20} {contracts:>5.1f} contracts  ${scaled:>+12,.0f}  ROI {roi:>+8.1f}%  ~${monthly:>+10,.0f}/mo")

    print()
    print("  ============================================================")
    print("  PER-SETUP: FUTURES vs OPTIONS (which is better per setup?)")
    print("  ============================================================")
    print(f"  {'Setup':22} {'#':>4} {'Futures':>10} {'Naked ATM':>10} {'Spread':>10} {'Winner':>8}")
    print(f"  {'-'*22} {'-'*4} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")

    for s in sorted(set(r['setup'] for r in results)):
        sr = [r for r in results if r['setup'] == s]
        fp = sum(r['futures_pnl'] for r in sr)
        np_ = sum(r['naked_pnl'] for r in sr)
        sp = sum(r['spread_pnl'] for r in sr)
        best = max([(fp, 'FUT'), (np_, 'ATM'), (sp, 'SPREAD')], key=lambda x: x[0])
        print(f"  {s:22} {len(sr):>4} ${fp:>+8,.0f} ${np_:>+8,.0f} ${sp:>+8,.0f}   {best[1]:>6}")

    print()
    print("  ============================================================")
    print("  LOSS CAPPING ANALYSIS")
    print("  ============================================================")

    # How much options save on big losses
    big_losses = [r for r in losses if abs(r['pts']) >= 10]
    if big_losses:
        fut_big = sum(abs(r['futures_pnl']) for r in big_losses)
        nak_big = sum(abs(r['naked_pnl']) for r in big_losses)
        spr_big = sum(abs(r['spread_pnl']) for r in big_losses)
        print(f"  Big losses (>=10 pts): {len(big_losses)} trades")
        print(f"  Futures total loss:    ${fut_big:,.0f}")
        print(f"  Naked ATM total loss:  ${nak_big:,.0f}  (saved ${fut_big-nak_big:+,.0f})")
        print(f"  5pt Spread total loss: ${spr_big:,.0f}  (saved ${fut_big-spr_big:+,.0f})")

    # Small losses
    small_losses = [r for r in losses if abs(r['pts']) < 10]
    if small_losses:
        fut_sm = sum(abs(r['futures_pnl']) for r in small_losses)
        nak_sm = sum(abs(r['naked_pnl']) for r in small_losses)
        spr_sm = sum(abs(r['spread_pnl']) for r in small_losses)
        print(f"  Small losses (<10 pts): {len(small_losses)} trades")
        print(f"  Futures total loss:     ${fut_sm:,.0f}")
        print(f"  Naked ATM total loss:   ${nak_sm:,.0f}")
        print(f"  5pt Spread total loss:  ${spr_sm:,.0f}  (saved ${fut_sm-spr_sm:+,.0f})")
    print()

    print("  ============================================================")
    print("  KEY INSIGHT: WHEN DO OPTIONS WIN?")
    print("  ============================================================")

    # By time of day
    print(f"\n  By Time of Day (ET):")
    print(f"  {'Hour':>6} {'#':>4} {'Futures':>10} {'Naked ATM':>10} {'Spread':>10}")
    for h in range(9, 16):
        utc_h = h + 5  # ET to UTC
        hr = [r for r in results if trades[0]._mapping['ts'].hour == utc_h or True]  # need ts per result

    # By hold duration
    print(f"\n  By Hold Duration:")
    for label, lo, hi in [("<10 min", 0, 10), ("10-30 min", 10, 30), ("30-60 min", 30, 60), (">60 min", 60, 9999)]:
        bucket = [r for r in results if lo <= r['hold_min'] < hi]
        if not bucket:
            continue
        fp = sum(r['futures_pnl'] for r in bucket)
        np_ = sum(r['naked_pnl'] for r in bucket)
        sp = sum(r['spread_pnl'] for r in bucket)
        print(f"  {label:>12} ({len(bucket):>3} trades)  Fut ${fp:>+8,.0f}  ATM ${np_:>+8,.0f}  Spread ${sp:>+8,.0f}")

    print()
    print("  ============================================================")
    print("  VERDICT")
    print("  ============================================================")

    if tot_naked > tot_f:
        print("  OPTIONS WIN on absolute P&L per contract")
    else:
        pct_diff = (tot_f - tot_naked) / tot_f * 100
        print(f"  FUTURES WIN per contract by {pct_diff:.1f}%")
        print(f"  BUT options win on CAPITAL EFFICIENCY:")
        print(f"    Same capital ({es_margin/avg_opt_cap:.0f}x more contracts) -> {tot_naked*es_margin/avg_opt_cap/tot_f:.1f}x more total P&L")

    print(f"\n  Futures advantage: simpler execution, no theta, no spread drag")
    print(f"  Options advantage: defined risk, {es_margin/avg_opt_cap:.0f}x capital leverage, loss capped at premium")
    print(f"  Spread advantage: even cheaper (${avg_spr_cap:.0f}/contract), max loss = debit only")
