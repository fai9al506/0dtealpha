"""Scan historical data for LIS convergence patterns."""
import json
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')

def parse_dollar(s):
    if not s:
        return None
    parts = s.split(' - ')
    s0 = parts[0].strip().replace('$', '').replace(',', '')
    try:
        return float(s0)
    except:
        return None

with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT
            v.ts as v_ts,
            v.payload->'statistics'->>'lines_in_sand' as lis_str,
            v.payload->'statistics'->>'paradigm' as paradigm,
            v.payload->'statistics'->>'target' as target_str,
            v.payload->'statistics'->>'aggregatedCharm' as charm,
            c.spot
        FROM volland_snapshots v
        LEFT JOIN LATERAL (
            SELECT spot, ts FROM chain_snapshots
            WHERE ts BETWEEN v.ts - interval '3 minutes' AND v.ts + interval '3 minutes'
              AND spot IS NOT NULL
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - v.ts)))
            LIMIT 1
        ) c ON true
        WHERE v.payload->'statistics'->>'lines_in_sand' IS NOT NULL
          AND c.spot IS NOT NULL
        ORDER BY v.ts ASC
    """)).mappings().all()

    by_day = defaultdict(list)
    for row in r:
        day = str(row['v_ts'])[:10]
        lis = parse_dollar(row['lis_str'])
        spot = float(row['spot'])
        if lis is None:
            continue
        charm_val = row['charm']
        if isinstance(charm_val, str):
            try:
                charm_val = float(charm_val)
            except:
                charm_val = None
        by_day[day].append({
            'ts': str(row['v_ts'])[11:19],
            'spot': spot,
            'lis': lis,
            'gap': spot - lis,
            'paradigm': row['paradigm'],
            'charm': charm_val,
        })

    # CONVERGENCE DEFINITION:
    # 1. Gap (spot - LIS) must be -3 to +12 (spot near LIS)
    # 2. Gap was > 15 within last 5 cycles
    # 3. Spot declined at least 5 pts from 5 cycles ago
    # 4. LIS stable or rising (not dropped > 5)

    convergences = []

    for day, snaps in sorted(by_day.items()):
        if len(snaps) < 6:
            continue

        for i in range(5, len(snaps)):
            curr = snaps[i]

            if curr['gap'] < -3 or curr['gap'] > 12:
                continue

            max_prev_gap = max(snaps[j]['gap'] for j in range(i-5, i))
            if max_prev_gap < 15:
                continue

            spot_drop = snaps[i-5]['spot'] - curr['spot']
            if spot_drop < 5:
                continue

            lis_change = curr['lis'] - snaps[i-5]['lis']
            if lis_change < -5:
                continue

            # Look forward up to 15 cycles (~30 min)
            future_max = curr['spot']
            future_min = curr['spot']
            bounce_time = None
            future_end = min(i + 15, len(snaps))
            for j in range(i+1, future_end):
                if snaps[j]['spot'] > future_max:
                    future_max = snaps[j]['spot']
                    if future_max - curr['spot'] >= 10 and bounce_time is None:
                        bounce_time = snaps[j]['ts']
                if snaps[j]['spot'] < future_min:
                    future_min = snaps[j]['spot']

            bounce = future_max - curr['spot']
            drawdown = curr['spot'] - future_min

            convergences.append({
                'day': day,
                'time': curr['ts'],
                'spot': curr['spot'],
                'lis': curr['lis'],
                'gap': curr['gap'],
                'max_prev_gap': max_prev_gap,
                'spot_drop': spot_drop,
                'lis_change': lis_change,
                'paradigm': curr['paradigm'],
                'charm': curr['charm'],
                'bounce': bounce,
                'drawdown': drawdown,
                'bounce_time': bounce_time,
            })

    # Deduplicate (first per day within 15 min)
    deduped = []
    for c in convergences:
        skip = False
        for d in deduped:
            if d['day'] == c['day']:
                t1 = int(c['time'][:2])*60 + int(c['time'][3:5])
                t2 = int(d['time'][:2])*60 + int(d['time'][3:5])
                if abs(t1 - t2) < 15:
                    skip = True
                    break
        if not skip:
            deduped.append(c)

    print(f'=== LIS CONVERGENCE (TIGHT FILTER) -- {len(deduped)} signals ===')
    print(f'Criteria: gap -3 to +12, was >15 in last 5 cycles, spot dropped 5+, LIS stable/rising')
    print()
    print(f'{"Date":>12} {"Time":>8} {"Spot":>8} {"LIS":>8} {"Gap":>5} {"SpDrop":>7} {"LIS+":>5} {"Paradigm":>15} {"Bounce":>7} {"DD":>6} {"10pt?":>5}')
    print('-' * 105)

    wins = 0
    total_bounce = 0
    total_dd = 0

    for c in deduped:
        win = c['bounce'] >= 10
        if win:
            wins += 1
        total_bounce += c['bounce']
        total_dd += c['drawdown']

        print(f'{c["day"]:>12} {c["time"]:>8} {c["spot"]:8.1f} {c["lis"]:8.1f} {c["gap"]:5.1f} '
              f'{c["spot_drop"]:+7.1f} {c["lis_change"]:+5.0f} {c["paradigm"]:>15} '
              f'{c["bounce"]:+7.1f} {c["drawdown"]:6.1f} {"YES" if win else "no":>5}')

    n = len(deduped)
    if n:
        print(f'\n--- SUMMARY ---')
        print(f'Signals: {n} | Wins (10pt bounce): {wins} | WR: {wins/n*100:.0f}%')
        print(f'Avg bounce: {total_bounce/n:.1f} pts | Avg drawdown: {total_dd/n:.1f} pts')

        # By paradigm group
        print(f'\n--- BY PARADIGM ---')
        by_para = defaultdict(list)
        for c in deduped:
            p = c['paradigm'] or '?'
            if 'GEX' in p:
                key = 'GEX-*'
            elif 'BOFA' in p or 'BofA' in p:
                key = 'BofA-*'
            elif 'AG' in p:
                key = 'AG-*'
            elif 'SIDIAL' in p:
                key = 'SIDIAL-*'
            else:
                key = p
            by_para[key].append(c)

        for para, signals in sorted(by_para.items()):
            w = sum(1 for s in signals if s['bounce'] >= 10)
            avg_b = sum(s['bounce'] for s in signals) / len(signals)
            avg_d = sum(s['drawdown'] for s in signals) / len(signals)
            print(f'  {para:>12}: {len(signals)} signals, {w}W, WR={w/len(signals)*100:.0f}%, avg_bounce={avg_b:.1f}, avg_dd={avg_d:.1f}')

        # By gap size
        print(f'\n--- BY GAP SIZE AT SIGNAL ---')
        for label, lo, hi in [('gap <= 3', -3, 3), ('gap 4-7', 4, 7), ('gap 8-12', 8, 12)]:
            subset = [c for c in deduped if lo <= c['gap'] <= hi]
            if subset:
                w = sum(1 for s in subset if s['bounce'] >= 10)
                avg_b = sum(s['bounce'] for s in subset) / len(subset)
                print(f'  {label}: {len(subset)} signals, {w}W, WR={w/len(subset)*100:.0f}%, avg_bounce={avg_b:.1f}')

        # By charm sign
        print(f'\n--- BY CHARM ---')
        pos_charm = [c for c in deduped if c['charm'] and c['charm'] > 0]
        neg_charm = [c for c in deduped if c['charm'] and c['charm'] < 0]
        if pos_charm:
            w = sum(1 for s in pos_charm if s['bounce'] >= 10)
            print(f'  Charm > 0: {len(pos_charm)} signals, {w}W, WR={w/len(pos_charm)*100:.0f}%')
        if neg_charm:
            w = sum(1 for s in neg_charm if s['bounce'] >= 10)
            print(f'  Charm < 0: {len(neg_charm)} signals, {w}W, WR={w/len(neg_charm)*100:.0f}%')

        # By time of day (ET = UTC - 5)
        print(f'\n--- BY TIME OF DAY (ET) ---')
        for label, h_start, h_end in [('Morning 9:30-11', 14, 16), ('Midday 11-13', 16, 18), ('Afternoon 13-15:30', 18, 21)]:
            subset = [c for c in deduped if h_start <= int(c['time'][:2]) < h_end]
            if subset:
                w = sum(1 for s in subset if s['bounce'] >= 10)
                avg_b = sum(s['bounce'] for s in subset) / len(subset)
                print(f'  {label}: {len(subset)} signals, {w}W, WR={w/len(subset)*100:.0f}%, avg_bounce={avg_b:.1f}')

        # Simulated P&L: 10pt target, 12pt stop
        print(f'\n--- SIMULATED P&L (target=10, stop=12) ---')
        sim_pnl = 0
        sim_wins = 0
        sim_losses = 0
        for c in deduped:
            if c['drawdown'] >= 12:
                # Stop hit first? Check if bounce reached 10 before drawdown reached 12
                # We don't have bar-by-bar, so use heuristic: if bounce >= 10, assume target hit first
                if c['bounce'] >= 10:
                    sim_pnl += 10
                    sim_wins += 1
                else:
                    sim_pnl -= 12
                    sim_losses += 1
            elif c['bounce'] >= 10:
                sim_pnl += 10
                sim_wins += 1
            else:
                # Neither target nor stop hit in 30 min -- expires
                sim_pnl += c['bounce'] - c['drawdown']  # net at expiry
                sim_losses += 1
        print(f'  {sim_wins}W / {sim_losses}L | Net: {sim_pnl:+.1f} pts')
    else:
        print('No convergence signals found')
