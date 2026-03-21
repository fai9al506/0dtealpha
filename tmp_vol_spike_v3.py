"""
Vol Spike v3 -- Pre-compute forward prices, then filter in memory.

Key optimization: track forward price for EVERY spike as we process each day's
snapshots (since we already have them loaded). Then filter/evaluate offline.
"""
import sys
import psycopg2
import psycopg2.extras
import json
from collections import defaultdict

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

C_VOL=0; C_OI=1; C_IV=2; C_GAMMA=3; C_DELTA=4; C_BID=5; C_ASK=7; C_LAST=9
STRIKE=10
P_LAST=11; P_ASK=12; P_BID=14; P_DELTA=16; P_GAMMA=17; P_IV=18; P_OI=19; P_VOL=20

def sf(v, d=0.0):
    try: return float(v) if v is not None else d
    except: return d
def si(v, d=0):
    try: return int(float(v)) if v is not None else d
    except: return d

def parse_snap(rows_json):
    rows = json.loads(rows_json) if isinstance(rows_json, str) else rows_json
    data = {}
    for row in rows:
        strike = sf(row[STRIKE])
        if strike <= 0: continue
        data[strike] = (
            si(row[C_VOL]), si(row[C_OI]), sf(row[C_BID]), sf(row[C_ASK]),
            sf(row[C_LAST]), sf(row[C_DELTA]), sf(row[C_IV]), sf(row[C_GAMMA]),
            si(row[P_VOL]), si(row[P_OI]), sf(row[P_BID]), sf(row[P_ASK]),
            sf(row[P_LAST]), sf(row[P_DELTA]), sf(row[P_IV]), sf(row[P_GAMMA]),
        )
    return data


def process_day(snaps):
    """Process a full day: detect spikes AND compute forward P&L inline."""
    if len(snaps) < 3:
        return []

    # Parse all snapshots upfront
    parsed = []
    for snap in snaps:
        parsed.append({
            'ts': snap['ts'],
            'spot': sf(snap['spot']),
            'data': parse_snap(snap['rows']),
        })

    results = []

    for i in range(1, len(parsed)):
        curr = parsed[i]
        prev = parsed[i-1]
        ts = curr['ts']
        spot = curr['spot']

        gap_min = (ts - prev['ts']).total_seconds() / 60
        if gap_min > 5:
            continue

        # Market hours (9:30 - 15:45 ET)
        hour = ts.hour if hasattr(ts, 'hour') else 0
        minute = ts.minute if hasattr(ts, 'minute') else 0
        t_min = hour * 60 + minute
        if t_min < 9*60+30 or t_min > 15*60+45:
            continue

        # Detect spikes this snapshot
        call_spikes = {}
        put_spikes = {}

        for strike in curr['data']:
            if strike not in prev['data']:
                continue
            c = curr['data'][strike]
            p = prev['data'][strike]

            # Call vol delta
            cvd = c[0] - p[0]
            if cvd > 50:
                mid = (c[2] + c[3]) / 2 if c[3] > 0 else c[4]
                if mid > 0.10:
                    call_spikes[strike] = {
                        'vol_delta': cvd, 'premium': cvd * mid * 100,
                        'ask': c[3], 'bid': c[2], 'mid': mid,
                        'delta': c[5], 'iv': c[6], 'oi': c[1],
                    }

            # Put vol delta
            pvd = c[8] - p[8]
            if pvd > 50:
                mid = (c[10] + c[11]) / 2 if c[11] > 0 else c[12]
                if mid > 0.10:
                    put_spikes[strike] = {
                        'vol_delta': pvd, 'premium': pvd * mid * 100,
                        'ask': c[11], 'bid': c[10], 'mid': mid,
                        'delta': c[13], 'iv': c[14], 'oi': c[9],
                    }

        # Spread detection
        spread_strikes = set(call_spikes.keys()) & set(put_spikes.keys())

        # For each spike, compute forward P&L from remaining snapshots
        all_spikes = []
        for strike, info in call_spikes.items():
            all_spikes.append((strike, 'CALL', info, strike in spread_strikes))
        for strike, info in put_spikes.items():
            all_spikes.append((strike, 'PUT', info, strike in spread_strikes))

        for strike, side, info, is_spread in all_spikes:
            entry_price = info['ask']
            if entry_price <= 0:
                continue

            moneyness = strike - spot
            # OTM check: CALL OTM = strike > spot, PUT OTM = strike < spot
            is_otm = (side == 'CALL' and moneyness > 0) or (side == 'PUT' and moneyness < 0)

            # Track forward from remaining snapshots
            max_profit = 0.0
            max_loss = 0.0
            exit_prices = {}

            for j in range(i+1, len(parsed)):
                elapsed = (parsed[j]['ts'] - ts).total_seconds() / 60
                fdata = parsed[j]['data']

                if strike not in fdata:
                    continue

                fc = fdata[strike]
                if side == 'CALL':
                    cur_price = fc[2]  # call bid
                else:
                    cur_price = fc[10]  # put bid

                if cur_price < 0:
                    continue

                pnl = (cur_price - entry_price) * 100
                pct = (cur_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                max_profit = max(max_profit, pnl)
                max_loss = min(max_loss, pnl)

                for mins in [2, 5, 10, 15, 20, 30, 45, 60, 90]:
                    if mins not in exit_prices and elapsed >= mins:
                        exit_prices[mins] = {'pnl': round(pnl, 2), 'pct': round(pct, 1),
                                              'price': cur_price}

            results.append({
                'ts': ts,
                'date': ts.date(),
                'strike': strike,
                'side': side,
                'spot': spot,
                'vol_delta': info['vol_delta'],
                'oi': info['oi'],
                'premium': info['premium'],
                'entry_ask': entry_price,
                'entry_mid': info['mid'],
                'delta': info['delta'],
                'iv': info['iv'],
                'moneyness': round(moneyness, 1),
                'abs_moneyness': abs(moneyness),
                'is_otm': is_otm,
                'is_spread': is_spread,
                'time_min': t_min,
                'max_profit': round(max_profit, 2),
                'max_loss': round(max_loss, 2),
                'exit_prices': exit_prices,
            })

    return results


def eval_set(results, hold_min=15):
    """Evaluate a set of results."""
    pnls = []
    for r in results:
        if hold_min in r['exit_prices']:
            pnls.append(r['exit_prices'][hold_min]['pnl'])
        elif r['exit_prices']:
            # Use last available
            last_min = max(r['exit_prices'].keys())
            pnls.append(r['exit_prices'][last_min]['pnl'])
        else:
            pnls.append(0)

    if not pnls:
        return None

    w = sum(1 for p in pnls if p > 0)
    l = sum(1 for p in pnls if p < 0)
    t = sum(pnls)
    avg = t / len(pnls)
    wr = w / (w+l) * 100 if (w+l) > 0 else 0
    return {'n': len(pnls), 'w': w, 'l': l, 'wr': wr, 'total': t, 'avg': avg}


def pr(label, e, prefix="  "):
    if e is None or e['n'] < 3:
        return
    print(f"{prefix}{label:<55} {e['n']:>4} | "
          f"{e['w']}W/{e['l']}L WR={e['wr']:.0f}% | "
          f"PnL ${e['total']:>10,.0f} Avg ${e['avg']:>6,.0f}")


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 110)
    print("VOL SPIKE v3 -- OTM Whale Detection (all forward P&L pre-computed)")
    print("=" * 110)
    sys.stdout.flush()

    cur.execute("""
        SELECT DISTINCT ts::date as d, COUNT(*) as n
        FROM chain_snapshots GROUP BY ts::date ORDER BY d
    """)
    dates = cur.fetchall()
    print(f"Dates: {len(dates)}\n")
    sys.stdout.flush()

    all_results = []

    for di, d_row in enumerate(dates):
        trade_date = d_row['d']
        print(f"  [{di+1}/{len(dates)}] {trade_date}...", end=" ", flush=True)

        cur.execute("""
            SELECT ts, spot, rows FROM chain_snapshots
            WHERE ts::date = %s ORDER BY ts ASC
        """, (trade_date,))
        snaps = cur.fetchall()

        day_results = process_day(snaps)
        all_results.extend(day_results)
        print(f"{len(day_results)} spikes+fwd")
        sys.stdout.flush()

    conn.close()

    print(f"\nTotal spikes with forward P&L: {len(all_results):,}")

    # ============================================================
    # ANALYSIS (all in memory, instant)
    # ============================================================

    # First: apply OTM filter as user requested
    otm_all = [r for r in all_results if r['is_otm']]
    print(f"OTM only: {len(otm_all):,} ({len(otm_all)/len(all_results)*100:.0f}%)")

    print(f"\n{'='*110}")
    print("FILTER GRID -- OTM Only, dedup by (date, strike, side), top 3/day by premium")
    print(f"{'='*110}")

    def select_top(pool, top_n=3):
        """Dedup by (date,strike,side), then top N per day by premium."""
        best = {}
        for s in pool:
            key = (s['date'], s['strike'], s['side'])
            if key not in best or s['premium'] > best[key]['premium']:
                best[key] = s

        by_date = defaultdict(list)
        for s in best.values():
            by_date[s['date']].append(s)

        selected = []
        for d in sorted(by_date.keys()):
            day_sigs = sorted(by_date[d], key=lambda x: x['premium'], reverse=True)[:top_n]
            selected.extend(day_sigs)
        return selected

    for hold in [10, 15, 20, 30]:
        print(f"\n--- {hold}-min hold ---\n")

        # Grid of filters
        configs = [
            ("ALL OTM, prem>=$100K",
             lambda r: r['premium'] >= 100000),
            ("ALL OTM, prem>=$250K",
             lambda r: r['premium'] >= 250000),
            ("ALL OTM, prem>=$500K",
             lambda r: r['premium'] >= 500000),
            ("ALL OTM, prem>=$1M",
             lambda r: r['premium'] >= 1000000),
            ("OTM 5-15pts, prem>=$100K",
             lambda r: 5 <= r['abs_moneyness'] < 15 and r['premium'] >= 100000),
            ("OTM 5-15pts, prem>=$250K",
             lambda r: 5 <= r['abs_moneyness'] < 15 and r['premium'] >= 250000),
            ("OTM 10-30pts, prem>=$100K",
             lambda r: 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 100000),
            ("OTM 10-30pts, prem>=$250K",
             lambda r: 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("OTM 10-30pts, prem>=$500K",
             lambda r: 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 500000),
            ("OTM 10-30pts, prem>=$1M",
             lambda r: 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 1000000),
            ("OTM 15-50pts, prem>=$250K",
             lambda r: 15 <= r['abs_moneyness'] <= 50 and r['premium'] >= 250000),
            ("OTM 15-50pts, prem>=$500K",
             lambda r: 15 <= r['abs_moneyness'] <= 50 and r['premium'] >= 500000),
            ("OTM 15-50pts, prem>=$1M",
             lambda r: 15 <= r['abs_moneyness'] <= 50 and r['premium'] >= 1000000),
            ("OTM 20-60pts, prem>=$250K",
             lambda r: 20 <= r['abs_moneyness'] <= 60 and r['premium'] >= 250000),
            ("OTM 20-60pts, prem>=$500K",
             lambda r: 20 <= r['abs_moneyness'] <= 60 and r['premium'] >= 500000),
            ("Non-spread OTM, prem>=$250K",
             lambda r: not r['is_spread'] and r['premium'] >= 250000),
            ("Non-spread OTM 10-30, prem>=$250K",
             lambda r: not r['is_spread'] and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("Non-spread OTM 10-30, prem>=$500K",
             lambda r: not r['is_spread'] and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 500000),
            ("CALL OTM 10-30, prem>=$250K",
             lambda r: r['side'] == 'CALL' and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("PUT OTM 10-30, prem>=$250K",
             lambda r: r['side'] == 'PUT' and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("Morning OTM 10-30, prem>=$250K (9:30-11)",
             lambda r: r['time_min'] < 11*60 and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("Midday OTM 10-30, prem>=$250K (11-14)",
             lambda r: 11*60 <= r['time_min'] < 14*60 and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("Afternoon OTM 10-30, prem>=$250K (14+)",
             lambda r: r['time_min'] >= 14*60 and 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000),
            ("Vol>2x OI, OTM, prem>=$100K",
             lambda r: r['oi'] > 0 and r['vol_delta']/r['oi'] > 2 and r['premium'] >= 100000),
            ("Vol>5x OI, OTM, prem>=$100K",
             lambda r: r['oi'] > 0 and r['vol_delta']/r['oi'] > 5 and r['premium'] >= 100000),
            ("Vol>10x OI, OTM, prem>=$100K",
             lambda r: r['oi'] > 0 and r['vol_delta']/r['oi'] > 10 and r['premium'] >= 100000),
            ("Mega whale OTM prem>=$5M",
             lambda r: r['premium'] >= 5000000),
            ("Mega whale OTM prem>=$10M",
             lambda r: r['premium'] >= 10000000),
            # Delta-based (true OTM conviction)
            ("|Delta| 0.15-0.35, prem>=$250K",
             lambda r: 0.15 <= abs(r['delta']) <= 0.35 and r['premium'] >= 250000),
            ("|Delta| 0.15-0.35, prem>=$500K",
             lambda r: 0.15 <= abs(r['delta']) <= 0.35 and r['premium'] >= 500000),
            ("|Delta| 0.20-0.40, prem>=$250K",
             lambda r: 0.20 <= abs(r['delta']) <= 0.40 and r['premium'] >= 250000),
        ]

        for label, filt in configs:
            pool = [r for r in otm_all if filt(r)]
            selected = select_top(pool, top_n=3)
            e = eval_set(selected, hold)
            pr(label, e)

    # ============================================================
    # DEEP DIVE on best-looking filter
    # ============================================================
    print(f"\n{'='*110}")
    print("DEEP DIVE: OTM 10-30pts, Premium >= $250K, Top 3/day")
    print(f"{'='*110}")

    best_pool = [r for r in otm_all
                 if 10 <= r['abs_moneyness'] <= 30 and r['premium'] >= 250000]
    best_selected = select_top(best_pool, top_n=3)

    print(f"\n{len(best_selected)} trades across {len(set(r['date'] for r in best_selected))} days\n")

    # All trades
    daily_pnl = defaultdict(float)
    for r in best_selected:
        p15 = r['exit_prices'].get(15, {}).get('pnl', 0)
        p15_pct = r['exit_prices'].get(15, {}).get('pct', 0)
        daily_pnl[r['date']] += p15

        marker = '+' if p15 > 0 else '-' if p15 < 0 else ' '
        ts_str = r['ts'].strftime('%m/%d %H:%M')
        print(f"  {marker} {ts_str} {r['side']:<4} {r['strike']:.0f} OTM {r['abs_moneyness']:>3.0f}pts "
              f"| Vol {r['vol_delta']:>6,} Prem ${r['premium']/1000:>6.0f}K "
              f"| Entry ${r['entry_ask']:.2f} D={r['delta']:.2f} "
              f"| 15m: ${p15:>7.0f} ({p15_pct}%) "
              f"| Max+${r['max_profit']:>6.0f} Max-${r['max_loss']:>6.0f} "
              f"| Spread={r['is_spread']}")

    # Daily
    print(f"\n  DAILY P&L (15min hold):")
    running = 0; win_d = 0; loss_d = 0
    for d in sorted(daily_pnl.keys()):
        running += daily_pnl[d]
        if daily_pnl[d] > 0: win_d += 1
        elif daily_pnl[d] < 0: loss_d += 1
        print(f"    {d}: ${daily_pnl[d]:>8,.0f} (running: ${running:>10,.0f})")
    if win_d + loss_d > 0:
        print(f"\n    Win days: {win_d} | Loss days: {loss_d} | Day WR: {win_d/(win_d+loss_d)*100:.0f}%")

    # Hold time sweep
    print(f"\n  HOLD TIME SWEEP:")
    for hold in [5, 10, 15, 20, 30, 45, 60]:
        e = eval_set(best_selected, hold)
        pr(f"{hold}min", e, "    ")

    # By side
    print(f"\n  BY SIDE (15min):")
    for side in ['CALL', 'PUT']:
        e = eval_set([r for r in best_selected if r['side'] == side], 15)
        pr(side, e, "    ")

    # By moneyness
    print(f"\n  BY MONEYNESS (15min):")
    for lo, hi, label in [(10, 15, "10-15pts"), (15, 20, "15-20pts"), (20, 30, "20-30pts")]:
        e = eval_set([r for r in best_selected if lo <= r['abs_moneyness'] < hi], 15)
        pr(label, e, "    ")

    # By premium
    print(f"\n  BY PREMIUM (15min):")
    for lo, hi, label in [(250, 500, "$250-500K"), (500, 1000, "$500K-1M"),
                           (1000, 5000, "$1-5M"), (5000, 99999, "$5M+")]:
        e = eval_set([r for r in best_selected if lo*1000 <= r['premium'] < hi*1000], 15)
        pr(label, e, "    ")

    # By time
    print(f"\n  BY TIME OF DAY (15min):")
    for sh, eh, label in [(9, 11, "9:30-11"), (11, 13, "11-13"), (13, 15, "13-15"), (15, 16, "15-16")]:
        e = eval_set([r for r in best_selected if sh <= r['ts'].hour < eh], 15)
        pr(label, e, "    ")

    # ============================================================
    # DEEP DIVE 2: Delta-based OTM (0.15-0.35)
    # ============================================================
    print(f"\n{'='*110}")
    print("DEEP DIVE 2: |Delta| 0.15-0.35, Premium >= $500K, Top 3/day")
    print(f"{'='*110}")

    delta_pool = [r for r in otm_all
                  if 0.15 <= abs(r['delta']) <= 0.35 and r['premium'] >= 500000]
    delta_selected = select_top(delta_pool, top_n=3)

    print(f"\n{len(delta_selected)} trades\n")

    for r in delta_selected:
        p15 = r['exit_prices'].get(15, {}).get('pnl', 0)
        marker = '+' if p15 > 0 else '-' if p15 < 0 else ' '
        ts_str = r['ts'].strftime('%m/%d %H:%M')
        print(f"  {marker} {ts_str} {r['side']:<4} {r['strike']:.0f} OTM {r['abs_moneyness']:>3.0f}pts "
              f"| Vol {r['vol_delta']:>6,} Prem ${r['premium']/1000:>6.0f}K "
              f"| D={r['delta']:.2f} IV={r['iv']:.0%} "
              f"| 15m: ${p15:>7.0f} Max+${r['max_profit']:>6.0f}")

    for hold in [10, 15, 20, 30]:
        e = eval_set(delta_selected, hold)
        pr(f"{hold}min", e, "    ")

    # ============================================================
    # ITM vs OTM comparison (to validate user's intuition)
    # ============================================================
    print(f"\n{'='*110}")
    print("ITM vs OTM COMPARISON (prem>=$250K, 15min hold)")
    print(f"{'='*110}")

    itm_all = [r for r in all_results if not r['is_otm']]
    for label, pool in [("OTM", otm_all), ("ITM", itm_all)]:
        sub = [r for r in pool if r['premium'] >= 250000]
        sel = select_top(sub, top_n=3)
        e = eval_set(sel, 15)
        pr(f"{label} (prem>=$250K, top 3/day)", e)

    print(f"\n{'='*110}")
    print("DONE")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
