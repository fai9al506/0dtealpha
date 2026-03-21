"""
Vol Spike v4 -- User's exact filters:
1. OTM Only
2. Premium > $50,000 (vol_delta * mid * 100)
3. Volume spike > 1,000 contracts in 2-min window
4. 0DTE (all our data is 0DTE)
5. OTM but not more than 2% from spot
"""
import sys
import psycopg2
import psycopg2.extras
import json
from collections import defaultdict

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

C_VOL=0; C_OI=1; C_BID=5; C_ASK=7; C_LAST=9; C_DELTA=4; C_IV=2
STRIKE=10
P_BID=14; P_ASK=12; P_LAST=11; P_DELTA=16; P_IV=18; P_VOL=20; P_OI=19

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
            sf(row[C_LAST]), sf(row[C_DELTA]), sf(row[C_IV]),
            si(row[P_VOL]), si(row[P_OI]), sf(row[P_BID]), sf(row[P_ASK]),
            sf(row[P_LAST]), sf(row[P_DELTA]), sf(row[P_IV]),
        )
    return data
    # call: 0=vol,1=oi,2=bid,3=ask,4=last,5=delta,6=iv
    # put:  7=vol,8=oi,9=bid,10=ask,11=last,12=delta,13=iv


def process_day(snaps):
    if len(snaps) < 2:
        return []

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
        if spot <= 0: continue

        gap_min = (ts - prev['ts']).total_seconds() / 60
        if gap_min > 5: continue

        # Market hours 9:30-15:45
        h = ts.hour; m = ts.minute
        t_min = h * 60 + m
        if t_min < 9*60+30 or t_min > 15*60+45: continue

        max_otm = spot * 0.02  # 2% of spot

        for strike in curr['data']:
            if strike not in prev['data']: continue
            c = curr['data'][strike]
            p = prev['data'][strike]

            # === CALL side ===
            cvd = c[0] - p[0]
            if cvd >= 1000:
                # OTM call: strike > spot
                if strike > spot:
                    dist = strike - spot
                    if dist <= max_otm:
                        mid = (c[2] + c[3]) / 2 if c[3] > 0 else c[4]
                        if mid > 0:
                            prem = cvd * mid * 100
                            if prem >= 50000:
                                # Track forward
                                fwd = _track(parsed, i, strike, 'CALL', c[3])
                                results.append({
                                    'ts': ts, 'date': ts.date(),
                                    'strike': strike, 'side': 'CALL', 'spot': spot,
                                    'vol_delta': cvd, 'oi': c[1],
                                    'premium': prem,
                                    'entry_ask': c[3], 'entry_mid': mid,
                                    'delta': c[5], 'iv': c[6],
                                    'dist_pts': round(dist, 1),
                                    'dist_pct': round(dist/spot*100, 2),
                                    'time_min': t_min,
                                    **fwd,
                                })

            # === PUT side ===
            pvd = c[7] - p[7]
            if pvd >= 1000:
                # OTM put: strike < spot
                if strike < spot:
                    dist = spot - strike
                    if dist <= max_otm:
                        mid = (c[9] + c[10]) / 2 if c[10] > 0 else c[11]
                        if mid > 0:
                            prem = pvd * mid * 100
                            if prem >= 50000:
                                fwd = _track(parsed, i, strike, 'PUT', c[10])
                                results.append({
                                    'ts': ts, 'date': ts.date(),
                                    'strike': strike, 'side': 'PUT', 'spot': spot,
                                    'vol_delta': pvd, 'oi': c[8],
                                    'premium': prem,
                                    'entry_ask': c[10], 'entry_mid': mid,
                                    'delta': c[12], 'iv': c[13],
                                    'dist_pts': round(dist, 1),
                                    'dist_pct': round(dist/spot*100, 2),
                                    'time_min': t_min,
                                    **fwd,
                                })

    return results


def _track(parsed, start_idx, strike, side, entry_price):
    """Track forward from parsed snapshots."""
    if entry_price <= 0:
        return {'max_profit': 0, 'max_loss': 0, 'exit_prices': {}, 'final_pnl': 0}

    ts0 = parsed[start_idx]['ts']
    max_profit = 0.0; max_loss = 0.0
    exit_prices = {}
    last_pnl = 0.0

    for j in range(start_idx + 1, len(parsed)):
        elapsed = (parsed[j]['ts'] - ts0).total_seconds() / 60
        fdata = parsed[j]['data']
        if strike not in fdata: continue

        fc = fdata[strike]
        price = fc[2] if side == 'CALL' else fc[9]  # bid to exit
        if price < 0: continue

        pnl = (price - entry_price) * 100
        pct = (price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        max_profit = max(max_profit, pnl)
        max_loss = min(max_loss, pnl)
        last_pnl = pnl

        for mins in [2, 5, 10, 15, 20, 30, 45, 60, 90]:
            if mins not in exit_prices and elapsed >= mins:
                exit_prices[mins] = {'pnl': round(pnl, 2), 'pct': round(pct, 1), 'price': price}

    return {
        'max_profit': round(max_profit, 2),
        'max_loss': round(max_loss, 2),
        'exit_prices': exit_prices,
        'final_pnl': round(last_pnl, 2),
    }


def eval_set(results, hold_min=15):
    pnls = []
    for r in results:
        if hold_min in r['exit_prices']:
            pnls.append(r['exit_prices'][hold_min]['pnl'])
        elif r['exit_prices']:
            pnls.append(r['exit_prices'][max(r['exit_prices'].keys())]['pnl'])
        else:
            pnls.append(0)
    if not pnls: return None
    w = sum(1 for p in pnls if p > 0)
    l = sum(1 for p in pnls if p < 0)
    t = sum(pnls)
    return {'n': len(pnls), 'w': w, 'l': l, 'wr': w/(w+l)*100 if (w+l) else 0,
            'total': t, 'avg': t/len(pnls)}


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 110)
    print("VOL SPIKE v4 -- User Filters: OTM, Vol>1000, Prem>$50K, <2% from spot")
    print("=" * 110)
    sys.stdout.flush()

    cur.execute("""
        SELECT DISTINCT ts::date as d, COUNT(*) as n
        FROM chain_snapshots GROUP BY ts::date ORDER BY d
    """)
    dates = cur.fetchall()
    print(f"Dates: {len(dates)}\n")

    all_results = []
    for di, d_row in enumerate(dates):
        trade_date = d_row['d']
        print(f"  [{di+1}/{len(dates)}] {trade_date}...", end=" ", flush=True)

        cur.execute("""
            SELECT ts, spot, rows FROM chain_snapshots
            WHERE ts::date = %s ORDER BY ts ASC
        """, (trade_date,))
        snaps = cur.fetchall()
        day_res = process_day(snaps)
        all_results.extend(day_res)
        print(f"{len(day_res)} signals")
        sys.stdout.flush()

    conn.close()

    print(f"\n{'='*110}")
    print(f"TOTAL SIGNALS: {len(all_results)}")
    print(f"{'='*110}")

    if not all_results:
        print("No signals found with these filters!")
        return

    # Print every signal
    daily_pnl = defaultdict(list)

    print(f"\n{'Date':<14} {'Side':<5} {'Strike':>7} {'Spot':>7} {'Dist':>5} {'%OTM':>5} "
          f"{'VolD':>7} {'Prem$':>10} {'Entry$':>7} {'Delta':>6} {'IV':>5} "
          f"{'5m$':>7} {'10m$':>7} {'15m$':>7} {'30m$':>7} {'MaxP$':>7} {'MaxL$':>7}")
    print("-" * 140)

    for r in all_results:
        ts_str = r['ts'].strftime('%m/%d %H:%M')
        p5 = r['exit_prices'].get(5, {}).get('pnl', '--')
        p10 = r['exit_prices'].get(10, {}).get('pnl', '--')
        p15 = r['exit_prices'].get(15, {}).get('pnl', '--')
        p30 = r['exit_prices'].get(30, {}).get('pnl', '--')

        # Use 15min P&L for daily tracking
        pnl_val = r['exit_prices'].get(15, {}).get('pnl', r['final_pnl'])
        daily_pnl[r['date']].append(pnl_val)

        marker = '+' if isinstance(pnl_val, (int, float)) and pnl_val > 0 else '-' if isinstance(pnl_val, (int, float)) and pnl_val < 0 else ' '

        def fmt(v):
            if isinstance(v, (int, float)): return f"{v:>7.0f}"
            return f"{'--':>7}"

        print(f"{marker} {ts_str:<12} {r['side']:<5} {r['strike']:>7.0f} {r['spot']:>7.0f} "
              f"{r['dist_pts']:>5.0f} {r['dist_pct']:>4.1f}% "
              f"{r['vol_delta']:>7,} ${r['premium']/1000:>8.0f}K {r['entry_ask']:>7.2f} "
              f"{r['delta']:>6.2f} {r['iv']:>4.0%} "
              f"{fmt(p5)} {fmt(p10)} {fmt(p15)} {fmt(p30)} "
              f"{r['max_profit']:>7.0f} {r['max_loss']:>7.0f}")

    # ── Summary by hold time ──
    print(f"\n{'='*110}")
    print("P&L SUMMARY BY HOLD TIME")
    print(f"{'='*110}")

    for hold in [2, 5, 10, 15, 20, 30, 45, 60]:
        e = eval_set(all_results, hold)
        if e and e['n'] >= 3:
            print(f"  {hold:3d}min: {e['n']:>4} trades | {e['w']}W/{e['l']}L "
                  f"WR={e['wr']:.0f}% | Total ${e['total']:>10,.0f} Avg ${e['avg']:>6,.0f}")

    # ── By side ──
    print(f"\n  BY SIDE (15min):")
    for side in ['CALL', 'PUT']:
        sr = [r for r in all_results if r['side'] == side]
        e = eval_set(sr, 15)
        if e and e['n'] >= 2:
            print(f"    {side}: {e['n']} | {e['w']}W/{e['l']}L WR={e['wr']:.0f}% | ${e['total']:,.0f}")

    # ── By distance OTM ──
    print(f"\n  BY DISTANCE OTM (15min):")
    for lo, hi, label in [(0, 0.3, "<0.3%"), (0.3, 0.5, "0.3-0.5%"),
                           (0.5, 1.0, "0.5-1%"), (1.0, 1.5, "1-1.5%"), (1.5, 2.0, "1.5-2%")]:
        sr = [r for r in all_results if lo <= r['dist_pct'] < hi]
        e = eval_set(sr, 15)
        if e and e['n'] >= 2:
            print(f"    {label}: {e['n']} | {e['w']}W/{e['l']}L WR={e['wr']:.0f}% | ${e['total']:,.0f}")

    # ── By premium size ──
    print(f"\n  BY PREMIUM (15min):")
    for lo, hi, label in [(50, 100, "$50-100K"), (100, 250, "$100-250K"),
                           (250, 500, "$250-500K"), (500, 1000, "$500K-1M"),
                           (1000, 99999, "$1M+")]:
        sr = [r for r in all_results if lo*1000 <= r['premium'] < hi*1000]
        e = eval_set(sr, 15)
        if e and e['n'] >= 2:
            print(f"    {label}: {e['n']} | {e['w']}W/{e['l']}L WR={e['wr']:.0f}% | ${e['total']:,.0f}")

    # ── By volume spike size ──
    print(f"\n  BY VOLUME SPIKE SIZE (15min):")
    for lo, hi, label in [(1000, 2000, "1-2K"), (2000, 5000, "2-5K"),
                           (5000, 10000, "5-10K"), (10000, 999999, "10K+")]:
        sr = [r for r in all_results if lo <= r['vol_delta'] < hi]
        e = eval_set(sr, 15)
        if e and e['n'] >= 2:
            print(f"    {label}: {e['n']} | {e['w']}W/{e['l']}L WR={e['wr']:.0f}% | ${e['total']:,.0f}")

    # ── By time of day ──
    print(f"\n  BY TIME OF DAY (15min):")
    for sh, eh, label in [(9, 10, "9:30-10"), (10, 11, "10-11"), (11, 12, "11-12"),
                           (12, 13, "12-13"), (13, 14, "13-14"), (14, 15, "14-15"),
                           (15, 16, "15-16")]:
        sr = [r for r in all_results if sh <= r['ts'].hour < eh]
        e = eval_set(sr, 15)
        if e and e['n'] >= 2:
            print(f"    {label}: {e['n']} | {e['w']}W/{e['l']}L WR={e['wr']:.0f}% | ${e['total']:,.0f}")

    # ── By delta ──
    print(f"\n  BY DELTA (15min):")
    for lo, hi, label in [(0, 0.15, "|D|<0.15 (deep OTM)"), (0.15, 0.30, "|D|0.15-0.30"),
                           (0.30, 0.45, "|D|0.30-0.45"), (0.45, 1.0, "|D|>0.45 (near ATM)")]:
        sr = [r for r in all_results if lo <= abs(r['delta']) < hi]
        e = eval_set(sr, 15)
        if e and e['n'] >= 2:
            print(f"    {label}: {e['n']} | {e['w']}W/{e['l']}L WR={e['wr']:.0f}% | ${e['total']:,.0f}")

    # ── Daily P&L ──
    print(f"\n{'='*110}")
    print("DAILY P&L (15min hold)")
    print(f"{'='*110}")

    running = 0; win_d = 0; loss_d = 0
    for d in sorted(daily_pnl.keys()):
        day_total = sum(daily_pnl[d])
        running += day_total
        n_trades = len(daily_pnl[d])
        if day_total > 0: win_d += 1
        elif day_total < 0: loss_d += 1
        marker = '+' if day_total > 0 else '-' if day_total < 0 else ' '
        print(f"  {marker} {d}: {n_trades} trades | ${day_total:>8,.0f} | Running: ${running:>10,.0f}")

    print(f"\n  Win days: {win_d} | Loss days: {loss_d} | "
          f"Day WR: {win_d/(win_d+loss_d)*100:.0f}%" if (win_d+loss_d) > 0 else "")

    # ── Max Favorable Excursion ──
    print(f"\n{'='*110}")
    print("MAX FAVORABLE EXCURSION")
    print(f"{'='*110}")

    mfes = [r['max_profit'] for r in all_results]
    if mfes:
        print(f"  Avg MFE: ${sum(mfes)/len(mfes):,.0f}")
        print(f"  Med MFE: ${sorted(mfes)[len(mfes)//2]:,.0f}")
        pct_reach_100 = sum(1 for m in mfes if m >= 100) / len(mfes) * 100
        pct_reach_500 = sum(1 for m in mfes if m >= 500) / len(mfes) * 100
        pct_reach_1000 = sum(1 for m in mfes if m >= 1000) / len(mfes) * 100
        print(f"  Reach $100+: {pct_reach_100:.0f}% | $500+: {pct_reach_500:.0f}% | $1000+: {pct_reach_1000:.0f}%")

    # ── Frequency ──
    trading_days = len([d for d in daily_pnl.keys()])
    if trading_days > 0:
        print(f"\n  Frequency: {len(all_results)} signals across {trading_days} days "
              f"= {len(all_results)/trading_days:.1f} signals/day")

    print(f"\n{'='*110}")
    print("DONE")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
