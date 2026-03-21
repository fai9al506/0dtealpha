"""
Vol Spike v5 -- Tighten to 2-6 trades/day. Test filter grid.
OTM CALL = strike > spot (bullish bet)
OTM PUT  = strike < spot (bearish bet)
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


def process_day(snaps):
    """Collect ALL OTM spikes with forward P&L for one day."""
    if len(snaps) < 2: return []

    parsed = []
    for snap in snaps:
        parsed.append({
            'ts': snap['ts'], 'spot': sf(snap['spot']),
            'data': parse_snap(snap['rows']),
        })

    spikes = []
    for i in range(1, len(parsed)):
        curr = parsed[i]; prev = parsed[i-1]
        ts = curr['ts']; spot = curr['spot']
        if spot <= 0: continue

        gap = (ts - prev['ts']).total_seconds() / 60
        if gap > 5: continue

        h = ts.hour; m = ts.minute; t_min = h*60+m
        if t_min < 9*60+30 or t_min > 15*60+45: continue

        for strike in curr['data']:
            if strike not in prev['data']: continue
            c = curr['data'][strike]; p = prev['data'][strike]

            # CALL: OTM = strike > spot
            cvd = c[0] - p[0]
            if cvd >= 500 and strike > spot:
                dist = strike - spot
                mid = (c[2]+c[3])/2 if c[3] > 0 else c[4]
                if mid > 0:
                    prem = cvd * mid * 100
                    fwd = _track(parsed, i, strike, 'CALL', c[3])
                    spikes.append({
                        'ts': ts, 'date': ts.date(), 'strike': strike,
                        'side': 'CALL', 'spot': spot,
                        'vol_delta': cvd, 'oi': c[1], 'premium': prem,
                        'entry_ask': c[3], 'entry_mid': mid,
                        'delta': c[5], 'iv': c[6],
                        'dist_pts': round(dist, 1),
                        'dist_pct': round(dist/spot*100, 2),
                        't_min': t_min, **fwd,
                    })

            # PUT: OTM = strike < spot
            pvd = c[7] - p[7]
            if pvd >= 500 and strike < spot:
                dist = spot - strike
                mid = (c[9]+c[10])/2 if c[10] > 0 else c[11]
                if mid > 0:
                    prem = pvd * mid * 100
                    fwd = _track(parsed, i, strike, 'PUT', c[10])
                    spikes.append({
                        'ts': ts, 'date': ts.date(), 'strike': strike,
                        'side': 'PUT', 'spot': spot,
                        'vol_delta': pvd, 'oi': c[8], 'premium': prem,
                        'entry_ask': c[10], 'entry_mid': mid,
                        'delta': c[12], 'iv': c[13],
                        'dist_pts': round(dist, 1),
                        'dist_pct': round(dist/spot*100, 2),
                        't_min': t_min, **fwd,
                    })

    return spikes


def _track(parsed, start_idx, strike, side, entry_price):
    if entry_price <= 0:
        return {'max_profit': 0, 'max_loss': 0, 'exit_prices': {}, 'final_pnl': 0}
    ts0 = parsed[start_idx]['ts']
    max_p = 0.0; max_l = 0.0; exit_prices = {}; last_pnl = 0.0
    for j in range(start_idx+1, len(parsed)):
        elapsed = (parsed[j]['ts'] - ts0).total_seconds() / 60
        fd = parsed[j]['data']
        if strike not in fd: continue
        fc = fd[strike]
        price = fc[2] if side == 'CALL' else fc[9]
        if price < 0: continue
        pnl = (price - entry_price) * 100
        pct = (price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        max_p = max(max_p, pnl); max_l = min(max_l, pnl); last_pnl = pnl
        for mins in [2, 5, 10, 15, 20, 30, 45, 60]:
            if mins not in exit_prices and elapsed >= mins:
                exit_prices[mins] = {'pnl': round(pnl, 2), 'pct': round(pct, 1), 'price': price}
    return {'max_profit': round(max_p, 2), 'max_loss': round(max_l, 2),
            'exit_prices': exit_prices, 'final_pnl': round(last_pnl, 2)}


def select_top(pool, top_n=3):
    """Dedup by (date,strike,side) keeping biggest premium, then top N/day."""
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
        day = sorted(by_date[d], key=lambda x: x['premium'], reverse=True)[:top_n]
        selected.extend(day)
    return selected


def ev(results, hold=15):
    pnls = []
    for r in results:
        if hold in r['exit_prices']:
            pnls.append(r['exit_prices'][hold]['pnl'])
        elif r['exit_prices']:
            pnls.append(r['exit_prices'][max(r['exit_prices'].keys())]['pnl'])
        else:
            pnls.append(0)
    if not pnls: return None
    w = sum(1 for p in pnls if p > 0)
    l = sum(1 for p in pnls if p < 0)
    t = sum(pnls)
    return {'n': len(pnls), 'w': w, 'l': l, 'wr': w/(w+l)*100 if (w+l) else 0,
            'total': t, 'avg': t/len(pnls),
            'days': len(set(r['date'] for r in results)),
            'per_day': len(pnls) / max(1, len(set(r['date'] for r in results)))}


def pr(label, e, prefix="  "):
    if e is None or e['n'] < 3: return
    print(f"{prefix}{label:<60} {e['n']:>4} ({e['per_day']:.1f}/day) | "
          f"{e['w']}W/{e['l']}L WR={e['wr']:.0f}% | "
          f"${e['total']:>10,.0f} Avg ${e['avg']:>6,.0f}")


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 120)
    print("VOL SPIKE v5 -- Tighten to 2-6 trades/day")
    print("=" * 120)
    sys.stdout.flush()

    cur.execute("""
        SELECT DISTINCT ts::date as d, COUNT(*) as n
        FROM chain_snapshots GROUP BY ts::date ORDER BY d
    """)
    dates = cur.fetchall()
    print(f"Dates: {len(dates)}\n")

    all_spikes = []
    for di, d_row in enumerate(dates):
        td = d_row['d']
        print(f"  [{di+1}/{len(dates)}] {td}...", end=" ", flush=True)
        cur.execute("SELECT ts, spot, rows FROM chain_snapshots WHERE ts::date=%s ORDER BY ts", (td,))
        snaps = cur.fetchall()
        day = process_day(snaps)
        all_spikes.extend(day)
        print(f"{len(day)} raw OTM spikes")
        sys.stdout.flush()

    conn.close()
    print(f"\nTotal raw OTM spikes (vol>=500): {len(all_spikes):,}")

    # =========================================================================
    # FILTER GRID -- find combos that give 2-6 trades/day
    # =========================================================================
    print(f"\n{'='*120}")
    print("FILTER GRID (targeting 2-6 trades/day)")
    print(f"{'='*120}")

    for hold in [10, 15, 20, 30]:
        print(f"\n--- {hold}-min hold ---\n")

        configs = []

        # Vary: min_vol, min_prem, max_dist_pct, top_n_per_day
        for min_vol in [1000, 2000, 3000, 5000]:
            for min_prem in [100000, 250000, 500000, 1000000]:
                for max_pct in [0.5, 1.0, 1.5, 2.0]:
                    for top_n in [2, 3, 5]:
                        pool = [s for s in all_spikes
                                if s['vol_delta'] >= min_vol
                                and s['premium'] >= min_prem
                                and s['dist_pct'] <= max_pct]
                        sel = select_top(pool, top_n)
                        if len(sel) < 10: continue
                        e = ev(sel, hold)
                        if e is None: continue
                        # Only show combos with 1.5-7 trades/day
                        if not (1.5 <= e['per_day'] <= 7): continue
                        configs.append((
                            f"vol>={min_vol} prem>=${min_prem//1000}K "
                            f"otm<={max_pct}% top{top_n}/day",
                            e
                        ))

        # Sort by total PnL descending
        configs.sort(key=lambda x: x[1]['total'], reverse=True)

        # Show top 20
        for label, e in configs[:20]:
            pr(label, e)

    # =========================================================================
    # DEEP DIVE on best filter
    # =========================================================================
    # Find the best combo at 15min
    best_configs_15 = []
    for min_vol in [1000, 2000, 3000, 5000]:
        for min_prem in [100000, 250000, 500000, 1000000]:
            for max_pct in [0.5, 1.0, 1.5, 2.0]:
                for top_n in [2, 3, 5]:
                    pool = [s for s in all_spikes
                            if s['vol_delta'] >= min_vol
                            and s['premium'] >= min_prem
                            and s['dist_pct'] <= max_pct]
                    sel = select_top(pool, top_n)
                    if len(sel) < 10: continue
                    e = ev(sel, 15)
                    if e and 1.5 <= e['per_day'] <= 7:
                        best_configs_15.append((min_vol, min_prem, max_pct, top_n, sel, e))

    if best_configs_15:
        best_configs_15.sort(key=lambda x: x[5]['total'], reverse=True)
        bv, bp, bm, bn, bsel, be = best_configs_15[0]

        print(f"\n{'='*120}")
        print(f"DEEP DIVE: BEST FILTER -- vol>={bv} prem>=${bp//1000}K otm<={bm}% top{bn}/day")
        print(f"{'='*120}")
        print(f"{be['n']} trades across {be['days']} days ({be['per_day']:.1f}/day)")

        # Print each trade
        daily_pnl = defaultdict(float)
        for r in bsel:
            p = r['exit_prices'].get(15, {}).get('pnl', r['final_pnl'])
            pp = r['exit_prices'].get(15, {}).get('pct', '--')
            daily_pnl[r['date']] += p
            m = '+' if p > 0 else '-' if p < 0 else ' '
            ts_str = r['ts'].strftime('%m/%d %H:%M')
            print(f"  {m} {ts_str} {r['side']:<4} {r['strike']:.0f} "
                  f"OTM {r['dist_pts']:>4.0f}pts ({r['dist_pct']:.1f}%) "
                  f"| Vol {r['vol_delta']:>6,} Prem ${r['premium']/1000:>7.0f}K "
                  f"| Ask ${r['entry_ask']:.2f} D={r['delta']:.2f} IV={r['iv']:.0%} "
                  f"| 15m: ${p:>7.0f} ({pp}%) "
                  f"| MaxP ${r['max_profit']:>6.0f} MaxL ${r['max_loss']:>6.0f}")

        # Hold time sweep
        print(f"\n  HOLD TIME SWEEP:")
        for h in [5, 10, 15, 20, 30, 45, 60]:
            e2 = ev(bsel, h)
            if e2: pr(f"{h}min", e2, "    ")

        # By side
        print(f"\n  BY SIDE (15min):")
        for side in ['CALL', 'PUT']:
            e2 = ev([r for r in bsel if r['side'] == side], 15)
            if e2 and e2['n'] >= 2: pr(side, e2, "    ")

        # By OTM distance
        print(f"\n  BY OTM DISTANCE (15min):")
        for lo, hi, lab in [(0, 0.2, "<0.2%"), (0.2, 0.5, "0.2-0.5%"),
                             (0.5, 1.0, "0.5-1%"), (1.0, 2.0, "1-2%")]:
            e2 = ev([r for r in bsel if lo <= r['dist_pct'] < hi], 15)
            if e2 and e2['n'] >= 2: pr(lab, e2, "    ")

        # By volume
        print(f"\n  BY VOLUME SPIKE (15min):")
        for lo, hi, lab in [(1000, 3000, "1-3K"), (3000, 5000, "3-5K"),
                             (5000, 10000, "5-10K"), (10000, 999999, "10K+")]:
            e2 = ev([r for r in bsel if lo <= r['vol_delta'] < hi], 15)
            if e2 and e2['n'] >= 2: pr(lab, e2, "    ")

        # By premium
        print(f"\n  BY PREMIUM (15min):")
        for lo, hi, lab in [(100, 500, "$100-500K"), (500, 1000, "$500K-1M"),
                             (1000, 5000, "$1-5M"), (5000, 99999, "$5M+")]:
            e2 = ev([r for r in bsel if lo*1000 <= r['premium'] < hi*1000], 15)
            if e2 and e2['n'] >= 2: pr(lab, e2, "    ")

        # By time
        print(f"\n  BY TIME (15min):")
        for sh, eh, lab in [(9, 11, "9:30-11"), (11, 13, "11-13"),
                             (13, 15, "13-15"), (15, 16, "15-16")]:
            e2 = ev([r for r in bsel if sh <= r['ts'].hour < eh], 15)
            if e2 and e2['n'] >= 2: pr(lab, e2, "    ")

        # Daily P&L
        print(f"\n  DAILY P&L (15min):")
        running = 0; wd = 0; ld = 0
        for d in sorted(daily_pnl.keys()):
            running += daily_pnl[d]
            if daily_pnl[d] > 0: wd += 1
            elif daily_pnl[d] < 0: ld += 1
            m = '+' if daily_pnl[d] > 0 else '-'
            print(f"    {m} {d}: ${daily_pnl[d]:>8,.0f} (running: ${running:>10,.0f})")
        if wd+ld:
            print(f"\n    Win days: {wd} | Loss days: {ld} | Day WR: {wd/(wd+ld)*100:.0f}%")

        # Also show top 5 other combos for comparison
        print(f"\n  TOP 5 ALTERNATIVE FILTERS (15min):")
        for i, (v, p, m, n, s, e) in enumerate(best_configs_15[1:6]):
            pr(f"#{i+2}: vol>={v} prem>=${p//1000}K otm<={m}% top{n}/day", e, "    ")

    print(f"\n{'='*120}")
    print("DONE")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
