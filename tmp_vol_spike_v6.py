"""
Vol Spike v6 -- Verify OTM logic + deep CALL vs PUT analysis.

User says calls should be better (directional, not hedging).
Need to verify:
1. OTM direction is correct (CALL OTM = strike > spot, PUT OTM = strike < spot)
2. Forward tracking uses correct bid column for exit
3. Entry uses correct ask column
4. Print sample trades to manually verify
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
        data[strike] = row  # Keep raw row for verification
    return data


def process_day(snaps):
    if len(snaps) < 2: return []

    parsed = []
    for snap in snaps:
        parsed.append({
            'ts': snap['ts'], 'spot': sf(snap['spot']),
            'data': parse_snap(snap['rows']),
        })

    results = []
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
            crow = curr['data'][strike]
            prow = prev['data'][strike]

            # === OTM CALL: strike > spot (bullish directional bet) ===
            call_vol_now = si(crow[C_VOL])
            call_vol_prev = si(prow[C_VOL])
            cvd = call_vol_now - call_vol_prev

            if cvd >= 500 and strike > spot:
                dist = strike - spot
                c_bid = sf(crow[C_BID])
                c_ask = sf(crow[C_ASK])
                c_last = sf(crow[C_LAST])
                c_delta = sf(crow[C_DELTA])
                c_iv = sf(crow[C_IV])
                c_oi = si(crow[C_OI])
                mid = (c_bid + c_ask) / 2 if c_ask > 0 else c_last

                if mid > 0 and c_ask > 0:
                    prem = cvd * mid * 100
                    entry = c_ask  # Buy at ask

                    # Track forward using CALL BID to exit
                    fwd = _track_call(parsed, i, strike, entry)

                    results.append({
                        'ts': ts, 'date': ts.date(), 'strike': strike,
                        'side': 'CALL', 'spot': spot,
                        'vol_delta': cvd, 'oi': c_oi, 'premium': prem,
                        'entry_ask': c_ask, 'entry_bid': c_bid, 'entry_mid': mid,
                        'delta': c_delta, 'iv': c_iv,
                        'dist_pts': round(dist, 1),
                        'dist_pct': round(dist/spot*100, 2),
                        't_min': t_min,
                        # Verify fields
                        'v_call_vol_now': call_vol_now,
                        'v_call_vol_prev': call_vol_prev,
                        **fwd,
                    })

            # === OTM PUT: strike < spot (bearish directional bet) ===
            put_vol_now = si(crow[P_VOL])
            put_vol_prev = si(prow[P_VOL])
            pvd = put_vol_now - put_vol_prev

            if pvd >= 500 and strike < spot:
                dist = spot - strike
                p_bid = sf(crow[P_BID])
                p_ask = sf(crow[P_ASK])
                p_last = sf(crow[P_LAST])
                p_delta = sf(crow[P_DELTA])
                p_iv = sf(crow[P_IV])
                p_oi = si(crow[P_OI])
                mid = (p_bid + p_ask) / 2 if p_ask > 0 else p_last

                if mid > 0 and p_ask > 0:
                    prem = pvd * mid * 100
                    entry = p_ask  # Buy at ask

                    # Track forward using PUT BID to exit
                    fwd = _track_put(parsed, i, strike, entry)

                    results.append({
                        'ts': ts, 'date': ts.date(), 'strike': strike,
                        'side': 'PUT', 'spot': spot,
                        'vol_delta': pvd, 'oi': p_oi, 'premium': prem,
                        'entry_ask': p_ask, 'entry_bid': p_bid, 'entry_mid': mid,
                        'delta': p_delta, 'iv': p_iv,
                        'dist_pts': round(dist, 1),
                        'dist_pct': round(dist/spot*100, 2),
                        't_min': t_min,
                        'v_put_vol_now': put_vol_now,
                        'v_put_vol_prev': put_vol_prev,
                        **fwd,
                    })

    return results


def _track_call(parsed, start_idx, strike, entry_price):
    """Track CALL forward: exit at call BID."""
    if entry_price <= 0:
        return {'max_profit': 0, 'max_loss': 0, 'exit_prices': {}, 'final_pnl': 0,
                'spot_path': []}
    ts0 = parsed[start_idx]['ts']
    max_p = 0.0; max_l = 0.0; exits = {}; last_pnl = 0.0
    spot_path = []

    for j in range(start_idx+1, len(parsed)):
        elapsed = (parsed[j]['ts'] - ts0).total_seconds() / 60
        fd = parsed[j]['data']
        if strike not in fd: continue
        row = fd[strike]

        exit_bid = sf(row[C_BID])  # CALL bid for exit
        if exit_bid < 0: continue
        spot_now = parsed[j]['spot']

        pnl = (exit_bid - entry_price) * 100
        pct = (exit_bid - entry_price) / entry_price * 100 if entry_price > 0 else 0
        max_p = max(max_p, pnl); max_l = min(max_l, pnl); last_pnl = pnl

        spot_path.append({'min': round(elapsed, 1), 'spot': spot_now,
                          'bid': exit_bid, 'pnl': round(pnl, 2)})

        for mins in [2, 5, 10, 15, 20, 30, 45, 60]:
            if mins not in exits and elapsed >= mins:
                exits[mins] = {'pnl': round(pnl, 2), 'pct': round(pct, 1),
                               'price': exit_bid, 'spot': spot_now}

    return {'max_profit': round(max_p, 2), 'max_loss': round(max_l, 2),
            'exit_prices': exits, 'final_pnl': round(last_pnl, 2),
            'spot_path': spot_path[:30]}  # Keep first 30 for debug


def _track_put(parsed, start_idx, strike, entry_price):
    """Track PUT forward: exit at put BID."""
    if entry_price <= 0:
        return {'max_profit': 0, 'max_loss': 0, 'exit_prices': {}, 'final_pnl': 0,
                'spot_path': []}
    ts0 = parsed[start_idx]['ts']
    max_p = 0.0; max_l = 0.0; exits = {}; last_pnl = 0.0
    spot_path = []

    for j in range(start_idx+1, len(parsed)):
        elapsed = (parsed[j]['ts'] - ts0).total_seconds() / 60
        fd = parsed[j]['data']
        if strike not in fd: continue
        row = fd[strike]

        exit_bid = sf(row[P_BID])  # PUT bid for exit
        if exit_bid < 0: continue
        spot_now = parsed[j]['spot']

        pnl = (exit_bid - entry_price) * 100
        pct = (exit_bid - entry_price) / entry_price * 100 if entry_price > 0 else 0
        max_p = max(max_p, pnl); max_l = min(max_l, pnl); last_pnl = pnl

        spot_path.append({'min': round(elapsed, 1), 'spot': spot_now,
                          'bid': exit_bid, 'pnl': round(pnl, 2)})

        for mins in [2, 5, 10, 15, 20, 30, 45, 60]:
            if mins not in exits and elapsed >= mins:
                exits[mins] = {'pnl': round(pnl, 2), 'pct': round(pct, 1),
                               'price': exit_bid, 'spot': spot_now}

    return {'max_profit': round(max_p, 2), 'max_loss': round(max_l, 2),
            'exit_prices': exits, 'final_pnl': round(last_pnl, 2),
            'spot_path': spot_path[:30]}


def select_top(pool, top_n=3):
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
    days = len(set(r['date'] for r in results))
    return {'n': len(pnls), 'w': w, 'l': l, 'wr': w/(w+l)*100 if (w+l) else 0,
            'total': t, 'avg': t/len(pnls), 'days': days,
            'per_day': len(pnls)/max(1, days)}


def pr(label, e, prefix="  "):
    if e is None or e['n'] < 3: return
    print(f"{prefix}{label:<60} {e['n']:>4} ({e['per_day']:.1f}/d) | "
          f"{e['w']}W/{e['l']}L WR={e['wr']:.0f}% | "
          f"${e['total']:>10,.0f} Avg ${e['avg']:>6,.0f}")


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 120)
    print("VOL SPIKE v6 -- Verify + Deep CALL/PUT analysis")
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
        c = sum(1 for s in day if s['side'] == 'CALL')
        p = sum(1 for s in day if s['side'] == 'PUT')
        print(f"{len(day)} spikes ({c}C/{p}P)")
        sys.stdout.flush()

    conn.close()

    calls = [s for s in all_spikes if s['side'] == 'CALL']
    puts = [s for s in all_spikes if s['side'] == 'PUT']
    print(f"\nTotal: {len(all_spikes):,} ({len(calls):,} calls, {len(puts):,} puts)")

    # ── VERIFICATION: Print 5 sample CALL trades with full detail ──
    print(f"\n{'='*120}")
    print("VERIFICATION: Sample CALL trades (first 5 with vol>=3000, prem>=$500K)")
    print(f"{'='*120}")

    sample_calls = [s for s in calls if s['vol_delta'] >= 3000 and s['premium'] >= 500000][:5]
    for r in sample_calls:
        ts_str = r['ts'].strftime('%Y-%m-%d %H:%M')
        print(f"\n  {ts_str} CALL {r['strike']:.0f} | Spot={r['spot']:.1f}")
        print(f"    Strike > Spot? {r['strike']} > {r['spot']:.1f} = {r['strike'] > r['spot']} (must be True for OTM CALL)")
        print(f"    Vol: {r['v_call_vol_now']:,} - {r['v_call_vol_prev']:,} = {r['vol_delta']:,}")
        print(f"    Entry: ask=${r['entry_ask']:.2f} bid=${r['entry_bid']:.2f} mid=${r['entry_mid']:.2f}")
        print(f"    Delta={r['delta']:.3f} IV={r['iv']:.1%} OI={r['oi']:,}")
        print(f"    Premium: {r['vol_delta']} x ${r['entry_mid']:.2f} x 100 = ${r['premium']:,.0f}")
        print(f"    OTM dist: {r['dist_pts']:.1f} pts ({r['dist_pct']:.2f}%)")
        # Show spot path
        if r['spot_path']:
            print(f"    Forward path (spot, call_bid, pnl):")
            for sp in r['spot_path'][:10]:
                direction = "UP" if sp['spot'] > r['spot'] else "DOWN"
                print(f"      +{sp['min']:>5.1f}min: spot={sp['spot']:.1f} ({direction} {abs(sp['spot']-r['spot']):.1f}pts) "
                      f"call_bid=${sp['bid']:.2f} pnl=${sp['pnl']:.0f}")
        p15 = r['exit_prices'].get(15, {})
        if p15:
            spot15 = p15.get('spot', 0)
            spot_move = spot15 - r['spot'] if spot15 else 0
            print(f"    @15min: spot={spot15:.1f} (moved {spot_move:+.1f}pts) "
                  f"call_bid=${p15.get('price', 0):.2f} pnl=${p15['pnl']:.0f}")
            if spot_move > 0 and p15['pnl'] < 0:
                print(f"    ** ANOMALY: Spot moved UP but CALL lost money! Theta decay > delta gain?")

    # ── VERIFICATION: Sample PUT trades ──
    print(f"\n{'='*120}")
    print("VERIFICATION: Sample PUT trades (first 5 with vol>=3000, prem>=$500K)")
    print(f"{'='*120}")

    sample_puts = [s for s in puts if s['vol_delta'] >= 3000 and s['premium'] >= 500000][:5]
    for r in sample_puts:
        ts_str = r['ts'].strftime('%Y-%m-%d %H:%M')
        print(f"\n  {ts_str} PUT {r['strike']:.0f} | Spot={r['spot']:.1f}")
        print(f"    Strike < Spot? {r['strike']} < {r['spot']:.1f} = {r['strike'] < r['spot']} (must be True for OTM PUT)")
        print(f"    Vol: {r['v_put_vol_now']:,} - {r['v_put_vol_prev']:,} = {r['vol_delta']:,}")
        print(f"    Entry: ask=${r['entry_ask']:.2f} bid=${r['entry_bid']:.2f} mid=${r['entry_mid']:.2f}")
        print(f"    Delta={r['delta']:.3f} IV={r['iv']:.1%} OI={r['oi']:,}")
        print(f"    OTM dist: {r['dist_pts']:.1f} pts ({r['dist_pct']:.2f}%)")
        if r['spot_path']:
            print(f"    Forward path (spot, put_bid, pnl):")
            for sp in r['spot_path'][:10]:
                direction = "DOWN" if sp['spot'] < r['spot'] else "UP"
                print(f"      +{sp['min']:>5.1f}min: spot={sp['spot']:.1f} ({direction} {abs(sp['spot']-r['spot']):.1f}pts) "
                      f"put_bid=${sp['bid']:.2f} pnl=${sp['pnl']:.0f}")
        p15 = r['exit_prices'].get(15, {})
        if p15:
            spot15 = p15.get('spot', 0)
            spot_move = spot15 - r['spot'] if spot15 else 0
            print(f"    @15min: spot={spot15:.1f} (moved {spot_move:+.1f}pts) "
                  f"put_bid=${p15.get('price', 0):.2f} pnl=${p15['pnl']:.0f}")
            if spot_move < 0 and p15['pnl'] < 0:
                print(f"    ** ANOMALY: Spot moved DOWN but PUT lost money!")

    # ── Now the REAL analysis: massive filter grid, CALL-only and PUT-only ──
    print(f"\n{'='*120}")
    print("COMPREHENSIVE FILTER GRID (CALL-only, PUT-only, and BOTH)")
    print(f"{'='*120}")

    for hold in [15, 20, 30, 45]:
        print(f"\n--- {hold}-min hold ---\n")

        configs = []
        for min_vol in [1000, 2000, 3000, 5000]:
            for min_prem in [50000, 100000, 250000, 500000, 1000000]:
                for max_pct in [0.3, 0.5, 0.75, 1.0, 1.5]:
                    for top_n in [2, 3, 5]:
                        for side_filter in ['BOTH', 'CALL', 'PUT']:
                            if side_filter == 'CALL':
                                pool = [s for s in calls
                                        if s['vol_delta'] >= min_vol
                                        and s['premium'] >= min_prem
                                        and s['dist_pct'] <= max_pct]
                            elif side_filter == 'PUT':
                                pool = [s for s in puts
                                        if s['vol_delta'] >= min_vol
                                        and s['premium'] >= min_prem
                                        and s['dist_pct'] <= max_pct]
                            else:
                                pool = [s for s in all_spikes
                                        if s['vol_delta'] >= min_vol
                                        and s['premium'] >= min_prem
                                        and s['dist_pct'] <= max_pct]

                            sel = select_top(pool, top_n)
                            if len(sel) < 10: continue
                            e = ev(sel, hold)
                            if e is None: continue
                            if not (1.5 <= e['per_day'] <= 7): continue

                            configs.append((
                                f"[{side_filter}] vol>={min_vol} prem>=${min_prem//1000}K "
                                f"otm<={max_pct}% top{top_n}/d",
                                e
                            ))

        configs.sort(key=lambda x: x[1]['total'], reverse=True)

        # Top 30
        shown = 0
        for label, e in configs:
            if shown >= 30: break
            pr(label, e)
            shown += 1

    # ── Spot direction analysis ──
    print(f"\n{'='*120}")
    print("SPOT DIRECTION ANALYSIS")
    print("When vol spike fires, did spot actually move in the expected direction?")
    print(f"{'='*120}")

    # For calls: did spot go UP? For puts: did spot go DOWN?
    for side, pool in [("CALL", calls), ("PUT", puts)]:
        big = [s for s in pool if s['vol_delta'] >= 3000 and s['premium'] >= 500000]
        big = select_top(big, 3)
        if not big: continue

        print(f"\n  {side} (vol>=3K, prem>=$500K, top3/day): {len(big)} trades")

        for hold in [10, 15, 20, 30]:
            correct = 0; wrong = 0; flat = 0
            total_spot_move = 0
            for r in big:
                ep = r['exit_prices'].get(hold)
                if not ep: continue
                spot_move = ep['spot'] - r['spot']
                total_spot_move += spot_move

                if side == 'CALL':
                    if spot_move > 2: correct += 1
                    elif spot_move < -2: wrong += 1
                    else: flat += 1
                else:
                    if spot_move < -2: correct += 1
                    elif spot_move > 2: wrong += 1
                    else: flat += 1

            n = correct + wrong
            pct = correct / n * 100 if n > 0 else 0
            avg_move = total_spot_move / len(big) if big else 0
            print(f"    @{hold}min: {correct} correct / {wrong} wrong / {flat} flat "
                  f"= {pct:.0f}% | avg spot move: {avg_move:+.1f}pts")

    # ── Time-decay analysis ──
    print(f"\n{'='*120}")
    print("THETA DECAY ANALYSIS")
    print("How much value does the option lose from theta alone?")
    print(f"{'='*120}")

    for side, pool in [("CALL", calls), ("PUT", puts)]:
        big = [s for s in pool if s['vol_delta'] >= 3000 and s['premium'] >= 500000
               and s['dist_pct'] <= 1.0]
        big = select_top(big, 3)
        if not big: continue

        print(f"\n  {side} OTM<=1% (vol>=3K, prem>=$500K):")

        # Classify: spot moved right direction but option still lost money
        theta_kills = 0
        direction_wins = 0
        direction_losses = 0

        for r in big:
            ep = r['exit_prices'].get(15)
            if not ep: continue
            spot_move = ep['spot'] - r['spot']
            pnl = ep['pnl']

            if side == 'CALL':
                right_dir = spot_move > 0
            else:
                right_dir = spot_move < 0

            if right_dir and pnl < 0:
                theta_kills += 1
            elif right_dir and pnl > 0:
                direction_wins += 1
            elif not right_dir:
                direction_losses += 1

        total = theta_kills + direction_wins + direction_losses
        if total:
            print(f"    Spot moved RIGHT + profit:     {direction_wins:>4} ({direction_wins/total*100:.0f}%)")
            print(f"    Spot moved RIGHT + STILL LOST: {theta_kills:>4} ({theta_kills/total*100:.0f}%) <-- theta killed it")
            print(f"    Spot moved WRONG:              {direction_losses:>4} ({direction_losses/total*100:.0f}%)")

    print(f"\n{'='*120}")
    print("DONE")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
