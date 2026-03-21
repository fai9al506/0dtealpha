"""
Vol Spike Backtest v2 — Refined Whale Detection on SPX 0DTE

Key changes from v1:
- 0DTE SPX always has vol > OI, so that filter is useless
- Instead: detect OUTLIER volume spikes per 2-min window
- Use percentile-based filtering: only top N spikes per day
- Minimum premium $100K (serious money)
- Track per-strike running average to detect anomalous spikes
- Focus on OTM/ATM where whales place directional bets
"""
import sys
import psycopg2
import psycopg2.extras
import json
import statistics
from datetime import datetime, timedelta
from collections import defaultdict

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

# Column indices
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
    # Indices: call: 0=vol,1=oi,2=bid,3=ask,4=last,5=delta,6=iv,7=gamma
    #          put:  8=vol,9=oi,10=bid,11=ask,12=last,13=delta,14=iv,15=gamma


def process_day(snaps):
    """
    Process one day's snapshots. Returns:
    - all_spikes: every volume delta event with metadata
    - Caller will filter for outliers
    """
    if len(snaps) < 2:
        return []

    all_spikes = []
    prev_data = parse_snap(snaps[0]['rows'])
    prev_ts = snaps[0]['ts']

    for i in range(1, len(snaps)):
        snap = snaps[i]
        curr_data = parse_snap(snap['rows'])
        spot = sf(snap['spot'])
        ts = snap['ts']

        gap_min = (ts - prev_ts).total_seconds() / 60
        if gap_min > 5:
            prev_data = curr_data; prev_ts = ts
            continue

        for strike in curr_data:
            if strike not in prev_data: continue
            c, p = curr_data[strike], prev_data[strike]

            # Call side
            cvd = c[0] - p[0]
            if cvd > 50:  # Minimum floor to avoid noise
                mid = (c[2] + c[3]) / 2 if c[3] > 0 else c[4]
                if mid > 0.05:  # Filter out near-worthless options
                    prem = cvd * mid * 100
                    all_spikes.append({
                        'ts': ts, 'strike': strike, 'side': 'CALL', 'spot': spot,
                        'vol_delta': cvd, 'oi': c[1], 'premium': prem,
                        'entry_bid': c[2], 'entry_ask': c[3], 'entry_mid': mid,
                        'entry_last': c[4],
                        'delta': c[5], 'iv': c[6], 'gamma': c[7],
                        'moneyness': round(strike - spot, 1),
                        'cum_vol': c[0],  # total volume at this point
                    })

            # Put side
            pvd = c[8] - p[8]
            if pvd > 50:
                mid = (c[10] + c[11]) / 2 if c[11] > 0 else c[12]
                if mid > 0.05:
                    prem = pvd * mid * 100
                    all_spikes.append({
                        'ts': ts, 'strike': strike, 'side': 'PUT', 'spot': spot,
                        'vol_delta': pvd, 'oi': c[9], 'premium': prem,
                        'entry_bid': c[10], 'entry_ask': c[11], 'entry_mid': mid,
                        'entry_last': c[12],
                        'delta': c[13], 'iv': c[14], 'gamma': c[15],
                        'moneyness': round(strike - spot, 1),
                        'cum_vol': c[8],
                    })

        prev_data = curr_data; prev_ts = ts

    return all_spikes


def filter_whale_signals(all_spikes, top_n=5, min_prem=100000):
    """
    From all spikes in a day, pick the most likely whale signals.

    Strategy:
    1. Sort by premium (money committed = conviction)
    2. Take top N by premium
    3. Must exceed min_prem threshold
    4. Deduplicate: if same strike/side fires multiple times, keep the biggest
    """
    if not all_spikes:
        return []

    # Deduplicate: keep biggest spike per (strike, side)
    best = {}
    for s in all_spikes:
        key = (s['strike'], s['side'])
        if key not in best or s['premium'] > best[key]['premium']:
            best[key] = s

    # Filter by minimum premium
    candidates = [s for s in best.values() if s['premium'] >= min_prem]

    # Sort by premium descending, take top N
    candidates.sort(key=lambda x: x['premium'], reverse=True)
    return candidates[:top_n]


def track_forward(cur, signal):
    """Track forward price after entry."""
    entry_ts = signal['ts']
    strike = signal['strike']
    side = signal['side']
    entry_price = signal['entry_ask']  # Buy at ask
    trade_date = entry_ts.date()

    if entry_price <= 0:
        return {'max_profit': 0, 'max_loss': 0, 'final_pnl': 0, 'exit_prices': {},
                'max_pct': 0, 'min_pct': 0}

    cur.execute("""
        SELECT ts, spot, rows FROM chain_snapshots
        WHERE ts > %s AND ts::date = %s::date
        ORDER BY ts ASC
    """, (entry_ts, trade_date))

    max_profit = 0.0; max_loss = 0.0
    exit_prices = {}
    last_pnl = 0.0

    for snap in cur:
        elapsed = (snap['ts'] - entry_ts).total_seconds() / 60
        rows = json.loads(snap['rows']) if isinstance(snap['rows'], str) else snap['rows']

        current_price = None
        for row in rows:
            s = sf(row[STRIKE])
            if abs(s - strike) < 0.01:
                current_price = sf(row[C_BID]) if side == 'CALL' else sf(row[P_BID])
                break

        if current_price is None or current_price < 0:
            continue

        pnl = (current_price - entry_price) * 100
        pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        max_profit = max(max_profit, pnl)
        max_loss = min(max_loss, pnl)
        last_pnl = pnl

        for mins in [2, 5, 10, 15, 20, 30, 45, 60, 90, 120]:
            if mins not in exit_prices and elapsed >= mins:
                exit_prices[mins] = {'pnl': round(pnl, 2), 'pct': round(pct, 1),
                                      'price': current_price}

    return {
        'max_profit': round(max_profit, 2),
        'max_loss': round(max_loss, 2),
        'final_pnl': round(last_pnl, 2),
        'exit_prices': exit_prices,
        'max_pct': round(max_profit / entry_price if entry_price > 0 else 0, 1),
        'min_pct': round(max_loss / entry_price if entry_price > 0 else 0, 1),
    }


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 90)
    print("VOL SPIKE v2 — Whale Volume Detection (Premium-Based)")
    print("=" * 90)
    sys.stdout.flush()

    # Get trading dates only (skip weekends/holidays with 0 vol events)
    cur.execute("""
        SELECT DISTINCT ts::date as d, COUNT(*) as n
        FROM chain_snapshots GROUP BY ts::date ORDER BY d
    """)
    dates = cur.fetchall()
    print(f"Total dates: {len(dates)}")
    sys.stdout.flush()

    # ── Phase 1: Collect all spikes across all days ──
    print(f"\n-- PHASE 1: Scanning volume spikes across all trading days --\n")

    daily_signals = {}  # date -> filtered signals
    daily_stats = {}    # date -> stats
    all_premium_values = []  # for percentile analysis

    for di, d_row in enumerate(dates):
        trade_date = d_row['d']
        print(f"  [{di+1}/{len(dates)}] {trade_date}...", end=" ", flush=True)

        cur.execute("""
            SELECT ts, spot, rows FROM chain_snapshots
            WHERE ts::date = %s ORDER BY ts ASC
        """, (trade_date,))
        snaps = cur.fetchall()

        all_spikes = process_day(snaps)

        if not all_spikes:
            print("no trades")
            continue

        # Collect premium distribution
        prems = [s['premium'] for s in all_spikes]
        all_premium_values.extend(prems)

        # Get whale signals (top 5 per day by premium, min $100K)
        whales = filter_whale_signals(all_spikes, top_n=5, min_prem=100000)

        daily_signals[trade_date] = whales
        daily_stats[trade_date] = {
            'total_spikes': len(all_spikes),
            'whale_count': len(whales),
            'max_premium': max(prems),
            'avg_premium': sum(prems) / len(prems),
        }

        wc = len(whales)
        mp = max(prems) if prems else 0
        print(f"{len(all_spikes)} spikes, {wc} whales, max prem ${mp:,.0f}")
        sys.stdout.flush()

    # ── Premium Distribution ──
    print(f"\n{'=' * 90}")
    print("PREMIUM DISTRIBUTION (all spikes, all days)")
    print(f"{'=' * 90}")

    if all_premium_values:
        all_premium_values.sort()
        n = len(all_premium_values)
        for pct in [50, 75, 90, 95, 99, 99.5, 99.9]:
            idx = int(n * pct / 100)
            print(f"  P{pct}: ${all_premium_values[min(idx, n-1)]:,.0f}")

        for thresh in [50000, 100000, 250000, 500000, 1000000, 2500000, 5000000]:
            count = sum(1 for p in all_premium_values if p >= thresh)
            print(f"  >= ${thresh:>10,}: {count:>6} ({count/n*100:.2f}%)")

    # ── Phase 2: Track forward for whale signals ──
    all_whale_signals = []
    for trade_date in sorted(daily_signals.keys()):
        all_whale_signals.extend(daily_signals[trade_date])

    print(f"\n{'=' * 90}")
    print(f"WHALE SIGNALS: {len(all_whale_signals)} (top 5/day, min $100K premium)")
    print(f"{'=' * 90}")

    if not all_whale_signals:
        print("No whale signals found!")
        conn.close()
        return

    print(f"\nTracking forward prices for {len(all_whale_signals)} signals...")
    sys.stdout.flush()

    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    results = []
    for i, sig in enumerate(all_whale_signals):
        fwd = track_forward(cur2, sig)
        results.append({**sig, **fwd})
        if (i + 1) % 25 == 0:
            print(f"  Tracked {i+1}/{len(all_whale_signals)}", flush=True)
    cur2.close()

    # ── Phase 3: Analysis ──
    print(f"\n{'=' * 90}")
    print("ALL SIGNALS + OUTCOMES")
    print(f"{'=' * 90}")

    for r in results:
        side = r['side']
        itm = (side=='CALL' and r['moneyness']<0) or (side=='PUT' and r['moneyness']>0)
        tag = "ITM" if itm else "OTM"

        p15 = r['exit_prices'].get(15, {}).get('pnl', '--')
        p30 = r['exit_prices'].get(30, {}).get('pnl', '--')
        pct15 = r['exit_prices'].get(15, {}).get('pct', '--')
        pct30 = r['exit_prices'].get(30, {}).get('pct', '--')

        ts_str = r['ts'].strftime('%m/%d %H:%M')
        print(f"  {ts_str} {side:<4} {r['strike']:.0f} {tag} {abs(r['moneyness']):>4.0f}pts "
              f"| VolΔ {r['vol_delta']:>6,} OI {r['oi']:>6,} "
              f"| Prem ${r['premium']:>10,.0f} "
              f"| Entry ${r['entry_ask']:.2f} Δ{r['delta']:.2f} "
              f"| Max+${r['max_profit']:>6.0f} Max-${r['max_loss']:>6.0f} "
              f"| 15m: {p15} ({pct15}%) 30m: {p30} ({pct30}%)")

    # ── Summary by hold time ──
    print(f"\n{'=' * 90}")
    print("P&L BY HOLD TIME (per contract, $100 multiplier)")
    print(f"{'=' * 90}")

    for hold in [5, 10, 15, 20, 30, 45, 60]:
        pnls = []
        for r in results:
            if hold in r['exit_prices']:
                pnls.append(r['exit_prices'][hold]['pnl'])
        if not pnls:
            continue
        w = sum(1 for p in pnls if p > 0)
        l = sum(1 for p in pnls if p < 0)
        t = sum(pnls)
        avg = t / len(pnls)
        wr = w / (w+l) * 100 if (w+l) > 0 else 0
        med = sorted(pnls)[len(pnls)//2]
        print(f"  {hold:3d}min: {len(pnls):3d} trades | {w}W/{l}L WR={wr:.0f}% "
              f"| Total ${t:>8,.0f} Avg ${avg:>6,.0f} Med ${med:>6,.0f}")

    # ── By Max Favorable Excursion ──
    print(f"\n{'=' * 90}")
    print("MAX FAVORABLE EXCURSION (how far did price go in our favor?)")
    print(f"{'=' * 90}")

    mfes = [r['max_profit'] for r in results]
    if mfes:
        w_mfe = [r['max_profit'] for r in results if r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) > 0]
        l_mfe = [r['max_profit'] for r in results if r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) < 0]
        print(f"  All signals:   Avg MFE=${sum(mfes)/len(mfes):>6.0f}  Med=${sorted(mfes)[len(mfes)//2]:>6.0f}")
        if w_mfe:
            print(f"  Winners:       Avg MFE=${sum(w_mfe)/len(w_mfe):>6.0f}  Med=${sorted(w_mfe)[len(w_mfe)//2]:>6.0f}")
        if l_mfe:
            print(f"  Losers:        Avg MFE=${sum(l_mfe)/len(l_mfe):>6.0f}  Med=${sorted(l_mfe)[len(l_mfe)//2]:>6.0f}")

    # ── By side ──
    print(f"\n{'=' * 90}")
    print("BY SIDE (30-min hold)")
    print(f"{'=' * 90}")

    for side in ['CALL', 'PUT']:
        sr = [r for r in results if r['side'] == side]
        if not sr: continue
        pnls = [r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) for r in sr]
        w = sum(1 for p in pnls if p > 0)
        l = sum(1 for p in pnls if p < 0)
        t = sum(pnls)
        wr = w / (w+l) * 100 if (w+l) > 0 else 0
        print(f"  {side}: {len(sr)} signals | {w}W/{l}L WR={wr:.0f}% | PnL ${t:,.0f}")

    # ── By moneyness ──
    print(f"\n{'=' * 90}")
    print("BY MONEYNESS (30-min hold)")
    print(f"{'=' * 90}")

    for lo, hi, label in [(0, 5, "ATM 0-5pts"), (5, 15, "Near 5-15pts"),
                           (15, 30, "OTM 15-30pts"), (30, 60, "Far OTM 30-60pts"),
                           (60, 200, "Deep OTM 60+pts")]:
        sr = [r for r in results if lo <= abs(r['moneyness']) < hi]
        if not sr: continue
        pnls = [r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) for r in sr]
        w = sum(1 for p in pnls if p > 0)
        l = sum(1 for p in pnls if p < 0)
        t = sum(pnls)
        wr = w / (w+l) * 100 if (w+l) > 0 else 0
        print(f"  {label}: {len(sr)} | {w}W/{l}L WR={wr:.0f}% | PnL ${t:,.0f}")

    # ── By premium band ──
    print(f"\n{'=' * 90}")
    print("BY PREMIUM BAND (30-min hold)")
    print(f"{'=' * 90}")

    for lo, hi, label in [(100000, 250000, "$100-250K"), (250000, 500000, "$250-500K"),
                           (500000, 1000000, "$500K-1M"), (1000000, 5000000, "$1-5M"),
                           (5000000, 999999999, "$5M+")]:
        sr = [r for r in results if lo <= r['premium'] < hi]
        if not sr: continue
        pnls = [r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) for r in sr]
        w = sum(1 for p in pnls if p > 0)
        l = sum(1 for p in pnls if p < 0)
        t = sum(pnls)
        wr = w / (w+l) * 100 if (w+l) > 0 else 0
        print(f"  {label}: {len(sr)} | {w}W/{l}L WR={wr:.0f}% | PnL ${t:,.0f}")

    # ── By time of day ──
    print(f"\n{'=' * 90}")
    print("BY TIME OF DAY (30-min hold)")
    print(f"{'=' * 90}")

    for sh, eh, label in [(9, 10, "9:30-10"), (10, 11, "10-11"), (11, 12, "11-12"),
                           (12, 13, "12-13"), (13, 14, "13-14"), (14, 15, "14-15"),
                           (15, 16, "15-16")]:
        sr = [r for r in results if sh <= r['ts'].hour < eh]
        if not sr: continue
        pnls = [r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) for r in sr]
        w = sum(1 for p in pnls if p > 0)
        l = sum(1 for p in pnls if p < 0)
        t = sum(pnls)
        wr = w / (w+l) * 100 if (w+l) > 0 else 0
        print(f"  {label}: {len(sr)} | {w}W/{l}L WR={wr:.0f}% | PnL ${t:,.0f}")

    # ── Directional accuracy: did the whale get the direction right? ──
    print(f"\n{'=' * 90}")
    print("DIRECTIONAL ACCURACY (did spot move in whale's direction?)")
    print(f"{'=' * 90}")

    for hold in [5, 15, 30, 60]:
        correct = 0; wrong = 0; flat = 0
        for r in results:
            ep = r['exit_prices'].get(hold)
            if not ep: continue
            pnl = ep['pnl']
            if pnl > 50: correct += 1
            elif pnl < -50: wrong += 1
            else: flat += 1
        total = correct + wrong
        if total:
            print(f"  {hold:3d}min: {correct} correct / {wrong} wrong / {flat} flat "
                  f"= {correct/total*100:.0f}% directional accuracy")

    # ── Best strategy: trailing stop or fixed exit? ──
    print(f"\n{'=' * 90}")
    print("SIMULATED STRATEGIES")
    print(f"{'=' * 90}")

    # Strategy A: Fixed 15-min hold
    pnls_15 = [r['exit_prices'].get(15, {}).get('pnl', r['final_pnl']) for r in results]
    # Strategy B: Fixed 30-min hold
    pnls_30 = [r['exit_prices'].get(30, {}).get('pnl', r['final_pnl']) for r in results]
    # Strategy C: Take profit at +100% (entry doubles), stop at -50%
    pnls_c = []
    for r in results:
        entry = r['entry_ask']
        if entry <= 0:
            pnls_c.append(0)
            continue
        tp = entry * 2  # 100% gain
        sl = entry * 0.5  # 50% loss

        result_pnl = r['final_pnl']
        for mins in sorted(r['exit_prices'].keys()):
            ep = r['exit_prices'][mins]
            price = ep.get('price', 0)
            if price >= tp:
                result_pnl = (tp - entry) * 100; break
            if price <= sl:
                result_pnl = (sl - entry) * 100; break
        pnls_c.append(result_pnl)

    # Strategy D: Take profit at +50%, stop at -30%
    pnls_d = []
    for r in results:
        entry = r['entry_ask']
        if entry <= 0:
            pnls_d.append(0)
            continue
        tp = entry * 1.5
        sl = entry * 0.7

        result_pnl = r['final_pnl']
        for mins in sorted(r['exit_prices'].keys()):
            ep = r['exit_prices'][mins]
            price = ep.get('price', 0)
            if price >= tp:
                result_pnl = (tp - entry) * 100; break
            if price <= sl:
                result_pnl = (sl - entry) * 100; break
        pnls_d.append(result_pnl)

    for label, pnls in [("A: 15min hold", pnls_15), ("B: 30min hold", pnls_30),
                          ("C: +100%/-50%", pnls_c), ("D: +50%/-30%", pnls_d)]:
        if not pnls: continue
        w = sum(1 for p in pnls if p > 0)
        l = sum(1 for p in pnls if p < 0)
        t = sum(pnls)
        wr = w / (w+l) * 100 if (w+l) > 0 else 0
        print(f"  {label}: {w}W/{l}L WR={wr:.0f}% | Total ${t:>10,.0f} | Avg ${t/len(pnls):>6,.0f}")

    conn.close()
    print(f"\n{'=' * 90}")
    print("DONE")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
