"""Bar-level ES Absorption study.
For each signal, pull 3 bars BEFORE trigger + trigger bar itself, compute bar-level
features, compare winners vs losers. User hypothesis: the 3-bar pre-trigger delta
pattern may distinguish real absorption from noise.
"""
import psycopg2
import json
from collections import defaultdict
import statistics
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# ES Absorption trades with bar_idx in abs_details
cur.execute("""
SELECT id, ts, direction, outcome_result, outcome_pnl, grade, paradigm,
       abs_details, abs_es_price, abs_vol_ratio,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE setup_name = 'ES Absorption' AND abs_details IS NOT NULL
  AND outcome_result IS NOT NULL
  AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
ORDER BY ts
""")
trades = []
for r in cur.fetchall():
    tid, ts, dirx, outcome, pnl, grade, paradigm, details, es_price, vol_ratio, d = r
    if not details or 'bar_idx' not in details:
        continue
    trades.append({
        'id': tid, 'ts': ts, 'd': d, 'direction': dirx, 'outcome': outcome,
        'pnl': float(pnl or 0), 'grade': grade, 'paradigm': paradigm,
        'bar_idx': details['bar_idx'], 'vol_ratio': float(vol_ratio or 0),
        'div_raw': details.get('div_raw', 0),
        'dd_raw': details.get('dd_raw', 0),
        'lis_raw': details.get('lis_raw', 0),
        'para_raw': details.get('para_raw', 0),
        'es_price': float(es_price or 0),
    })
print(f"ES Abs trades with bar_idx: {len(trades)}")

# For each trade, pull trigger bar + 8 preceding bars
def get_bars(trade_date, trigger_idx, n_before=8):
    cur.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
           cumulative_delta, cvd_close
    FROM es_range_bars
    WHERE trade_date = %s AND source = 'rithmic'
      AND bar_idx BETWEEN %s AND %s
    ORDER BY bar_idx
    """, (trade_date, trigger_idx - n_before, trigger_idx))
    return [dict(zip(['idx','open','high','low','close','vol','buy','sell','delta','cvd','cvd_close'], r)) for r in cur.fetchall()]

# Enrich each trade with bar context
print("Enriching with bar context...", flush=True)
enriched = []
missed = 0
for i, t in enumerate(trades):
    bars = get_bars(t['d'], t['bar_idx'])
    if len(bars) < 4:
        missed += 1
        continue
    # Find trigger bar
    trigger = next((b for b in bars if b['idx'] == t['bar_idx']), None)
    if not trigger or trigger['delta'] is None:
        missed += 1
        continue
    # 3 bars before trigger
    pre3 = [b for b in bars if b['idx'] < t['bar_idx']][-3:]
    if len(pre3) < 3:
        missed += 1
        continue
    t['trigger'] = trigger
    t['pre3'] = pre3
    t['bars'] = bars
    enriched.append(t)
    if (i+1) % 100 == 0:
        print(f"  {i+1}/{len(trades)}", flush=True)
print(f"Enriched: {len(enriched)}, missed: {missed}")

# Separate by direction + outcome
def tag(t): return (t['direction'], t['outcome'])

def pretty(val, fmt="{:+.1f}"):
    return fmt.format(val) if val is not None else 'n/a'

# =========================================================
# 1. Trigger bar features
# =========================================================
print("\n"+"="*70)
print("1. TRIGGER BAR — Features by outcome")
print("="*70)

def trigger_features(t):
    tr = t['trigger']
    price_range = tr['high'] - tr['low']
    body = abs(tr['close'] - tr['open'])
    up_wick = tr['high'] - max(tr['open'], tr['close'])
    low_wick = min(tr['open'], tr['close']) - tr['low']
    close_pos = (tr['close'] - tr['low']) / price_range if price_range > 0 else 0.5
    return {
        'delta': int(tr['delta'] or 0),
        'delta_pct': (int(tr['delta'] or 0) / tr['vol'] * 100) if tr['vol'] else 0,
        'vol': int(tr['vol'] or 0),
        'body': round(body, 2),
        'up_wick': round(up_wick, 2),
        'low_wick': round(low_wick, 2),
        'close_pos': round(close_pos, 2),  # 0=low, 1=high
        'range': round(price_range, 2),
    }

for dirx in ['bullish', 'bearish']:
    print(f"\n{dirx.upper()}:")
    for outcome in ['WIN', 'LOSS']:
        sub = [t for t in enriched if t['direction']==dirx and t['outcome']==outcome]
        if len(sub) < 5: continue
        feats = [trigger_features(t) for t in sub]
        medians = {k: statistics.median([f[k] for f in feats]) for k in feats[0]}
        means = {k: statistics.mean([f[k] for f in feats]) for k in feats[0]}
        print(f"  {outcome} (n={len(sub)}):")
        for k in ['delta', 'delta_pct', 'vol', 'body', 'up_wick', 'low_wick', 'close_pos', 'range']:
            print(f"    {k:<12} median={medians[k]:>+8.2f}  mean={means[k]:>+8.2f}")

# =========================================================
# 2. Pre-3 bar delta features
# =========================================================
print("\n"+"="*70)
print("2. PRE-3 BARS — Delta patterns by outcome")
print("="*70)

def pre3_features(t):
    p3 = t['pre3']
    sum_delta = sum(b['delta'] or 0 for b in p3)
    sum_vol = sum(b['vol'] or 0 for b in p3)
    # For a bearish signal (selling absorption), "aligned" = buy pressure in prev bars
    # because we're absorbing sellers during price rise (delta was positive = buys)
    # For bullish signal, aligned = sell pressure before (delta negative)
    # For a bullish signal we expect falling price with selling (negative delta pre-bars)
    # For a bearish signal we expect rising price with buying (positive delta pre-bars)
    # So: if direction=bullish, "correct" prev delta is NEGATIVE (contrarian). Let me define:
    # pre3_against = sum(delta) with sign matching CONTRARIAN direction
    #   bullish signal: price was falling, delta negative = absorption of sellers
    #   bearish signal: price was rising, delta positive = absorption of buyers
    # So: pre3_against = -sum_delta for bullish; +sum_delta for bearish
    if t['direction'] == 'bullish':
        pre3_against = -sum_delta  # negative delta expected (sellers being absorbed)
    else:
        pre3_against = sum_delta  # positive delta expected (buyers being absorbed)
    # All 3 bars same direction as the absorption side?
    if t['direction'] == 'bullish':
        # expect all 3 pre bars to have delta <= 0
        all_aligned = all((b['delta'] or 0) <= 0 for b in p3)
    else:
        all_aligned = all((b['delta'] or 0) >= 0 for b in p3)
    # Price action in pre-3
    pre3_price_change = p3[-1]['close'] - p3[0]['open']
    # CVD change
    cvd_change = (p3[-1]['cvd_close'] or 0) - (p3[0].get('cvd_open') or p3[0].get('cvd_close') or 0)
    return {
        'sum_delta': sum_delta,
        'sum_vol': sum_vol,
        'pre3_against': pre3_against,  # how much "absorption setup" they showed
        'all_aligned': all_aligned,
        'price_change': round(pre3_price_change, 2),
        'cvd_change': cvd_change,
        'delta_to_vol_ratio': (sum_delta / sum_vol * 100) if sum_vol else 0,
    }

for dirx in ['bullish', 'bearish']:
    print(f"\n{dirx.upper()}:")
    for outcome in ['WIN', 'LOSS']:
        sub = [t for t in enriched if t['direction']==dirx and t['outcome']==outcome]
        if len(sub) < 5: continue
        feats = [pre3_features(t) for t in sub]
        print(f"  {outcome} (n={len(sub)}):")
        for k in ['sum_delta', 'pre3_against', 'price_change', 'cvd_change', 'delta_to_vol_ratio']:
            vals = [f[k] for f in feats]
            med = statistics.median(vals)
            mean = statistics.mean(vals)
            print(f"    {k:<22} median={med:>+10.2f}  mean={mean:>+10.2f}")
        # Proportion with all_aligned
        aligned_count = sum(1 for f in feats if f['all_aligned'])
        print(f"    all_3_aligned:       {aligned_count}/{len(feats)} = {100*aligned_count/len(feats):.0f}%")

# =========================================================
# 3. BINARY FILTER TESTS — does pre3_against threshold help?
# =========================================================
print("\n"+"="*70)
print("3. FILTER CANDIDATES — Pre3 delta thresholds")
print("="*70)

def stats(trades):
    if not trades: return None
    pnl = sum(t['pnl'] for t in trades)
    w = sum(1 for t in trades if t['outcome']=='WIN')
    l = sum(1 for t in trades if t['outcome']=='LOSS')
    wr = 100*w/max(1,w+l)
    return {'n':len(trades),'pnl':round(pnl,1),'wr':round(wr,1),'w':w,'l':l}

def test_rule(trades, predicate, name):
    kept = [t for t in trades if predicate(t)]
    blk = [t for t in trades if not predicate(t)]
    # OOS split
    dates = sorted(set(t['d'] for t in trades))
    if not dates: return
    mid = dates[len(dates)//2]
    tr_all = [t for t in trades if t['d']<=mid]
    te_all = [t for t in trades if t['d']>mid]
    tr_k = [t for t in tr_all if predicate(t)]
    te_k = [t for t in te_all if predicate(t)]
    tr_d = stats(tr_k)['pnl'] - stats(tr_all)['pnl'] if tr_all else 0
    te_d = stats(te_k)['pnl'] - stats(te_all)['pnl'] if te_all else 0
    stable = tr_d >= 0 and te_d >= 0
    mark = "✅" if stable else "⚠️"
    print(f"\n  {name}")
    print(f"    Before: {stats(trades)}  After: {stats(kept)}")
    print(f"    Blocked: {len(blk)} pnl={stats(blk)['pnl'] if blk else 0:+.1f}")
    print(f"    OOS: train Δ={tr_d:+.1f}  test Δ={te_d:+.1f}  {mark}")

# Bullish: pre3_against should be positive (pre bars had sell delta being absorbed)
# Bearish: pre3_against should be positive too (pre bars had buy delta being absorbed)
# Threshold ideas:
for dirx in ['bullish', 'bearish']:
    sub = [t for t in enriched if t['direction']==dirx]
    print(f"\n### {dirx.upper()} filters ({len(sub)} trades) ###")
    # Enrich with computed features
    for t in sub:
        t['_f'] = pre3_features(t)
        t['_tf'] = trigger_features(t)

    # Rule: require pre3_against >= 0 (some absorption pressure)
    test_rule(sub, lambda t: t['_f']['pre3_against'] >= 0, f"{dirx}: pre3_against >= 0 (any absorption signal)")
    # Rule: require pre3_against >= 100
    test_rule(sub, lambda t: t['_f']['pre3_against'] >= 100, f"{dirx}: pre3_against >= 100")
    # Rule: require all 3 bars aligned with absorption
    test_rule(sub, lambda t: t['_f']['all_aligned'], f"{dirx}: require all 3 pre-bars aligned")
    # Rule: trigger close position (continuation bar)
    if dirx == 'bullish':
        # Bullish absorption: closed above low (continuation up)
        test_rule(sub, lambda t: t['_tf']['close_pos'] >= 0.6, f"{dirx}: trigger close in top 40% of range")
        test_rule(sub, lambda t: t['_tf']['delta'] > 0, f"{dirx}: trigger delta positive (buy pressure)")
    else:
        test_rule(sub, lambda t: t['_tf']['close_pos'] <= 0.4, f"{dirx}: trigger close in bottom 40% of range")
        test_rule(sub, lambda t: t['_tf']['delta'] < 0, f"{dirx}: trigger delta negative (sell pressure)")
    # Rule: trigger delta_pct (delta as % of volume — conviction)
    test_rule(sub, lambda t: abs(t['_tf']['delta_pct']) >= 10, f"{dirx}: |trigger delta|/vol >= 10%")
    test_rule(sub, lambda t: abs(t['_tf']['delta_pct']) >= 20, f"{dirx}: |trigger delta|/vol >= 20%")

# =========================================================
# 4. Deeper: cross-tab pre3 against × trigger signature
# =========================================================
print("\n"+"="*70)
print("4. PRE3 × TRIGGER cross-tabs")
print("="*70)

for dirx in ['bullish', 'bearish']:
    sub = [t for t in enriched if t['direction']==dirx]
    # Split by pre3 direction
    good_pre = [t for t in sub if t['_f']['pre3_against'] >= 100]
    neutral_pre = [t for t in sub if abs(t['_f']['pre3_against']) < 100]
    bad_pre = [t for t in sub if t['_f']['pre3_against'] < -100]
    print(f"\n{dirx.upper()} pre3_against buckets:")
    print(f"  pre3>=100 (absorption signal strong): {stats(good_pre)}")
    print(f"  |pre3|<100 (neutral):                 {stats(neutral_pre)}")
    print(f"  pre3<-100 (against absorption):       {stats(bad_pre)}")

# =========================================================
# 5. Final stacked rule test — pre3 + trigger confirm
# =========================================================
print("\n"+"="*70)
print("5. STACKED RULE: strong pre3 + confirming trigger")
print("="*70)
for dirx in ['bullish', 'bearish']:
    sub = [t for t in enriched if t['direction']==dirx]
    def rule(t, d=dirx):
        if t['_f']['pre3_against'] < 0: return False  # require SOME absorption setup
        if d == 'bullish' and t['_tf']['close_pos'] < 0.4: return False  # trigger bar closed weak
        if d == 'bearish' and t['_tf']['close_pos'] > 0.6: return False
        return True
    test_rule(sub, rule, f"{dirx} STACKED: pre3_against>=0 AND trigger close_pos confirms")

print("\nDONE")
