"""Combine VIX regime + bar-level features for ES Absorption.
Test if bar patterns that fail OOS globally are stable within specific VIX regimes.
"""
import psycopg2, json
from collections import defaultdict
import statistics
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# Pull ES Absorption trades with bar details + VIX
cur.execute("""
SELECT id, ts, direction, outcome_result, outcome_pnl, vix, abs_details,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE setup_name = 'ES Absorption' AND abs_details IS NOT NULL AND vix IS NOT NULL
  AND outcome_result IS NOT NULL
  AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
ORDER BY ts
""")
trades = []
for r in cur.fetchall():
    tid, ts, dirx, outcome, pnl, vix, details, d = r
    if not details or 'bar_idx' not in details: continue
    trades.append({
        'id': tid, 'ts': ts, 'd': d, 'direction': dirx, 'outcome': outcome,
        'pnl': float(pnl or 0), 'vix': float(vix), 'bar_idx': details['bar_idx'],
    })
print(f"ES Abs trades with VIX + bar_idx: {len(trades)}")

# Get bars per trade
def get_bars(trade_date, trigger_idx):
    cur.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_delta, cvd_close
    FROM es_range_bars
    WHERE trade_date = %s AND source = 'rithmic'
      AND bar_idx BETWEEN %s AND %s
    ORDER BY bar_idx
    """, (trade_date, trigger_idx - 8, trigger_idx))
    return [dict(zip(['idx','open','high','low','close','vol','delta','cvd'], r)) for r in cur.fetchall()]

enr = []
for i, t in enumerate(trades):
    bars = get_bars(t['d'], t['bar_idx'])
    if len(bars) < 4: continue
    trigger = next((b for b in bars if b['idx'] == t['bar_idx']), None)
    pre3 = [b for b in bars if b['idx'] < t['bar_idx']][-3:]
    if not trigger or len(pre3) < 3 or trigger['delta'] is None: continue
    sum_delta = sum(b['delta'] or 0 for b in pre3)
    pre3_against = (-sum_delta) if t['direction'] == 'bullish' else sum_delta
    rng = (trigger['high'] - trigger['low']) or 1
    close_pos = (trigger['close'] - trigger['low']) / rng
    t['pre3_against'] = pre3_against
    t['trig_delta'] = trigger['delta']
    t['close_pos'] = close_pos
    t['trig_vol'] = trigger['vol']
    t['delta_pct'] = (trigger['delta'] / trigger['vol'] * 100) if trigger['vol'] else 0
    enr.append(t)
    if (i+1) % 100 == 0: print(f"  {i+1}/{len(trades)}", flush=True)
print(f"Enriched: {len(enr)}")

def stats(tr):
    if not tr: return {'n':0,'pnl':0,'wr':0}
    pnl = sum(t['pnl'] for t in tr)
    w = sum(1 for t in tr if t['outcome']=='WIN')
    l = sum(1 for t in tr if t['outcome']=='LOSS')
    return {'n':len(tr),'pnl':round(pnl,1),'wr':round(100*w/max(1,w+l),1), 'w':w,'l':l}

def fmt(s, label):
    mark = ""
    if s['n']>=10 and s['wr']>=65: mark = "★"
    if s['n']>=10 and s['pnl']<-30: mark = "⚠️"
    return f"  {label:<42} n={s['n']:>3} WR={s['wr']:>5.1f}% PnL={s['pnl']:>+7.1f} {mark}"

# Test OOS-stability of a rule
def oos_test(trades, predicate, name):
    if not trades: return
    dates = sorted(set(t['d'] for t in trades))
    mid = dates[len(dates)//2]
    train = [t for t in trades if t['d']<=mid]
    test = [t for t in trades if t['d']>mid]
    tk = [t for t in train if predicate(t)]
    ek = [t for t in test if predicate(t)]
    td = stats(tk)['pnl'] - stats(train)['pnl']
    ed = stats(ek)['pnl'] - stats(test)['pnl']
    stable = td >= 0 and ed >= 0
    kept = [t for t in trades if predicate(t)]
    blocked = [t for t in trades if not predicate(t)]
    print(f"\n  {name}")
    print(f"    Before: {stats(trades)}  After: {stats(kept)}")
    print(f"    Blocks: {len(blocked)} pnl={stats(blocked)['pnl']:+.1f}")
    print(f"    OOS train Δ={td:+.1f}  test Δ={ed:+.1f}  {'✅ STABLE' if stable else '⚠️ UNSTABLE'}")

# =================================================================
# Split by VIX regime first, then by direction, then by bar features
# =================================================================
vix_buckets = [(0, 22, '<22'), (22, 26, '22-26'), (26, 30, '26-30'), (30, 100, '30+')]

print("\n" + "="*72)
print("VIX × DIRECTION × BAR-LEVEL FEATURES")
print("="*72)

for vix_lo, vix_hi, vix_lbl in vix_buckets:
    vix_trades = [t for t in enr if vix_lo <= t['vix'] < vix_hi]
    if len(vix_trades) < 20:
        print(f"\nVIX {vix_lbl}: {len(vix_trades)} trades — too small, skip")
        continue
    print(f"\n{'='*50}")
    print(f"VIX {vix_lbl} ({len(vix_trades)} trades)")
    print('='*50)
    for dirx in ['bullish', 'bearish']:
        sub = [t for t in vix_trades if t['direction'] == dirx]
        if len(sub) < 10: continue
        print(f"\n--- {dirx.upper()} (n={len(sub)}) in VIX {vix_lbl} ---")
        base = stats(sub)
        print(f"  Baseline: {base}")

        # Pre3 bucket
        print("\n  Pre3_against bucket:")
        print(fmt(stats([t for t in sub if t['pre3_against']>=100]), "pre3_against>=100 (absorption)"))
        print(fmt(stats([t for t in sub if abs(t['pre3_against'])<100]), "|pre3_against|<100 (neutral)"))
        print(fmt(stats([t for t in sub if t['pre3_against']<-100]), "pre3_against<-100 (trend)"))

        # Trigger close_pos
        print("\n  Trigger close position:")
        print(fmt(stats([t for t in sub if t['close_pos']>=0.7]), "close_pos>=0.7 (strong close up)"))
        print(fmt(stats([t for t in sub if 0.3<=t['close_pos']<0.7]), "0.3<=close_pos<0.7"))
        print(fmt(stats([t for t in sub if t['close_pos']<0.3]), "close_pos<0.3 (strong close down)"))

        # Trigger delta direction
        pos = [t for t in sub if t['trig_delta']>0]
        neg = [t for t in sub if t['trig_delta']<0]
        print("\n  Trigger delta sign:")
        print(fmt(stats(pos), f"delta>0 (buy pressure)"))
        print(fmt(stats(neg), f"delta<0 (sell pressure)"))

# =================================================================
# Focused rule test: within each VIX regime
# =================================================================
print("\n" + "="*72)
print("VIX-CONDITIONAL RULE TESTS")
print("="*72)

for vix_lo, vix_hi, vix_lbl in vix_buckets:
    vix_trades = [t for t in enr if vix_lo <= t['vix'] < vix_hi]
    if len(vix_trades) < 30: continue
    for dirx in ['bullish', 'bearish']:
        sub = [t for t in vix_trades if t['direction'] == dirx]
        if len(sub) < 20: continue
        print(f"\n### VIX {vix_lbl} × {dirx.upper()} (n={len(sub)}) ###")
        # Bullish: prefer trend-continuation (pre3_against < 0)
        # Bearish: prefer absorption (pre3_against >= 0)
        if dirx == 'bullish':
            oos_test(sub, lambda t: t['pre3_against'] < 100,
                     f"BULL VIX {vix_lbl}: block pre3_against>=100 (weak absorption trap)")
            oos_test(sub, lambda t: t['close_pos'] >= 0.5,
                     f"BULL VIX {vix_lbl}: require close_pos>=0.5")
            oos_test(sub, lambda t: t['trig_delta'] > 0,
                     f"BULL VIX {vix_lbl}: require delta>0 confirm")
        else:
            oos_test(sub, lambda t: t['pre3_against'] >= 0,
                     f"BEAR VIX {vix_lbl}: require pre3_against>=0")
            oos_test(sub, lambda t: t['close_pos'] <= 0.5,
                     f"BEAR VIX {vix_lbl}: require close_pos<=0.5")

# =================================================================
# Best combo per regime (stacked rules)
# =================================================================
print("\n" + "="*72)
print("BEST COMBINED RULES PER VIX REGIME")
print("="*72)

# VIX 22-26 bearish is already good — see if bar features narrow further
bear_22_26 = [t for t in enr if 22 <= t['vix'] < 26 and t['direction']=='bearish']
print(f"\nBEAR VIX 22-26 baseline: {stats(bear_22_26)}")
print("Combined: pre3_against>=0 AND close_pos<=0.5:")
oos_test(bear_22_26,
         lambda t: t['pre3_against']>=0 and t['close_pos']<=0.5,
         "BEAR VIX 22-26 combined")

# Bullish across regimes
for vix_lo, vix_hi, vix_lbl in vix_buckets:
    sub = [t for t in enr if vix_lo<=t['vix']<vix_hi and t['direction']=='bullish']
    if len(sub) < 30: continue
    print(f"\nBULL VIX {vix_lbl} combined: close_pos>=0.5 AND delta>0:")
    oos_test(sub,
             lambda t: t['close_pos']>=0.5 and t['trig_delta']>0,
             f"BULL VIX {vix_lbl} combined")

print("\nDONE")
