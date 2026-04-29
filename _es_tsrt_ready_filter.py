"""Find a SIMPLE CONSISTENT ES Absorption filter for TSRT readiness.
Target: WR >= 65%, PF >= 2, MaxDD small, stable OOS.
"""
import psycopg2, json
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute("""
SELECT id, ts, direction, grade, paradigm, outcome_result, outcome_pnl,
       greek_alignment, vix, abs_details,
       (ts AT TIME ZONE 'America/New_York')::date as d,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h
FROM setup_log
WHERE setup_name = 'ES Absorption' AND abs_details IS NOT NULL AND vix IS NOT NULL
  AND outcome_result IS NOT NULL
  AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
ORDER BY ts
""")
trades = []
for r in cur.fetchall():
    tid, ts, dirx, grade, paradigm, outcome, pnl, align, vix, details, d, h = r
    if not details or 'bar_idx' not in details: continue
    trades.append({
        'id': tid, 'ts': ts, 'd': d, 'h': h, 'direction': dirx,
        'grade': grade, 'paradigm': paradigm,
        'outcome': outcome, 'pnl': float(pnl or 0),
        'align': int(align or 0), 'vix': float(vix), 'bar_idx': details['bar_idx'],
    })
print(f"Total ES Abs trades: {len(trades)}")

# Enrich with bar-level pre3_against
def bars(d, idx):
    cur.execute("""
    SELECT bar_idx, bar_delta FROM es_range_bars
    WHERE trade_date=%s AND source='rithmic' AND bar_idx BETWEEN %s AND %s
    ORDER BY bar_idx
    """, (d, idx-4, idx))
    return cur.fetchall()

enr = []
for t in trades:
    bs = bars(t['d'], t['bar_idx'])
    pre3 = [r for r in bs if r[0] < t['bar_idx']][-3:]
    if len(pre3) < 3: continue
    sum_d = sum(r[1] or 0 for r in pre3)
    t['pre3_against'] = -sum_d if t['direction']=='bullish' else sum_d
    enr.append(t)
print(f"Enriched: {len(enr)}")

def stats(tr):
    if not tr: return {'n':0,'pnl':0,'w':0,'l':0,'wr':0,'maxdd':0,'pf':0,'avg':0}
    pnl = sum(t['pnl'] for t in tr)
    w = sum(1 for t in tr if t['outcome']=='WIN')
    l = sum(1 for t in tr if t['outcome']=='LOSS')
    wr = 100*w/max(1,w+l)
    gp = sum(t['pnl'] for t in tr if t['pnl']>0)
    gl = abs(sum(t['pnl'] for t in tr if t['pnl']<0))
    pf = gp/gl if gl>0 else 0
    cum=0; peak=0; mdd=0
    for t in sorted(tr, key=lambda x: x['ts']):
        cum += t['pnl']; peak = max(peak, cum); mdd = max(mdd, peak-cum)
    return {'n':len(tr),'pnl':round(pnl,1),'w':w,'l':l,
            'wr':round(wr,1),'maxdd':round(mdd,1),'pf':round(pf,2),'avg':round(pnl/max(1,len(tr)),2)}

def oos_check(tr, fn):
    dates = sorted(set(t['d'] for t in tr))
    mid = dates[len(dates)//2]
    train = [t for t in tr if t['d']<=mid]
    test = [t for t in tr if t['d']>mid]
    tk = [t for t in train if fn(t)]
    ek = [t for t in test if fn(t)]
    return (stats(tk)['pnl'] - stats(train)['pnl'],
            stats(ek)['pnl'] - stats(test)['pnl'])

def monthly_check(tr):
    from collections import defaultdict
    by_m = defaultdict(list)
    for t in tr:
        by_m[f"{t['d'].year}-{t['d'].month:02d}"].append(t)
    return {m: stats(trades) for m, trades in sorted(by_m.items())}

def evaluate(name, predicate, base=enr):
    kept = [t for t in base if predicate(t)]
    s = stats(kept)
    td, ed = oos_check(base, predicate)
    stable = td >= 0 and ed >= 0
    monthly = monthly_check(kept)
    # TSRT criteria
    tsrt_ready = (s['n'] >= 30 and s['wr'] >= 65 and s['pf'] >= 2.0
                  and s['maxdd'] <= 40 and stable)
    tag = "🎯 TSRT-READY" if tsrt_ready else ("✅ OOS stable" if stable else "⚠️ unstable")
    print(f"\n{'='*65}")
    print(f"{name}")
    print(f"  {s}")
    print(f"  OOS train Δ={td:+.1f}  test Δ={ed:+.1f}  {tag}")
    print(f"  Monthly:")
    for m, ms in monthly.items():
        print(f"    {m}: {ms}")
    return s, stable, tsrt_ready

print("\n"+"="*65)
print("BASELINE (all ES Abs)")
print("="*65)
print(f"  {stats(enr)}")

# Option 1: Sweet-spot only (bear VIX 22-26)
evaluate("Option 1: BEAR only, VIX 22-26 only",
         lambda t: t['direction']=='bearish' and 22<=t['vix']<26)

# Option 2: Bearish sweet spot + exclude C/LOG
evaluate("Option 2: BEAR VIX 22-26, grade not C/LOG",
         lambda t: t['direction']=='bearish' and 22<=t['vix']<26 and t['grade'] not in ('C','LOG'))

# Option 3: Sweet spot + grade A/A+ only
evaluate("Option 3: BEAR VIX 22-26, grade A/A+",
         lambda t: t['direction']=='bearish' and 22<=t['vix']<26 and t['grade'] in ('A','A+'))

# Option 4: Bullish only VIX <22 sweet spot
evaluate("Option 4: BULL only, VIX <22",
         lambda t: t['direction']=='bullish' and t['vix']<22)

# Option 5: Both directions, their respective sweet spots
evaluate("Option 5: BULL VIX<22 + BEAR VIX 22-26",
         lambda t: (t['direction']=='bullish' and t['vix']<22) or
                   (t['direction']=='bearish' and 22<=t['vix']<26))

# Option 6: Both sweet spots, exclude C/LOG
evaluate("Option 6: BULL VIX<22 + BEAR VIX 22-26, no C/LOG",
         lambda t: ((t['direction']=='bullish' and t['vix']<22) or
                    (t['direction']=='bearish' and 22<=t['vix']<26))
                   and t['grade'] not in ('C','LOG'))

# Option 7: Both sweet spots + grade A/A+ only
evaluate("Option 7: sweet spots + grade A/A+",
         lambda t: ((t['direction']=='bullish' and t['vix']<22) or
                    (t['direction']=='bearish' and 22<=t['vix']<26))
                   and t['grade'] in ('A','A+'))

# Option 8: Bearish 22-26 + grade A + pre3 confirm
evaluate("Option 8: BEAR VIX 22-26, grade A/A+, pre3_against>0",
         lambda t: t['direction']=='bearish' and 22<=t['vix']<26
                   and t['grade'] in ('A','A+') and t['pre3_against']>0)

# Option 9: Widen bear to 22-28
evaluate("Option 9: BEAR VIX 22-28, grade A/A+",
         lambda t: t['direction']=='bearish' and 22<=t['vix']<28 and t['grade'] in ('A','A+'))

# Option 10: BEAR VIX 22-26 grade A/A+ (core shortable regime)
evaluate("Option 10: BEAR VIX 22-26 A+/A grades — STRICTEST",
         lambda t: t['direction']=='bearish' and 22<=t['vix']<26 and t['grade'] in ('A','A+'))

print("\nDONE")
