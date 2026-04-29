"""Dedicated ES family deep dive.
Setups: ES Absorption, SB Absorption, SB2 Absorption, SB10 Absorption, Delta Absorption.

Focus:
  1. ES Absorption - multi-dim analysis, find all OOS-stable edges
  2. Why Bearish C+LOG fails so hard (-160 pts)
  3. abs_vol_ratio feature - does volume gate quality correlate with PnL?
  4. Time-of-day patterns
  5. SB/SB2 family comparison
"""
import psycopg2
from collections import defaultdict
import json
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

def pull(setup, start='2026-02-11'):
    cur.execute("""
    SELECT id, ts, direction, grade, paradigm, spot, outcome_result, outcome_pnl,
           greek_alignment, vix, overvix,
           EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
           EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
           (ts AT TIME ZONE 'America/New_York')::date as d,
           outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
           abs_vol_ratio, abs_es_price, abs_details,
           charm_limit_entry, v13_gex_above, v13_dd_near
    FROM setup_log
    WHERE setup_name = %s AND outcome_result IS NOT NULL AND spot IS NOT NULL
      AND (ts AT TIME ZONE 'America/New_York')::date >= %s
      AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
    ORDER BY ts
    """, (setup, start))
    return [dict(zip(['id','ts','direction','grade','paradigm','spot','outcome','pnl',
                      'align','vix','overvix','h','m','d','mfe','mae','elapsed',
                      'vol_ratio','es_price','details','charm_limit',
                      'v13_gex','v13_dd'], r)) for r in cur.fetchall()]

def stats(trades):
    if not trades: return {'n':0,'pnl':0,'w':0,'l':0,'wr':0,'maxdd':0,'pf':0,'avg':0}
    pnl = sum(float(t['pnl'] or 0) for t in trades)
    w = sum(1 for t in trades if t['outcome']=='WIN')
    l = sum(1 for t in trades if t['outcome']=='LOSS')
    wr = 100*w/max(1,w+l)
    gp = sum(float(t['pnl'] or 0) for t in trades if float(t['pnl'] or 0)>0)
    gl = abs(sum(float(t['pnl'] or 0) for t in trades if float(t['pnl'] or 0)<0))
    pf = gp/gl if gl > 0 else 0
    cum=0; peak=0; mdd=0
    for t in sorted(trades, key=lambda x: x['ts']):
        cum += float(t['pnl'] or 0); peak = max(peak, cum); mdd = max(mdd, peak-cum)
    return {'n':len(trades),'pnl':round(pnl,1),'w':w,'l':l,'wr':round(wr,1),'maxdd':round(mdd,1),'pf':round(pf,2),'avg':round(pnl/max(1,len(trades)),2)}

def fmt(s, label):
    if s['n']==0: return f"  {label:<32} (empty)"
    star = "★" if s['n']>=15 and s['wr']>=65 and s['pf']>=2.0 else ""
    warn = "⚠️" if s['n']>=10 and s['pnl']<-30 else ""
    return (f"  {label:<32} n={s['n']:>3} WR={s['wr']:>5.1f}% PnL={s['pnl']:>+7.1f} "
            f"MDD=-{s['maxdd']:>5.1f} PF={s['pf']:.2f} avg={s['avg']:+.2f} {star}{warn}")

def oos_test(trades, fn, name):
    kept = [t for t in trades if fn(t)]
    blocked = [t for t in trades if not fn(t)]
    before = stats(trades); after = stats(kept); blk = stats(blocked)
    dates = sorted(set(t['d'] for t in trades))
    mid = dates[len(dates)//2] if dates else None
    train = [t for t in trades if t['d'] <= mid]
    test = [t for t in trades if t['d'] > mid]
    train_k = [t for t in train if fn(t)]
    test_k = [t for t in test if fn(t)]
    train_d = stats(train_k)['pnl'] - stats(train)['pnl']
    test_d = stats(test_k)['pnl'] - stats(test)['pnl']
    stable = train_d >= 0 and test_d >= 0
    print(f"\n  Rule: {name}")
    print(f"    Before: {before}  After: {after}")
    print(f"    Blocks: {blk['n']} pnl={blk['pnl']:+.1f} (saves {-blk['pnl']:+.1f})")
    print(f"    OOS: train Δ={train_d:+.1f}  test Δ={test_d:+.1f}  {'✅ STABLE' if stable else '⚠️ UNSTABLE'}")

# ========= ES ABSORPTION — Multi-dimensional =========
print("="*72)
print("ES ABSORPTION — DEEP DIVE")
print("="*72)
es = pull('ES Absorption')
print(f"Total: {len(es)} trades, {stats(es)}")

es_bull = [t for t in es if t['direction']=='bullish']
es_bear = [t for t in es if t['direction']=='bearish']
print(f"Bullish: {stats(es_bull)}")
print(f"Bearish: {stats(es_bear)}")

# BREAKDOWN 1: grade x direction (already known)
print("\n### BY GRADE x DIRECTION ###")
for dirx, tr in [('Bullish', es_bull), ('Bearish', es_bear)]:
    print(f"\n{dirx}:")
    for g in ['A+','A','B','C','LOG']:
        sub = [t for t in tr if t['grade']==g]
        if sub: print(fmt(stats(sub), f"grade={g}"))

# BREAKDOWN 2: alignment x direction (ES uses align)
print("\n### BY ALIGNMENT x DIRECTION ###")
for dirx, tr in [('Bullish', es_bull), ('Bearish', es_bear)]:
    print(f"\n{dirx}:")
    for a in [-3,-2,-1,0,1,2,3]:
        sub = [t for t in tr if t['align']==a]
        if sub: print(fmt(stats(sub), f"align={a:+d}"))

# BREAKDOWN 3: VIX x direction
print("\n### BY VIX x DIRECTION ###")
for dirx, tr in [('Bullish', es_bull), ('Bearish', es_bear)]:
    print(f"\n{dirx}:")
    for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
        sub = [t for t in tr if t['vix'] and lo<=float(t['vix'])<hi]
        if sub: print(fmt(stats(sub), f"VIX {lbl}"))

# BREAKDOWN 4: Hour x direction
print("\n### BY HOUR x DIRECTION ###")
for dirx, tr in [('Bullish', es_bull), ('Bearish', es_bear)]:
    print(f"\n{dirx}:")
    for h in range(9, 16):
        sub = [t for t in tr if t['h']==h]
        if sub: print(fmt(stats(sub), f"h={h:02d}"))

# BREAKDOWN 5: Volume ratio buckets (ES Abs trigger = vol >= 1.5x avg)
print("\n### BY VOL RATIO x DIRECTION ###")
for dirx, tr in [('Bullish', es_bull), ('Bearish', es_bear)]:
    print(f"\n{dirx}:")
    for lo, hi, lbl in [(0,1.5,'<1.5'),(1.5,2,'1.5-2'),(2,3,'2-3'),(3,5,'3-5'),(5,100,'>=5')]:
        sub = [t for t in tr if t['vol_ratio'] and lo<=float(t['vol_ratio'])<hi]
        if sub: print(fmt(stats(sub), f"vol_ratio {lbl}"))

# BREAKDOWN 6: Paradigm x direction (small samples per paradigm — combined view)
print("\n### BY PARADIGM x DIRECTION (top 10 each) ###")
for dirx, tr in [('Bullish', es_bull), ('Bearish', es_bear)]:
    print(f"\n{dirx}:")
    by_par = defaultdict(list)
    for t in tr: by_par[t['paradigm'] or 'NONE'].append(t)
    for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl'])[:10]:
        print(fmt(stats(sub), f"{par}"))

# BREAKDOWN 7: V13 gex_above / dd_near also recorded for ES — does structure help?
print("\n### ES Bearish × GEX above spot ###")
for lo, hi, lbl in [(0,50,'<50'),(50,100,'50-100'),(100,200,'100-200'),(200,500,'200-500'),(500,10000,'>=500')]:
    sub = [t for t in es_bear if t['v13_gex'] and lo<=float(t['v13_gex'])<hi]
    if sub: print(fmt(stats(sub), f"Bear GEX-above {lbl}"))

print("\n### ES Bullish × GEX above spot ###")
for lo, hi, lbl in [(0,50,'<50'),(50,100,'50-100'),(100,200,'100-200'),(200,500,'200-500'),(500,10000,'>=500')]:
    sub = [t for t in es_bull if t['v13_gex'] and lo<=float(t['v13_gex'])<hi]
    if sub: print(fmt(stats(sub), f"Bull GEX-above {lbl}"))

# BREAKDOWN 8: combined rules test
print("\n### ES ABSORPTION FILTER CANDIDATES ###")

# Rule: block bearish C+LOG
oos_test(es, lambda t: not (t['direction']=='bearish' and t['grade'] in ('C','LOG')),
         "ES: block bearish grade C+LOG (known winner)")

# Rule: block very low vol_ratio (insufficient conviction)
oos_test(es, lambda t: not (t['vol_ratio'] and float(t['vol_ratio']) < 1.5),
         "ES: require vol_ratio >= 1.5")

# Rule: block late-day bullish (heavy time pressure)
oos_test(es_bull, lambda t: t['h'] < 15, "ES Bull: block h>=15")

# Rule: block late-day bearish
oos_test(es_bear, lambda t: t['h'] < 15, "ES Bear: block h>=15")

# Rule: combined bearish (C+LOG + time 15:00+)
oos_test(es_bear, lambda t: not (t['grade'] in ('C','LOG') or t['h']>=15),
         "ES Bear: block (C+LOG) OR h>=15")

# Rule: block bullish align=-3 (fully opposing alignment)
# This is contrarian setup — align=-3 means Greeks fully bearish, trade is bullish contrarian
# This is counter-intuitive
oos_test(es_bull, lambda t: t['align'] != -3, "ES Bull: block align=-3 contrarian")
oos_test(es_bear, lambda t: t['align'] != 3, "ES Bear: block align=+3 contrarian")

# Rule: block Bull align in [-2,-1,0] (weakest buckets)
oos_test(es_bull, lambda t: t['align'] not in (-1, 0), "ES Bull: block align in {-1, 0}")

# Combined ES rule
def es_combined(t):
    if t['direction']=='bearish':
        if t['grade'] in ('C','LOG'): return False
        if t['h'] >= 15: return False
    if t['direction']=='bullish':
        if t['grade'] == 'C': return False
        if t['h'] >= 15: return False
    return True
oos_test(es, es_combined, "ES COMBINED: block Bear C+LOG + both h>=15 + Bull C")

# =========
# SB, SB2, SB10, Delta
# =========
print("\n"+"="*72)
print("SB / SB2 / SB10 / DELTA — OVERVIEW")
print("="*72)
for s in ['SB Absorption', 'SB2 Absorption', 'SB10 Absorption', 'Delta Absorption']:
    tr = pull(s)
    print(f"\n{s}: {stats(tr)}")
    tbull = [t for t in tr if t['direction'] in ('bullish','long')]
    tbear = [t for t in tr if t['direction'] in ('bearish','short')]
    print(fmt(stats(tbull), f"bullish"))
    print(fmt(stats(tbear), f"bearish"))

# SB2 detailed retest (since it's larger)
print("\n"+"="*72)
print("SB2 ABSORPTION — Detailed (92 trades)")
print("="*72)
sb2 = pull('SB2 Absorption')
sb2_bull = [t for t in sb2 if t['direction']=='bullish']
sb2_bear = [t for t in sb2 if t['direction']=='bearish']

print("\nSB2 Bull × grade:")
for g in ['A+','A','B','C','LOG']:
    sub = [t for t in sb2_bull if t['grade']==g]
    if sub: print(fmt(stats(sub), f"Bull {g}"))

print("\nSB2 Bull × vol ratio:")
for lo, hi, lbl in [(0,1.5,'<1.5'),(1.5,2,'1.5-2'),(2,3,'2-3'),(3,5,'3-5'),(5,100,'>=5')]:
    sub = [t for t in sb2_bull if t['vol_ratio'] and lo<=float(t['vol_ratio'])<hi]
    if sub: print(fmt(stats(sub), f"Bull vol_ratio {lbl}"))

print("\nSB2 Bull × hour:")
for h in range(9, 16):
    sub = [t for t in sb2_bull if t['h']==h]
    if sub: print(fmt(stats(sub), f"Bull h={h:02d}"))

oos_test(sb2_bull, lambda t: t['grade'] != 'C', "SB2 Bull: block grade C")
oos_test(sb2_bull, lambda t: t['vol_ratio'] and float(t['vol_ratio']) >= 2, "SB2 Bull: require vol_ratio>=2")

print("\nDONE")
