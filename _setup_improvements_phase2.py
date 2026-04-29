"""Phase 2: propose + OOS-validate filter improvements for:
- BofA Scalp
- DD Exhaustion
- Vanna Pivot Bounce
- SB2 Absorption
- Paradigm Reversal
- ES Absorption

For each: identify best/worst buckets, propose filter, train/test OOS split.
"""
import psycopg2
from collections import defaultdict
import json
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

def pull(setup):
    cur.execute("""
    SELECT id, ts, direction, grade, paradigm, spot, outcome_result, outcome_pnl,
           greek_alignment, vix, overvix,
           EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
           EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
           (ts AT TIME ZONE 'America/New_York')::date as d,
           outcome_max_profit, outcome_max_loss
    FROM setup_log
    WHERE setup_name = %s AND outcome_result IS NOT NULL AND spot IS NOT NULL
      AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
    ORDER BY ts
    """, (setup,))
    return [dict(zip(['id','ts','direction','grade','paradigm','spot','outcome','pnl',
                      'align','vix','overvix','h','m','d','mfe','mae'], r)) for r in cur.fetchall()]

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
        cum += float(t['pnl'] or 0)
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
    return {'n':len(trades),'pnl':round(pnl,1),'w':w,'l':l,'wr':round(wr,1),'maxdd':round(mdd,1),'pf':round(pf,2),'avg':round(pnl/max(1,len(trades)),2)}

def oos_split(trades):
    """Split 50/50 by date for OOS validation."""
    dates = sorted(set(t['d'] for t in trades))
    mid = dates[len(dates)//2] if dates else None
    train = [t for t in trades if t['d'] <= mid]
    test = [t for t in trades if t['d'] > mid]
    return train, test, mid

def test_filter(trades, filter_fn, rule_name):
    """Apply filter_fn(trade)→keep? Returns before, after, OOS consistency."""
    kept = [t for t in trades if filter_fn(t)]
    blocked = [t for t in trades if not filter_fn(t)]
    before = stats(trades)
    after = stats(kept)
    blk = stats(blocked)
    improvement = after['pnl'] - before['pnl']

    train, test, split = oos_split(trades)
    train_blk = [t for t in train if not filter_fn(t)]
    test_blk = [t for t in test if not filter_fn(t)]
    train_kept = [t for t in train if filter_fn(t)]
    test_kept = [t for t in test if filter_fn(t)]
    train_delta = stats(train_kept)['pnl'] - stats(train)['pnl']
    test_delta = stats(test_kept)['pnl'] - stats(test)['pnl']
    oos_stable = (train_delta >= 0 and test_delta >= 0)

    return {
        'rule': rule_name,
        'before': before, 'after': after, 'blocked': blk,
        'improvement': round(improvement, 1),
        'train_delta': round(train_delta, 1),
        'test_delta': round(test_delta, 1),
        'oos_stable': oos_stable,
        'split_date': str(split),
    }

def print_filter_result(r):
    marker = "✅ STABLE" if r['oos_stable'] else "⚠️ UNSTABLE"
    print(f"\n  Rule: {r['rule']}")
    print(f"  Before: n={r['before']['n']} pnl={r['before']['pnl']:+.1f} WR={r['before']['wr']:.0f}% MDD={r['before']['maxdd']} PF={r['before']['pf']}")
    print(f"  After:  n={r['after']['n']} pnl={r['after']['pnl']:+.1f} WR={r['after']['wr']:.0f}% MDD={r['after']['maxdd']} PF={r['after']['pf']}")
    print(f"  Blocked: n={r['blocked']['n']} pnl={r['blocked']['pnl']:+.1f} (saves {-r['blocked']['pnl']:+.1f})")
    print(f"  OOS: train Δ={r['train_delta']:+.1f}, test Δ={r['test_delta']:+.1f} {marker}")

# =====================================================
# 1. BofA Scalp
# =====================================================
print("="*70)
print("1. BofA SCALP — Filter Candidates")
print("="*70)
bofa = pull('BofA Scalp')
print(f"Total BofA: {len(bofa)}")

# Rule 1: block after 14:00 (currently blocks after 14:30)
# Our earlier finding: 14:00-14:30 longs = 12.5% WR, -49 pts
print_filter_result(test_filter(bofa, lambda t: t['h'] < 14, "BofA: block time>=14:00"))

# Rule 2: block grade A+
print_filter_result(test_filter(bofa, lambda t: t['grade'] != 'A+', "BofA: block grade A+"))

# Rule 3: block VIX 22-30 (bad chop zone)
print_filter_result(test_filter(bofa, lambda t: t['vix'] is None or float(t['vix']) < 22 or float(t['vix']) >= 30, "BofA: block VIX 22-30"))

# Rule 4: block BofA-LIS paradigm
print_filter_result(test_filter(bofa, lambda t: t['paradigm'] != 'BofA-LIS', "BofA: block BofA-LIS paradigm"))

# Rule 5: block long × align=+3 (14 trades, 25% WR, -61)
print_filter_result(test_filter(bofa, lambda t: not (t['direction'] in ('long','bullish') and t['align']==3), "BofA: block long+align=+3"))

# Rule 6: Long-only VIX<20 specialist
# (not a "filter" per se — isolate the sweet spot)
print("\n  Sweet spot check: BofA long + VIX<20")
sweet = [t for t in bofa if t['direction'] in ('long','bullish') and t['vix'] and float(t['vix']) < 20]
print(f"  Long + VIX<20: {stats(sweet)}")

# Combined rule: block (A+) OR (VIX 22-30) OR (after 14:00) OR (long+align+3) OR (BofA-LIS)
def bofa_combined(t):
    if t['grade'] == 'A+': return False
    if t['vix'] and 22 <= float(t['vix']) < 30: return False
    if t['h'] >= 14: return False
    if t['direction'] in ('long','bullish') and t['align']==3: return False
    if t['paradigm'] == 'BofA-LIS': return False
    return True
print_filter_result(test_filter(bofa, bofa_combined, "BofA COMBINED: block A+ OR VIX22-30 OR h>=14 OR long+3 OR BofA-LIS"))

# =====================================================
# 2. DD Exhaustion
# =====================================================
print("\n" + "="*70)
print("2. DD EXHAUSTION — Filter Candidates")
print("="*70)
dd = pull('DD Exhaustion')
print(f"Total DD: {len(dd)}")

# Rule 1: block long + align=+3 (the 118t -312 monster)
print_filter_result(test_filter(dd, lambda t: not (t['direction']=='long' and t['align']==3), "DD: block long align=+3"))

# Rule 2: block long + VIX>=22 (longs fail in high VIX)
print_filter_result(test_filter(dd, lambda t: not (t['direction']=='long' and t['vix'] and float(t['vix'])>=22), "DD: block long VIX>=22"))

# Rule 3: block long + bad paradigms (GEX-LIS, AG-LIS, AG-PURE — all losing)
bad_long_pars = ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY','SIDIAL-EXTREME')
print_filter_result(test_filter(dd, lambda t: not (t['direction']=='long' and t['paradigm'] in bad_long_pars), "DD: block long on bad paradigms"))

# Rule 4: block short + GEX-LIS (already in V13), + BOFA-PURE (big loser)
print_filter_result(test_filter(dd, lambda t: not (t['direction']=='short' and t['paradigm']=='BOFA-PURE'), "DD: block short BOFA-PURE"))

# Rule 5: block short A+ grade (already V12's grade rule for SC — try for DD)
print_filter_result(test_filter(dd, lambda t: not (t['direction']=='short' and t['grade']=='A+'), "DD: block short A+ grade"))

# Rule 6: block grade=C (DD long C is -112, DD short C is -45)
print_filter_result(test_filter(dd, lambda t: t['grade'] != 'C', "DD: block grade C"))

# Combined DD rule
def dd_combined(t):
    # Longs
    if t['direction'] == 'long':
        if t['align'] == 3: return False
        if t['vix'] and float(t['vix']) >= 22: return False
        if t['paradigm'] in bad_long_pars: return False
    # Shorts
    if t['direction'] == 'short':
        if t['paradigm'] == 'BOFA-PURE': return False
        if t['grade'] == 'A+': return False
    # Both
    if t['grade'] == 'C': return False
    return True
print_filter_result(test_filter(dd, dd_combined, "DD COMBINED: all above rules"))

# =====================================================
# 3. Vanna Pivot Bounce
# =====================================================
print("\n" + "="*70)
print("3. VANNA PIVOT BOUNCE")
print("="*70)
vpb = pull('Vanna Pivot Bounce')
print(f"Total VPB: {len(vpb)}")

print(f"\nOverall stats: {stats(vpb)}")
print(f"  Longs: {stats([t for t in vpb if t['direction']=='long'])}")
print(f"  Shorts: {stats([t for t in vpb if t['direction']=='short'])}")

print("\nBy paradigm:")
for par in sorted({t['paradigm'] or 'NONE' for t in vpb}):
    sub = [t for t in vpb if (t['paradigm'] or 'NONE') == par]
    if sub: print(f"  {par:<20} {stats(sub)}")

print("\nBy grade:")
for g in ['A+','A','A-Entry','B','C']:
    sub = [t for t in vpb if t['grade'] == g]
    if sub: print(f"  grade={g} {stats(sub)}")

print("\nBy VIX:")
for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in vpb if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(f"  VIX {lbl} {stats(sub)}")

print("\nVPB long by hour:")
vpb_longs = [t for t in vpb if t['direction']=='long']
for h in range(9,16):
    sub = [t for t in vpb_longs if t['h']==h]
    if sub: print(f"  h={h:02d} {stats(sub)}")

# =====================================================
# 4. SB2 Absorption
# =====================================================
print("\n" + "="*70)
print("4. SB2 ABSORPTION — Why bearish losing?")
print("="*70)
sb2 = pull('SB2 Absorption')
print(f"Total SB2: {len(sb2)}")
sb2_long = [t for t in sb2 if t['direction']=='bullish']
sb2_short = [t for t in sb2 if t['direction']=='bearish']
print(f"Bullish: {stats(sb2_long)}")
print(f"Bearish: {stats(sb2_short)}")

print("\nSB2 Bearish × VIX:")
for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in sb2_short if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(f"  VIX {lbl} {stats(sub)}")

print("\nSB2 Bearish × grade:")
for g in ['A+','A','A-Entry','B','C','LOG']:
    sub = [t for t in sb2_short if t['grade']==g]
    if sub: print(f"  grade={g} {stats(sub)}")

print("\nSB2 Bearish × paradigm (top):")
by_par = defaultdict(list)
for t in sb2_short: by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl'])[:10]:
    print(f"  {par:<20} {stats(sub)}")

# Test: SB2 bearish filter — block VIX<22 or bad paradigms
# Observation: bearish better at high VIX, bullish fine in low VIX
print_filter_result(test_filter(sb2_short, lambda t: t['vix'] and float(t['vix'])>=22, "SB2 bearish: require VIX>=22"))

# =====================================================
# 5. Paradigm Reversal
# =====================================================
print("\n" + "="*70)
print("5. PARADIGM REVERSAL — Can longs be saved?")
print("="*70)
pr = pull('Paradigm Reversal')
print(f"Total PR: {len(pr)}")
pr_long = [t for t in pr if t['direction']=='long']
pr_short = [t for t in pr if t['direction']=='short']
print(f"Long: {stats(pr_long)}")
print(f"Short: {stats(pr_short)}")

print("\nPR Long by paradigm:")
by_par = defaultdict(list)
for t in pr_long: by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    print(f"  {par:<20} {stats(sub)}")

print("\nPR Long by VIX:")
for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in pr_long if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(f"  VIX {lbl} {stats(sub)}")

# =====================================================
# 6. ES Absorption (recent grading v3 change Apr 13)
# =====================================================
print("\n" + "="*70)
print("6. ES ABSORPTION — post-v3 grading check")
print("="*70)
es = pull('ES Absorption')
es_bull = [t for t in es if t['direction']=='bullish']
es_bear = [t for t in es if t['direction']=='bearish']
print(f"Bullish: {stats(es_bull)}")
print(f"Bearish: {stats(es_bear)}")

# Grading v3 since Apr 13 — check that window
es_v3_bull = [t for t in es_bull if str(t['d']) >= '2026-04-13']
es_v3_bear = [t for t in es_bear if str(t['d']) >= '2026-04-13']
print(f"\nPost-v3 (Apr 13+):")
print(f"  Bullish: {stats(es_v3_bull)}")
print(f"  Bearish: {stats(es_v3_bear)}")

print("\nES Absorption by grade (all time):")
for d, trades in [('bullish', es_bull), ('bearish', es_bear)]:
    print(f"  {d}:")
    for g in ['A+','A','B','C','LOG']:
        sub = [t for t in trades if t['grade']==g]
        if sub: print(f"    grade={g} {stats(sub)}")

print("\nDONE")
