"""Phase 3: Deep-dive on setups NOT covered in Phase 2.
Focus: SC, AG Short (TSRT setups), ES Absorption, GEX Long, Delta Absorption.
All analysis with V13 filter applied first — find what's LEFT after V13.
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
           outcome_max_profit, outcome_max_loss,
           v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side
    FROM setup_log
    WHERE setup_name = %s AND outcome_result IS NOT NULL AND spot IS NOT NULL
      AND (ts AT TIME ZONE 'America/New_York')::date >= %s
      AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
    ORDER BY ts
    """, (setup, start))
    return [dict(zip(['id','ts','direction','grade','paradigm','spot','outcome','pnl',
                      'align','vix','overvix','h','m','d','mfe','mae',
                      'v13_gex','v13_dd','vc','vp'], r)) for r in cur.fetchall()]

def passes_v13(t, setup_name):
    """Mirror _passes_live_filter()'s current V13 logic for SC/AG/DD only."""
    grade = t.get('grade'); paradigm = t.get('paradigm'); align = t.get('align') or 0
    vix = float(t['vix']) if t['vix'] else None
    ovx = float(t['overvix']) if t['overvix'] else -99
    h, m = t['h'], t['m']
    is_long = t['direction'] in ('long','bullish')
    # Common gates
    if setup_name == 'Skew Charm' and grade in ('C','LOG'):
        return False
    if setup_name in ('Skew Charm','DD Exhaustion'):
        if (h==14 and m>=30) or h==15: return False
    if is_long and paradigm == 'SIDIAL-EXTREME':
        return False
    if is_long:
        if align < 2: return False
        if setup_name != 'Skew Charm' and vix is not None and vix > 22 and ovx < 2:
            return False
        # DD long extras (V13)
        if setup_name == 'DD Exhaustion':
            if align >= 3: return False
            if vix is not None and vix >= 22: return False
            if paradigm in ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return False
            if grade == 'C': return False
        # SC long vanna
        if setup_name == 'Skew Charm':
            if t['vc'] == 'A' and t['vp'] == 'B': return False
        return True
    # Shorts
    if setup_name in ('Skew Charm','DD Exhaustion') and paradigm == 'GEX-LIS': return False
    if setup_name == 'AG Short' and paradigm == 'AG-TARGET': return False
    # GEX/DD magnet block
    if setup_name in ('Skew Charm','DD Exhaustion'):
        if t.get('v13_gex') and float(t['v13_gex']) >= 75: return False
        if t.get('v13_dd') and float(t['v13_dd']) >= 3_000_000_000: return False
    # Vanna blocks
    if t.get('vc'):
        if setup_name == 'DD Exhaustion' and t['vc']=='A': return False
        if setup_name == 'Skew Charm' and t['vc']=='A' and t['vp']=='B': return False
        if setup_name == 'AG Short' and t['vc']=='B' and t['vp']=='A': return False
    # DD short extras
    if setup_name == 'DD Exhaustion':
        if paradigm == 'BOFA-PURE': return False
        if grade == 'A+': return False
        if grade == 'C': return False
        return align != 0
    return True

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

def oos(trades):
    dates = sorted(set(t['d'] for t in trades))
    if not dates: return None, None, None
    mid = dates[len(dates)//2]
    return [t for t in trades if t['d'] <= mid], [t for t in trades if t['d'] > mid], mid

def fmt_stats(s, label, mark_good=True):
    if s['n']==0: return f"  {label:<30} (empty)"
    star = ""
    if mark_good and s['n']>=15 and s['wr']>=65 and s['pf']>=2.0: star = "★"
    warn = ""
    if s['n']>=10 and s['pnl']<-30: warn = "⚠️"
    return (f"  {label:<30} n={s['n']:>3} WR={s['wr']:>5.1f}% PnL={s['pnl']:>+7.1f} "
            f"MDD=-{s['maxdd']:>5.1f} PF={s['pf']:.2f} avg={s['avg']:+.2f} {star}{warn}")

def test_rule(base, fn, name):
    kept = [t for t in base if fn(t)]
    blocked = [t for t in base if not fn(t)]
    before = stats(base); after = stats(kept); blk = stats(blocked)
    train, test, split = oos(base)
    train_k = [t for t in train if fn(t)] if train else []
    test_k = [t for t in test if fn(t)] if test else []
    train_d = stats(train_k)['pnl'] - stats(train)['pnl'] if train else 0
    test_d = stats(test_k)['pnl'] - stats(test)['pnl'] if test else 0
    stable = train_d >= 0 and test_d >= 0
    mark = "✅ STABLE" if stable else "⚠️ UNSTABLE"
    print(f"\n  Rule: {name}")
    print(f"    Before: {before}")
    print(f"    After:  {after}")
    print(f"    Blocks: {blk['n']} trades pnl={blk['pnl']:+.1f} (saves {-blk['pnl']:+.1f})")
    print(f"    OOS: train Δ={train_d:+.1f}  test Δ={test_d:+.1f}  {mark}")
    return {'name': name, 'stable': stable, 'after': after, 'before': before, 'saves': -blk['pnl']}

# ============================================================
# 1. Skew Charm — Deep dive post-V13 (what's LEFT)
# ============================================================
print("="*72)
print("1. SKEW CHARM — Post-V13 residual analysis")
print("="*72)

sc = pull('Skew Charm')
sc_v13 = [t for t in sc if passes_v13(t, 'Skew Charm')]
print(f"All SC trades: {len(sc)}")
print(f"After V13: {len(sc_v13)}")
print(f"V13 SC stats: {stats(sc_v13)}")

sc_v13_long = [t for t in sc_v13 if t['direction'] in ('long','bullish')]
sc_v13_short = [t for t in sc_v13 if t['direction'] in ('short','bearish')]
print(f"  V13 SC longs: {stats(sc_v13_long)}")
print(f"  V13 SC shorts: {stats(sc_v13_short)}")

print("\nSC LONG (post-V13) × paradigm:")
by_par = defaultdict(list)
for t in sc_v13_long: by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    print(fmt_stats(stats(sub), f"long {par}"))

print("\nSC LONG (post-V13) × VIX:")
for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in sc_v13_long if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"long VIX {lbl}"))

print("\nSC LONG (post-V13) × alignment:")
for a in [-3,-2,-1,0,1,2,3]:
    sub = [t for t in sc_v13_long if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"long align={a:+d}"))

print("\nSC LONG (post-V13) × hour:")
for h in range(9, 16):
    sub = [t for t in sc_v13_long if t['h']==h]
    if sub: print(fmt_stats(stats(sub), f"long h={h:02d}"))

print("\nSC SHORT (post-V13) × paradigm:")
by_par = defaultdict(list)
for t in sc_v13_short: by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    print(fmt_stats(stats(sub), f"short {par}"))

print("\nSC SHORT (post-V13) × grade:")
for g in ['A+','A','B']:
    sub = [t for t in sc_v13_short if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"short {g}"))

print("\nSC SHORT (post-V13) × alignment:")
for a in [-3,-2,-1,0,1,2,3]:
    sub = [t for t in sc_v13_short if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"short align={a:+d}"))

# Test candidate filters on SC post-V13
print("\nSC LONG candidate filters (on V13-passing set):")
test_rule(sc_v13_long, lambda t: t['align']>=2, "SC long: require align>=2 (V13 already has >=2)")
test_rule(sc_v13_long, lambda t: not (t['vix'] and float(t['vix'])>=26), "SC long: block VIX>=26")
test_rule(sc_v13_long, lambda t: t['paradigm'] not in ('GEX-LIS','AG-LIS'), "SC long: block GEX-LIS/AG-LIS paradigms")

print("\nSC SHORT candidate filters:")
test_rule(sc_v13_short, lambda t: t['grade']!='A+', "SC short: block A+ grade")
test_rule(sc_v13_short, lambda t: t['align']!=0, "SC short: require align!=0")
test_rule(sc_v13_short, lambda t: t['paradigm'] not in ('AG-PURE','AG-LIS'), "SC short: block AG-PURE/AG-LIS")

# ============================================================
# 2. AG Short — Already 77% WR, but where does it lose?
# ============================================================
print("\n\n"+"="*72)
print("2. AG SHORT — Deep dive")
print("="*72)
ag = pull('AG Short')
ag_v13 = [t for t in ag if passes_v13(t, 'AG Short')]
print(f"All AG shorts: {stats(ag)}")
print(f"V13 AG shorts: {stats(ag_v13)}")

print("\nAG Short × paradigm:")
by_par = defaultdict(list)
for t in ag_v13: by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    print(fmt_stats(stats(sub), f"AG {par}"))

print("\nAG Short × alignment:")
for a in [-3,-2,-1,0,1,2,3]:
    sub = [t for t in ag_v13 if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"align={a:+d}"))

print("\nAG Short × VIX:")
for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in ag_v13 if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"VIX {lbl}"))

print("\nAG Short × grade:")
for g in ['A+','A','A-Entry','B','C']:
    sub = [t for t in ag_v13 if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"grade={g}"))

print("\nAG Short × hour:")
for h in range(9, 17):
    sub = [t for t in ag_v13 if t['h']==h]
    if sub: print(fmt_stats(stats(sub), f"h={h:02d}"))

print("\nAG Short candidate filters:")
test_rule(ag_v13, lambda t: t['grade']!='C', "AG: block grade C")
test_rule(ag_v13, lambda t: t['align']!=0, "AG: require align!=0")
test_rule(ag_v13, lambda t: t['paradigm'] != 'AG-LIS' or t['align']!=0, "AG: block AG-LIS when align=0")

# ============================================================
# 3. ES Absorption — grade v3 check, find residual patterns
# ============================================================
print("\n\n"+"="*72)
print("3. ES ABSORPTION — Deep dive")
print("="*72)
es = pull('ES Absorption')
es_bull = [t for t in es if t['direction']=='bullish']
es_bear = [t for t in es if t['direction']=='bearish']
print(f"ES bullish: {stats(es_bull)}")
print(f"ES bearish: {stats(es_bear)}")

print("\nES Bull × grade × time period:")
for g in ['A+','A','B','C','LOG']:
    sub_pre = [t for t in es_bull if t['grade']==g and str(t['d']) < '2026-04-13']
    sub_post = [t for t in es_bull if t['grade']==g and str(t['d']) >= '2026-04-13']
    if sub_pre: print(fmt_stats(stats(sub_pre), f"Bull {g} pre-v3"))
    if sub_post: print(fmt_stats(stats(sub_post), f"Bull {g} post-v3"))

print("\nES Bear × grade × time period:")
for g in ['A+','A','B','C','LOG']:
    sub_pre = [t for t in es_bear if t['grade']==g and str(t['d']) < '2026-04-13']
    sub_post = [t for t in es_bear if t['grade']==g and str(t['d']) >= '2026-04-13']
    if sub_pre: print(fmt_stats(stats(sub_pre), f"Bear {g} pre-v3"))
    if sub_post: print(fmt_stats(stats(sub_post), f"Bear {g} post-v3"))

print("\nES Absorption filter candidates (all time):")
test_rule(es_bear, lambda t: t['grade'] not in ('C','LOG'), "ES Bear: block C+LOG grades")
test_rule(es_bull, lambda t: t['grade'] != 'C', "ES Bull: block grade C")
test_rule(es, lambda t: not (t['direction']=='bearish' and t['grade'] in ('C','LOG')), "ES (both dirs): block bearish C+LOG")

# ============================================================
# 4. GEX Long — currently dormant (VIX>22 blocks it)
# ============================================================
print("\n\n"+"="*72)
print("4. GEX LONG — Current status + revival check")
print("="*72)
gl = pull('GEX Long')
print(f"All GEX Long: {stats(gl)}")

print("\nGEX Long × VIX × alignment:")
for lo, hi, lbl in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in gl if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"VIX {lbl}"))
for a in [-3,-2,-1,0,1,2,3]:
    sub = [t for t in gl if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"align={a:+d}"))

print("\nGEX Long × VIX<22 × alignment ≥ 2 (V13-compatible):")
filtered = [t for t in gl if t['vix'] and float(t['vix']) < 22 and t['align']>=2]
print(fmt_stats(stats(filtered), "GEX Long V13-compat"))

print("\nGEX Long × paradigm:")
by_par = defaultdict(list)
for t in gl: by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    print(fmt_stats(stats(sub), f"GL {par}"))

# ============================================================
# 5. Delta Absorption — small sample, check if tunable
# ============================================================
print("\n\n"+"="*72)
print("5. DELTA ABSORPTION")
print("="*72)
da = pull('Delta Absorption')
print(f"All DA: {stats(da)}")
da_bull = [t for t in da if t['direction']=='bullish']
da_bear = [t for t in da if t['direction']=='bearish']
print(f"DA Bullish: {stats(da_bull)}")
print(f"DA Bearish: {stats(da_bear)}")

if len(da) >= 10:
    print("\nDA × grade:")
    for g in ['A+','A','B','C','LOG']:
        sub = [t for t in da if t['grade']==g]
        if sub: print(fmt_stats(stats(sub), f"grade={g}"))

# ============================================================
# 6. SB Absorption (NOT SB2) — different setup
# ============================================================
print("\n\n"+"="*72)
print("6. SB ABSORPTION (core — not SB2)")
print("="*72)
sb = pull('SB Absorption')
print(f"All SB: {stats(sb)}")
if sb:
    sb_bull = [t for t in sb if t['direction']=='bullish']
    sb_bear = [t for t in sb if t['direction']=='bearish']
    print(f"SB Bullish: {stats(sb_bull)}")
    print(f"SB Bearish: {stats(sb_bear)}")

print("\nDONE")
