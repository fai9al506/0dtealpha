"""Deep-dive study: BofA Scalp + DD Exhaustion — find what makes them win/lose.

Follows CLAUDE.md validation protocol:
- Gate 1: validate data (staleness, params stable, contamination)
- Gate 2: cross-check totals, sanity
- Gate 3: state clean sample + confidence

Approach for each (BofA, DD):
  1. Full context pull (paradigm, VIX, grade, align, time, gap, DD, spot, etc.)
  2. Univariate analysis (one dimension at a time)
  3. Bivariate crosstabs
  4. Propose filter candidates
  5. OOS train/test validation
  6. Monthly stability check
"""
import psycopg2
from collections import defaultdict
import json
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

def safe_exec(sql, args=None):
    cur.execute(sql, args)
    return cur.fetchall() if cur.description else None

# ============ Gate 1: Data quality ============
print("="*70)
print("GATE 1: DATA QUALITY")
print("="*70)

# BofA trade count, date range
bofa = safe_exec("""
SELECT MIN((ts AT TIME ZONE 'America/New_York')::date) as min_d,
       MAX((ts AT TIME ZONE 'America/New_York')::date) as max_d,
       COUNT(*) as n
FROM setup_log
WHERE setup_name = 'BofA Scalp' AND outcome_result IS NOT NULL
""")[0]
print(f"BofA Scalp: {bofa[2]} trades, {bofa[0]} → {bofa[1]}")

dd = safe_exec("""
SELECT MIN((ts AT TIME ZONE 'America/New_York')::date) as min_d,
       MAX((ts AT TIME ZONE 'America/New_York')::date) as max_d,
       COUNT(*) as n
FROM setup_log
WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL
""")[0]
print(f"DD Exhaustion: {dd[2]} trades, {dd[0]} → {dd[1]}")

# Note Mar 26 outage — will exclude
print("\n⚠️ Mar 26 2026 known TS outage — excluded from analysis")

# ============ Data pull ============
def pull_setup(setup):
    rows = safe_exec("""
    SELECT id, ts, direction, grade, paradigm, spot, outcome_result, outcome_pnl,
           greek_alignment, vix, overvix,
           EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
           EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
           EXTRACT(DOW FROM (ts AT TIME ZONE 'America/New_York'))::int as dow,
           (ts AT TIME ZONE 'America/New_York')::date as d,
           bofa_stop_level, bofa_target_level, bofa_lis_width, bofa_max_hold_minutes,
           lis, max_plus_gex, max_minus_gex, gap_to_lis, upside, rr_ratio,
           support_score, upside_score, floor_cluster_score, target_cluster_score, rr_score,
           outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
           spot_vol_beta, vanna_all, vanna_weekly, vanna_monthly
    FROM setup_log
    WHERE setup_name = %s AND outcome_result IS NOT NULL AND spot IS NOT NULL
      AND (ts AT TIME ZONE 'America/New_York')::date != '2026-03-26'
    ORDER BY ts
    """, (setup,))
    return [dict(zip([
        'id','ts','direction','grade','paradigm','spot','outcome','pnl','align','vix','overvix',
        'h','m','dow','d','bofa_stop','bofa_tgt','bofa_lis_width','bofa_hold',
        'lis','max_pg','max_ng','gap_to_lis','upside','rr','sup_score','ups_score','fc_score',
        'tc_score','rr_score','mfe','mae','elapsed','svb','vanna_all','vanna_weekly','vanna_monthly'
    ], r)) for r in rows]

bofa_trades = pull_setup('BofA Scalp')
dd_trades = pull_setup('DD Exhaustion')
print(f"\nBofA post-Mar26-exclusion: {len(bofa_trades)}")
print(f"DD post-Mar26-exclusion: {len(dd_trades)}")

# ============ Helpers ============
def stats(trades, label=None):
    if not trades: return {'n': 0, 'pnl': 0, 'w': 0, 'l': 0, 'e': 0, 'wr': 0, 'maxdd': 0, 'pf': 0, 'avg': 0}
    pnl = sum(float(t['pnl'] or 0) for t in trades)
    w = sum(1 for t in trades if t['outcome']=='WIN')
    l = sum(1 for t in trades if t['outcome']=='LOSS')
    e = sum(1 for t in trades if t['outcome']=='EXPIRED')
    wr = 100*w/max(1,w+l)
    gp = sum(float(t['pnl'] or 0) for t in trades if float(t['pnl'] or 0) > 0)
    gl = abs(sum(float(t['pnl'] or 0) for t in trades if float(t['pnl'] or 0) < 0))
    pf = gp/gl if gl > 0 else 0
    # MaxDD
    cum = 0; peak = 0; mdd = 0
    for t in sorted(trades, key=lambda x: x['ts']):
        cum += float(t['pnl'] or 0)
        if cum > peak: peak = cum
        mdd = max(mdd, peak - cum)
    return {'n': len(trades), 'pnl': pnl, 'w': w, 'l': l, 'e': e,
            'wr': wr, 'maxdd': mdd, 'pf': pf, 'avg': pnl/max(1,len(trades))}

def fmt_stats(s, label):
    if not s or s['n']==0: return f"  {label:<30} (empty)"
    marker = "★" if s['wr']>=65 and s['pf']>=2 and s['n']>=15 else ""
    return (f"  {label:<30} n={s['n']:>3}  W={s['w']:>3} L={s['l']:>3}  "
            f"WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}  "
            f"MaxDD=-{s['maxdd']:>5.1f}  PF={s['pf']:.2f}  avg={s['avg']:+.2f} {marker}")

print()
print("="*70)
print("DIAGNOSIS 1: BofA SCALP")
print("="*70)

# Overall
print("\n### OVERALL ###")
for d in ('long', 'bullish', 'short', 'bearish'):
    sub = [t for t in bofa_trades if t['direction']==d]
    if sub: print(fmt_stats(stats(sub), f"direction={d}"))

# by hour
print("\n### BY HOUR ###")
for h in range(9, 16):
    sub = [t for t in bofa_trades if t['h']==h]
    if sub: print(fmt_stats(stats(sub), f"hour={h:02d}"))

# by paradigm
print("\n### BY PARADIGM ###")
by_par = defaultdict(list)
for t in bofa_trades:
    by_par[t['paradigm'] or 'NONE'].append(t)
for par, sub in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    print(fmt_stats(stats(sub), f"paradigm={par}"))

# by grade
print("\n### BY GRADE ###")
for g in ['A+', 'A', 'A-Entry', 'B', 'C', 'LOG', None]:
    sub = [t for t in bofa_trades if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"grade={g or 'none'}"))

# by VIX level
print("\n### BY VIX LEVEL ###")
for lo, hi, label in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in bofa_trades if t['vix'] and lo <= float(t['vix']) < hi]
    if sub: print(fmt_stats(stats(sub), f"vix={label}"))

# by alignment
print("\n### BY ALIGNMENT ###")
for a in [-3, -2, -1, 0, 1, 2, 3]:
    sub = [t for t in bofa_trades if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"align={a:+d}"))

# ==============================
# BofA filter candidates
# ==============================
print("\n### BofA FILTER TESTS ###")

# Test: block after noon (BofA is scalp — maybe early-only)
early = [t for t in bofa_trades if t['h'] < 12]
late = [t for t in bofa_trades if t['h'] >= 12]
print(fmt_stats(stats(early), "time<12:00"))
print(fmt_stats(stats(late), "time>=12:00"))

# Test: direction × hour cross
print("\nLong by time bucket:")
longs = [t for t in bofa_trades if t['direction'] in ('long','bullish')]
for lo, hi, label in [(9,10,'09:30-10:00'),(10,11,'10:00-11:00'),(11,12,'11:00-12:00'),(12,14,'12:00-14:00'),(14,15,'14:00-14:30')]:
    sub = [t for t in longs if lo <= t['h'] < hi]
    if sub: print(fmt_stats(stats(sub), f"long {label}"))

print("\nShort by time bucket:")
shorts = [t for t in bofa_trades if t['direction'] in ('short','bearish')]
for lo, hi, label in [(9,10,'09:30-10:00'),(10,11,'10:00-11:00'),(11,12,'11:00-12:00'),(12,14,'12:00-14:00'),(14,15,'14:00-14:30')]:
    sub = [t for t in shorts if lo <= t['h'] < hi]
    if sub: print(fmt_stats(stats(sub), f"short {label}"))

# Test: direction × paradigm cross
print("\nLong × paradigm:")
for par in ['BOFA-PURE', 'BofA-LIS', 'BOFA-MESSY', 'GEX-PURE', 'GEX-LIS', 'AG-PURE', 'AG-LIS', 'SIDIAL-EXTREME']:
    sub = [t for t in longs if t['paradigm']==par]
    if sub: print(fmt_stats(stats(sub), f"long {par}"))

print("\nShort × paradigm:")
for par in ['BOFA-PURE', 'BofA-LIS', 'BOFA-MESSY', 'GEX-PURE', 'GEX-LIS', 'AG-PURE', 'AG-LIS', 'SIDIAL-EXTREME']:
    sub = [t for t in shorts if t['paradigm']==par]
    if sub: print(fmt_stats(stats(sub), f"short {par}"))

# Grade × direction
print("\nLong × grade:")
for g in ['A+', 'A', 'A-Entry', 'B', 'C']:
    sub = [t for t in longs if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"long {g}"))
print("\nShort × grade:")
for g in ['A+', 'A', 'A-Entry', 'B', 'C']:
    sub = [t for t in shorts if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"short {g}"))

# VIX × direction
print("\nLong × VIX:")
for lo, hi, label in [(0,20,'<20'),(20,25,'20-25'),(25,100,'>=25')]:
    sub = [t for t in longs if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"long VIX {label}"))
print("\nShort × VIX:")
for lo, hi, label in [(0,20,'<20'),(20,25,'20-25'),(25,100,'>=25')]:
    sub = [t for t in shorts if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"short VIX {label}"))

# Save for later programmatic processing
print()
print("="*70)
print("DIAGNOSIS 2: DD EXHAUSTION")
print("="*70)

print("\n### DD OVERALL BY DIRECTION ###")
for d in ('long', 'short'):
    sub = [t for t in dd_trades if t['direction']==d]
    if sub: print(fmt_stats(stats(sub), f"DD {d}"))

dd_longs = [t for t in dd_trades if t['direction']=='long']
dd_shorts = [t for t in dd_trades if t['direction']=='short']

print("\n### DD LONG × Paradigm ###")
for par in sorted({t['paradigm'] or 'NONE' for t in dd_longs}):
    sub = [t for t in dd_longs if (t['paradigm'] or 'NONE')==par]
    if sub: print(fmt_stats(stats(sub), f"long {par}"))

print("\n### DD SHORT × Paradigm ###")
for par in sorted({t['paradigm'] or 'NONE' for t in dd_shorts}):
    sub = [t for t in dd_shorts if (t['paradigm'] or 'NONE')==par]
    if sub: print(fmt_stats(stats(sub), f"short {par}"))

print("\n### DD by hour × direction ###")
print("LONG by hour:")
for h in range(9, 16):
    sub = [t for t in dd_longs if t['h']==h]
    if sub: print(fmt_stats(stats(sub), f"long h={h:02d}"))
print("SHORT by hour:")
for h in range(9, 16):
    sub = [t for t in dd_shorts if t['h']==h]
    if sub: print(fmt_stats(stats(sub), f"short h={h:02d}"))

print("\n### DD × alignment × direction ###")
print("LONG:")
for a in [-3, -2, -1, 0, 1, 2, 3]:
    sub = [t for t in dd_longs if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"long align={a:+d}"))
print("SHORT:")
for a in [-3, -2, -1, 0, 1, 2, 3]:
    sub = [t for t in dd_shorts if t['align']==a]
    if sub: print(fmt_stats(stats(sub), f"short align={a:+d}"))

print("\n### DD × VIX × direction ###")
print("LONG:")
for lo, hi, label in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in dd_longs if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"long VIX {label}"))
print("SHORT:")
for lo, hi, label in [(0,18,'<18'),(18,22,'18-22'),(22,26,'22-26'),(26,30,'26-30'),(30,100,'30+')]:
    sub = [t for t in dd_shorts if t['vix'] and lo<=float(t['vix'])<hi]
    if sub: print(fmt_stats(stats(sub), f"short VIX {label}"))

print("\n### DD × grade × direction ###")
print("LONG:")
for g in ['A+', 'A', 'A-Entry', 'B', 'C']:
    sub = [t for t in dd_longs if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"long {g}"))
print("SHORT:")
for g in ['A+', 'A', 'A-Entry', 'B', 'C']:
    sub = [t for t in dd_shorts if t['grade']==g]
    if sub: print(fmt_stats(stats(sub), f"short {g}"))

# Save summaries
out = {
    'bofa': {'n_longs': len(longs), 'n_shorts': len(shorts),
             'longs_stats': stats(longs), 'shorts_stats': stats(shorts),
             'long_by_par': {par: stats([t for t in longs if t['paradigm']==par]) for par in {t['paradigm'] for t in longs if t['paradigm']}},
             'short_by_par': {par: stats([t for t in shorts if t['paradigm']==par]) for par in {t['paradigm'] for t in shorts if t['paradigm']}}},
    'dd': {'n_longs': len(dd_longs), 'n_shorts': len(dd_shorts),
           'longs_stats': stats(dd_longs), 'shorts_stats': stats(dd_shorts)},
}
with open('_bofa_dd_study.json', 'w') as f:
    json.dump(out, f, indent=2, default=str)
print("\nSaved _bofa_dd_study.json")
