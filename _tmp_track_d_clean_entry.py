"""Track D: Clean-Entry Analysis (MAE <= 3 pts, era-agnostic).

Builds on Track B's _tmp_track_b_final.json which already has chain-walk MFE/MAE
for all 2,312 setup_log signals since 2026-03-01. Reclassifies into
CLEAN_ENTRY / MEDIUM_ENTRY / HEAVY_DRAWDOWN buckets, runs feature divergence,
builds filter candidates with strict validation, and computes stop-tightening
opportunities.

CRITICAL:
  - Era-agnostic: ALL 2,312 trades, regardless of filter version
  - chain-walk MFE/MAE (NEVER outcome_max_profit/loss)
  - Bootstrap CI strict, OOS halves, per-month consistency required
"""
from __future__ import annotations
import json
import math
import random
import html as html_lib
from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
import statistics

random.seed(42)
ET = ZoneInfo("America/New_York")

IN = 'G:/My Drive/Python/MyProject/GitHub/0dtealpha/_tmp_track_b_final.json'
OUT_JSON = 'G:/My Drive/Python/MyProject/GitHub/0dtealpha/_tmp_track_d_results.json'
OUT_HTML = 'G:/My Drive/Python/MyProject/GitHub/0dtealpha/_tmp_track_d_clean_entry.html'

print("Loading...")
with open(IN) as f:
    raw = json.load(f)
trades = raw['trades']
for t in trades:
    t['ts_dt'] = datetime.fromisoformat(t['ts'])
print(f"  {len(trades)} trades")

V14_LIVE = ('Skew Charm', 'AG Short', 'Vanna Pivot Bounce', 'VIX Divergence')

# ============================================================================
# PHASE 1: Re-classify trades by MAE bucket
# ============================================================================
def classify_d(t):
    mae = t.get('mae')
    if mae is None:
        return 'UNKNOWN'
    if mae <= 3:
        return 'CLEAN_ENTRY'
    if mae <= 8:
        return 'MEDIUM_ENTRY'
    return 'HEAVY_DRAWDOWN'


def clean_subcls(t):
    """Sub-classify CLEAN_ENTRY by outcome and MFE."""
    if classify_d(t) != 'CLEAN_ENTRY':
        return None
    pnl = t.get('outcome_pnl') or 0
    mfe = t.get('mfe') or 0
    if pnl > 0:
        if mfe >= 15:
            return 'CLEAN_BIG_WIN'
        if mfe >= 5:
            return 'CLEAN_MED_WIN'
        return 'CLEAN_SMALL_WIN'
    if pnl < 0:
        return 'CLEAN_THEN_FADED'  # MAE<=3 but later stopped (rare)
    return 'CLEAN_SCRATCH'


for t in trades:
    t['cls_d'] = classify_d(t)
    t['clean_sub'] = clean_subcls(t)

CLEAN = [t for t in trades if t['cls_d'] == 'CLEAN_ENTRY']
MEDIUM = [t for t in trades if t['cls_d'] == 'MEDIUM_ENTRY']
HEAVY = [t for t in trades if t['cls_d'] == 'HEAVY_DRAWDOWN']
OTHER = MEDIUM + HEAVY  # baseline for divergence

print(f"  CLEAN: {len(CLEAN)}  MEDIUM: {len(MEDIUM)}  HEAVY: {len(HEAVY)}")
print(f"  CLEAN sub: {Counter(t['clean_sub'] for t in CLEAN)}")


# ============================================================================
# PHASE 2: Feature divergence
# ============================================================================
def safe_p_val(a, b, c, d):
    """Yates-corrected chi-square p-value."""
    n = a + b + c + d
    if n == 0:
        return 1.0
    row1 = a + b
    row2 = c + d
    col1 = a + c
    col2 = b + d
    if row1 == 0 or row2 == 0 or col1 == 0 or col2 == 0:
        return 1.0
    def expected(r, col):
        return r * col / n
    chi = 0.0
    for obs, exp in [(a, expected(row1, col1)), (b, expected(row1, col2)),
                     (c, expected(row2, col1)), (d, expected(row2, col2))]:
        if exp > 0:
            chi += ((abs(obs - exp) - 0.5) ** 2) / exp
    return math.erfc(math.sqrt(chi / 2))


def time_bucket(t):
    ts = t['ts_dt'].astimezone(ET)
    h = ts.hour
    m = ts.minute
    if h == 9 and m >= 30:
        return '09:30-10'
    if h == 10:
        return '10-11'
    if h == 11:
        return '11-12'
    if h == 12:
        return '12-13'
    if h == 13:
        return '13-14'
    if h == 14:
        return '14-15'
    if h == 15:
        return '15-16'
    return None


def dow_bucket(t):
    return ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][t['ts_dt'].astimezone(ET).weekday()]


def month_bucket(t):
    return t['ts_dt'].astimezone(ET).strftime('%Y-%m')


def bin_num(v, edges, labels):
    if v is None:
        return None
    for i, e in enumerate(edges):
        if v < e:
            return labels[i]
    return labels[-1]


def vix_bucket(t):
    return bin_num(t.get('vix'), [16, 20, 24, 28], ['<16', '16-20', '20-24', '24-28', '>=28'])


def overvix_bucket(t):
    return bin_num(t.get('overvix'), [-2, 0, 2, 4], ['<-2', '-2..0', '0..2', '2..4', '>=4'])


def gap_to_lis_bucket(t):
    v = t.get('gap_to_lis')
    if v is None:
        return None
    av = abs(v)
    side = '+' if v >= 0 else '-'
    if av < 5:
        return 'near(<5)'
    if av < 15:
        return f'{side}near(5-15)'
    if av < 30:
        return f'{side}mid(15-30)'
    if av < 60:
        return f'{side}far(30-60)'
    return f'{side}very_far(>=60)'


def upside_bucket(t):
    return bin_num(t.get('upside'), [5, 10, 15, 25], ['<5', '5-10', '10-15', '15-25', '>=25'])


def rr_bucket(t):
    return bin_num(t.get('rr_ratio'), [0.5, 1, 1.5, 2.5], ['<0.5', '0.5-1', '1-1.5', '1.5-2.5', '>=2.5'])


CATEGORICAL_FEATURES = [
    ('paradigm', lambda t: t.get('paradigm')),
    ('grade', lambda t: t.get('grade')),
    ('alignment', lambda t: t.get('greek_alignment')),
    ('setup', lambda t: t.get('setup_name')),
    ('direction', lambda t: t.get('direction_norm')),
    ('vanna_cliff_side', lambda t: t.get('vanna_cliff_side')),
    ('vanna_peak_side', lambda t: t.get('vanna_peak_side')),
    ('vix_bucket', vix_bucket),
    ('overvix_bucket', overvix_bucket),
    ('time_bucket', time_bucket),
    ('day_of_week', dow_bucket),
    ('gap_to_lis_bucket', gap_to_lis_bucket),
    ('upside_bucket', upside_bucket),
    ('rr_bucket', rr_bucket),
    ('v13_gex_above_bucket',
     lambda t: bin_num(t.get('v13_gex_above'), [50, 100, 150], ['<50', '50-100', '100-150', '>=150'])),
    ('v13_dd_near_bucket',
     lambda t: bin_num((t.get('v13_dd_near') or 0) / 1e9 if t.get('v13_dd_near') is not None else None,
                      [1, 2, 4, 6], ['<1B', '1-2B', '2-4B', '4-6B', '>=6B'])),
]


def feature_divergence(name, key_fn, pool_a, pool_b, label_a='CLEAN', label_b='OTHER'):
    """Compare CLEAN vs OTHER rates per feature bucket."""
    counts_a = Counter(key_fn(t) for t in pool_a if key_fn(t) is not None)
    counts_b = Counter(key_fn(t) for t in pool_b if key_fn(t) is not None)
    all_buckets = sorted(set(list(counts_a.keys()) + list(counts_b.keys())), key=lambda x: str(x))
    rows = []
    a_total = sum(counts_a.values())
    b_total = sum(counts_b.values())
    base_rate = a_total / (a_total + b_total) * 100 if (a_total + b_total) > 0 else 0
    for b in all_buckets:
        a = counts_a.get(b, 0)
        c = counts_b.get(b, 0)
        if a + c < 15:
            continue
        rate = a / (a + c) * 100
        lift = rate - base_rate
        p = safe_p_val(a, a_total - a, c, b_total - c)
        rows.append({
            'bucket': str(b),
            'A_count': a,
            'B_count': c,
            'total': a + c,
            'A_rate': rate,
            'lift': lift,
            'p': p,
        })
    rows.sort(key=lambda r: -abs(r['lift']))
    return rows


print("\n=== Phase 2: Feature divergence (CLEAN vs OTHER) ===")
feature_results = {}
for name, fn in CATEGORICAL_FEATURES:
    rows = feature_divergence(name, fn, CLEAN, OTHER)
    feature_results[name] = rows
    sig = [r for r in rows if r['p'] < 0.05 and r['total'] >= 30]
    if sig:
        print(f"\n{name}: {len(sig)} significant buckets (base rate {len(CLEAN) / len(trades) * 100:.1f}%)")
        for r in sig[:6]:
            star = '***' if r['p'] < 0.001 else ('**' if r['p'] < 0.01 else '*')
            print(f"  {r['bucket']:<20} {r['A_count']:>4}/{r['total']:>5}  "
                  f"rate={r['A_rate']:>5.1f}%  lift={r['lift']:>+5.1f}  p={r['p']:.4f} {star}")

# ============================================================================
# PHASE 3: Per-direction split
# ============================================================================
print("\n=== Phase 3: Per-direction divergence ===")
CLEAN_LONG = [t for t in CLEAN if t['direction_norm'] == 'long']
CLEAN_SHORT = [t for t in CLEAN if t['direction_norm'] == 'short']
OTHER_LONG = [t for t in OTHER if t['direction_norm'] == 'long']
OTHER_SHORT = [t for t in OTHER if t['direction_norm'] == 'short']
print(f"  CLEAN long={len(CLEAN_LONG)}  CLEAN short={len(CLEAN_SHORT)}")
print(f"  OTHER long={len(OTHER_LONG)}  OTHER short={len(OTHER_SHORT)}")

per_dir_results = {'long': {}, 'short': {}}
for name, fn in CATEGORICAL_FEATURES:
    rows_long = feature_divergence(name, fn, CLEAN_LONG, OTHER_LONG)
    rows_short = feature_divergence(name, fn, CLEAN_SHORT, OTHER_SHORT)
    per_dir_results['long'][name] = rows_long
    per_dir_results['short'][name] = rows_short
    sig_l = [r for r in rows_long if r['p'] < 0.05 and r['total'] >= 30]
    sig_s = [r for r in rows_short if r['p'] < 0.05 and r['total'] >= 30]
    if sig_l or sig_s:
        print(f"\n{name}:  LONG sig={len(sig_l)}  SHORT sig={len(sig_s)}")
        for r in sig_l[:3]:
            print(f"  L {r['bucket']:<20} {r['A_count']:>4}/{r['total']:>5}  rate={r['A_rate']:>5.1f}%  lift={r['lift']:>+5.1f}  p={r['p']:.4f}")
        for r in sig_s[:3]:
            print(f"  S {r['bucket']:<20} {r['A_count']:>4}/{r['total']:>5}  rate={r['A_rate']:>5.1f}%  lift={r['lift']:>+5.1f}  p={r['p']:.4f}")


# ============================================================================
# PHASE 4: Filter candidate construction
# ============================================================================
def bootstrap_ci(values, n_iter=2000, alpha=0.05):
    if len(values) < 5:
        return (None, None)
    means = []
    for _ in range(n_iter):
        sample = [random.choice(values) for _ in values]
        means.append(sum(sample) / len(sample))
    means.sort()
    lo = means[int(n_iter * alpha / 2)]
    hi = means[int(n_iter * (1 - alpha / 2))]
    return (lo, hi)


def in_v14_live(t):
    return t['setup_name'] in V14_LIVE


def evaluate_filter(predicate, name, mechanism):
    selected = [t for t in trades if predicate(t)]
    n = len(selected)
    if n == 0:
        return None
    ce = sum(1 for t in selected if t['cls_d'] == 'CLEAN_ENTRY')
    ce_rate = ce / n * 100
    pnls = [t['outcome_pnl'] for t in selected if t.get('outcome_pnl') is not None]
    if not pnls:
        return None
    total = sum(pnls)
    mean_p = total / len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    dates = sorted(set(t['ts_dt'].astimezone(ET).date() for t in selected))
    n_dates = len(dates)
    months = (dates[-1] - dates[0]).days / 30.4 if len(dates) > 1 else 1
    pnl_per_mo = total / max(months, 0.1)
    ci_lo, ci_hi = bootstrap_ci(pnls)
    # OOS: first 60% / last 40% by date
    cut_idx = int(len(dates) * 0.6)
    h1_dates = set(dates[:cut_idx])
    h2_dates = set(dates[cut_idx:])
    h1_trades = [t for t in selected if t['ts_dt'].astimezone(ET).date() in h1_dates]
    h2_trades = [t for t in selected if t['ts_dt'].astimezone(ET).date() in h2_dates]
    h1_pnls = [t['outcome_pnl'] for t in h1_trades if t.get('outcome_pnl') is not None]
    h2_pnls = [t['outcome_pnl'] for t in h2_trades if t.get('outcome_pnl') is not None]
    h1_w = sum(1 for p in h1_pnls if p > 0)
    h1_l = sum(1 for p in h1_pnls if p < 0)
    h2_w = sum(1 for p in h2_pnls if p > 0)
    h2_l = sum(1 for p in h2_pnls if p < 0)
    h1_wr = h1_w / (h1_w + h1_l) * 100 if (h1_w + h1_l) > 0 else 0
    h2_wr = h2_w / (h2_w + h2_l) * 100 if (h2_w + h2_l) > 0 else 0
    h1_mean = sum(h1_pnls) / len(h1_pnls) if h1_pnls else 0
    h2_mean = sum(h2_pnls) / len(h2_pnls) if h2_pnls else 0
    # Monthly
    by_month = defaultdict(list)
    for t in selected:
        if t.get('outcome_pnl') is not None:
            by_month[month_bucket(t)].append(t['outcome_pnl'])
    monthly = [(m, sum(v), len(v)) for m, v in sorted(by_month.items())]
    months_pos = sum(1 for m, p, _n in monthly if p > 0)
    months_total = len(monthly)
    # MaxDD on trade sequence
    cum = 0
    peak = 0
    dd = 0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    max_dd = -dd
    return {
        'name': name,
        'mechanism': mechanism,
        'n': n,
        'n_dates': n_dates,
        'ce_count': ce,
        'ce_rate': ce_rate,
        'mean_pnl': mean_p,
        'total_pnl': total,
        'wr': wr,
        'wins': wins,
        'losses': losses,
        'pnl_per_mo_pts': pnl_per_mo,
        'pnl_per_mo_mes': pnl_per_mo * 5,
        'ci_lo': ci_lo,
        'ci_hi': ci_hi,
        'h1_mean': h1_mean,
        'h2_mean': h2_mean,
        'h1_wr': h1_wr,
        'h2_wr': h2_wr,
        'h1_n': len(h1_pnls),
        'h2_n': len(h2_pnls),
        'monthly': monthly,
        'months_pos': months_pos,
        'months_total': months_total,
        'max_dd': max_dd,
    }


# Build candidate filters informed by Phase 2/3 results.
CANDIDATES = [
    # Baselines
    ('B1: All trades baseline', lambda t: True,
     'Baseline universe of all signals.'),
    ('B2: V14-live setups only', in_v14_live,
     'V14 live universe (SC/AG/VPB/VIX Div).'),

    # Direction + alignment
    ('F1: LONG + align==+1', lambda t: t['direction_norm']=='long' and t.get('greek_alignment')==1,
     'Mild bullish alignment - dealers tilt long without chasing, leaves clean upside.'),
    ('F2: LONG + align>=+2', lambda t: t['direction_norm']=='long' and (t.get('greek_alignment') or -99) >= 2,
     'Strong bullish alignment - all Greeks WITH the trade. Force alignment principle.'),
    ('F3: SHORT + align==-3', lambda t: t['direction_norm']=='short' and t.get('greek_alignment')==-3,
     'All Greeks against price - dealer positioning offers no support, short fades face zero resistance.'),
    ('F4: SHORT + align<=-1', lambda t: t['direction_norm']=='short' and (t.get('greek_alignment') or 99) <= -1,
     'Any bearish-aligned short.'),

    # Paradigm
    ('F5: LONG + paradigm BOFA-PURE', lambda t: t['direction_norm']=='long' and t.get('paradigm')=='BOFA-PURE',
     'BofA-driven long paradigm with no LIS interference.'),
    ('F6: SHORT + paradigm in AG-PURE/BofA-LIS', lambda t: t['direction_norm']=='short' and t.get('paradigm') in ('AG-PURE','BofA-LIS','BOFA-LIS'),
     'Pure AG bear paradigm OR LIS-resistance pattern - both produce clean reversals.'),
    ('F7: AG Short + AG-aligned paradigm', lambda t: t['setup_name']=='AG Short' and t.get('paradigm') in ('AG-PURE','AG-LIS','BofA-LIS','BOFA-LIS'),
     'Track B PE filter F12 applied to clean-entry universe.'),

    # Setup-specific
    ('F8: AG Short setup', lambda t: t['setup_name']=='AG Short',
     'Highest base PE rate in Track B (31.3%); test if CLEAN_ENTRY rate also elevated.'),
    ('F9: SC Long + alignment >= +1', lambda t: t['setup_name']=='Skew Charm' and t['direction_norm']=='long' and (t.get('greek_alignment') or -99) >= 1,
     'SC long bias for clean entries.'),
    ('F10: VIX Divergence longs', lambda t: t['setup_name']=='VIX Divergence' and t['direction_norm']=='long',
     'VIX Div longs-only Strategy B (V14 live).'),
    ('F11: SC Short + AG-aligned paradigm', lambda t: t['setup_name']=='Skew Charm' and t['direction_norm']=='short' and t.get('paradigm') in ('AG-PURE','AG-LIS','BofA-LIS','BOFA-LIS'),
     'SC short with bear paradigm support.'),

    # Time-of-day
    ('F12: 14-15 ET window', lambda t: time_bucket(t)=='14-15',
     'Charm acceleration into close, dealer positioning resolution.'),
    ('F13: 10-11 ET window', lambda t: time_bucket(t)=='10-11',
     'Opening drive resolution window.'),

    # Combo from Phase 3 mechanism narrative
    ('F14: LONG + BOFA-PURE + align>=+1', lambda t: t['direction_norm']=='long' and t.get('paradigm')=='BOFA-PURE' and (t.get('greek_alignment') or -99) >= 1,
     'Combine clean long paradigm + supportive alignment.'),
    ('F15: SHORT + (AG-PURE OR align==-3)', lambda t: t['direction_norm']=='short' and (t.get('paradigm')=='AG-PURE' or t.get('greek_alignment')==-3),
     'Either dealer-AGG clean OR all Greeks against bull: ideal short conditions.'),
    ('F16: V14-live + align away from 0', lambda t: in_v14_live(t) and abs(t.get('greek_alignment') or 0) >= 1,
     'V14 universe with any non-neutral alignment.'),

    # gap_to_lis (depends on availability)
    ('F17: |gap_to_lis|<5 (near LIS)', lambda t: t.get('gap_to_lis') is not None and abs(t['gap_to_lis']) < 5,
     'Price near LIS - magnet/support effect creates clean entries.'),
    ('F18: LONG + gap_to_lis between 0 and 30',
     lambda t: t['direction_norm']=='long' and (t.get('gap_to_lis') is not None) and 0 <= t['gap_to_lis'] <= 30,
     'Long while above LIS but not too far - LIS as floor.'),
    ('F19: SHORT + gap_to_lis between -30 and 0',
     lambda t: t['direction_norm']=='short' and (t.get('gap_to_lis') is not None) and -30 <= t['gap_to_lis'] <= 0,
     'Short while below LIS - LIS as ceiling.'),

    # Grade
    ('F20: Grade A+/A only', lambda t: t.get('grade') in ('A+','A'),
     'Highest-grade signals only.'),

    # SC long V14 (already live)
    ('F21: SC long V14-style (align in any except block list)',
     lambda t: (t['setup_name']=='Skew Charm' and t['direction_norm']=='long'
                and not (t.get('greek_alignment')==3 and t.get('paradigm') in ('GEX-LIS','AG-LIS','AG-PURE','BOFA-MESSY','SIDIAL-EXTREME'))),
     'V14 SC long rule applied retroactively.'),

    # NEW candidate filters informed by data exploration
    ('F22: SC long + align==+1 + BOFA-PURE',
     lambda t: t['setup_name']=='Skew Charm' and t['direction_norm']=='long' and t.get('greek_alignment')==1 and t.get('paradigm')=='BOFA-PURE',
     'Tight SC long sweet spot: mild bull alignment + clean BofA paradigm. 42% CE rate found in exploration.'),

    ('F23: SC long + align in (1,2) ANY paradigm except SIDIAL-EXTREME',
     lambda t: t['setup_name']=='Skew Charm' and t['direction_norm']=='long' and (t.get('greek_alignment') in (1,2)) and t.get('paradigm')!='SIDIAL-EXTREME',
     'SC long mid-bullish alignment - block extreme regime only.'),

    ('F24: ES Abs LONG + align==+1 + GEX paradigm (GEX-LIS, GEX-PURE)',
     lambda t: t['setup_name']=='ES Absorption' and t['direction_norm']=='long' and t.get('greek_alignment')==1 and t.get('paradigm') in ('GEX-LIS','GEX-PURE'),
     'ES Abs long with mild bull alignment under GEX paradigm: 75/68% WR found in exploration.'),

    ('F25: SHORT entries when paradigm == AG-PURE',
     lambda t: t['direction_norm']=='short' and t.get('paradigm')=='AG-PURE',
     'Clean AG paradigm shorts - dealers fully short-aligned, no LIS interference.'),

    ('F26: AG Short EXCLUDING AG-TARGET paradigm',
     lambda t: t['setup_name']=='AG Short' and t.get('paradigm')!='AG-TARGET',
     'AG-TARGET paradigm wrecks AG Short (-38pts, 0% CE). Carve out to lift quality.'),

    ('F27: V14-live + SHORT + alignment in (-3,-1)',
     lambda t: in_v14_live(t) and t['direction_norm']=='short' and t.get('greek_alignment') in (-3,-1),
     'V14 universe constrained to bear-aligned shorts only.'),

    ('F28: V14-live + LONG + alignment in (1,2)',
     lambda t: in_v14_live(t) and t['direction_norm']=='long' and t.get('greek_alignment') in (1,2),
     'V14 universe constrained to mild-to-strong bull-aligned longs.'),

    ('F29: SC Long V14 + LONG sweet alignment',
     lambda t: (t['setup_name']=='Skew Charm' and t['direction_norm']=='long'
                and t.get('greek_alignment') in (1,2)
                and t.get('paradigm') != 'SIDIAL-EXTREME'),
     'SC Long V14 refined: 1-2 alignment, drop SIDIAL-EXTREME.'),

    ('F30: VPB strong alignment (align==-3 or +2)',
     lambda t: t['setup_name']=='Vanna Pivot Bounce' and t.get('greek_alignment') in (-3,2),
     'VPB sweet alignment - both ends had 85-100% WR (n<30 caveat).'),
]

print("\n=== Phase 4: Filter candidate evaluation ===")
print(f"{'Filter':<55}{'N':>6}{'CE%':>7}{'WR%':>7}{'Mean':>8}{'Tot':>9}{'$/mo':>9}{'h1WR':>7}{'h2WR':>7}{'mos+':>7}{'maxDD':>9}")
print("-" * 130)

results = []
for name, pred, mech in CANDIDATES:
    r = evaluate_filter(pred, name, mech)
    if r is None:
        continue
    results.append(r)
    print(f"{r['name'][:55]:<55}{r['n']:>6}{r['ce_rate']:>6.1f}%{r['wr']:>6.1f}%{r['mean_pnl']:>+8.2f}{r['total_pnl']:>+9.1f}{r['pnl_per_mo_mes']:>+8.0f}${r['h1_wr']:>6.1f}%{r['h2_wr']:>6.1f}%{r['months_pos']:>3}/{r['months_total']}{r['max_dd']:>+9.1f}")


# Rank surviving filters
def rank_score(r):
    if r['n'] < 30:
        return -1e9
    consistency = r['months_pos'] / r['months_total'] if r['months_total'] > 0 else 0
    # OOS - both halves positive PnL and WR drop <= 15pp
    oos_ok = r['h1_mean'] > 0 and r['h2_mean'] > 0 and abs(r['h1_wr'] - r['h2_wr']) <= 15
    oos_bonus = 1.0 if oos_ok else 0.5
    # Bootstrap CI excludes zero
    ci_ok = r['ci_lo'] is not None and r['ci_lo'] > 0
    ci_bonus = 1.0 if ci_ok else 0.6
    return r['pnl_per_mo_mes'] * consistency * oos_bonus * ci_bonus


viable = [r for r in results if r['n'] >= 30]
viable.sort(key=rank_score, reverse=True)

print("\n=== Top filters ranked by ($/mo × consistency × OOS × CI) ===")
for i, r in enumerate(viable[:8]):
    score = rank_score(r)
    ci_lo = r['ci_lo']
    ci_hi = r['ci_hi']
    oos_ok = r['h1_mean'] > 0 and r['h2_mean'] > 0 and abs(r['h1_wr'] - r['h2_wr']) <= 15
    print(f"\n#{i+1}: {r['name']}")
    print(f"    N={r['n']}  CE_rate={r['ce_rate']:.1f}%  WR={r['wr']:.0f}%  Mean={r['mean_pnl']:+.2f}  Total={r['total_pnl']:+.1f}pts  $/mo={r['pnl_per_mo_mes']:+.0f}  MaxDD={r['max_dd']:.1f}")
    if ci_lo is not None:
        print(f"    Bootstrap 95% CI on mean PnL: [{ci_lo:+.2f}, {ci_hi:+.2f}]  excludes_zero={'YES' if ci_lo > 0 else 'NO'}")
    print(f"    OOS halves: h1 WR={r['h1_wr']:.0f}% mean={r['h1_mean']:+.2f} (n={r['h1_n']})  h2 WR={r['h2_wr']:.0f}% mean={r['h2_mean']:+.2f} (n={r['h2_n']})  OOS_ok={oos_ok}")
    print(f"    Months: {r['months_pos']}/{r['months_total']} positive  -- " + ", ".join([f"{m[0]}={p:+.0f}({n})" for m, p, n in r['monthly']]))
    print(f"    Score: {score:.0f}")


# ============================================================================
# PHASE 7: Stop-tightening opportunity per setup
# ============================================================================
print("\n=== Phase 7: Stop-tightening analysis ===")
SETUPS = sorted(set(t['setup_name'] for t in trades))
stop_analysis = []
for s in SETUPS:
    rows = [t for t in trades if t['setup_name'] == s]
    n = len(rows)
    if n < 30:
        continue
    # MAE distribution
    maes = [t['mae'] for t in rows if t.get('mae') is not None]
    if not maes:
        continue
    pct_under = {th: sum(1 for m in maes if m <= th) / len(maes) * 100 for th in (3, 5, 8, 10, 12, 14, 16, 20)}
    # Compare current SL approximation per setup
    # SC=14, VIX Div longs=8, GEX Long=8, AG Short ~ 8-12, DD=20, ES Abs=8, BofA=8
    # If we tightened to 8pts: what % of CURRENT trades would have stopped early?
    # For trades where MAE > new_stop: assume they would have stopped at -new_stop instead.
    # For trades where MAE <= new_stop: outcome unchanged.
    current_pnls = [t['outcome_pnl'] for t in rows if t.get('outcome_pnl') is not None]
    current_total = sum(current_pnls)
    sim_results = {}
    for new_sl in (5, 6, 8, 10, 12):
        sim_pnls = []
        for t in rows:
            mae = t.get('mae')
            pnl = t.get('outcome_pnl')
            if mae is None or pnl is None:
                continue
            if mae > new_sl:
                # Would have stopped early at -new_sl
                sim_pnls.append(-new_sl)
            else:
                sim_pnls.append(pnl)
        if sim_pnls:
            sim_results[new_sl] = {
                'total': sum(sim_pnls),
                'mean': sum(sim_pnls) / len(sim_pnls),
                'wr': sum(1 for p in sim_pnls if p > 0) / len(sim_pnls) * 100,
            }
    stop_analysis.append({
        'setup': s,
        'n': n,
        'mae_pct_under': pct_under,
        'mae_median': statistics.median(maes),
        'mae_p75': sorted(maes)[int(len(maes) * 0.75)] if maes else None,
        'mae_p90': sorted(maes)[int(len(maes) * 0.90)] if maes else None,
        'current_total': current_total,
        'sim_results': sim_results,
    })

print(f"{'Setup':<22}{'N':>5}{'<3':>6}{'<5':>6}{'<8':>6}{'<10':>6}{'<12':>6}{'p50':>7}{'p75':>7}{'p90':>7}  Tighten sim totals (SL=5/6/8/10/12)")
for s in stop_analysis:
    pu = s['mae_pct_under']
    sims = s['sim_results']
    line = f"{s['setup']:<22}{s['n']:>5}{pu[3]:>5.0f}%{pu[5]:>5.0f}%{pu[8]:>5.0f}%{pu[10]:>5.0f}%{pu[12]:>5.0f}%{s['mae_median']:>7.1f}{s['mae_p75']:>7.1f}{s['mae_p90']:>7.1f}  "
    line += f"cur={s['current_total']:+.0f}  "
    for sl in (5, 6, 8, 10, 12):
        if sl in sims:
            line += f"SL{sl}={sims[sl]['total']:+.0f} "
    print(line)


# ============================================================================
# Save results JSON for HTML build
# ============================================================================
for t in trades:
    t.pop('ts_dt', None)

out = {
    'meta': {
        'generated_at': datetime.now(ET).isoformat(),
        'date_range': [trades[0]['ts'][:10], trades[-1]['ts'][:10]],
        'total_trades': len(trades),
        'clean_count': len(CLEAN),
        'medium_count': len(MEDIUM),
        'heavy_count': len(HEAVY),
        'clean_long_count': len(CLEAN_LONG),
        'clean_short_count': len(CLEAN_SHORT),
    },
    'feature_results': feature_results,
    'per_dir_results': per_dir_results,
    'filter_results': results,
    'filter_ranked': viable,
    'stop_analysis': stop_analysis,
    'clean_sub_counts': Counter(t['clean_sub'] for t in CLEAN if t['clean_sub']),
}

with open(OUT_JSON, 'w') as f:
    json.dump(out, f, default=str)
print(f"\nSaved JSON: {OUT_JSON}")
