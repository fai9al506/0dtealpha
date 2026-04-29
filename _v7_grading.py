"""V7 Delta-Price Divergence — Data-Driven Grading System
Build grading based on what ACTUALLY predicts winners in March data.
"""
import pandas as pd
import numpy as np
from datetime import time as dtime
from collections import defaultdict

# ============================================================
# LOAD DATA
# ============================================================
CSV = 'G:/My Drive/Python/MyProject/GitHub/0dtealpha/exports/v7_final_signals.csv'
df = pd.read_csv(CSV)
print(f"Loaded {len(df)} V7 signals")
print(f"Columns: {list(df.columns)}")

# Derive features not in CSV
df['abs_delta'] = df['delta'].abs()
df['hour'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.hour
df['minute'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.minute
df['tod'] = df['hour'] + df['minute'] / 60.0
df['is_win'] = (df['outcome'] == 'WIN').astype(int)

# Signal number per direction per day
df['sig_num'] = 0
for date in df['date'].unique():
    for d in ['BULL', 'BEAR']:
        mask = (df['date'] == date) & (df['dir'] == d)
        idxs = df.index[mask]
        # Sort by time within day
        sub = df.loc[idxs].sort_values('time')
        for rank, idx in enumerate(sub.index, 1):
            df.loc[idx, 'sig_num'] = rank

# Delta z-score: compute per-day rolling z-score
# Since signals are sparse, use all prior signals on that day as the window
# Fallback: use overall abs_delta statistics
overall_mean = df['abs_delta'].mean()
overall_std = df['abs_delta'].std()
print(f"Overall |delta| mean={overall_mean:.0f}, std={overall_std:.0f}")

# Per-day rolling z-score (using all prior signals that day)
dz_values = []
for i, row in df.iterrows():
    day_prior = df[(df['date'] == row['date']) & (df.index <= i)]
    if len(day_prior) >= 3:
        m = day_prior['abs_delta'].mean()
        s = day_prior['abs_delta'].std()
        if s > 0:
            dz_values.append(abs((row['abs_delta'] - m) / s))
        else:
            dz_values.append(abs((row['abs_delta'] - overall_mean) / overall_std))
    else:
        # Not enough same-day signals, use overall
        dz_values.append(abs((row['abs_delta'] - overall_mean) / overall_std))
df['dz'] = dz_values

# ============================================================
# STEP 1: Verify known predictors from V7 refinement
# ============================================================
print(f"\n{'='*80}")
print("STEP 1: VERIFY KNOWN PREDICTORS")
print(f"{'='*80}")

wins = df[df.is_win == 1]
losses = df[df.is_win == 0]
n_w, n_l = len(wins), len(losses)
wr = 100 * n_w / len(df)
print(f"\nOverall: {len(df)} signals, {n_w}W/{n_l}L, WR={wr:.1f}%, PnL={df.pnl.sum():+.1f}")

def bucket_report(col, buckets, label):
    print(f"\n--- {label} ---")
    results = []
    for lo, hi, blbl in buckets:
        sub = df[(df[col] >= lo) & (df[col] < hi)]
        if len(sub) == 0:
            continue
        w = sub.is_win.sum()
        p = sub.pnl.sum()
        wr_pct = 100 * w / len(sub) if len(sub) > 0 else 0
        print(f"  {blbl:<25} {len(sub):>3} sig, {wr_pct:>5.1f}% WR, {p:>+8.1f} PnL")
        results.append((blbl, len(sub), wr_pct, p))
    return results

# Body size
bucket_report('body', [
    (0, 0.5, 'body < 0.5'),
    (0.5, 1.0, 'body 0.5-1.0'),
    (1.0, 2.0, 'body 1.0-2.0'),
    (2.0, 3.0, 'body 2.0-3.0'),
    (3.0, 4.0, 'body 3.0-4.0'),
    (4.0, 99, 'body 4.0+'),
], 'Body Size (Doji = strong predictor)')

# Delta magnitude
bucket_report('abs_delta', [
    (100, 200, '100-200'),
    (200, 300, '200-300'),
    (300, 500, '300-500'),
    (500, 700, '500-700'),
    (700, 1000, '700-1000'),
    (1000, 9999, '1000+'),
], '|Delta| Magnitude')

# DZ
bucket_report('dz', [
    (0, 0.3, 'DZ < 0.3'),
    (0.3, 0.7, 'DZ 0.3-0.7'),
    (0.7, 1.2, 'DZ 0.7-1.2'),
    (1.2, 2.0, 'DZ 1.2-2.0'),
    (2.0, 99, 'DZ 2.0+'),
], 'Delta Z-Score (unusualness)')

# Peak ratio
bucket_report('peak_ratio', [
    (0, 0.5, 'PR < 0.5'),
    (0.5, 1.0, 'PR 0.5-1.0'),
    (1.0, 1.5, 'PR 1.0-1.5'),
    (1.5, 2.0, 'PR 1.5-2.0'),
    (2.0, 2.5, 'PR 2.0-2.5'),
], 'Peak Ratio')

# Time of day
bucket_report('tod', [
    (9.5, 10.0, '09:30-10:00'),
    (10.0, 11.0, '10:00-11:00'),
    (11.0, 12.0, '11:00-12:00'),
    (12.0, 12.5, '12:00-12:30'),
    (12.5, 13.0, '12:30-13:00'),
    (13.0, 13.5, '13:00-13:30'),
    (13.5, 14.0, '13:30-14:00'),
    (14.0, 14.5, '14:00-14:30'),
    (14.5, 15.0, '14:30-15:00'),
], 'Time of Day')

# Signal number
bucket_report('sig_num', [
    (1, 2, '#1 signal'),
    (2, 3, '#2 signal'),
    (3, 4, '#3 signal'),
    (4, 99, '#4+ signal'),
], 'Signal # Today (per direction)')

# Tier
bucket_report('tier', [
    (1, 2, 'Tier 1 (doji)'),
    (3, 4, 'Tier 3 (afternoon)'),
], 'Tier')

# Direction
print(f"\n--- Direction ---")
for d in ['BULL', 'BEAR']:
    sub = df[df.dir == d]
    w = sub.is_win.sum()
    print(f"  {d}: {len(sub)} sig, {100*w/len(sub):.1f}% WR, {sub.pnl.sum():+.1f}")

# ============================================================
# STEP 2: COMPOSITE GRADING — Attempt 1
# ============================================================
print(f"\n{'='*80}")
print("STEP 2: GRADING ATTEMPT 1 — Component scoring")
print(f"{'='*80}")

def grade_v1(row):
    """First attempt — based on user's proposed scoring (may not match data)"""
    score = 0

    # Component 1: Body absorption (0-25) — user's proposal
    body = row['body']
    if body < 0.5:
        score += 25
    elif body < 1.0:
        score += 20
    elif body < 2.0:
        score += 15
    elif body < 3.0:
        score += 10
    else:
        score += 5

    # Component 2: Delta sweet spot (0-25)
    ad = row['abs_delta']
    if 300 <= ad < 700:
        score += 25
    elif (200 <= ad < 300) or (700 <= ad < 1000):
        score += 15
    elif (100 <= ad < 200) or ad >= 1000:
        score += 5

    # Component 3: Delta unusualness (0-25)
    dz = row['dz']
    if 0.7 <= dz < 1.2:
        score += 25
    elif 0.3 <= dz < 0.7:
        score += 15
    elif dz < 0.3:
        score += 10
    else:  # > 1.2
        score += 5

    # Component 4: Context quality (0-25)
    ctx = 0
    if row['tier'] == 1:
        ctx += 10
    tod = row['tod']
    if 12.5 <= tod < 13.0:
        ctx += 10
    elif 14.5 <= tod < 15.0:
        ctx += 5
    if row['sig_num'] <= 2:
        ctx += 5
    pr = row['peak_ratio']
    if 1.0 <= pr < 1.5:
        ctx += 5
    score += min(ctx, 25)

    return score

df['score_v1'] = df.apply(grade_v1, axis=1)

# Grade thresholds
def assign_grade(score, thresholds):
    if score >= thresholds['A+']: return 'A+'
    if score >= thresholds['A']: return 'A'
    if score >= thresholds['B']: return 'B'
    if score >= thresholds['C']: return 'C'
    return 'LOG'

thresh_v1 = {'A+': 80, 'A': 65, 'B': 50, 'C': 35}
df['grade_v1'] = df['score_v1'].apply(lambda s: assign_grade(s, thresh_v1))

print(f"\nV1 Score distribution:")
print(f"  Min={df.score_v1.min()}, Max={df.score_v1.max()}, "
      f"Mean={df.score_v1.mean():.1f}, Median={df.score_v1.median():.0f}")
print(f"  P25={df.score_v1.quantile(0.25):.0f}, P75={df.score_v1.quantile(0.75):.0f}")

print(f"\nV1 Grade distribution & WR:")
print(f"  {'Grade':<6} {'Count':>5} {'WR':>6} {'PnL':>8} {'AvgPnL':>8}")
print(f"  {'-'*40}")
grade_order = ['A+', 'A', 'B', 'C', 'LOG']
for g in grade_order:
    sub = df[df.grade_v1 == g]
    if len(sub) == 0:
        print(f"  {g:<6} {0:>5} {'---':>6} {'---':>8} {'---':>8}")
        continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    print(f"  {g:<6} {len(sub):>5} {100*w/len(sub):>5.1f}% {p:>+8.1f} {p/len(sub):>+8.1f}")

# Cumulative analysis
print(f"\n  Cumulative (include grade and above):")
print(f"  {'Threshold':<15} {'Count':>5} {'WR':>6} {'PnL':>8} {'PnL/Sig':>8} {'MaxDD':>7}")
print(f"  {'-'*55}")
for i, g_cutoff in enumerate(grade_order):
    included_grades = grade_order[:i+1]
    sub = df[df.grade_v1.isin(included_grades)]
    if len(sub) == 0:
        continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    # MaxDD
    running = 0; peak_eq = 0; maxdd = 0
    for _, r in sub.sort_values(['date', 'time']).iterrows():
        running += r.pnl
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    print(f"  {'+'.join(included_grades):<15} {len(sub):>5} {100*w/len(sub):>5.1f}% "
          f"{p:>+8.1f} {p/len(sub):>+8.1f} {maxdd:>7.1f}")

# Check predictiveness
v1_grade_wr = {}
for g in grade_order:
    sub = df[df.grade_v1 == g]
    if len(sub) > 0:
        v1_grade_wr[g] = 100 * sub.is_win.sum() / len(sub)

print(f"\n  Predictiveness check (WR should decrease: A+ > A > B > C > LOG):")
prev_wr = None
is_monotonic = True
for g in grade_order:
    if g in v1_grade_wr:
        wr = v1_grade_wr[g]
        direction = ""
        if prev_wr is not None:
            if wr > prev_wr:
                direction = " <-- ANTI-PREDICTIVE!"
                is_monotonic = False
            else:
                direction = " OK"
        print(f"    {g}: {wr:.1f}%{direction}")
        prev_wr = wr

if is_monotonic:
    print(f"\n  >>> V1 GRADING IS PREDICTIVE! <<<")
else:
    print(f"\n  >>> V1 GRADING HAS ANTI-PREDICTIVE ELEMENTS — trying alternatives <<<")

# ============================================================
# STEP 3: ALTERNATIVE SCORING — Different weights
# ============================================================
print(f"\n{'='*80}")
print("STEP 3: ALTERNATIVE SCORING ATTEMPTS")
print(f"{'='*80}")

def grade_v2(row):
    """V2: DATA-DRIVEN body scoring (0.5-1.0 and 3-4 are BEST, <0.5 is WORST)"""
    score = 0

    # Body (0-30) — DATA says: 0.5-1.0=68%, 3-4=75%, <0.5=25% (worst!)
    body = row['body']
    if 0.5 <= body < 1.0:
        score += 30  # 68% WR, +166 PnL (best volume)
    elif 3.0 <= body < 4.0:
        score += 28  # 75% WR (best WR but small sample)
    elif 2.0 <= body < 3.0:
        score += 22  # 64% WR
    elif 1.0 <= body < 2.0:
        score += 18  # 60% WR
    elif body >= 4.0:
        score += 12  # 57% WR
    else:  # < 0.5
        score += 3   # 25% WR (terrible!)

    # Delta magnitude (0-25) — DATA: 200-500 best, 700-1000 worst
    ad = row['abs_delta']
    if 200 <= ad < 500:
        score += 25  # 70% WR, +181 PnL
    elif 500 <= ad < 700:
        score += 18  # 55% WR
    elif 100 <= ad < 200:
        score += 15  # 59% WR
    elif ad >= 1000:
        score += 10  # 57% WR
    else:  # 700-1000
        score += 5   # 33% WR (terrible)

    # Signal freshness (0-20) — DATA: #1-2 = 69%, #3+ = 50%
    sn = row['sig_num']
    if sn <= 2:
        score += 20
    elif sn == 3:
        score += 8
    else:
        score += 2

    # Time + context (0-25)
    ctx = 0
    tod = row['tod']
    # DATA: 12:30-13:00=75%, 14:30-15:00=72%, 12:00-12:30=20% (avoid!)
    if 12.5 <= tod < 13.0:
        ctx += 12
    elif 14.5 <= tod < 15.0:
        ctx += 10
    elif 10.0 <= tod < 11.0:
        ctx += 8
    elif 9.5 <= tod < 10.0:
        ctx += 8
    elif 11.0 <= tod < 12.0:
        ctx += 7
    elif 13.5 <= tod < 14.0:
        ctx += 7
    elif 13.0 <= tod < 13.5:
        ctx += 3   # 44% WR
    elif 12.0 <= tod < 12.5:
        ctx += 0   # 20% WR
    # Peak ratio: 1.5-2.0=73% vs 1.0-1.5=59% → higher PR is actually better
    pr = row['peak_ratio']
    if 1.5 <= pr < 2.0:
        ctx += 8
    elif pr >= 2.0:
        ctx += 5
    elif 1.0 <= pr < 1.5:
        ctx += 3
    score += min(ctx, 25)

    return score

def grade_v3(row):
    """V3: Heavy delta + signal freshness, body secondary"""
    score = 0

    # Delta sweet spot (0-30) — strongest actionable filter
    ad = row['abs_delta']
    if 200 <= ad < 500:
        score += 30
    elif 500 <= ad < 700:
        score += 20
    elif 100 <= ad < 200:
        score += 15
    elif ad >= 1000:
        score += 10
    else:
        score += 3

    # Body (0-25)
    body = row['body']
    if 0.5 <= body < 1.0:
        score += 25
    elif 3.0 <= body < 4.0:
        score += 22
    elif 2.0 <= body < 3.0:
        score += 18
    elif 1.0 <= body < 2.0:
        score += 14
    elif body >= 4.0:
        score += 10
    else:  # < 0.5
        score += 2

    # Signal freshness (0-20)
    sn = row['sig_num']
    if sn <= 2:
        score += 20
    elif sn == 3:
        score += 8
    else:
        score += 2

    # Time (0-25)
    tod = row['tod']
    if 12.5 <= tod < 13.0:
        score += 25
    elif 14.5 <= tod < 15.0:
        score += 20
    elif 10.0 <= tod < 11.0:
        score += 15
    elif 9.5 <= tod < 10.0:
        score += 15
    elif 11.0 <= tod < 12.0:
        score += 12
    elif 13.5 <= tod < 14.0:
        score += 12
    elif 13.0 <= tod < 13.5:
        score += 5
    else:
        score += 0

    return score

def grade_v4(row):
    """V4: Multi-factor composite — equal weight on the 4 real predictors"""
    score = 0

    # Body (0-25)
    body = row['body']
    if 0.5 <= body < 1.0:
        score += 25
    elif 3.0 <= body < 4.0:
        score += 23
    elif 2.0 <= body < 3.0:
        score += 18
    elif 1.0 <= body < 2.0:
        score += 15
    elif body >= 4.0:
        score += 10
    else:
        score += 2

    # Delta magnitude (0-25)
    ad = row['abs_delta']
    if 200 <= ad < 500:
        score += 25
    elif 500 <= ad < 700:
        score += 15
    elif 100 <= ad < 200:
        score += 12
    elif ad >= 1000:
        score += 8
    else:
        score += 3

    # Signal freshness (0-25)
    sn = row['sig_num']
    if sn == 1:
        score += 25
    elif sn == 2:
        score += 20
    elif sn == 3:
        score += 8
    else:
        score += 2

    # Time of day (0-25)
    tod = row['tod']
    if 12.5 <= tod < 13.0:
        score += 25
    elif 14.5 <= tod < 15.0:
        score += 22
    elif 9.5 <= tod < 11.0:
        score += 18
    elif 11.0 <= tod < 12.0:
        score += 15
    elif 13.5 <= tod < 14.0:
        score += 15
    elif 13.0 <= tod < 13.5:
        score += 5
    elif 12.0 <= tod < 12.5:
        score += 0
    else:
        score += 8

    return score

def grade_v5(row):
    """V5: Body-centric with penalty for bad combos"""
    score = 0

    # Body (0-30) — data-driven
    body = row['body']
    if 0.5 <= body < 1.0:
        score += 30
    elif 3.0 <= body < 4.0:
        score += 27
    elif 2.0 <= body < 3.0:
        score += 20
    elif 1.0 <= body < 2.0:
        score += 16
    elif body >= 4.0:
        score += 10
    else:
        score += 0  # <0.5 = penalty

    # Delta (0-25)
    ad = row['abs_delta']
    if 200 <= ad < 500:
        score += 25
    elif 500 <= ad < 700:
        score += 16
    elif 100 <= ad < 200:
        score += 13
    elif ad >= 1000:
        score += 8
    else:
        score += 3

    # Signal number (0-20)
    sn = row['sig_num']
    if sn <= 2:
        score += 20
    elif sn == 3:
        score += 8
    else:
        score += 0

    # Time (0-25)
    tod = row['tod']
    if 12.5 <= tod < 13.0:
        score += 25
    elif 14.5 <= tod < 15.0:
        score += 20
    elif 9.5 <= tod < 11.0:
        score += 16
    elif 11.0 <= tod < 12.0:
        score += 14
    elif 13.5 <= tod < 14.0:
        score += 14
    elif 13.0 <= tod < 13.5:
        score += 5
    elif 12.0 <= tod < 12.5:
        score += 0
    else:
        score += 8

    return score

# Test all grading versions
versions = {
    'V1': ('score_v1', thresh_v1),
    'V2': ('score_v2', None),
    'V3': ('score_v3', None),
    'V4': ('score_v4', None),
    'V5': ('score_v5', None),
}

# Apply scoring
df['score_v2'] = df.apply(grade_v2, axis=1)
df['score_v3'] = df.apply(grade_v3, axis=1)
df['score_v4'] = df.apply(grade_v4, axis=1)
df['score_v5'] = df.apply(grade_v5, axis=1)

# For each version, find optimal thresholds using quantiles
def find_best_thresholds(scores, outcomes, name):
    """Try multiple threshold sets, pick the one where grades are most predictive"""
    best_result = None
    best_mono_score = -999

    # Generate threshold candidates from percentiles
    p20 = int(np.percentile(scores, 20))
    p40 = int(np.percentile(scores, 40))
    p50 = int(np.percentile(scores, 50))
    p60 = int(np.percentile(scores, 60))
    p75 = int(np.percentile(scores, 75))
    p80 = int(np.percentile(scores, 80))
    p90 = int(np.percentile(scores, 90))

    threshold_candidates = [
        {'A+': p90, 'A': p75, 'B': p50, 'C': p20},
        {'A+': p80, 'A': p60, 'B': p40, 'C': p20},
        {'A+': 80, 'A': 65, 'B': 50, 'C': 35},
        {'A+': 85, 'A': 70, 'B': 55, 'C': 40},
        {'A+': 75, 'A': 60, 'B': 45, 'C': 30},
        {'A+': 70, 'A': 55, 'B': 40, 'C': 25},
        {'A+': 78, 'A': 62, 'B': 48, 'C': 33},
        {'A+': 82, 'A': 67, 'B': 52, 'C': 37},
    ]

    for thresh in threshold_candidates:
        grades = pd.Series([assign_grade(s, thresh) for s in scores])
        grade_order = ['A+', 'A', 'B', 'C', 'LOG']

        # Compute WR per grade
        grade_wrs = {}
        grade_counts = {}
        for g in grade_order:
            mask = grades == g
            n = mask.sum()
            if n >= 2:
                grade_wrs[g] = outcomes[mask].mean() * 100
                grade_counts[g] = n
            else:
                grade_wrs[g] = None
                grade_counts[g] = n

        # Monotonicity score: sum of correct ordering pairs
        mono = 0
        prev_wr = None
        for g in grade_order:
            if grade_wrs[g] is not None and grade_counts[g] >= 3:
                if prev_wr is not None:
                    if grade_wrs[g] < prev_wr:
                        mono += 1  # correct order
                    elif grade_wrs[g] > prev_wr:
                        mono -= 2  # anti-predictive penalty
                prev_wr = grade_wrs[g]

        # Spread bonus: difference between best and worst grade WR
        valid_wrs = [v for v in grade_wrs.values() if v is not None]
        spread = max(valid_wrs) - min(valid_wrs) if len(valid_wrs) >= 2 else 0

        # Combined quality = monotonicity + spread/20
        quality = mono + spread / 20

        if quality > best_mono_score:
            best_mono_score = quality
            best_result = {
                'thresh': thresh,
                'grade_wrs': grade_wrs,
                'grade_counts': grade_counts,
                'mono': mono,
                'spread': spread,
                'quality': quality,
                'grades': grades,
            }

    return best_result

print(f"\n{'='*80}")
print("SCORING VERSION COMPARISON")
print(f"{'='*80}")

best_version = None
best_quality = -999

for vname in ['V1', 'V2', 'V3', 'V4', 'V5']:
    col = f'score_{vname.lower()}'
    scores = df[col].values
    outcomes = df['is_win'].values

    result = find_best_thresholds(scores, outcomes, vname)
    if result is None:
        continue

    thresh = result['thresh']
    grade_wrs = result['grade_wrs']
    grade_counts = result['grade_counts']

    print(f"\n--- {vname} (best thresholds: A+>={thresh['A+']}, A>={thresh['A']}, "
          f"B>={thresh['B']}, C>={thresh['C']}) ---")
    print(f"  Score range: {scores.min()}-{scores.max()}, mean={scores.mean():.1f}")
    print(f"  {'Grade':<6} {'Count':>5} {'WR':>6}")

    grade_order = ['A+', 'A', 'B', 'C', 'LOG']
    for g in grade_order:
        cnt = grade_counts[g]
        wr = grade_wrs[g]
        wr_str = f"{wr:.1f}%" if wr is not None else "n/a"
        print(f"  {g:<6} {cnt:>5} {wr_str:>6}")

    print(f"  Monotonicity={result['mono']}, Spread={result['spread']:.1f}%, Quality={result['quality']:.1f}")

    if result['quality'] > best_quality:
        best_quality = result['quality']
        best_version = (vname, result)

print(f"\n>>> BEST VERSION: {best_version[0]} (quality={best_quality:.1f}) <<<")

# ============================================================
# STEP 4: DEEP DIVE on best version
# ============================================================
print(f"\n{'='*80}")
print(f"STEP 4: DEEP DIVE — {best_version[0]}")
print(f"{'='*80}")

vname, result = best_version
col = f'score_{vname.lower()}'
thresh = result['thresh']
df['best_grade'] = result['grades'].values

grade_order = ['A+', 'A', 'B', 'C', 'LOG']

# Per-grade detail
print(f"\nPer-grade detail:")
print(f"  {'Grade':<6} {'Count':>5} {'WR':>6} {'PnL':>8} {'AvgPnL':>8} {'AvgMFE':>7} {'AvgMAE':>7}")
print(f"  {'-'*55}")
for g in grade_order:
    sub = df[df.best_grade == g]
    if len(sub) == 0:
        continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    print(f"  {g:<6} {len(sub):>5} {100*w/len(sub):>5.1f}% {p:>+8.1f} {p/len(sub):>+8.1f} "
          f"{sub.mfe.mean():>7.1f} {sub.mae.mean():>7.1f}")

# Cumulative analysis
print(f"\n  Cumulative (include grade and above):")
print(f"  {'Threshold':<20} {'Count':>5} {'WR':>6} {'PnL':>8} {'PnL/Sig':>8} {'MaxDD':>7} {'PF':>6}")
print(f"  {'-'*65}")
for i, g_cutoff in enumerate(grade_order):
    included_grades = grade_order[:i+1]
    sub = df[df.best_grade.isin(included_grades)]
    if len(sub) == 0:
        continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    gw = sub[sub.pnl > 0].pnl.sum()
    gl = sub[sub.pnl <= 0].pnl.sum()
    pf = abs(gw/gl) if gl else 999
    # MaxDD
    running = 0; peak_eq = 0; maxdd = 0
    for _, r in sub.sort_values(['date', 'time']).iterrows():
        running += r.pnl
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    label = '+'.join(included_grades)
    print(f"  {label:<20} {len(sub):>5} {100*w/len(sub):>5.1f}% "
          f"{p:>+8.1f} {p/len(sub):>+8.1f} {maxdd:>7.1f} {pf:>6.2f}")

# ============================================================
# STEP 5: FINE-TUNE thresholds with grid search
# ============================================================
print(f"\n{'='*80}")
print("STEP 5: FINE-TUNE — Grid search on thresholds")
print(f"{'='*80}")

scores = df[col].values
outcomes = df['is_win'].values
pnls = df['pnl'].values

# Wider grid search
grid_results = []
smin, smax = int(scores.min()), int(scores.max())

for ap in range(smax-5, max(smin+15, smax-30), -2):
    for a in range(ap-5, max(smin+10, ap-25), -3):
        for b in range(a-5, max(smin+5, a-25), -3):
            for c in range(b-5, max(smin, b-20), -5):
                t = {'A+': ap, 'A': a, 'B': b, 'C': c}
                grades = np.array([assign_grade(s, t) for s in scores])

                # Need at least 3 in each populated grade
                counts = {g: np.sum(grades == g) for g in grade_order}
                populated = [g for g in grade_order if counts[g] >= 3]
                if len(populated) < 3:
                    continue

                # WR per grade
                wrs = {}
                for g in grade_order:
                    mask = grades == g
                    n = mask.sum()
                    if n >= 3:
                        wrs[g] = outcomes[mask].mean() * 100

                # Monotonicity
                mono = 0
                prev_wr = None
                for g in grade_order:
                    if g in wrs:
                        if prev_wr is not None:
                            if wrs[g] < prev_wr:
                                mono += 1
                            elif wrs[g] > prev_wr:
                                mono -= 2
                        prev_wr = wrs[g]

                # Top grade WR
                top_wr = wrs.get('A+', wrs.get('A', 50))
                bottom_wr = wrs.get('LOG', wrs.get('C', 50))
                spread = top_wr - bottom_wr

                # A+&A cumulative PnL
                top_mask = (grades == 'A+') | (grades == 'A')
                top_pnl = pnls[top_mask].sum()
                top_n = top_mask.sum()

                quality = mono + spread/15 + top_pnl/100

                grid_results.append({
                    'ap': ap, 'a': a, 'b': b, 'c': c,
                    'mono': mono, 'spread': spread,
                    'top_wr': top_wr, 'bottom_wr': bottom_wr,
                    'top_pnl': top_pnl, 'top_n': top_n,
                    'quality': quality,
                    'counts': counts, 'wrs': wrs,
                })

grid_df = pd.DataFrame(grid_results)
# Filter to monotonic only
mono_df = grid_df[grid_df.mono >= 2].sort_values('quality', ascending=False)

if len(mono_df) == 0:
    print("  No perfectly monotonic configurations found, showing best partial:")
    mono_df = grid_df.sort_values('quality', ascending=False)

print(f"\n  Top 10 threshold configurations (monotonic preferred):")
print(f"  {'#':<3} {'A+':>4} {'A':>4} {'B':>4} {'C':>4} {'Mono':>5} {'Spread':>7} "
      f"{'TopWR':>6} {'BotWR':>6} {'Top PnL':>8} {'TopN':>5} {'Quality':>8}")
print(f"  {'-'*75}")
for rank, (_, r) in enumerate(mono_df.head(10).iterrows(), 1):
    print(f"  {rank:<3} {r.ap:>4} {r.a:>4} {r.b:>4} {r.c:>4} {r.mono:>5.0f} {r.spread:>7.1f} "
          f"{r.top_wr:>5.1f}% {r.bottom_wr:>5.1f}% {r.top_pnl:>+8.1f} {r.top_n:>5.0f} {r.quality:>8.1f}")

# Apply best from grid search
if len(mono_df) > 0:
    best_grid = mono_df.iloc[0]
    best_thresh = {'A+': int(best_grid.ap), 'A': int(best_grid.a),
                   'B': int(best_grid.b), 'C': int(best_grid.c)}
    print(f"\n  BEST GRID: A+>={best_thresh['A+']}, A>={best_thresh['A']}, "
          f"B>={best_thresh['B']}, C>={best_thresh['C']}")

    df['final_grade'] = df[col].apply(lambda s: assign_grade(s, best_thresh))

    print(f"\n  Final grading applied:")
    print(f"  {'Grade':<6} {'Count':>5} {'WR':>6} {'PnL':>8} {'AvgPnL':>8} {'AvgMFE':>7} {'AvgMAE':>7}")
    print(f"  {'-'*55}")
    for g in grade_order:
        sub = df[df.final_grade == g]
        if len(sub) == 0:
            print(f"  {g:<6} {0:>5}")
            continue
        w = sub.is_win.sum()
        p = sub.pnl.sum()
        print(f"  {g:<6} {len(sub):>5} {100*w/len(sub):>5.1f}% {p:>+8.1f} {p/len(sub):>+8.1f} "
              f"{sub.mfe.mean():>7.1f} {sub.mae.mean():>7.1f}")

    # Cumulative
    print(f"\n  Cumulative (include grade and above):")
    print(f"  {'Threshold':<20} {'Count':>5} {'WR':>6} {'PnL':>8} {'PnL/Sig':>8} {'MaxDD':>7} {'PF':>6}")
    print(f"  {'-'*65}")
    for i, g_cutoff in enumerate(grade_order):
        included_grades = grade_order[:i+1]
        sub = df[df.final_grade.isin(included_grades)]
        if len(sub) == 0:
            continue
        w = sub.is_win.sum()
        p = sub.pnl.sum()
        gw = sub[sub.pnl > 0].pnl.sum()
        gl = sub[sub.pnl <= 0].pnl.sum()
        pf = abs(gw/gl) if gl else 999
        running = 0; peak_eq = 0; maxdd = 0
        for _, r in sub.sort_values(['date', 'time']).iterrows():
            running += r.pnl
            peak_eq = max(peak_eq, running)
            maxdd = min(maxdd, running - peak_eq)
        label = '+'.join(included_grades)
        print(f"  {label:<20} {len(sub):>5} {100*w/len(sub):>5.1f}% "
              f"{p:>+8.1f} {p/len(sub):>+8.1f} {maxdd:>7.1f} {pf:>6.2f}")

# ============================================================
# STEP 6: Show individual signals with grades
# ============================================================
print(f"\n{'='*80}")
print("STEP 6: ALL SIGNALS WITH FINAL GRADES")
print(f"{'='*80}")

grade_col = 'final_grade' if 'final_grade' in df.columns else 'best_grade'
score_col = col

print(f"\n  {'#':<3} {'Date':<12} {'Time':<9} {'Dir':<5} {'Delta':>6} {'Body':>5} {'PR':>5} "
      f"{'Score':>5} {'Grade':<4} {'Out':<5} {'PnL':>7} {'MFE':>6} {'MAE':>6}")
print(f"  {'-'*100}")
for idx, (_, r) in enumerate(df.sort_values(['date', 'time']).iterrows(), 1):
    out_mark = "W" if r.outcome == 'WIN' else "L"
    grade = r[grade_col]
    score = r[score_col]
    print(f"  {idx:<3} {r.date:<12} {r.time:<9} {r.dir:<5} {r.delta:>6} {r.body:>5.2f} "
          f"{r.peak_ratio:>5.2f} {score:>5.0f} {grade:<4} {out_mark:<5} {r.pnl:>+7.2f} "
          f"{r.mfe:>6.2f} {r.mae:>6.2f}")

# ============================================================
# STEP 7: SUMMARY
# ============================================================
print(f"\n{'='*80}")
print("FINAL SUMMARY")
print(f"{'='*80}")

print(f"\n  Scoring version: {best_version[0]}")
print(f"  Score column: {col}")
if 'final_grade' in df.columns:
    print(f"  Thresholds: A+>={best_thresh['A+']}, A>={best_thresh['A']}, "
          f"B>={best_thresh['B']}, C>={best_thresh['C']}")

print(f"\n  Component breakdown for {best_version[0]}:")
if best_version[0] == 'V1':
    print("    C1 Body (0-25): <0.5=25(USER PROPOSED), <1.0=20, <2.0=15, <3.0=10, 3.0+=5")
    print("    C2 Delta (0-25): 300-700=25, 200-300/700-1000=15, else=5")
    print("    C3 DZ (0-25): 0.7-1.2=25, 0.3-0.7=15, <0.3=10, >1.2=5")
    print("    C4 Context (0-25): doji+10, 12:30-13:00+10, 14:30-15:00+5, sig#1-2+5, PR 1.0-1.5+5")
elif best_version[0] == 'V2':
    print("    C1 Body (0-30): 0.5-1.0=30(68%WR), 3-4=28(75%WR), 2-3=22, 1-2=18, 4+=12, <0.5=3(25%WR)")
    print("    C2 Delta (0-25): 200-500=25(70%WR), 500-700=18, 100-200=15, 1000+=10, 700-1000=5(33%WR)")
    print("    C3 Signal# (0-20): #1-2=20(69%WR), #3=8, #4+=2(50%WR)")
    print("    C4 Context (0-25): time(12:30-13=12, 14:30-15=10, ...) + PR(1.5-2=8, ...)")
elif best_version[0] == 'V3':
    print("    C1 Delta (0-30): 200-500=30, 500-700=20, 100-200=15, 1000+=10, 700-1000=3")
    print("    C2 Body (0-25): 0.5-1.0=25, 3-4=22, 2-3=18, 1-2=14, 4+=10, <0.5=2")
    print("    C3 Signal# (0-20): #1-2=20, #3=8, #4+=2")
    print("    C4 Time (0-25): 12:30-13=25, 14:30-15=20, 10-11=15, ...")
elif best_version[0] == 'V4':
    print("    C1 Body (0-25): 0.5-1.0=25, 3-4=23, 2-3=18, 1-2=15, 4+=10, <0.5=2")
    print("    C2 Delta (0-25): 200-500=25, 500-700=15, 100-200=12, 1000+=8, 700-1000=3")
    print("    C3 Signal# (0-25): #1=25, #2=20, #3=8, #4+=2")
    print("    C4 Time (0-25): 12:30-13=25, 14:30-15=22, 9:30-11=18, 11-12=15, ...")
elif best_version[0] == 'V5':
    print("    C1 Body (0-30): 0.5-1.0=30, 3-4=27, 2-3=20, 1-2=16, 4+=10, <0.5=0(PENALTY)")
    print("    C2 Delta (0-25): 200-500=25, 500-700=16, 100-200=13, 1000+=8, 700-1000=3")
    print("    C3 Signal# (0-20): #1-2=20, #3=8, #4+=0")
    print("    C4 Time (0-25): 12:30-13=25, 14:30-15=20, 9:30-11=16, 11-12=14, ...")

# Correlation check
print(f"\n  Score-WR correlation:")
try:
    from scipy import stats as scipy_stats
    corr, pval = scipy_stats.pointbiserialr(df[col], df['is_win'])
    print(f"    Point-biserial r = {corr:.3f} (p={pval:.4f})")
    print(f"    {'SIGNIFICANT' if pval < 0.05 else 'NOT significant'} at p<0.05")
    print(f"    {'PREDICTIVE' if corr > 0 else 'ANTI-PREDICTIVE'} direction")
except ImportError:
    # Manual point-biserial correlation
    scores = df[col].values.astype(float)
    wins = df['is_win'].values.astype(float)
    n = len(scores)
    n1 = wins.sum()
    n0 = n - n1
    m1 = scores[wins == 1].mean()
    m0 = scores[wins == 0].mean()
    s = scores.std()
    if s > 0 and n0 > 0 and n1 > 0:
        rpb = (m1 - m0) / s * np.sqrt(n1 * n0 / (n * n))
        print(f"    Point-biserial r = {rpb:.3f} (manual calc, no p-value)")
        print(f"    Winner mean score: {m1:.1f}, Loser mean score: {m0:.1f}")
        print(f"    {'PREDICTIVE' if rpb > 0 else 'ANTI-PREDICTIVE'} direction")
    else:
        print(f"    Unable to compute correlation")

print(f"\n  DONE. Script saved as _v7_grading.py")
