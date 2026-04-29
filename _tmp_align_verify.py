"""Rigorous verification of SC long alignment rule.
Test multiple rules with train/test split to detect overfitting."""
import psycopg2
from datetime import time as dtime, date

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t,
         DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, spot, vix, greek_alignment,
         vanna_cliff_side, vanna_peak_side,
         outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL ORDER BY ts
""")
trades = cur.fetchall()
print(f"Total SC LONG trades Mar 1 - Apr 28: {len(trades)}")

# Period span
first_d = min(t[2] for t in trades)
last_d = max(t[2] for t in trades)
days = (last_d - first_d).days
print(f"Period: {first_d} to {last_d} = {days} calendar days = ~{days//7} weeks\n")

def stats(group, label):
    n = len(group)
    if n == 0: return f"{label:<55}0t"
    w = sum(1 for t in group if t[10] == "WIN")
    l = sum(1 for t in group if t[10] == "LOSS")
    e = sum(1 for t in group if t[10] == "EXPIRED")
    pnl = sum(float(t[11]) if t[11] else 0 for t in group)
    wr = w/(w+l)*100 if w+l else 0
    eq=0;pk=0;mdd=0
    for t in sorted(group, key=lambda x: x[1]):
        eq += float(t[11]) if t[11] else 0
        pk = max(pk, eq)
        mdd = max(mdd, pk - eq)
    return f"{label:<55}{n:>4}t W={w:<3} L={l:<3} E={e:<2} WR={wr:>5.1f}% PnL={pnl:+8.1f}pt ${pnl*5:+>5.0f} MaxDD={mdd:>5.1f}"

# Replicate V13 long filter (the rest of the gates besides align)
def passes_other_long_gates(t):
    """V13 long gates EXCEPT align."""
    lid, ts, d, grade, par, spot, vix, align, cliff, peak, res, pnl = t
    t_only = ts.time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if par == "SIDIAL-EXTREME": return False
    if cliff == 'A' and peak == 'B': return False
    return True

print("=== Apply different ALIGNMENT rules (with V13 other gates) ===\n")
print(stats(trades, "All SC longs (no filter)"))
filtered_other = [t for t in trades if passes_other_long_gates(t)]
print(stats(filtered_other, "After non-align V13 gates only"))

# Variants
def f_v13(t): return passes_other_long_gates(t) and (t[7] is not None and t[7] >= 2)
def f_drop_align(t): return passes_other_long_gates(t)  # no align gate
def f_no_3(t): return passes_other_long_gates(t) and (t[7] != 3)
def f_pos_only(t): return passes_other_long_gates(t) and (t[7] is not None and t[7] >= 1)
def f_in_1_2(t): return passes_other_long_gates(t) and (t[7] in (1, 2))
def f_in_neg1_to_2(t): return passes_other_long_gates(t) and (t[7] in (-1, 0, 1, 2))
def f_not_3(t): return passes_other_long_gates(t) and (t[7] != 3 and t[7] != -3)
def f_v13_no_3(t): return passes_other_long_gates(t) and (t[7] is not None and t[7] in (2,))

print()
print(stats([t for t in trades if f_v13(t)], "V13 current: align >= 2"))
print(stats([t for t in trades if f_drop_align(t)], "DROP align gate entirely"))
print(stats([t for t in trades if f_no_3(t)], "Block only align=3 (keep others)"))
print(stats([t for t in trades if f_pos_only(t)], "Require align >= 1"))
print(stats([t for t in trades if f_in_1_2(t)], "Allow only align in {1, 2}"))
print(stats([t for t in trades if f_in_neg1_to_2(t)], "Allow align in {-1, 0, 1, 2}"))
print(stats([t for t in trades if f_not_3(t)], "Block align in {3, -3}"))
print(stats([t for t in trades if f_v13_no_3(t)], "Allow only align==2"))

# === OUT-OF-SAMPLE TEST: split Mar 1 - Apr 11 (train) vs Apr 14 - Apr 28 (test) ===
print("\n\n=== OUT-OF-SAMPLE VALIDATION ===")
mid_date = date(2026, 4, 11)  # split date
train = [t for t in trades if t[2] <= mid_date]
test = [t for t in trades if t[2] > mid_date]
print(f"Train: {min(t[2] for t in train)} to {max(t[2] for t in train)} ({len(train)} trades)")
print(f"Test:  {min(t[2] for t in test)} to {max(t[2] for t in test)} ({len(test)} trades)")
print()

variants = [
    ("V13 align >= 2", f_v13),
    ("DROP align gate", f_drop_align),
    ("Block only align=3", f_no_3),
    ("align >= 1", f_pos_only),
    ("align in {1, 2}", f_in_1_2),
    ("align in {-1,0,1,2}", f_in_neg1_to_2),
    ("Block align in {3,-3}", f_not_3),
    ("align == 2 only", f_v13_no_3),
]

print(f"{'Rule':<30}{'TRAIN PnL':>12}{'TRAIN WR':>11}{'TEST PnL':>12}{'TEST WR':>11}{'Both pos?':>11}")
for label, fn in variants:
    train_filt = [t for t in train if fn(t)]
    test_filt = [t for t in test if fn(t)]
    train_pnl = sum(float(t[11]) if t[11] else 0 for t in train_filt)
    test_pnl = sum(float(t[11]) if t[11] else 0 for t in test_filt)
    train_w = sum(1 for t in train_filt if t[10]=="WIN")
    train_l = sum(1 for t in train_filt if t[10]=="LOSS")
    test_w = sum(1 for t in test_filt if t[10]=="WIN")
    test_l = sum(1 for t in test_filt if t[10]=="LOSS")
    train_wr = train_w/(train_w+train_l)*100 if train_w+train_l else 0
    test_wr = test_w/(test_w+test_l)*100 if test_w+test_l else 0
    both_pos = "YES" if train_pnl > 0 and test_pnl > 0 else "NO"
    print(f"  {label:<28}${train_pnl*5:>+8.0f}    {train_wr:>5.1f}%    ${test_pnl*5:>+8.0f}    {test_wr:>5.1f}%    {both_pos}")

# === Per-day breakdown for "best" rule ===
print("\n=== Best rule (Block align=3) — daily breakdown V13 era ===")
v13_era_trades = [t for t in trades if t[2] >= date(2026, 4, 17)]
v13_era_filtered = [t for t in v13_era_trades if f_no_3(t)]
print(stats(v13_era_filtered, "V13 era SC longs (align != 3)"))
from collections import defaultdict
by_day = defaultdict(list)
for t in v13_era_filtered:
    by_day[t[2]].append(t)
for d in sorted(by_day.keys()):
    sub = by_day[d]
    n = len(sub); w = sum(1 for t in sub if t[10] == "WIN"); l = sum(1 for t in sub if t[10] == "LOSS")
    pnl = sum(float(t[11]) if t[11] else 0 for t in sub)
    print(f"  {d}: {n}t  W={w} L={l}  PnL={pnl:+.1f}pt  ${pnl*5:+.0f}")

# === Real money impact: V13-era loss prevented ===
print("\n=== Estimated V13-era impact if SC longs were live ===")
fixed_v13_era = sum(float(t[11]) if t[11] else 0 for t in v13_era_filtered) * 5
v13_era_days = len(set(t[2] for t in v13_era_trades))
print(f"V13 era trading days: {v13_era_days}")
print(f"Portal PnL captured: ${fixed_v13_era:+.0f}")
print(f"$/day: ${fixed_v13_era/v13_era_days:+.0f}")
print(f"Monthly proj: ${fixed_v13_era/v13_era_days*21:+.0f}")
