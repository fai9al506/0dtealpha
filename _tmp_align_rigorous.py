"""Rigorous validation of SC long alignment rule.
Tests multiple angles to detect overfitting / data quality issues."""
import psycopg2
from datetime import time as dtime, date, timedelta
from collections import defaultdict

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

# === 1. DATA QUALITY: align column population ===
print("="*80)
print("1. DATA QUALITY CHECK")
print("="*80)
cur.execute("""
  SELECT
    DATE(ts AT TIME ZONE 'America/New_York') d,
    COUNT(*) total,
    COUNT(greek_alignment) populated,
    COUNT(*) FILTER (WHERE greek_alignment IS NULL) null_count
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01'
  GROUP BY 1 ORDER BY 1
  LIMIT 60
""")
nulls = 0
for r in cur.fetchall():
    if r[3] > 0: nulls += r[3]
print(f"SC long trades with NULL alignment: {nulls}")

# When did greek_alignment column start populating?
cur.execute("""
  SELECT DATE(ts AT TIME ZONE 'America/New_York') d, COUNT(*)
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND greek_alignment IS NULL AND ts >= '2026-03-01'
  GROUP BY 1 ORDER BY 1
""")
null_days = list(cur.fetchall())
if null_days:
    print(f"NULL align days (first 5): {null_days[:5]}")
    print(f"NULL align days (last 5): {null_days[-5:]}")

# === 2. ALIGN distribution + raw stats ===
print("\n"+"="*80)
print("2. ALIGN DISTRIBUTION (full Mar 1+)")
print("="*80)
cur.execute("""
  SELECT greek_alignment, COUNT(*),
    SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) w,
    SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) l,
    SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) e,
    ROUND(SUM(outcome_pnl)::numeric, 1) pnl,
    ROUND(AVG(CASE WHEN outcome_result='WIN' THEN outcome_pnl END)::numeric, 1) avg_win,
    ROUND(AVG(CASE WHEN outcome_result='LOSS' THEN outcome_pnl END)::numeric, 1) avg_loss
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
  GROUP BY greek_alignment ORDER BY greek_alignment
""")
print(f"{'align':>6}{'n':>6}{'W':>4}{'L':>4}{'E':>3}{'WR':>7}{'PnL':>8}{'AvgWin':>8}{'AvgLoss':>9}")
for r in cur.fetchall():
    a, n, w, l, e, pnl, avgw, avgl = r
    wr = w/(w+l)*100 if w+l else 0
    print(f"  {str(a):>6}{n:>6}{w:>4}{l:>4}{e:>3}{wr:>6.1f}%{pnl or 0:>+7.1f}{avgw or 0:>+7.1f}{avgl or 0:>+8.1f}")

# === 3. ALIGN=3 deep dive: when does it lose? ===
print("\n"+"="*80)
print("3. ALIGN=3 DEEP DIVE — when does it fail?")
print("="*80)
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t, grade, paradigm,
         spot, vix, outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND greek_alignment = 3 AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
""")
align3 = list(cur.fetchall())
print(f"Total align=3 SC longs: {len(align3)}")

# By paradigm
para_stats = defaultdict(list)
for r in align3:
    para_stats[r[3]].append(r)
print("\nBy paradigm:")
for p, rows in sorted(para_stats.items()):
    n = len(rows)
    w = sum(1 for r in rows if r[6] == "WIN")
    l = sum(1 for r in rows if r[6] == "LOSS")
    pnl = sum(float(r[7]) if r[7] else 0 for r in rows)
    wr = w/(w+l)*100 if w+l else 0
    print(f"  {p or 'NULL':<15} {n:>3}t  W={w:<3} L={l:<3}  WR={wr:>5.1f}%  PnL={pnl:+7.1f}pt  ${pnl*5:+>4.0f}")

# By time of day
print("\nBy hour:")
hour_stats = defaultdict(list)
for r in align3:
    hour_stats[r[1].hour].append(r)
for h in sorted(hour_stats.keys()):
    rows = hour_stats[h]
    n = len(rows)
    w = sum(1 for r in rows if r[6] == "WIN")
    l = sum(1 for r in rows if r[6] == "LOSS")
    pnl = sum(float(r[7]) if r[7] else 0 for r in rows)
    wr = w/(w+l)*100 if w+l else 0
    print(f"  {h:02d}:00  {n:>3}t  W={w:<3} L={l:<3}  WR={wr:>5.1f}%  PnL={pnl:+7.1f}pt")

# === 4. WALK-FORWARD VALIDATION ===
print("\n"+"="*80)
print("4. WALK-FORWARD VALIDATION (sliding 2-week window)")
print("="*80)
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t,
         DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, vix, greek_alignment,
         vanna_cliff_side, vanna_peak_side, outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
    AND greek_alignment IS NOT NULL
  ORDER BY ts
""")
all_sc_longs = cur.fetchall()
def passes_other(t):
    grade = t[3]
    par = t[4]
    cliff = t[7]
    peak = t[8]
    t_only = t[1].time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if par == "SIDIAL-EXTREME": return False
    if cliff == 'A' and peak == 'B': return False
    return True

# Walk-forward: 2-week chunks
print(f"{'Period':<25}{'V13(>=2)':>15}{'Block=3':>15}{'DropAlign':>15}")
start = date(2026, 3, 2)
period = 14
while start <= date(2026, 4, 28):
    end = start + timedelta(days=period)
    window = [t for t in all_sc_longs if start <= t[2] < end]

    v13_p = [t for t in window if passes_other(t) and t[6] >= 2]
    nb3 = [t for t in window if passes_other(t) and t[6] != 3]
    drop_a = [t for t in window if passes_other(t)]

    def total(g):
        return sum(float(t[10]) if t[10] else 0 for t in g) * 5
    label = f"{start} - {end-timedelta(days=1)}"
    print(f"  {label:<25}${total(v13_p):>+8.0f} ({len(v13_p):>2}t)  ${total(nb3):>+8.0f} ({len(nb3):>2}t)  ${total(drop_a):>+8.0f} ({len(drop_a):>2}t)")
    start = end

# === 5. PER-MONTH BREAKDOWN ===
print("\n"+"="*80)
print("5. PER-MONTH PROFITABILITY")
print("="*80)
month_buckets = defaultdict(list)
for t in all_sc_longs:
    if not passes_other(t): continue
    if t[6] != 3:  # block align=3 rule
        month_key = t[2].strftime("%Y-%m")
        month_buckets[month_key].append(t)

for m in sorted(month_buckets.keys()):
    g = month_buckets[m]
    n = len(g)
    w = sum(1 for t in g if t[9]=="WIN")
    l = sum(1 for t in g if t[9]=="LOSS")
    pnl = sum(float(t[10]) if t[10] else 0 for t in g)
    wr = w/(w+l)*100 if w+l else 0
    print(f"  {m}: {n}t  W={w} L={l}  WR={wr:.1f}%  PnL={pnl:+.1f}pt  ${pnl*5:+.0f}")

# === 6. PER-DAY OUTCOMES with rule applied ===
print("\n"+"="*80)
print("6. SINGLE-DAY CONCENTRATION CHECK")
print("="*80)
day_buckets = defaultdict(list)
for t in all_sc_longs:
    if not passes_other(t): continue
    if t[6] != 3:
        day_buckets[t[2]].append(t)
day_pnls = []
for d, g in day_buckets.items():
    pnl = sum(float(t[10]) if t[10] else 0 for t in g) * 5
    day_pnls.append((d, len(g), pnl))
day_pnls.sort(key=lambda x: x[2])
print("Top 5 BEST days:")
for d, n, pnl in day_pnls[-5:]:
    print(f"  {d}: {n}t  ${pnl:+.0f}")
print("Top 5 WORST days:")
for d, n, pnl in day_pnls[:5]:
    print(f"  {d}: {n}t  ${pnl:+.0f}")
total_all = sum(p[2] for p in day_pnls)
print(f"Total: ${total_all:+.0f}  Without top 1 best: ${total_all - day_pnls[-1][2]:+.0f}  Without top 3 best: ${total_all - sum(p[2] for p in day_pnls[-3:]):+.0f}")

# === 7. SIDIAL-EXTREME interaction ===
print("\n"+"="*80)
print("7. SIDIAL-EXTREME interaction")
print("="*80)
cur.execute("""
  SELECT paradigm, COUNT(*),
    SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) w,
    SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) l,
    ROUND(SUM(outcome_pnl)::numeric, 1) pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND greek_alignment != 3 AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
  GROUP BY paradigm ORDER BY pnl DESC
""")
print("Paradigm performance for align != 3:")
for r in cur.fetchall():
    p, n, w, l, pnl = r
    wr = w/(w+l)*100 if w+l else 0
    print(f"  {p or 'NULL':<15} {n:>3}t  W={w:<3} L={l:<3}  WR={wr:>5.1f}%  PnL={pnl}")

# === 8. CRITICAL: Check ALL combinations to find optimal ===
print("\n"+"="*80)
print("8. ALL ALIGN COMBINATIONS (with V13 other gates)")
print("="*80)
combos = [
    ("Allow all aligns",        lambda a: True),
    ("Block align=3",           lambda a: a != 3),
    ("Block align in {3, -3}",  lambda a: a not in (3, -3)),
    ("align in {1, 2}",         lambda a: a in (1, 2)),
    ("align in {-1, 0, 1, 2}",  lambda a: a in (-1, 0, 1, 2)),
    ("align >= 1",              lambda a: a is not None and a >= 1),
    ("align >= 2 (current)",    lambda a: a is not None and a >= 2),
    ("align == 2",              lambda a: a == 2),
    ("align in {2, -2}",        lambda a: a in (2, -2)),
    ("align in {1, 2, -1}",     lambda a: a in (1, 2, -1)),
    ("Block align in {2, 3}",   lambda a: a not in (2, 3)),
]
print(f"{'Rule':<30}{'Trades':>8}{'WR':>7}{'PnL':>10}{'$':>10}{'MaxDD':>9}")
for label, fn in combos:
    g = [t for t in all_sc_longs if passes_other(t) and t[6] is not None and fn(t[6])]
    n = len(g)
    if n == 0:
        print(f"  {label:<28}: 0t")
        continue
    w = sum(1 for t in g if t[9]=="WIN")
    l = sum(1 for t in g if t[9]=="LOSS")
    pnl = sum(float(t[10]) if t[10] else 0 for t in g)
    wr = w/(w+l)*100 if w+l else 0
    eq=0;pk=0;mdd=0
    for t in sorted(g, key=lambda x: x[1]):
        eq+=float(t[10]) if t[10] else 0; pk=max(pk,eq); mdd=max(mdd,pk-eq)
    print(f"  {label:<28}{n:>8}{wr:>6.1f}%{pnl:>+8.1f}pt${pnl*5:>+8.0f}{mdd:>+8.1f}")

# === 9. TWO INDEPENDENT TEST PERIODS ===
print("\n"+"="*80)
print("9. TWO INDEPENDENT TEST PERIODS (anti-overfitting)")
print("="*80)
test1 = [t for t in all_sc_longs if date(2026, 4, 14) <= t[2] <= date(2026, 4, 18)]  # week 1
test2 = [t for t in all_sc_longs if date(2026, 4, 21) <= t[2] <= date(2026, 4, 28)]  # week 2

for label, period in [("Week of Apr 14-18", test1), ("Week of Apr 21-28", test2)]:
    print(f"\n{label}: {len(period)} trades")
    for rule_label, fn in [("V13 align>=2", lambda a: a is not None and a >= 2),
                           ("Block align=3", lambda a: a != 3),
                           ("Drop align", lambda a: True)]:
        g = [t for t in period if passes_other(t) and (t[6] is not None) and fn(t[6])]
        n = len(g)
        w = sum(1 for t in g if t[9]=="WIN")
        l = sum(1 for t in g if t[9]=="LOSS")
        pnl = sum(float(t[10]) if t[10] else 0 for t in g)
        wr = w/(w+l)*100 if w+l else 0
        print(f"  {rule_label:<20} {n:>3}t  W={w:<3} L={l:<3}  WR={wr:>5.1f}%  ${pnl*5:+.0f}")
