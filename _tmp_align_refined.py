"""Refined SC long alignment rule — paradigm-aware."""
import psycopg2
from datetime import time as dtime, date, timedelta
from collections import defaultdict

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t,
         DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, vix, greek_alignment,
         vanna_cliff_side, vanna_peak_side, outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
    AND greek_alignment IS NOT NULL ORDER BY ts
""")
trades = cur.fetchall()

def passes_other(t):
    grade = t[3]; par = t[4]; cliff = t[7]; peak = t[8]
    t_only = t[1].time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if par == "SIDIAL-EXTREME": return False
    if cliff == 'A' and peak == 'B': return False
    return True

# Paradigms that lose with align=3 (from prior analysis)
BAD_PARADIGMS_ALIGN3 = ("GEX-LIS", "AG-LIS", "AG-PURE", "SIDIAL-EXTREME", "BOFA-MESSY")

def stats(group, label):
    n = len(group)
    if n == 0: return f"{label:<55}: 0t"
    w = sum(1 for t in group if t[9] == "WIN")
    l = sum(1 for t in group if t[9] == "LOSS")
    e = sum(1 for t in group if t[9] == "EXPIRED")
    pnl = sum(float(t[10]) if t[10] else 0 for t in group)
    wr = w/(w+l)*100 if w+l else 0
    eq=0;pk=0;mdd=0
    for t in sorted(group, key=lambda x: x[1]):
        eq+=float(t[10]) if t[10] else 0; pk=max(pk,eq); mdd=max(mdd,pk-eq)
    return f"{label:<55}{n:>4}t W={w} L={l} E={e} WR={wr:.1f}% PnL={pnl:+.1f}pt ${pnl*5:+.0f} MaxDD={mdd:.1f}"

print("=== ALL CANDIDATE RULES on SC LONGS (Mar 1 - Apr 28) ===\n")

rules = [
    ("V13 current (align>=2)",
     lambda t: passes_other(t) and t[6] >= 2),
    ("Block align=3 (simple)",
     lambda t: passes_other(t) and t[6] != 3),
    ("Block align=3 + bad paradigm",
     lambda t: passes_other(t) and not (t[6] == 3 and t[4] in BAD_PARADIGMS_ALIGN3)),
    ("Block align=3 only if bad paradigm",
     lambda t: passes_other(t) and not (t[6] == 3 and t[4] in BAD_PARADIGMS_ALIGN3)),
    ("Drop align gate entirely",
     lambda t: passes_other(t)),
    ("align in {2}",
     lambda t: passes_other(t) and t[6] == 2),
    ("align in {1, 2} ",
     lambda t: passes_other(t) and t[6] in (1, 2)),
    ("align in {1, 2} OR align=3 with good para",
     lambda t: passes_other(t) and (t[6] in (1, 2) or (t[6] == 3 and t[4] not in BAD_PARADIGMS_ALIGN3))),
    ("align != 3 OR (align=3 with good para)",
     lambda t: passes_other(t) and (t[6] != 3 or t[4] not in BAD_PARADIGMS_ALIGN3)),
]

for label, fn in rules:
    g = [t for t in trades if fn(t)]
    print(stats(g, label))

# Validate the "exceptional days" caveat — exclude top 3 days
print("\n\n=== Robustness: exclude top-3 best days from each rule ===")
for label, fn in rules[:5]:
    g = [t for t in trades if fn(t)]
    # Group by day, find top 3
    by_day = defaultdict(float)
    for t in g:
        by_day[t[2]] += float(t[10]) if t[10] else 0
    top3 = sorted(by_day.items(), key=lambda x: -x[1])[:3]
    top3_days = {d for d, _ in top3}
    g_filt = [t for t in g if t[2] not in top3_days]
    print(stats(g_filt, f"{label} (ex-top3 days)"))
    print(f"   Top 3 days excluded: {[(str(d), f'${p*5:+.0f}') for d, p in top3]}")

# Walk-forward with refined rules — show consistency
print("\n=== Walk-forward 2-week windows ===")
start = date(2026, 3, 2)
period = 14
print(f"{'Window':<28}{'V13 (>=2)':>13}{'Block=3':>13}{'Block=3+badpara':>18}")
while start <= date(2026, 4, 28):
    end = start + timedelta(days=period)
    window = [t for t in trades if start <= t[2] < end]
    v13 = [t for t in window if passes_other(t) and t[6] >= 2]
    nb3 = [t for t in window if passes_other(t) and t[6] != 3]
    refined = [t for t in window if passes_other(t) and not (t[6] == 3 and t[4] in BAD_PARADIGMS_ALIGN3)]
    pnl = lambda g: sum(float(t[10]) if t[10] else 0 for t in g) * 5
    label = f"{start} to {end-timedelta(days=1)}"
    print(f"  {label:<26}${pnl(v13):>+8.0f}({len(v13):>2}t) ${pnl(nb3):>+8.0f}({len(nb3):>2}t) ${pnl(refined):>+8.0f}({len(refined):>2}t)")
    start = end

# Per-day for the BEST rule
print("\n=== Per-day breakdown: 'Block align=3 + bad paradigm' ===")
best_rule = lambda t: passes_other(t) and not (t[6] == 3 and t[4] in BAD_PARADIGMS_ALIGN3)
g_best = [t for t in trades if best_rule(t)]
day_pnls = defaultdict(list)
for t in g_best:
    day_pnls[t[2]].append(t)
total_d = 0
worst_5 = []
best_5 = []
for d in sorted(day_pnls.keys()):
    rows = day_pnls[d]
    n = len(rows)
    pnl = sum(float(t[10]) if t[10] else 0 for t in rows)
    total_d += pnl
    worst_5.append((d, n, pnl))
    best_5.append((d, n, pnl))
worst_5.sort(key=lambda x: x[2])
best_5.sort(key=lambda x: -x[2])
print(f"\nBest 5 days:")
for d, n, p in best_5[:5]:
    print(f"  {d}: {n}t  ${p*5:+.0f}")
print(f"Worst 5 days:")
for d, n, p in worst_5[:5]:
    print(f"  {d}: {n}t  ${p*5:+.0f}")
print(f"\nTotal: ${total_d*5:+.0f}")
print(f"Without top 1: ${(total_d - best_5[0][2])*5:+.0f}")
print(f"Without top 3: ${(total_d - sum(x[2] for x in best_5[:3]))*5:+.0f}")
print(f"Without top 5: ${(total_d - sum(x[2] for x in best_5[:5]))*5:+.0f}")

# Test on V13-era specifically
print("\n=== V13 era only (Apr 17+) ===")
v13_era = [t for t in trades if t[2] >= date(2026, 4, 17)]
for label, fn in rules[:5]:
    g = [t for t in v13_era if fn(t)]
    print(stats(g, label))
