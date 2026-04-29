"""Verify the 104% claim from scratch — trace EVERY trade."""
import psycopg2
from datetime import time as dtime, date

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t, DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, vix, greek_alignment,
         vanna_cliff_side, vanna_peak_side, outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
    AND greek_alignment IS NOT NULL ORDER BY ts
""")
trades = cur.fetchall()
print(f"Total SC LONGS Mar 1 - Apr 28 with align populated: {len(trades)}\n")

# Step 1: Apply non-align gates (replicate V13 SC-long-side gates besides align)
def non_align_gates(t):
    par=t[4]; cliff=t[7]; peak=t[8]; t_only=t[1].time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if par == "SIDIAL-EXTREME": return False
    if cliff == 'A' and peak == 'B': return False
    return True

after_non_align = [t for t in trades if non_align_gates(t)]
print(f"After non-align gates: {len(after_non_align)}")
print(f"Dropped by non-align gates: {len(trades) - len(after_non_align)}")

# Step 2: Distribution by alignment after non-align gates
print("\n=== Distribution after non-align gates ===")
align_dist = {}
for t in after_non_align:
    a = t[6]
    align_dist[a] = align_dist.get(a, 0) + 1
for a in sorted(align_dist.keys()):
    print(f"  align={a}: {align_dist[a]}t")

# Step 3: Show V13 (align>=2) and V14 (refined) sets
v13 = [t for t in after_non_align if t[6] >= 2]
BAD = ("GEX-LIS", "AG-LIS", "AG-PURE", "SIDIAL-EXTREME", "BOFA-MESSY")
v14 = [t for t in after_non_align if not (t[6] == 3 and t[4] in BAD)]

print(f"\nV13 (align >= 2): {len(v13)}")
print(f"V14 (NOT align=3+bad_para): {len(v14)}")

# Step 4: Decompose each set
print("\n=== V13 set composition ===")
for a in sorted(set(t[6] for t in v13)):
    sub = [t for t in v13 if t[6] == a]
    pnl = sum(float(t[10]) if t[10] else 0 for t in sub)
    w = sum(1 for t in sub if t[9] == "WIN")
    l = sum(1 for t in sub if t[9] == "LOSS")
    e = sum(1 for t in sub if t[9] == "EXPIRED")
    print(f"  align={a}: {len(sub)}t W={w} L={l} E={e}  PnL={pnl:+.1f}pt  ${pnl*5:+.0f}")

print(f"V13 total: {sum(1 for t in v13)}t  PnL={sum(float(t[10]) if t[10] else 0 for t in v13):+.1f}pt  ${sum(float(t[10]) if t[10] else 0 for t in v13)*5:+.0f}")

print("\n=== V14 set composition ===")
# Bucket: align in {-1, 0, 1, 2} OR align=3+good_para
def bucket(t):
    a = t[6]
    if a != 3: return f"align={a}"
    if t[4] not in BAD: return "align=3 good_para"
    return "align=3 bad_para"

bucket_data = {}
for t in v14:
    b = bucket(t)
    bucket_data.setdefault(b, []).append(t)

for b in sorted(bucket_data.keys()):
    sub = bucket_data[b]
    pnl = sum(float(t[10]) if t[10] else 0 for t in sub)
    w = sum(1 for t in sub if t[9] == "WIN")
    l = sum(1 for t in sub if t[9] == "LOSS")
    e = sum(1 for t in sub if t[9] == "EXPIRED")
    print(f"  {b}: {len(sub)}t W={w} L={l} E={e}  PnL={pnl:+.1f}pt  ${pnl*5:+.0f}")

v14_pnl = sum(float(t[10]) if t[10] else 0 for t in v14)
print(f"V14 total: {len(v14)}t  PnL={v14_pnl:+.1f}pt  ${v14_pnl*5:+.0f}")

# Step 5: SWAP analysis
print("\n=== THE SWAP: what V14 includes that V13 doesn't, and vice versa ===")
v13_ids = set(t[0] for t in v13)
v14_ids = set(t[0] for t in v14)

# In V13 but NOT V14 (V13 keeps but V14 blocks)
v13_only = [t for t in trades if t[0] in (v13_ids - v14_ids)]
# In V14 but NOT V13 (V14 keeps but V13 blocks)
v14_only = [t for t in trades if t[0] in (v14_ids - v13_ids)]
# In both
both = [t for t in trades if t[0] in (v13_ids & v14_ids)]

print(f"\nIn BOTH V13 and V14: {len(both)} trades")
both_pnl = sum(float(t[10]) if t[10] else 0 for t in both)
print(f"  PnL: {both_pnl:+.1f}pt  ${both_pnl*5:+.0f}")

print(f"\nIn V13 ONLY (V14 blocks these): {len(v13_only)} trades")
v13_only_pnl = sum(float(t[10]) if t[10] else 0 for t in v13_only)
print(f"  PnL: {v13_only_pnl:+.1f}pt  ${v13_only_pnl*5:+.0f}")
# Show their composition
for t in v13_only[:10]:
    print(f"    align={t[6]} para={t[4]} {t[9]} {t[10]}")
if len(v13_only) > 10:
    print(f"    ... and {len(v13_only)-10} more")

print(f"\nIn V14 ONLY (V13 blocks these): {len(v14_only)} trades")
v14_only_pnl = sum(float(t[10]) if t[10] else 0 for t in v14_only)
print(f"  PnL: {v14_only_pnl:+.1f}pt  ${v14_only_pnl*5:+.0f}")
for t in v14_only[:10]:
    print(f"    align={t[6]} para={t[4]} {t[9]} {t[10]}")
if len(v14_only) > 10:
    print(f"    ... and {len(v14_only)-10} more")

# Sanity: V14 total = both + v14_only
print(f"\nVerify: V14 PnL = both ({both_pnl:.1f}) + v14_only ({v14_only_pnl:.1f}) = {(both_pnl + v14_only_pnl):.1f}")
print(f"V14 expected: {v14_pnl:.1f}")
print(f"V13 PnL = both ({both_pnl:.1f}) + v13_only ({v13_only_pnl:.1f}) = {(both_pnl + v13_only_pnl):.1f}")
print(f"V13 expected: {sum(float(t[10]) if t[10] else 0 for t in v13):.1f}")

print(f"\n=== THE SWAP IMPACT ===")
print(f"V14 ADDS:    +${v14_only_pnl*5:+.0f} (trades V13 was missing)")
print(f"V14 REMOVES: ${v13_only_pnl*5:+.0f} (trades V13 was including, V14 blocks)")
print(f"Net swap:    ${(v14_only_pnl - v13_only_pnl)*5:+.0f}")
