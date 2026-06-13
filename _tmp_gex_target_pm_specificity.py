"""Is PM-longs-bad specific to GEX-TARGET, or general?

If general (all paradigms get worse in PM), then a time-of-day rule belongs in V16.
If specific to GEX-TARGET (other paradigms keep their PM edge), then the
paradigm × time interaction is the real signal — V17 candidate.
"""
import psycopg2
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

# Per paradigm × AM/PM split, longs only, all setups
cur.execute("""
    SELECT paradigm,
           CASE WHEN EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York')) < 13 THEN 'AM' ELSE 'PM' END as session,
           COUNT(*),
           SUM(outcome_pnl),
           AVG(outcome_pnl),
           SUM(CASE WHEN outcome_pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as wr
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
      AND paradigm IS NOT NULL
    GROUP BY paradigm, session
    HAVING COUNT(*) >= 3
    ORDER BY paradigm, session
""")

print("LONGS by paradigm x AM/PM (SC/DD/ES Abs, no V16 filter):")
print(f"\n  {'paradigm':18s} {'sess':>5s} {'n':>4s} {'wr':>6s} {'total':>9s} {'mean':>7s}")
prev_para = None
am_data = {}
pm_data = {}
for r in cur.fetchall():
    para, sess, n, total, mean, wr = r
    if prev_para and para != prev_para:
        # Print AM vs PM delta if both exist
        if prev_para in am_data and prev_para in pm_data:
            ad = am_data[prev_para]
            pd = pm_data[prev_para]
            delta_mean = pd[3] - ad[3]
            print(f"  {'':>18s}  AM->PM mean delta: {delta_mean:+5.2f}pt  "
                  f"({'-' if delta_mean < 0 else '+'} PM is "
                  f"{'WORSE' if delta_mean < 0 else 'BETTER'})")
        print()
    print(f"  {para:18s} {sess:>5s} {n:>4d} {wr:>5.1f}% {float(total):+8.1f}pt {float(mean):+6.2f}pt")
    if sess == 'AM':
        am_data[para] = r
    else:
        pm_data[para] = r
    prev_para = para
# Last paradigm
if prev_para in am_data and prev_para in pm_data:
    ad = am_data[prev_para]
    pd = pm_data[prev_para]
    delta_mean = float(pd[3]) - float(ad[3])
    print(f"  {'':>18s}  AM->PM mean delta: {delta_mean:+5.2f}pt")

# Summary: rank paradigms by how much WORSE they get from AM to PM
print("\n\n" + "="*70)
print("Rank paradigms by PM-degradation (longs):")
print("="*70)
print(f"  {'paradigm':18s} {'AM n':>5s} {'AM mn':>7s} {'PM n':>5s} {'PM mn':>7s} {'delta':>7s}")
deltas = []
# Tuple: (paradigm, session, n=2, total=3, mean=4, wr=5)
for para in set(am_data.keys()) | set(pm_data.keys()):
    if para in am_data and para in pm_data:
        am = am_data[para]; pm = pm_data[para]
        delta_mean = float(pm[4]) - float(am[4])
        deltas.append((para, int(am[2]), float(am[4]), int(pm[2]), float(pm[4]), delta_mean))
deltas.sort(key=lambda x: x[5])
for d in deltas:
    print(f"  {d[0]:18s} {d[1]:>5d} {d[2]:>+6.2f} {d[3]:>5d} {d[4]:>+6.2f} {d[5]:>+6.2f}")

# Also: just OVERALL hour split (no paradigm)
print("\n\n" + "="*70)
print("OVERALL longs by hour (SC/DD/ES Abs, all paradigms):")
print("="*70)
cur.execute("""
    SELECT EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York')) as hr,
           COUNT(*),
           SUM(outcome_pnl),
           AVG(outcome_pnl),
           SUM(CASE WHEN outcome_pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as wr
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
    GROUP BY hr
    ORDER BY hr
""")
print(f"  {'hour':>6s} {'n':>4s} {'wr':>6s} {'total':>9s} {'mean':>7s}")
for r in cur.fetchall():
    hr, n, total, mean, wr = r
    print(f"  {int(hr):>4d}:00 {n:>4d} {float(wr):>5.1f}% {float(total):+8.1f}pt {float(mean):+6.2f}pt")

# Refined rule simulation: "block long in GEX-TARGET when hour >= 13"
# vs broad "block all longs hour >= 13"
print("\n\n" + "="*70)
print("RULE SIMULATIONS (V16-eligible longs only — apply current V16 filter)")
print("="*70)

# V16 filter approximation
def v16_eligible(setup, grade, paradigm, align, vix, et_ts):
    if paradigm == "SIDIAL-EXTREME": return False
    if setup == "Skew Charm":
        if grade in ("C", "LOG"): return False
        if align == 3 and paradigm in ("GEX-LIS","AG-LIS","AG-PURE","BOFA-MESSY"): return False
        if paradigm == "GEX-LIS": return False
        if et_ts and et_ts.weekday() == 4 and 15 <= et_ts.day <= 21: return False
        return True
    elif setup == "DD Exhaustion":
        if align is None or align < 0: return False
        if align == 3: return False
        if vix is not None and vix >= 22: return False
        if paradigm in ("GEX-LIS","AG-LIS","AG-PURE","BofA-LIS","BOFA-MESSY"): return False
        if grade == "C": return False
        return True
    elif setup == "ES Absorption":
        if grade not in ("A","A+"): return False
        if paradigm in ("AG-TARGET","AG-LIS"): return False
        if align is None or align < 0: return False
        return True
    return False

cur.execute("""
    SELECT id, setup_name, grade, paradigm, greek_alignment, vix,
           outcome_pnl, (ts AT TIME ZONE 'America/New_York') as et_ts
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
""")
all_long = cur.fetchall()
v16 = [r for r in all_long if v16_eligible(r[1], r[2], r[3], r[4], r[5], r[7])]

baseline_total = sum(float(r[6]) for r in v16)
baseline_n = len(v16)
baseline_wr = sum(1 for r in v16 if float(r[6]) > 0) / baseline_n * 100

def sim_block(v16_list, predicate):
    kept = [r for r in v16_list if not predicate(r)]
    total = sum(float(r[6]) for r in kept)
    wr = sum(1 for r in kept if float(r[6]) > 0) / len(kept) * 100 if kept else 0
    blocked = [r for r in v16_list if predicate(r)]
    blocked_pnl = sum(float(r[6]) for r in blocked)
    return (len(kept), total, wr, len(blocked), blocked_pnl)

rules = [
    ("BASELINE (no new block)", lambda r: False),
    ("Block GEX-TARGET longs ALL", lambda r: r[3] == "GEX-TARGET"),
    ("Block GEX-TARGET longs PM (>=13:00 ET)", lambda r: r[3] == "GEX-TARGET" and r[7].hour >= 13),
    ("Block GEX-TARGET longs PM (>=14:00 ET)", lambda r: r[3] == "GEX-TARGET" and r[7].hour >= 14),
    ("Block GEX-TARGET DD longs only", lambda r: r[3] == "GEX-TARGET" and r[1] == "DD Exhaustion"),
    ("Block GEX-TARGET DD longs PM only", lambda r: r[3]=="GEX-TARGET" and r[1]=="DD Exhaustion" and r[7].hour>=13),
    ("Block ALL longs >=13:00", lambda r: r[7].hour >= 13),
    ("Block GEX-TARGET + GEX-MESSY longs PM", lambda r: r[3] in ("GEX-TARGET","GEX-MESSY") and r[7].hour>=13),
]
print(f"\n  {'rule':45s} {'n_kept':>7s} {'total':>9s} {'wr':>6s} {'blocked':>8s} {'block_pnl':>10s} {'delta':>7s}")
for name, pred in rules:
    n, total, wr, nb, bp = sim_block(v16, pred)
    delta = total - baseline_total
    print(f"  {name:45s} {n:>7d} {total:>+8.1f}pt {wr:>5.1f}% {nb:>8d} {bp:>+9.1f}pt {delta:>+6.1f}pt")

conn.close()
