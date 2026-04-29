"""Drill down on BOFA-PURE filter — find optimal time threshold and combine with other rules."""
import psycopg2
from datetime import time as dtime, date

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t, DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, spot, vix, greek_alignment,
         v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side,
         outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='short'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL ORDER BY ts
""")
trades = cur.fetchall()

def v13_pass(t):
    lid, ts, d, grade, par, spot, vix, align, gex, dd, cliff, peak, res, pnl = t
    if grade in ("C", "LOG"): return False
    if dtime(14, 30) <= ts.time() < dtime(15, 0): return False
    if ts.time() >= dtime(15, 30): return False
    if gex is not None and float(gex) >= 75: return False
    if dd is not None and float(dd) >= 3_000_000_000: return False
    if par == "GEX-LIS": return False
    if cliff == 'A' and peak == 'B': return False
    return True

def stats(group, label):
    n = len(group)
    if n == 0: return f"{label:<55}0t"
    w = sum(1 for t in group if t[12]=="WIN")
    l = sum(1 for t in group if t[12]=="LOSS")
    e = sum(1 for t in group if t[12]=="EXPIRED")
    pnl = sum(float(t[13]) if t[13] else 0 for t in group)
    wr = w/(w+l)*100 if w+l else 0
    eq=0;pk=0;mdd=0
    for t in sorted(group, key=lambda x: x[1]):
        eq += float(t[13]) if t[13] else 0; pk=max(pk,eq); mdd=max(mdd,pk-eq)
    return f"{label:<55}{n:>4}t W={w:<3} L={l:<3} E={e:<2} WR={wr:>5.1f}% PnL={pnl:+8.1f}pt ${pnl*5:+>5.0f} MaxDD={mdd:>5.1f}"

# V13 baseline
v13 = [t for t in trades if v13_pass(t)]
print(stats(v13, "BASELINE V13"))

# Sweep BOFA-PURE block by hour
print("\n=== BOFA-PURE block at various time thresholds ===")
for hr in (10, 11, 12, 13, 14):
    def f(t, hr=hr):
        return not (t[4] == "BOFA-PURE" and t[1].time() >= dtime(hr, 0))
    kept = [t for t in v13 if f(t)]
    blkd = [t for t in v13 if not f(t)]
    print(stats(kept, f"V13 + block BOFA-PURE >= {hr}:00"))
    print(stats(blkd, f"  blocked"))

# Sweep BOFA-PURE always vs grade-conditional
print("\n=== BOFA-PURE all-day variants ===")
def fAll(t): return t[4] != "BOFA-PURE"
print(stats([t for t in v13 if fAll(t)], "V13 + block ALL BOFA-PURE shorts"))
print(stats([t for t in v13 if not fAll(t)], "  blocked: all BOFA-PURE"))

def fAfterPM(t):
    return not (t[4] == "BOFA-PURE" and t[1].time() >= dtime(12, 0))
def fAfter11(t):
    return not (t[4] == "BOFA-PURE" and t[1].time() >= dtime(11, 0))
def fAfterPM_LowGrade(t):
    return not (t[4] == "BOFA-PURE" and t[1].time() >= dtime(12, 0) and t[3] in ("A", "B", "C"))

print(stats([t for t in v13 if fAfterPM_LowGrade(t)], "V13 + BOFA-PURE >= 12:00 + grade<=A only"))

# What about charm + BOFA-PURE specifically?
print("\n=== Look at BOFA-PURE shorts breakdown by hour ===")
bofa = [t for t in v13 if t[4] == "BOFA-PURE"]
print(f"Total BOFA-PURE shorts in V13: {len(bofa)}")
for hr_lo, hr_hi in [(9, 10), (10, 11), (11, 12), (12, 13), (13, 14), (14, 15), (15, 16)]:
    sub = [t for t in bofa if dtime(hr_lo, 0) <= t[1].time() < dtime(hr_hi, 0)]
    if not sub: continue
    n = len(sub)
    w = sum(1 for t in sub if t[12] == "WIN")
    l = sum(1 for t in sub if t[12] == "LOSS")
    pnl = sum(float(t[13]) if t[13] else 0 for t in sub)
    wr = w/(w+l)*100 if w+l else 0
    print(f"  {hr_lo:02d}:00-{hr_hi:02d}:00 BOFA-PURE: {n}t  {w}W/{l}L  WR={wr:.1f}%  PnL={pnl:+.1f}pt")

# Composite: Rule 3 + CB
print("\n=== Composite: V13 + BOFA-PURE>=12:00 block + CB ===")
def apply_cb(trade_list):
    out = []
    cur_d = None; consec = 0; paused = False
    for t in trade_list:
        d = t[2]
        if d != cur_d:
            cur_d = d; consec = 0; paused = False
        if paused: continue
        out.append(t)
        if t[12] == "LOSS":
            consec += 1
            if consec >= 2: paused = True
        elif t[12] == "WIN":
            consec = 0
    return out

filtered_pre_cb = [t for t in v13 if fAfterPM(t)]
combined = apply_cb(sorted(filtered_pre_cb, key=lambda x: x[1]))
print(stats(combined, "V13 + BOFA-PURE PM block + CB"))

# Apr 24-27 specifically
recent = [t for t in v13 if t[2] >= date(2026, 4, 24)]
print(f"\n=== Apr 24-27 specific ===")
print(stats(recent, "V13 baseline"))
print(stats([t for t in recent if fAfterPM(t)], "Rule 3 (BOFA-PURE >= 12:00 block)"))
print(stats(apply_cb(sorted([t for t in recent if fAfterPM(t)], key=lambda x: x[1])), "Rule 3 + CB"))

# Comparison: Apollo's daily call vs BOFA-PURE rule
print(f"\n=== Apr 24 trade-by-trade: BOFA-PURE PM rule ===")
apr24 = [t for t in v13 if t[2] == date(2026, 4, 24)]
for t in apr24:
    lid, ts, d, grade, par, spot, vix, align, gex, dd, cliff, peak, res, pnl = t
    blocked = (par == "BOFA-PURE" and ts.time() >= dtime(12, 0))
    print(f"  #{lid} {ts.strftime('%H:%M')} grade={grade or '?'} par={par or '?':12} spot={float(spot):.1f}  {'[BLOCKED-Rule3]' if blocked else '[KEPT]':16}  {res:7} {pnl}")
