"""P2 — Historical multi-signal-day analysis: what does cap=1 cost us?

For each trading day in 2026, find clusters where 2+ refined-whitelist signals
fired close enough in time that cap=1 would have rejected at least one.

Refined whitelist (S182 eval):
  - DD Exhaustion LONG (paradigm=BOFA-PURE only)
  - AG Short (any paradigm)
  - ES Absorption LONG (any paradigm)

Plus must pass V14/V16/S180 base filter (so it would have actually fired live).

Cap=1 lockout = signal arrived while another refined-eligible position still
open from earlier signal. We approximate "still open" via signal time vs prior
signal's outcome_resolved_at (or +60 min fallback).

Output: per-day clusters, $ left on table at cap=2 vs cap=1, monthly trend.
"""
import psycopg2
from datetime import timedelta
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

def refined_eligible(setup, direction, paradigm, grade, align):
    """Pre-filter for refined whitelist."""
    is_long = direction in ("long","bullish")
    is_short = direction in ("short","bearish")
    if setup == "DD Exhaustion":
        if not is_long: return False
        if paradigm != "BOFA-PURE": return False
        if align is None or align < 0 or align == 3: return False
        if grade == "C": return False
        return True
    if setup == "AG Short":
        if not is_short: return False
        return True
    if setup == "ES Absorption":
        if not is_long: return False
        if grade not in ("A","A+"): return False
        if paradigm in ("AG-TARGET","AG-LIS"): return False
        if align is None or align < 0: return False
        return True
    return False

cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm, greek_alignment, vix,
           (ts AT TIME ZONE 'America/New_York') AS et_ts,
           outcome_result, outcome_pnl,
           outcome_elapsed_min
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND outcome_pnl IS NOT NULL
      AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
    ORDER BY ts
""")
all_rows = cur.fetchall()

# Keep only refined-eligible signals
eligible = []
for r in all_rows:
    lid, setup, direction, grade, paradigm, align, vix, et_ts, out, pnl, elapsed = r
    if refined_eligible(setup, direction, paradigm, grade, align):
        eligible.append({
            "lid": lid, "setup": setup, "dir": direction, "grade": grade,
            "paradigm": paradigm, "et_ts": et_ts, "outcome": out,
            "pnl_pt": float(pnl) if pnl is not None else 0.0,
            "elapsed_min": int(elapsed) if elapsed is not None else 60,
        })

print(f"Total refined-eligible signals Feb 1 - May 26: {len(eligible)}\n")

# Group by date, then walk chronologically with cap=1 and cap=2 simulators
by_date = defaultdict(list)
for e in eligible:
    by_date[e["et_ts"].date()].append(e)

# Simulator: track "slots" of open positions per direction.
# Cap=1: 1 slot total. Cap=2: 2 slots total (per VPS Claude's stack_cap_contracts=4 = 2 positions * 2 MES).
def simulate(signals, cap):
    """Given chronologically-sorted signals, simulate cap behavior.
    Returns (allowed_signals, rejected_signals)."""
    open_until = []  # list of (close_dt) for slots currently held
    allowed = []
    rejected = []
    for s in signals:
        # Free slots whose close-time has passed
        open_until = [t for t in open_until if t > s["et_ts"]]
        if len(open_until) < cap:
            # Take slot — assume held for elapsed_min minutes
            close_dt = s["et_ts"] + timedelta(minutes=s["elapsed_min"])
            open_until.append(close_dt)
            allowed.append(s)
        else:
            rejected.append(s)
    return allowed, rejected

# Per-day analysis
total_cap1_pnl = 0.0
total_cap2_pnl = 0.0
total_signals = 0
total_rejected_cap1 = 0
days_with_lockout = 0
days_analyzed = 0
monthly = defaultdict(lambda: {"days": 0, "lockout_days": 0,
                               "cap1_pnl": 0.0, "cap2_pnl": 0.0, "delta": 0.0,
                               "n_rejected_cap1": 0})
all_lockouts = []  # list of (date, n_rejected, $ left on table)

for date, sigs in sorted(by_date.items()):
    days_analyzed += 1
    sigs.sort(key=lambda x: x["et_ts"])
    ymonth = date.strftime("%Y-%m")
    monthly[ymonth]["days"] += 1

    a1, r1 = simulate(sigs, cap=1)
    a2, r2 = simulate(sigs, cap=2)
    pnl1 = sum(s["pnl_pt"] for s in a1)
    pnl2 = sum(s["pnl_pt"] for s in a2)
    delta = pnl2 - pnl1
    total_cap1_pnl += pnl1
    total_cap2_pnl += pnl2
    total_signals += len(sigs)
    total_rejected_cap1 += len(r1)
    monthly[ymonth]["cap1_pnl"] += pnl1
    monthly[ymonth]["cap2_pnl"] += pnl2
    monthly[ymonth]["delta"] += delta
    monthly[ymonth]["n_rejected_cap1"] += len(r1)
    if r1:
        days_with_lockout += 1
        monthly[ymonth]["lockout_days"] += 1
        # Save lockout incidents
        all_lockouts.append({
            "date": date, "n_sigs": len(sigs), "n_rejected": len(r1),
            "cap1_pnl": pnl1, "cap2_pnl": pnl2, "delta": delta,
            "rejected": r1,
        })

print(f"Trading days analyzed: {days_analyzed}")
print(f"Days with cap=1 lockout: {days_with_lockout} ({days_with_lockout/days_analyzed*100:.1f}%)")
print(f"Total signals would have fired: {total_signals}")
print(f"  Cap=1 took: {total_signals - total_rejected_cap1}")
print(f"  Cap=1 rejected: {total_rejected_cap1} ({total_rejected_cap1/total_signals*100:.1f}%)")
print()
print(f"=== TOTAL P&L (at qty=2 MES per trade = $5/pt × 2) ===")
print(f"Cap=1 sim P&L: {total_cap1_pnl:+.1f} pt = ${total_cap1_pnl * 10:+,.0f} (2 MES)")
print(f"Cap=2 sim P&L: {total_cap2_pnl:+.1f} pt = ${total_cap2_pnl * 10:+,.0f} (2 MES)")
delta_total = total_cap2_pnl - total_cap1_pnl
days = days_analyzed
print(f"Cap=2 lift:    {delta_total:+.1f} pt = ${delta_total * 10:+,.0f} over {days} days = ${delta_total*10/days:+.2f}/day")
print()

print("=== MONTHLY breakdown ===")
print(f"{'month':>8s} {'days':>5s} {'lockout-d':>10s} {'cap1$':>8s} {'cap2$':>8s} {'lift$':>8s} {'rejected':>10s}")
for m in sorted(monthly):
    d = monthly[m]
    print(f"{m:>8s} {d['days']:>5d} {d['lockout_days']:>10d} "
          f"${d['cap1_pnl']*10:>+8,.0f} ${d['cap2_pnl']*10:>+8,.0f} "
          f"${d['delta']*10:>+8,.0f} {d['n_rejected_cap1']:>10d}")

print(f"\n=== Top 10 biggest lockout days (cap=1 -> cap=2 lift) ===")
all_lockouts.sort(key=lambda x: -x["delta"])
for lk in all_lockouts[:10]:
    print(f"  {lk['date']}  n_sigs={lk['n_sigs']}  rejected={lk['n_rejected']}  "
          f"cap1=${lk['cap1_pnl']*10:+,.0f}  cap2=${lk['cap2_pnl']*10:+,.0f}  "
          f"lift=${lk['delta']*10:+,.0f}")

print(f"\n=== Verification: today's data (2026-05-26) ===")
from datetime import date as _d
today_data = by_date.get(_d(2026,5,26), [])
print(f"Refined-eligible signals today: {len(today_data)}")
for s in today_data:
    print(f"  lid={s['lid']} {s['setup']} {s['dir']} et={s['et_ts'].strftime('%H:%M')} "
          f"pnl_pt={s['pnl_pt']:+.1f}")

conn.close()
