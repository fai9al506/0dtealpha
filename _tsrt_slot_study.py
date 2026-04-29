"""TSRT concurrency study: did 1-per-direction cap hurt us?

Simulates TSRT behavior on historical V13-passing signals:
- 2 accounts: LONGS (SC only), SHORTS (SC + AG)
- MAX_CONCURRENT_PER_DIR = 1
- Each trade occupies slot for outcome_elapsed_min minutes
- If signal fires while slot occupied, it's SKIPPED (missed opportunity)

Tracks: what fired, what skipped, what was the opportunity cost.
"""
import psycopg2
from collections import defaultdict
from datetime import timedelta

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
def reconnect():
    c = psycopg2.connect(DB, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
    return c, c.cursor()
conn, cur = reconnect()

def safe_exec(sql, args=None):
    global conn, cur
    for attempt in range(3):
        try:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else None
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try: conn.close()
            except: pass
            conn, cur = reconnect()
    raise

START = '2026-03-01'
END = '2026-04-17'

raw = safe_exec("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction,
       outcome_elapsed_min,
       outcome_max_profit, outcome_max_loss,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""", (START, END))

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, el, mfe, mae, h, m, d = t
    if setup in ("VIX Divergence", "IV Momentum", "Vanna Butterfly"): return False
    if setup == 'Skew Charm' and grade and grade in ('C', 'LOG'): return False
    if setup in ('Skew Charm', 'DD Exhaustion'):
        if (h == 14 and m >= 30) or h == 15: return False
    if setup == 'BofA Scalp' and ((h == 14 and m >= 30) or h >= 15): return False
    is_long = dirx in ('long', 'bullish')
    if is_long and paradigm == 'SIDIAL-EXTREME': return False
    if is_long:
        if align is None or align < 2: return False
        if setup == 'Skew Charm': return True
        vix_f = float(vix) if vix else None
        ovx_f = float(ovx) if ovx else -99
        if vix_f is not None and vix_f > 22 and ovx_f < 2: return False
        return True
    else:
        if setup in ('Skew Charm','DD Exhaustion') and paradigm == 'GEX-LIS': return False
        if setup == 'AG Short' and paradigm == 'AG-TARGET': return False
        if setup in ('Skew Charm', 'AG Short'): return True
        if setup == 'DD Exhaustion' and align != 0: return True
        return False

v12 = [t for t in raw if passes_v12fix(t)]
print(f"V12-fix eligible: {len(v12)}")

# Apply V13 on top (GEX/DD + vanna) — reuse cached features
def get_v13_gex(ts, spot):
    r = safe_exec("SELECT columns, rows FROM chain_snapshots WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL ORDER BY ts DESC LIMIT 1", (ts, ts))
    if not r: return 0.0
    cols, rows = r[0]
    try:
        s_i = cols.index('Strike'); c_oi = cols.index('Open Int'); c_g = cols.index('Gamma')
        p_g = cols.index('Gamma', c_g+1); p_oi = cols.index('Open Int', c_oi+1)
        mg = 0
        for row in rows:
            s = row[s_i]
            if s is None or float(s) <= float(spot): continue
            ng = float(row[c_g] or 0)*float(row[c_oi] or 0) - float(row[p_g] or 0)*float(row[p_oi] or 0)
            if ng > mg: mg = ng
        return mg
    except: return 0.0

def get_v13_dd(ts, spot):
    r = safe_exec("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc=(SELECT mts FROM lts) AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    return float(r[0][0]) if r and r[0][0] else 0.0

def get_vanna(ts, spot):
    pts = safe_exec("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='vanna' AND expiration_option='THIS_WEEK'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc=(SELECT mts FROM lts) ORDER BY strike
    """, (ts, ts))
    if not pts: return None, None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None, None
    s0 = sorted(near); cr = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1-v0 != 0: cr.append(x0 + (-v0/(v1-v0))*(x1-x0))
    cs = None
    if cr:
        nearest = min(cr, key=lambda s: abs(s - float(spot)))
        cs = 'A' if nearest > float(spot) else 'B'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    ps = 'A' if pk > float(spot) else 'B'
    return cs, ps

def v13_block(gex, dd, vc, vp, setup, dirx):
    if dirx in ('short','bearish') and setup in ('Skew Charm','DD Exhaustion'):
        if gex >= 75: return True
        if dd >= 3_000_000_000: return True
    if vc is not None:
        if dirx in ('short','bearish'):
            if setup == 'DD Exhaustion' and vc == 'A': return True
            if setup == 'Skew Charm' and vc == 'A' and vp == 'B': return True
            if setup == 'AG Short' and vc == 'B' and vp == 'A': return True
        if dirx in ('long','bullish'):
            if setup == 'Skew Charm' and vc == 'A' and vp == 'B': return True
    return False

# Filter to TSRT scope: SC long, SC short, AG short
def tsrt_scope(t):
    dirx = t[11]; setup = t[2]
    if setup == 'Skew Charm': return True
    if setup == 'AG Short' and dirx in ('short','bearish'): return True
    return False

tsrt = [t for t in v12 if tsrt_scope(t)]
print(f"TSRT-scope V12-fix: {len(tsrt)} (SC long + SC short + AG short)")

# Apply V13
print("Applying V13 filter...", flush=True)
tsrt_v13 = []
for i, t in enumerate(tsrt):
    gx = get_v13_gex(t[1], t[5])
    dd = get_v13_dd(t[1], t[5])
    vc, vp = get_vanna(t[1], t[5])
    if not v13_block(gx, dd, vc, vp, t[2], t[11]):
        tsrt_v13.append(t)
    if (i+1) % 50 == 0: print(f"  {i+1}/{len(tsrt)}", flush=True)
print(f"TSRT V13 signals (unblocked): {len(tsrt_v13)}")

# ============ Simulate slot contention ============
# Each trade occupies slot for (outcome_elapsed_min) minutes from ts
# LONGS account: only SC longs. One slot.
# SHORTS account: SC shorts + AG shorts. One slot (shared between SC/AG).

def simulate_with_cap(signals, slot_limit=1):
    """Returns (fired, skipped) with ts-based contention."""
    active_slots = []  # list of (slot_end_ts, trade_tid)
    fired = []; skipped = []
    for t in signals:
        tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, elapsed, mfe, mae, h, m, d = t
        # expire old slots
        active_slots = [(e, tid0) for (e, tid0) in active_slots if e > ts]
        if len(active_slots) < slot_limit:
            # fire this trade
            minutes = elapsed if elapsed else 60  # default 60 min if missing
            slot_end = ts + timedelta(minutes=minutes)
            active_slots.append((slot_end, tid))
            fired.append(t)
        else:
            skipped.append(t)
    return fired, skipped

# Split TSRT V13 by direction-account
longs_v13 = [t for t in tsrt_v13 if t[11] in ('long','bullish')]  # SC longs only
shorts_v13 = [t for t in tsrt_v13 if t[11] in ('short','bearish')]  # SC+AG shorts

print(f"\nTSRT V13 breakdown:")
print(f"  Longs: {len(longs_v13)} signals")
print(f"  Shorts: {len(shorts_v13)} signals")

# Simulate both accounts
longs_fired, longs_skipped = simulate_with_cap(longs_v13, slot_limit=1)
shorts_fired, shorts_skipped = simulate_with_cap(shorts_v13, slot_limit=1)

def stats(rs, label):
    pnl = sum(float(r[7] or 0) for r in rs)
    w = sum(1 for r in rs if r[6]=='WIN')
    l = sum(1 for r in rs if r[6]=='LOSS')
    wr = 100*w/max(1,w+l)
    return f"{label}: n={len(rs)}, pnl={pnl:+.1f}, WR={wr:.1f}%, W={w}, L={l}"

print()
print("=" * 70)
print("SLOT SIMULATION (1-per-direction cap)")
print("=" * 70)
print(stats(longs_fired, "Longs FIRED (actual TSRT)"))
print(stats(longs_skipped, "Longs SKIPPED (slot busy)"))
print(stats(shorts_fired, "Shorts FIRED (actual TSRT)"))
print(stats(shorts_skipped, "Shorts SKIPPED (slot busy)"))

print()
# Compare: slot-constrained vs no-cap
print("=" * 70)
print("WHAT THE CAP COST US")
print("=" * 70)

def total(rs): return sum(float(r[7] or 0) for r in rs)
all_fired = longs_fired + shorts_fired
all_skipped = longs_skipped + shorts_skipped

nocap_pnl = sum(float(r[7] or 0) for r in tsrt_v13)
capped_pnl = total(all_fired)
missed = total(all_skipped)
print(f"TSRT V13 no-cap PnL:      {nocap_pnl:+.1f} pts ({len(tsrt_v13)} trades)")
print(f"TSRT V13 capped (1-slot): {capped_pnl:+.1f} pts ({len(all_fired)} trades)")
print(f"Missed by cap:            {missed:+.1f} pts ({len(all_skipped)} trades)")
print(f"Cap cost: {nocap_pnl-capped_pnl:+.1f} pts ({100*(nocap_pnl-capped_pnl)/abs(nocap_pnl):.1f}% of uncapped PnL)")

# ============ Alternative scenarios ============
print()
print("=" * 70)
print("WHAT-IF SCENARIOS")
print("=" * 70)

# Scenario A: 2-per-direction cap
longs_f2, longs_s2 = simulate_with_cap(longs_v13, slot_limit=2)
shorts_f2, shorts_s2 = simulate_with_cap(shorts_v13, slot_limit=2)
pnl_2 = total(longs_f2) + total(shorts_f2)
missed_2 = total(longs_s2) + total(shorts_s2)
print(f"Scenario A: 2-per-direction cap: {pnl_2:+.1f} pts ({len(longs_f2)+len(shorts_f2)}t), missed={missed_2:+.1f}")

# Scenario B: 3-per-direction cap
longs_f3, longs_s3 = simulate_with_cap(longs_v13, slot_limit=3)
shorts_f3, shorts_s3 = simulate_with_cap(shorts_v13, slot_limit=3)
pnl_3 = total(longs_f3) + total(shorts_f3)
missed_3 = total(longs_s3) + total(shorts_s3)
print(f"Scenario B: 3-per-direction cap: {pnl_3:+.1f} pts ({len(longs_f3)+len(shorts_f3)}t), missed={missed_3:+.1f}")

# Scenario C: 5-per-direction (effectively uncapped)
longs_f5, longs_s5 = simulate_with_cap(longs_v13, slot_limit=5)
shorts_f5, shorts_s5 = simulate_with_cap(shorts_v13, slot_limit=5)
pnl_5 = total(longs_f5) + total(shorts_f5)
print(f"Scenario C: 5-per-direction cap: {pnl_5:+.1f} pts ({len(longs_f5)+len(shorts_f5)}t)")

# Scenario D: priority queue — take NEXT signal only if current trade is in drawdown?
# (Complex, skip for now)

# ============ What was missed? ============
print()
print("=" * 70)
print("ANATOMY OF MISSED TRADES (1-cap)")
print("=" * 70)
missed_by_outcome = defaultdict(list)
for t in all_skipped:
    missed_by_outcome[t[6]].append(t)
for outcome, trades in missed_by_outcome.items():
    pnl = sum(float(t[7] or 0) for t in trades)
    print(f"  {outcome}: {len(trades)} trades, pnl={pnl:+.1f}")

# Sample of BEST missed trades
best_missed = sorted(all_skipped, key=lambda x: -float(x[7] or 0))[:10]
print()
print("TOP 10 MISSED WINNERS:")
print(f"{'Date/Time':<15}{'Setup':<15}{'Dir':<10}{'PnL':>8}")
for t in best_missed:
    d_str = t[1].astimezone().strftime('%m-%d %H:%M')
    print(f"{d_str:<15}{t[2][:14]:<15}{t[11]:<10}{float(t[7] or 0):>+8.1f}")

# ============ Daily cap impact ============
print()
print("=" * 70)
print("DAILY CAP IMPACT")
print("=" * 70)
daily_impact = defaultdict(lambda: {'fired': 0, 'skipped': 0, 'fired_pnl': 0, 'skipped_pnl': 0})
for t in all_fired:
    d = str(t[17])
    daily_impact[d]['fired'] += 1
    daily_impact[d]['fired_pnl'] += float(t[7] or 0)
for t in all_skipped:
    d = str(t[17])
    daily_impact[d]['skipped'] += 1
    daily_impact[d]['skipped_pnl'] += float(t[7] or 0)

# Days where cap cost us the most
worst_cap_days = sorted(daily_impact.items(), key=lambda x: x[1]['skipped_pnl'])[:5]
print("\nTop days where cap HURT us (missed positive PnL):")
for d, v in sorted(daily_impact.items(), key=lambda x: -max(0, x[1]['skipped_pnl']))[:5]:
    if v['skipped_pnl'] > 0:
        print(f"  {d}: fired {v['fired']} ({v['fired_pnl']:+.1f}), skipped {v['skipped']} ({v['skipped_pnl']:+.1f})")

print("\nTop days where cap HELPED us (missed negative PnL):")
for d, v in sorted(daily_impact.items(), key=lambda x: x[1]['skipped_pnl'])[:5]:
    if v['skipped_pnl'] < 0:
        print(f"  {d}: fired {v['fired']} ({v['fired_pnl']:+.1f}), skipped {v['skipped']} ({v['skipped_pnl']:+.1f})")

# Save for report
import json
summary = {
    'period': f"{START} to {END}",
    'tsrt_scope_n': len(tsrt),
    'v13_passing_n': len(tsrt_v13),
    'v13_nocap': {'pnl': nocap_pnl, 'n': len(tsrt_v13)},
    'v13_1cap': {'pnl': capped_pnl, 'n': len(all_fired), 'fired': len(all_fired), 'skipped': len(all_skipped), 'missed_pnl': missed},
    'v13_2cap': {'pnl': pnl_2, 'n': len(longs_f2)+len(shorts_f2), 'missed_pnl': missed_2},
    'v13_3cap': {'pnl': pnl_3, 'n': len(longs_f3)+len(shorts_f3), 'missed_pnl': missed_3},
    'v13_5cap': {'pnl': pnl_5, 'n': len(longs_f5)+len(shorts_f5)},
    'longs': {'signals': len(longs_v13), 'fired': len(longs_fired), 'skipped': len(longs_skipped), 'fired_pnl': total(longs_fired), 'skipped_pnl': total(longs_skipped)},
    'shorts': {'signals': len(shorts_v13), 'fired': len(shorts_fired), 'skipped': len(shorts_skipped), 'fired_pnl': total(shorts_fired), 'skipped_pnl': total(shorts_skipped)},
    'missed_by_outcome': {o: {'n': len(ts), 'pnl': sum(float(t[7] or 0) for t in ts)} for o, ts in missed_by_outcome.items()},
    'best_missed': [{'date': t[1].astimezone().strftime('%m-%d %H:%M'), 'setup': t[2], 'dir': t[11], 'pnl': float(t[7] or 0)} for t in best_missed],
    'daily_cap_impact': {d: v for d, v in daily_impact.items()},
}
with open('_tsrt_slot_study.json', 'w') as f:
    json.dump(summary, f, indent=2, default=str)
print(f"\nSaved to _tsrt_slot_study.json")
