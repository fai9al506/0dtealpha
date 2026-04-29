"""Simple 4-scenario comparison for TSRT Mar 24 - Apr 16."""
import psycopg2
from datetime import timedelta
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
def reconnect():
    c = psycopg2.connect(DB, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
    return c, c.cursor()
conn, cur = reconnect()
def safe(sql, args=None):
    global conn, cur
    for _ in range(3):
        try:
            cur.execute(sql, args); return cur.fetchall() if cur.description else None
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try: conn.close()
            except: pass
            conn, cur = reconnect()
    raise

START, END = '2026-03-24', '2026-04-16'
MES_MULT = 5  # $ per SPX point

# ============ SCENARIO 1: Actual TSRT ============
# From real_trade_orders — use actual SPX fills
act = safe("""
SELECT r.state, s.direction FROM real_trade_orders r
LEFT JOIN setup_log s ON s.id = r.setup_log_id
""")
actual_pnl = 0; actual_n = 0
for state, dirx in act:
    if not state or state.get('status') != 'closed': continue
    fill = state.get('fill_price'); sf = state.get('stop_fill_price'); tp = state.get('target_price')
    reason = state.get('close_reason', '')
    pnl = None
    if fill and reason == 'stop_filled' and sf:
        pnl = (fill - sf) if dirx == 'short' else (sf - fill)
    elif fill and reason == 'target_filled' and tp:
        pnl = (fill - tp) if dirx == 'short' else (tp - fill)
    if pnl is not None:
        actual_pnl += pnl; actual_n += 1

# ============ Get all V12-fix signals same period ============
raw = safe("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction,
       outcome_elapsed_min,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL ORDER BY ts
""", (START, END))

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, el, h, m = t
    if setup in ("VIX Divergence", "IV Momentum", "Vanna Butterfly"): return False
    if setup == 'Skew Charm' and grade in ('C', 'LOG'): return False
    if setup in ('Skew Charm', 'DD Exhaustion'):
        if (h == 14 and m >= 30) or h == 15: return False
    if setup == 'BofA Scalp' and ((h == 14 and m >= 30) or h >= 15): return False
    is_long = dirx in ('long', 'bullish')
    if is_long and paradigm == 'SIDIAL-EXTREME': return False
    if is_long:
        if align is None or align < 2: return False
        if setup == 'Skew Charm': return True
        vf = float(vix) if vix else None
        of = float(ovx) if ovx else -99
        if vf is not None and vf > 22 and of < 2: return False
        return True
    else:
        if setup in ('Skew Charm','DD Exhaustion') and paradigm == 'GEX-LIS': return False
        if setup == 'AG Short' and paradigm == 'AG-TARGET': return False
        if setup in ('Skew Charm', 'AG Short'): return True
        if setup == 'DD Exhaustion' and align != 0: return True
        return False

def tsrt_scope(t):
    if t[2] == 'Skew Charm': return True
    if t[2] == 'AG Short' and t[11] in ('short','bearish'): return True
    return False

v12 = [t for t in raw if passes_v12fix(t) and tsrt_scope(t)]

def get_vanna(ts, spot):
    r = safe("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='vanna' AND expiration_option='THIS_WEEK'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc=(SELECT mts FROM lts) ORDER BY strike
    """, (ts, ts))
    if not r: return None, None
    near = [(float(s), float(v)) for s, v in r if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None, None
    s0 = sorted(near); cr = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1-v0 != 0: cr.append(x0 + (-v0/(v1-v0))*(x1-x0))
    cs = None
    if cr: cs = 'A' if min(cr, key=lambda s: abs(s-float(spot))) > float(spot) else 'B'
    ps = 'A' if max(near, key=lambda x: abs(x[1]))[0] > float(spot) else 'B'
    return cs, ps

def get_gex(ts, spot):
    r = safe("SELECT columns, rows FROM chain_snapshots WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL ORDER BY ts DESC LIMIT 1", (ts, ts))
    if not r: return 0.0
    cols, rows = r[0]
    try:
        si = cols.index('Strike'); coi = cols.index('Open Int'); cg = cols.index('Gamma')
        pg = cols.index('Gamma', cg+1); poi = cols.index('Open Int', coi+1)
        mg = 0
        for row in rows:
            s = row[si]
            if s is None or float(s) <= float(spot): continue
            ng = float(row[cg] or 0)*float(row[coi] or 0) - float(row[pg] or 0)*float(row[poi] or 0)
            if ng > mg: mg = ng
        return mg
    except: return 0.0

def get_dd(ts, spot):
    r = safe("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc=(SELECT mts FROM lts) AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    return float(r[0][0]) if r and r[0][0] else 0.0

def v13block(gx, dd, vc, vp, setup, dirx):
    if dirx in ('short','bearish') and setup in ('Skew Charm','DD Exhaustion'):
        if gx >= 75 or dd >= 3_000_000_000: return True
    if vc:
        if dirx in ('short','bearish'):
            if setup == 'DD Exhaustion' and vc == 'A': return True
            if setup == 'Skew Charm' and vc == 'A' and vp == 'B': return True
            if setup == 'AG Short' and vc == 'B' and vp == 'A': return True
        if dirx in ('long','bullish'):
            if setup == 'Skew Charm' and vc == 'A' and vp == 'B': return True
    return False

# Compute v13 flag on each V12-fix signal
print(f"Computing V13 flags on {len(v12)} V12-fix signals...")
enr = []
for i, t in enumerate(v12):
    gx = get_gex(t[1], t[5]); dd = get_dd(t[1], t[5]); vc, vp = get_vanna(t[1], t[5])
    enr.append({'t': t, 'v13blocked': v13block(gx, dd, vc, vp, t[2], t[11])})
    if (i+1) % 40 == 0: print(f"  {i+1}/{len(v12)}")

def simulate(signals, long_cap=1, short_cap=1):
    """1 slot per direction sim. Returns total PnL of fired trades."""
    long_active = []; short_active = []
    fired_pnl = 0; fired_n = 0; skipped_pnl = 0; skipped_n = 0
    for r in signals:
        t = r['t']
        dirx = t[11]
        if dirx in ('long','bullish'):
            long_active = [(e, i) for (e, i) in long_active if e > t[1]]
            if len(long_active) < long_cap:
                long_active.append((t[1] + timedelta(minutes=t[12] or 60), t[0]))
                fired_pnl += float(t[7] or 0); fired_n += 1
            else:
                skipped_pnl += float(t[7] or 0); skipped_n += 1
        else:
            short_active = [(e, i) for (e, i) in short_active if e > t[1]]
            if len(short_active) < short_cap:
                short_active.append((t[1] + timedelta(minutes=t[12] or 60), t[0]))
                fired_pnl += float(t[7] or 0); fired_n += 1
            else:
                skipped_pnl += float(t[7] or 0); skipped_n += 1
    return {'fired_pnl': fired_pnl, 'fired_n': fired_n, 'skipped_pnl': skipped_pnl, 'skipped_n': skipped_n}

# ============ Build 4 scenarios ============
# Scenario 1: Actual (pulled above)
# Scenario 2: V12-fix no bugs, 1-slot (same filter that was running, but no execution bugs)
sc2 = simulate(enr, 1, 1)  # v12-fix, 1-slot

# Scenario 3: V12-fix no bugs, 2-slot shorts (V12-fix + raise short cap)
sc3 = simulate(enr, 1, 2)

# Scenario 4: V13 no bugs, 1-slot (filter upgrade without slot change)
v13_signals = [r for r in enr if not r['v13blocked']]
sc4 = simulate(v13_signals, 1, 1)

# Scenario 5 bonus: V13 + 2-slot shorts (what we'd get if BOTH changes)
sc5 = simulate(v13_signals, 1, 2)

print()
print("="*75)
print(f"TSRT-LIVE COMPARISON ({START} to {END}, 17 trading days)")
print("="*75)
print(f"{'Scenario':<55}{'Trades':>8}{'SPX pts':>10}{'$ 1 MES':>10}")
print("-"*75)
print(f"{'1. Actual TSRT (bugs + V12-fix + 1-slot)':<55}{actual_n:>8}{actual_pnl:>+10.1f}{actual_pnl*MES_MULT:>+10.0f}")
print(f"{'2. If no bugs (V12-fix + 1-slot, clean execution)':<55}{sc2['fired_n']:>8}{sc2['fired_pnl']:>+10.1f}{sc2['fired_pnl']*MES_MULT:>+10.0f}")
print(f"{'3. If 2-slot shorts (V12-fix + 2-slot shorts, clean)':<55}{sc3['fired_n']:>8}{sc3['fired_pnl']:>+10.1f}{sc3['fired_pnl']*MES_MULT:>+10.0f}")
print(f"{'4. If V13 from day one (V13 + 1-slot, clean)':<55}{sc4['fired_n']:>8}{sc4['fired_pnl']:>+10.1f}{sc4['fired_pnl']*MES_MULT:>+10.0f}")
print("-"*75)
print(f"{'5. BONUS: V13 + 2-slot shorts (the target future state)':<55}{sc5['fired_n']:>8}{sc5['fired_pnl']:>+10.1f}{sc5['fired_pnl']*MES_MULT:>+10.0f}")
print()

# Deltas
print("Step-by-step improvement gained from each change:")
print(f"  Actual → remove bugs:              {(sc2['fired_pnl']-actual_pnl)*MES_MULT:+.0f} USD")
print(f"  V12-fix 1-slot → V13 filter:       {(sc4['fired_pnl']-sc2['fired_pnl'])*MES_MULT:+.0f} USD")
print(f"  V13 1-slot → 2-slot shorts:        {(sc5['fired_pnl']-sc4['fired_pnl'])*MES_MULT:+.0f} USD")
print(f"  TOTAL: actual → V13 + 2-slot:      {(sc5['fired_pnl']-actual_pnl)*MES_MULT:+.0f} USD")

# Monthly extrapolation (17 days × 22/17 ≈ 1.3x)
monthly = 22/17
print()
print("Monthly extrapolation (22 trading days):")
print(f"  Scenario 1 (actual):       ${actual_pnl*MES_MULT*monthly:+.0f}/mo")
print(f"  Scenario 2 (no bugs):      ${sc2['fired_pnl']*MES_MULT*monthly:+.0f}/mo")
print(f"  Scenario 3 (+2-slot):      ${sc3['fired_pnl']*MES_MULT*monthly:+.0f}/mo")
print(f"  Scenario 4 (+V13):         ${sc4['fired_pnl']*MES_MULT*monthly:+.0f}/mo")
print(f"  Scenario 5 (V13 + 2-slot): ${sc5['fired_pnl']*MES_MULT*monthly:+.0f}/mo")

# Save
import json
out = {
    'period': {'start': START, 'end': END, 'trading_days': 17},
    'scenarios': {
        '1_actual': {'n': actual_n, 'spx_pts': actual_pnl, 'usd_1mes': actual_pnl*MES_MULT, 'monthly': actual_pnl*MES_MULT*monthly, 'label': 'Actual TSRT (bugs + V12-fix + 1-slot)'},
        '2_no_bugs': {'n': sc2['fired_n'], 'spx_pts': sc2['fired_pnl'], 'usd_1mes': sc2['fired_pnl']*MES_MULT, 'monthly': sc2['fired_pnl']*MES_MULT*monthly, 'label': 'If no bugs (V12-fix + 1-slot clean)'},
        '3_2slot':   {'n': sc3['fired_n'], 'spx_pts': sc3['fired_pnl'], 'usd_1mes': sc3['fired_pnl']*MES_MULT, 'monthly': sc3['fired_pnl']*MES_MULT*monthly, 'label': 'If 2-slot shorts (V12-fix + 2-slot clean)'},
        '4_v13':     {'n': sc4['fired_n'], 'spx_pts': sc4['fired_pnl'], 'usd_1mes': sc4['fired_pnl']*MES_MULT, 'monthly': sc4['fired_pnl']*MES_MULT*monthly, 'label': 'If V13 from day one (V13 + 1-slot clean)'},
        '5_combined': {'n': sc5['fired_n'], 'spx_pts': sc5['fired_pnl'], 'usd_1mes': sc5['fired_pnl']*MES_MULT, 'monthly': sc5['fired_pnl']*MES_MULT*monthly, 'label': 'V13 + 2-slot shorts (target future)'},
    },
    'deltas': {
        'bugs_cost': (sc2['fired_pnl']-actual_pnl)*MES_MULT,
        'v13_gain': (sc4['fired_pnl']-sc2['fired_pnl'])*MES_MULT,
        '2slot_gain': (sc5['fired_pnl']-sc4['fired_pnl'])*MES_MULT,
        'total_improvement': (sc5['fired_pnl']-actual_pnl)*MES_MULT,
    }
}
with open('_tsrt_scenarios.json', 'w') as f:
    json.dump(out, f, indent=2)
print("\nSaved to _tsrt_scenarios.json")
