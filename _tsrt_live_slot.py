"""TSRT-LIVE PERIOD ONLY slot cap analysis."""
import psycopg2
from datetime import timedelta
from collections import defaultdict
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'

def reconnect():
    c = psycopg2.connect(DB, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
    return c, c.cursor()
conn, cur = reconnect()
def safe(sql, args=None):
    global conn, cur
    for _ in range(3):
        try:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else None
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try: conn.close()
            except: pass
            conn, cur = reconnect()
    raise

START = '2026-03-24'
END = '2026-04-16'

raw = safe("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction,
       outcome_elapsed_min,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""", (START, END))

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, el, h, m = t
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

def tsrt_scope(t):
    if t[2] == 'Skew Charm': return True
    if t[2] == 'AG Short' and t[11] in ('short','bearish'): return True
    return False

v12 = [t for t in raw if passes_v12fix(t)]
tsrt_v12 = [t for t in v12 if tsrt_scope(t)]
print(f"TSRT-scope V12-fix (TSRT-live: Mar 24 - Apr 16): {len(tsrt_v12)}")

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
    if cr:
        nearest = min(cr, key=lambda s: abs(s - float(spot)))
        cs = 'A' if nearest > float(spot) else 'B'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    ps = 'A' if pk > float(spot) else 'B'
    return cs, ps

def get_v13_gex(ts, spot):
    r = safe("SELECT columns, rows FROM chain_snapshots WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL ORDER BY ts DESC LIMIT 1", (ts, ts))
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
    r = safe("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc=(SELECT mts FROM lts) AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    return float(r[0][0]) if r and r[0][0] else 0.0

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

v13 = []
for i, t in enumerate(tsrt_v12):
    gx = get_v13_gex(t[1], t[5])
    dd = get_v13_dd(t[1], t[5])
    vc, vp = get_vanna(t[1], t[5])
    if not v13_block(gx, dd, vc, vp, t[2], t[11]):
        v13.append(t)
    if (i+1) % 40 == 0: print(f"  {i+1}/{len(tsrt_v12)}")

print(f"V13-passing: {len(v13)}")

def simulate(signals, slot=1):
    active = []; fired = []; skipped = []
    for t in signals:
        active = [(e, i) for (e, i) in active if e > t[1]]
        if len(active) < slot:
            minutes = t[12] if t[12] else 60
            active.append((t[1] + timedelta(minutes=minutes), t[0]))
            fired.append(t)
        else:
            skipped.append(t)
    return fired, skipped

longs = [t for t in v13 if t[11] in ('long','bullish')]
shorts = [t for t in v13 if t[11] in ('short','bearish')]

lf1, ls1 = simulate(longs, 1); sf1, ss1 = simulate(shorts, 1)
lf2, ls2 = simulate(longs, 2); sf2, ss2 = simulate(shorts, 2)

def s(rs):
    pnl = sum(float(t[7] or 0) for t in rs)
    w = sum(1 for t in rs if t[6]=='WIN')
    l = sum(1 for t in rs if t[6]=='LOSS')
    return f"n={len(rs)}, pnl={pnl:+.1f}, WR={100*w/max(1,w+l):.1f}%"

print()
print("="*70)
print(f"TSRT-LIVE PERIOD ONLY ({START} to {END})")
print("="*70)
print(f"V13 SHORTS baseline: {s(shorts)}")
print(f"  1-cap fired:   {s(sf1)}")
print(f"  1-cap SKIPPED: {s(ss1)}")
print(f"  2-cap fired:   {s(sf2)}")
print(f"  2-cap SKIPPED: {s(ss2)}")
print()
print(f"V13 LONGS baseline: {s(longs)}")
print(f"  1-cap fired:   {s(lf1)}")
print(f"  1-cap SKIPPED: {s(ls1)}")

ss_pnl = sum(float(t[7] or 0) for t in ss1)
recovered_s = sum(float(t[7] or 0) for t in sf2) - sum(float(t[7] or 0) for t in sf1)
print()
print(f">>> TSRT-live 1-cap missed SHORTS: {len(ss1)} trades worth {ss_pnl:+.1f} pts ({ss_pnl*5:+.0f} USD at 1 MES)")
print(f">>> Switching to 2-slot shorts recovers {recovered_s:+.1f} pts ({recovered_s*5:+.0f} USD)")
