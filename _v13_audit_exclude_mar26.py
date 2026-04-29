"""Recompute V12 vs V13 EXCLUDING Mar 26 (known TS outage) — verify claim holds."""
import psycopg2
DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

START = '2026-03-01'
END = '2026-04-17'

cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""", (START, END))
raw = cur.fetchall()

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m, d = t
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

def get_v13_gex(ts, spot):
    cur.execute("SELECT columns, rows FROM chain_snapshots WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL ORDER BY ts DESC LIMIT 1", (ts, ts))
    r = cur.fetchone()
    if not r: return 0.0
    cols, rows = r
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
    cur.execute("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc=(SELECT mts FROM lts) AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    r = cur.fetchone()
    return float(r[0]) if r and r[0] else 0.0

def get_vanna(ts, spot):
    cur.execute("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='vanna' AND expiration_option='THIS_WEEK'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc=(SELECT mts FROM lts) ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
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

print("Computing with and without Mar 26...", flush=True)
v12 = [t for t in raw if passes_v12fix(t)]

# Apply V13 filter to each trade
v13_blocked_ids = set()
all_results = []
for i, t in enumerate(v12):
    gx = get_v13_gex(t[1], t[5])
    dd = get_v13_dd(t[1], t[5])
    vc, vp = get_vanna(t[1], t[5])
    blocked = v13_block(gx, dd, vc, vp, t[2], t[11])
    all_results.append({'t': t, 'blocked': blocked})
    if (i+1) % 100 == 0: print(f"  {i+1}/{len(v12)}", flush=True)

def stats(rs, label):
    pnl = sum(float(r['t'][7] or 0) for r in rs)
    w = sum(1 for r in rs if r['t'][6]=='WIN')
    l = sum(1 for r in rs if r['t'][6]=='LOSS')
    wr = 100*w/max(1,w+l)
    # MaxDD
    cum = 0; peak = 0; mdd = 0
    for r in sorted(rs, key=lambda x: x['t'][1]):
        cum += float(r['t'][7] or 0)
        if cum > peak: peak = cum
        if peak - cum > mdd: mdd = peak - cum
    # PF
    gp = sum(float(r['t'][7] or 0) for r in rs if float(r['t'][7] or 0) > 0)
    gl = abs(sum(float(r['t'][7] or 0) for r in rs if float(r['t'][7] or 0) < 0))
    pf = gp/gl if gl > 0 else 0
    return {'n': len(rs), 'pnl': pnl, 'wr': wr, 'maxdd': mdd, 'pf': pf, 'w': w, 'l': l}

# WITH Mar 26 (original)
v12_all = all_results
v13_all = [r for r in all_results if not r['blocked']]
print("\n=== INCLUDING Mar 26 (original baseline) ===")
s12 = stats(v12_all, 'V12-fix')
s13 = stats(v13_all, 'V13')
print(f"V12-fix: {s12['n']}t, pnl={s12['pnl']:+.1f}, WR={s12['wr']:.1f}%, MaxDD=-{s12['maxdd']:.1f}, PF={s12['pf']:.2f}")
print(f"V13:     {s13['n']}t, pnl={s13['pnl']:+.1f}, WR={s13['wr']:.1f}%, MaxDD=-{s13['maxdd']:.1f}, PF={s13['pf']:.2f}")
print(f"Δ PnL: {s13['pnl']-s12['pnl']:+.1f}, Δ MaxDD: {s13['maxdd']-s12['maxdd']:+.1f}")

# EXCLUDING Mar 26
v12_clean = [r for r in v12_all if str(r['t'][14]) != '2026-03-26']
v13_clean = [r for r in v13_all if str(r['t'][14]) != '2026-03-26']
print("\n=== EXCLUDING Mar 26 (known TS outage day) ===")
s12c = stats(v12_clean, 'V12-fix')
s13c = stats(v13_clean, 'V13')
print(f"V12-fix: {s12c['n']}t, pnl={s12c['pnl']:+.1f}, WR={s12c['wr']:.1f}%, MaxDD=-{s12c['maxdd']:.1f}, PF={s12c['pf']:.2f}")
print(f"V13:     {s13c['n']}t, pnl={s13c['pnl']:+.1f}, WR={s13c['wr']:.1f}%, MaxDD=-{s13c['maxdd']:.1f}, PF={s13c['pf']:.2f}")
print(f"Δ PnL: {s13c['pnl']-s12c['pnl']:+.1f}, Δ MaxDD: {s13c['maxdd']-s12c['maxdd']:+.1f}")

# How many Mar 26 trades are in both buckets?
mar26_v12 = [r for r in v12_all if str(r['t'][14]) == '2026-03-26']
mar26_v13 = [r for r in v13_all if str(r['t'][14]) == '2026-03-26']
print(f"\nMar 26 V12-fix trades: {len(mar26_v12)}, PnL: {sum(float(r['t'][7] or 0) for r in mar26_v12):+.1f}")
print(f"Mar 26 V13 trades (kept): {len(mar26_v13)}, PnL: {sum(float(r['t'][7] or 0) for r in mar26_v13):+.1f}")
print(f"Mar 26 trades V13 blocked: {len(mar26_v12) - len(mar26_v13)}")

# Also investigate contamination trades in V12-fix whitelist
print("\n=== Contamination check: trades with MFE>50 or MAE<-30 in V12-fix subset ===")
cur.execute("""
SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI'),
       setup_name, direction, grade, outcome_result, outcome_pnl,
       outcome_max_profit, outcome_max_loss
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND (outcome_max_profit > 50 OR outcome_max_loss < -30)
  AND (
    (direction IN ('short','bearish') AND setup_name IN ('Skew Charm','DD Exhaustion','AG Short'))
    OR (direction IN ('long','bullish') AND setup_name = 'Skew Charm')
  )
  AND outcome_result IS NOT NULL
  AND NOT (setup_name = 'Skew Charm' AND grade IN ('C','LOG'))
ORDER BY outcome_pnl DESC LIMIT 10
""", (START, END))
print(f"{'Date':<12}{'Setup':<15}{'Dir':<10}{'Out':<8}{'PnL':>8}{'MFE':>8}{'MAE':>8}")
for r in cur.fetchall():
    print(f"{r[1]:<12}{r[2][:14]:<15}{r[3]:<10}{r[5]:<8}{float(r[6]):>+8.1f}{float(r[7] or 0):>+8.1f}{float(r[8] or 0):>+8.1f}")
EOF