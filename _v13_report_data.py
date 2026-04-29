"""Gather all data needed for V13 explainer report to Tel Res."""
import psycopg2, json
from collections import defaultdict

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
       (ts AT TIME ZONE 'America/New_York')::date as d,
       to_char(ts AT TIME ZONE 'America/New_York', 'MM-DD HH24:MI') as et_str
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""", (START, END))
raw = cur.fetchall()

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m, d, et = t
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

def get_v13_gex(ts, spot):
    cur.execute("""
    SELECT columns, rows FROM chain_snapshots
    WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL
    ORDER BY ts DESC LIMIT 1
    """, (ts, ts))
    r = cur.fetchone()
    if not r: return 0.0
    cols, rows = r
    try:
        s_i = cols.index('Strike')
        c_oi = cols.index('Open Int')
        c_g = cols.index('Gamma')
        p_g = cols.index('Gamma', c_g + 1)
        p_oi = cols.index('Open Int', c_oi + 1)
        mg = 0
        for row in rows:
            s = row[s_i]
            if s is None or float(s) <= float(spot): continue
            ng = float(row[c_g] or 0)*float(row[c_oi] or 0) - float(row[p_g] or 0)*float(row[p_oi] or 0)
            if ng > mg: mg = ng
        return mg
    except Exception:
        return 0.0

def get_v13_dd(ts, spot):
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
        AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc = (SELECT mts FROM latest_ts)
      AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    r = cur.fetchone()
    return float(r[0]) if r and r[0] else 0.0

def get_vanna(ts, spot):
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option='THIS_WEEK'
        AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc = (SELECT mts FROM latest_ts)
    ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
    if not pts: return None, None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None, None
    s0 = sorted(near)
    cr = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0: cr.append(x0 + (-v0/(v1-v0))*(x1-x0))
    cs = None
    if cr:
        nearest = min(cr, key=lambda s: abs(s - float(spot)))
        cs = 'A' if nearest > float(spot) else 'B'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    ps = 'A' if pk > float(spot) else 'B'
    return cs, ps

print("Enriching...", flush=True)
enr = []
for i, t in enumerate(v12):
    gx = get_v13_gex(t[1], t[5])
    dd = get_v13_dd(t[1], t[5])
    vc, vp = get_vanna(t[1], t[5])
    enr.append({'t': t, 'gex': gx, 'dd': dd, 'vc': vc, 'vp': vp})
    if (i+1) % 50 == 0: print(f"  {i+1}/{len(v12)}", flush=True)

def v13_gex_dd_block(r):
    t = r['t']; setup = t[2]; dirx = t[11]
    if dirx in ('long','bullish'): return False
    if setup not in ('Skew Charm','DD Exhaustion'): return False
    if r['gex'] >= 75: return True
    if r['dd'] >= 3_000_000_000: return True
    return False

def v13_vanna_block(r):
    t = r['t']; setup = t[2]; dirx = t[11]; c = r['vc']; p = r['vp']
    if c is None: return False
    if dirx in ('short','bearish'):
        if setup == 'DD Exhaustion' and c == 'A': return True
        if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
        if setup == 'AG Short' and c == 'B' and p == 'A': return True
    if dirx in ('long','bullish'):
        if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
    return False

def v13_any_block(r): return v13_gex_dd_block(r) or v13_vanna_block(r)

def pnl(rs): return sum(float(r['t'][7] or 0) for r in rs)
def wl(rs):
    w = sum(1 for r in rs if r['t'][6]=='WIN')
    l = sum(1 for r in rs if r['t'][6]=='LOSS')
    return w, l

# ============ BUILD REPORT DATA ============
data = {
    'period': f"{START} to {END}",
    'baseline_trades': len(enr),
    'baseline_pnl': pnl(enr),
}

# V12-fix by setup
v12_by_setup = defaultdict(list)
for r in enr:
    v12_by_setup[r['t'][2]].append(r)
data['v12_by_setup'] = {
    k: {'n': len(v), 'pnl': pnl(v), 'wr': (wl(v)[0] / max(1, sum(wl(v)))) * 100}
    for k, v in v12_by_setup.items()
}

# Part A: GEX/DD only
gex_blocked = [r for r in enr if v13_gex_dd_block(r)]
gex_kept = [r for r in enr if not v13_gex_dd_block(r)]
data['gex_dd'] = {
    'blocks': len(gex_blocked),
    'blocked_pnl': pnl(gex_blocked),
    'kept_pnl': pnl(gex_kept),
    'saved': pnl(gex_kept) - pnl(enr),
    'blocked_wl': wl(gex_blocked),
}

# Part B: Vanna only
van_blocked = [r for r in enr if v13_vanna_block(r)]
van_kept = [r for r in enr if not v13_vanna_block(r)]
data['vanna'] = {
    'blocks': len(van_blocked),
    'blocked_pnl': pnl(van_blocked),
    'kept_pnl': pnl(van_kept),
    'saved': pnl(van_kept) - pnl(enr),
    'blocked_wl': wl(van_blocked),
}

# Combined
combo_blocked = [r for r in enr if v13_any_block(r)]
combo_kept = [r for r in enr if not v13_any_block(r)]
data['combined'] = {
    'blocks': len(combo_blocked),
    'blocked_pnl': pnl(combo_blocked),
    'kept_pnl': pnl(combo_kept),
    'saved': pnl(combo_kept) - pnl(enr),
    'blocked_wl': wl(combo_blocked),
}

# Overlap
overlap = [r for r in enr if v13_gex_dd_block(r) and v13_vanna_block(r)]
gex_only = [r for r in enr if v13_gex_dd_block(r) and not v13_vanna_block(r)]
van_only = [r for r in enr if v13_vanna_block(r) and not v13_gex_dd_block(r)]
data['overlap'] = {
    'both': {'n': len(overlap), 'pnl': pnl(overlap)},
    'gex_only': {'n': len(gex_only), 'pnl': pnl(gex_only)},
    'van_only': {'n': len(van_only), 'pnl': pnl(van_only)},
}

# Per-rule savings
data['rules'] = {}
per_rule = [
    ('GEX magnet above >=75',    lambda r: r['t'][2] in ('Skew Charm','DD Exhaustion') and r['t'][11] in ('short','bearish') and r['gex'] >= 75),
    ('DD magnet near >=3B',      lambda r: r['t'][2] in ('Skew Charm','DD Exhaustion') and r['t'][11] in ('short','bearish') and r['dd'] >= 3_000_000_000 and not (r['gex'] >= 75)),
    ('DD short cliff=ABOVE',     lambda r: r['t'][2]=='DD Exhaustion' and r['t'][11] in ('short','bearish') and r['vc']=='A' and not (r['gex']>=75 or r['dd']>=3_000_000_000)),
    ('SC short cliff=A+peak=B',  lambda r: r['t'][2]=='Skew Charm' and r['t'][11] in ('short','bearish') and r['vc']=='A' and r['vp']=='B' and not (r['gex']>=75 or r['dd']>=3_000_000_000)),
    ('AG short cliff=B+peak=A',  lambda r: r['t'][2]=='AG Short' and r['t'][11] in ('short','bearish') and r['vc']=='B' and r['vp']=='A'),
    ('SC long cliff=A+peak=B',   lambda r: r['t'][2]=='Skew Charm' and r['t'][11] in ('long','bullish') and r['vc']=='A' and r['vp']=='B'),
]
for name, fn in per_rule:
    b = [r for r in enr if fn(r)]
    w_, l_ = wl(b)
    data['rules'][name] = {'n': len(b), 'wr': 100*w_/max(1,w_+l_), 'pnl': pnl(b)}

# Example trades for each rule (best loser blocked + best winner missed)
def sample_trades(rule_fn, n=3):
    b = [r for r in enr if rule_fn(r)]
    worst = sorted(b, key=lambda r: float(r['t'][7] or 0))[:n]
    best = sorted(b, key=lambda r: -float(r['t'][7] or 0))[:n]
    def fmt_trade(r):
        t = r['t']
        return {
            'date': t[15],  # MM-DD HH:MM ET
            'setup': t[2],
            'direction': t[11],
            'grade': t[3],
            'paradigm': t[4],
            'spot': float(t[5]),
            'outcome': t[6],
            'pnl': float(t[7] or 0),
            'gex': r['gex'],
            'dd_b': r['dd'] / 1e9,
            'vc': r['vc'], 'vp': r['vp'],
        }
    return [fmt_trade(r) for r in worst], [fmt_trade(r) for r in best]

data['examples'] = {}
for name, fn in per_rule:
    worst, best = sample_trades(fn, 2)
    data['examples'][name] = {'worst_losers_blocked': worst, 'best_winners_blocked': best}

# ============ TSRT-specific (SC long + SC short + AG short) ============
tsrt_setups = lambda r: (r['t'][2] == 'Skew Charm') or (r['t'][2] == 'AG Short' and r['t'][11] in ('short','bearish'))
tsrt = [r for r in enr if tsrt_setups(r)]
tsrt_blocked_all = [r for r in tsrt if v13_any_block(r)]
tsrt_kept = [r for r in tsrt if not v13_any_block(r)]
tsrt_blocked_gex = [r for r in tsrt if v13_gex_dd_block(r)]
tsrt_blocked_van = [r for r in tsrt if v13_vanna_block(r)]

data['tsrt'] = {
    'setups_in_scope': 'SC long + SC short + AG short',
    'baseline_n': len(tsrt),
    'baseline_pnl': pnl(tsrt),
    'baseline_wl': wl(tsrt),
    'v13_combined_n_blocked': len(tsrt_blocked_all),
    'v13_combined_kept_n': len(tsrt_kept),
    'v13_combined_kept_pnl': pnl(tsrt_kept),
    'v13_saved': pnl(tsrt_kept) - pnl(tsrt),
    'gex_dd_blocks_count': len(tsrt_blocked_gex),
    'gex_dd_blocks_pnl': pnl(tsrt_blocked_gex),
    'van_blocks_count': len(tsrt_blocked_van),
    'van_blocks_pnl': pnl(tsrt_blocked_van),
}

# Month-by-month for TSRT
data['tsrt_monthly'] = {}
by_m = defaultdict(list)
for r in tsrt:
    k = f"{r['t'][14].year}-{r['t'][14].month:02d}"
    by_m[k].append(r)
for k in sorted(by_m.keys()):
    rs = by_m[k]
    kept = [r for r in rs if not v13_any_block(r)]
    data['tsrt_monthly'][k] = {
        'n': len(rs), 'v12_pnl': pnl(rs),
        'v13_kept_n': len(kept), 'v13_pnl': pnl(kept),
        'saved': pnl(kept) - pnl(rs),
    }

# Daily for charts
data['daily'] = {}
by_d = defaultdict(list)
for r in enr:
    by_d[str(r['t'][14])].append(r)
for d in sorted(by_d.keys()):
    rs = by_d[d]
    kept_all = [r for r in rs if not v13_any_block(r)]
    data['daily'][d] = {'n': len(rs), 'v12': pnl(rs), 'v13': pnl(kept_all)}

# Save
with open('_v13_report_data.json', 'w') as f:
    def conv(o):
        if hasattr(o, 'item'): return o.item()
        if hasattr(o, 'isoformat'): return o.isoformat()
        return str(o)
    json.dump(data, f, indent=2, default=conv)
print("Written to _v13_report_data.json")
print()
print(f"V12-fix baseline: {data['baseline_pnl']:+.1f} pts / {data['baseline_trades']} trades")
print(f"V13 combined:     {data['combined']['kept_pnl']:+.1f} pts (saved {data['combined']['saved']:+.1f})")
print(f"  GEX/DD alone:   saved {data['gex_dd']['saved']:+.1f}")
print(f"  Vanna alone:    saved {data['vanna']['saved']:+.1f}")
print(f"  Overlap:        {data['overlap']['both']['n']} trades / {data['overlap']['both']['pnl']:.1f} pnl")
print()
print(f"TSRT scope (SC long + SC short + AG short):")
print(f"  Baseline: {data['tsrt']['baseline_n']}t, {data['tsrt']['baseline_pnl']:+.1f} pts")
print(f"  V13 combined: saves {data['tsrt']['v13_saved']:+.1f} pts")
