"""Compose the strongest combined filter using cliff + peak on BOTH longs and shorts."""
import psycopg2

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# Pull ALL V12-fix trades (both directions) once
def pull_trades():
    cur.execute("""
    SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
           greek_alignment, vix, overvix, direction,
           EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
           EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m
    FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-11' AND '2026-04-16'
      AND outcome_result IS NOT NULL AND spot IS NOT NULL
    ORDER BY ts
    """)
    return cur.fetchall()

def passes_v12(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m = t
    if dirx in ('short', 'bearish'):
        if setup not in ('Skew Charm', 'DD Exhaustion', 'AG Short'): return False
        if setup == 'Skew Charm' and grade not in ('A+','A','B'): return False
        if setup in ('Skew Charm','DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'): return False
        if setup in ('Skew Charm','DD Exhaustion') and ((h==14 and m>=30) or h==15): return False
        if setup == 'DD Exhaustion' and align == 0: return False
        return True
    if dirx in ('long', 'bullish'):
        if align is None or align < 2: return False
        vix_f = float(vix) if vix else None
        ovx_f = float(ovx) if ovx else None
        sc_exempt = setup == 'Skew Charm'
        vix_ok = vix_f is not None and vix_f <= 22
        ovx_ok = ovx_f is not None and ovx_f >= 2
        if not (sc_exempt or vix_ok or ovx_ok): return False
        return True
    return False

raw = pull_trades()
v12 = [t for t in raw if passes_v12(t)]
shorts = [t for t in v12 if t[11] in ('short','bearish')]
longs = [t for t in v12 if t[11] in ('long','bullish')]
print(f"V12-fix total: {len(v12)} (shorts={len(shorts)}, longs={len(longs)})")
print(f"V12 baseline PnL: {sum(float(t[7] or 0) for t in v12):+.1f}")
print(f"  shorts: {sum(float(t[7] or 0) for t in shorts):+.1f}")
print(f"  longs:  {sum(float(t[7] or 0) for t in longs):+.1f}")

def fetch_near(ts, spot, band=50):
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option='THIS_WEEK' AND ts_utc <= %s
        AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc = (SELECT mts FROM latest_ts)
    ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
    if not pts: return None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= band]
    return near if len(near) >= 2 else None

def analyze(near, spot):
    s0 = sorted(near)
    crossings = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0: crossings.append(x0 + (-v0/(v1-v0))*(x1-x0))
    cliff_side = None
    if crossings:
        nearest = min(crossings, key=lambda s: abs(s - float(spot)))
        cliff_side = 'ABOVE' if nearest > float(spot) else 'BELOW'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    peak_side = 'ABOVE' if pk > float(spot) else 'BELOW'
    return cliff_side, peak_side

print("Enriching all trades with cliff+peak...", flush=True)
enriched = []
for i, t in enumerate(v12):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m = t
    near = fetch_near(ts, float(spot))
    if not near:
        enriched.append({'t': t, 'cliff': None, 'peak': None})
    else:
        c, p = analyze(near, float(spot))
        enriched.append({'t': t, 'cliff': c, 'peak': p})
    if (i+1) % 100 == 0: print(f"  ...{i+1}/{len(v12)}", flush=True)

def pnl_of(rs): return sum(float(r['t'][7] or 0) for r in rs)
def count_wl(rs):
    w = sum(1 for r in rs if r['t'][6]=='WIN')
    l = sum(1 for r in rs if r['t'][6]=='LOSS')
    return w, l

def fmt(name, rs):
    if not rs: return f"  {name:<32} (empty)"
    w,l = count_wl(rs)
    p = pnl_of(rs)
    return f"  {name:<32} n={len(rs):>3} WR={100*w/max(1,w+l):>5.1f}% pnl={p:+8.1f} avg={p/len(rs):+.2f}"

# === Explore sub-buckets ===
print()
print("SHORTS by (setup, cliff_side, peak_side):")
for setup in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
    for cs in ['ABOVE', 'BELOW']:
        for ps in ['ABOVE', 'BELOW']:
            rs = [r for r in enriched if r['t'][11] in ('short','bearish') and r['t'][2]==setup
                  and r['cliff']==cs and r['peak']==ps]
            print(fmt(f"{setup[:13]:<13} cliff={cs} peak={ps}", rs))

print()
print("LONGS by (setup, cliff_side, peak_side):")
long_setups = sorted(set(r['t'][2] for r in enriched if r['t'][11] in ('long','bullish')))
for setup in long_setups:
    for cs in ['ABOVE', 'BELOW']:
        for ps in ['ABOVE', 'BELOW']:
            rs = [r for r in enriched if r['t'][11] in ('long','bullish') and r['t'][2]==setup
                  and r['cliff']==cs and r['peak']==ps]
            if rs: print(fmt(f"{setup[:13]:<13} cliff={cs} peak={ps}", rs))

# === Design the strongest filter ===
# Rules to test:
rules = {
    'R1_DD_above_cliff': lambda r: r['t'][2]=='DD Exhaustion' and r['t'][11] in ('short','bearish') and r['cliff']=='ABOVE',
    'R2_DD_peak_below': lambda r: r['t'][2]=='DD Exhaustion' and r['t'][11] in ('short','bearish') and r['peak']=='BELOW',
    'R3_SC_both_below': lambda r: r['t'][2]=='Skew Charm' and r['t'][11] in ('short','bearish') and r['cliff']=='ABOVE' and r['peak']=='BELOW',
    'R4_AG_both_above': lambda r: r['t'][2]=='AG Short' and r['t'][11] in ('short','bearish') and r['cliff']=='ABOVE' and r['peak']=='BELOW',
    'R5_LONG_cliff_above_peak_below': lambda r: r['t'][11] in ('long','bullish') and r['cliff']=='ABOVE' and r['peak']=='BELOW',
}
print()
print("Rule evaluation (blocked trades = what the rule filters out):")
for rname, fn in rules.items():
    blocked = [r for r in enriched if fn(r)]
    if not blocked:
        print(f"  {rname}: no matches")
        continue
    p = pnl_of(blocked); w,l = count_wl(blocked)
    print(f"  {rname}: blocks {len(blocked)}t WR={100*w/max(1,w+l):.1f}% pnl_of_blocked={p:+.1f} → save={-p:+.1f}")

# === Combined "V13" candidate ===
def v13_block(r):
    t = r['t']; dirx = t[11]; setup = t[2]
    if r['cliff'] is None: return False
    # shorts
    if dirx in ('short','bearish'):
        if setup == 'DD Exhaustion' and r['cliff'] == 'ABOVE': return True
        if setup == 'DD Exhaustion' and r['peak'] == 'BELOW': return True
        if setup == 'Skew Charm' and r['cliff']=='ABOVE' and r['peak']=='BELOW': return True
        if setup == 'AG Short' and r['cliff']=='ABOVE' and r['peak']=='BELOW': return True
    if dirx in ('long','bullish'):
        if r['cliff']=='ABOVE' and r['peak']=='BELOW': return True
    return False

v13_blocked = [r for r in enriched if v13_block(r)]
v13_kept = [r for r in enriched if not v13_block(r)]
base_pnl = pnl_of(enriched)
kept_pnl = pnl_of(v13_kept)
print()
print("=" * 70)
print("V13 CANDIDATE FILTER")
print("=" * 70)
print(f"Baseline (V12-fix): {base_pnl:+.1f} pts over {len(enriched)} trades")
print(f"V13 blocks: {len(v13_blocked)} trades with combined pnl={pnl_of(v13_blocked):+.1f}")
print(f"V13 keeps:  {len(v13_kept)} trades")
print(f"V13 PnL:    {kept_pnl:+.1f} pts")
print(f"DELTA:      {kept_pnl - base_pnl:+.1f} pts ({100*(kept_pnl-base_pnl)/abs(base_pnl):+.1f}%)")
print()
print("V13 blocked breakdown:")
for setup in sorted(set(r['t'][2] for r in v13_blocked)):
    for dirx in ['short','bearish','long','bullish']:
        rs = [r for r in v13_blocked if r['t'][2]==setup and r['t'][11]==dirx]
        if rs:
            w,l = count_wl(rs); p = pnl_of(rs)
            print(f"  {setup:<18} {dirx:<8} n={len(rs):>3} WR={100*w/max(1,w+l):>5.1f}% pnl={p:+7.1f}")
