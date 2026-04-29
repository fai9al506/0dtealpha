"""V13 designed from the actual sub-bucket data."""
import psycopg2

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

def pull_all():
    cur.execute("""
    SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
           greek_alignment, vix, overvix, direction,
           EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
           EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
           (ts AT TIME ZONE 'America/New_York')::date as d
    FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-11' AND '2026-04-16'
      AND outcome_result IS NOT NULL AND spot IS NOT NULL
    ORDER BY ts
    """)
    return cur.fetchall()

def passes_v12(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m, d = t
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

raw = pull_all()
v12 = [t for t in raw if passes_v12(t)]

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
    cs = None
    if crossings:
        nearest = min(crossings, key=lambda s: abs(s - float(spot)))
        cs = 'A' if nearest > float(spot) else 'B'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    ps = 'A' if pk > float(spot) else 'B'
    return cs, ps

print("Enriching V12-fix trades...", flush=True)
enr = []
for i, t in enumerate(v12):
    near = fetch_near(t[1], float(t[5]))
    if not near:
        enr.append({'t': t, 'c': None, 'p': None})
    else:
        c, p = analyze(near, float(t[5]))
        enr.append({'t': t, 'c': c, 'p': p})
    if (i+1) % 100 == 0: print(f"  ...{i+1}/{len(v12)}", flush=True)

def pnl_of(rs): return sum(float(r['t'][7] or 0) for r in rs)
def wl(rs):
    w = sum(1 for r in rs if r['t'][6]=='WIN')
    l = sum(1 for r in rs if r['t'][6]=='LOSS')
    return w, l

# === V13 rules — derived from sub-bucket data ===
def v13_block(r):
    t = r['t']; setup = t[2]; dirx = t[11]; c = r['c']; p = r['p']
    if c is None: return False
    # SHORTS
    if dirx in ('short','bearish'):
        if setup == 'DD Exhaustion':
            # both ABOVE cliff sub-buckets lose
            if c == 'A': return True  # covers both DD-A-A and DD-A-B
        if setup == 'Skew Charm':
            # only SC-above-below is bad
            if c == 'A' and p == 'B': return True
        if setup == 'AG Short':
            # only AG-below-above is mildly bad (keep conservative)
            if c == 'B' and p == 'A': return True
    # LONGS
    if dirx in ('long','bullish'):
        # SC-long cliff=above, peak=below (the only meaningfully bad bucket)
        if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
        # Small-sample losers — skip to avoid overfitting
    return False

baseline = pnl_of(enr)
blocked = [r for r in enr if v13_block(r)]
kept = [r for r in enr if not v13_block(r)]
kept_pnl = pnl_of(kept)

print()
print("=" * 70)
print("V13 FILTER (derived from sub-bucket data)")
print("=" * 70)
print(f"V12-fix baseline: {baseline:+.1f} pts on {len(enr)} trades")
print(f"V13 blocks: {len(blocked)} trades pnl={pnl_of(blocked):+.1f}")
print(f"V13 keeps:  {len(kept)} trades pnl={kept_pnl:+.1f}")
print(f"Improvement: {kept_pnl-baseline:+.1f} pts ({100*(kept_pnl-baseline)/abs(baseline):+.1f}%)")
print()
print("V13 rules (what gets blocked):")
print("  SHORTS:")
print("    - DD Exhaustion when cliff is ABOVE spot")
print("    - Skew Charm when cliff=ABOVE AND peak=BELOW")
print("    - AG Short when cliff=BELOW AND peak=ABOVE")
print("  LONGS:")
print("    - Skew Charm when cliff=ABOVE AND peak=BELOW")

# Breakdown
print()
print("Per-rule savings:")
rules = {
    'DD short cliff=ABOVE':     lambda r: r['t'][2]=='DD Exhaustion' and r['t'][11] in ('short','bearish') and r['c']=='A',
    'SC short A+B':             lambda r: r['t'][2]=='Skew Charm' and r['t'][11] in ('short','bearish') and r['c']=='A' and r['p']=='B',
    'AG short B+A':             lambda r: r['t'][2]=='AG Short' and r['t'][11] in ('short','bearish') and r['c']=='B' and r['p']=='A',
    'SC long A+B':              lambda r: r['t'][2]=='Skew Charm' and r['t'][11] in ('long','bullish') and r['c']=='A' and r['p']=='B',
}
for rname, fn in rules.items():
    b = [r for r in enr if fn(r)]
    if not b:
        print(f"  {rname}: no matches")
        continue
    w_,l_ = wl(b)
    print(f"  {rname}: {len(b)}t WR={100*w_/max(1,w_+l_):.1f}% pnl={pnl_of(b):+.1f} → saves {-pnl_of(b):+.1f}")

# Monthly stability check
print()
print("Monthly V13 vs V12 stability:")
from collections import defaultdict
by_month = defaultdict(list)
for r in enr:
    k = (r['t'][14].year, r['t'][14].month)
    by_month[k].append(r)
print(f"{'Month':<10}{'Trades':>8}{'V12':>8}{'V13kept':>8}{'Blocked':>9}{'BlockPnL':>10}{'Delta':>8}")
for k in sorted(by_month.keys()):
    rs = by_month[k]
    v12_pnl = pnl_of(rs)
    blk = [r for r in rs if v13_block(r)]
    kpt = [r for r in rs if not v13_block(r)]
    v13_pnl = pnl_of(kpt)
    print(f"{k[0]}-{k[1]:02d}   {len(rs):>8}{v12_pnl:>+8.1f}{v13_pnl:>+8.1f}{len(blk):>9}{pnl_of(blk):>+10.1f}{v13_pnl-v12_pnl:>+8.1f}")
