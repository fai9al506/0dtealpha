"""Combined backtest: deployed V13 (GEX+DD magnet) + vanna cliff/peak rules.
Question: Do vanna rules overlap with GEX/DD magnet blocks or are they independent?
"""
import psycopg2

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

START = '2026-03-01'
END = '2026-04-17'

# 1. Pull all trades
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
print(f"Total trades in {START} - {END}: {len(raw)}")

# 2. V12-fix filter (same as current _passes_live_filter but WITHOUT V13 GEX/DD layer)
def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m, d = t
    if setup in ("VIX Divergence", "IV Momentum", "Vanna Butterfly"):
        return False
    if setup == 'Skew Charm' and grade and grade in ('C', 'LOG'):
        return False
    # time-of-day
    if setup in ('Skew Charm', 'DD Exhaustion'):
        if (h == 14 and m >= 30) or (h == 15 and m < 0) or (h == 14 and m < 60 and h == 14 and m >= 30) or (h == 14 and m >= 30) and h < 15:
            pass
        if (h == 14 and m >= 30) or (h == 15 and m < 60):
            return False  # 14:30-16:00
    # hmm let me redo this cleanly:
    if setup in ('Skew Charm', 'DD Exhaustion'):
        if (h == 14 and m >= 30) or h == 15:
            return False
    if setup == 'BofA Scalp' and h >= 15:
        return False
    if setup == 'BofA Scalp' and h == 14 and m >= 30:
        return False
    is_long = dirx in ('long', 'bullish')
    # SIDIAL-EXTREME long block
    if is_long and paradigm == 'SIDIAL-EXTREME':
        return False
    if is_long:
        if align is None or align < 2: return False
        if setup == 'Skew Charm': return True
        vix_f = float(vix) if vix else None
        ovx_f = float(ovx) if ovx else -99
        if vix_f is not None and vix_f > 22:
            if ovx_f < 2: return False
        return True
    else:
        # shorts
        if setup in ('Skew Charm','DD Exhaustion') and paradigm == 'GEX-LIS':
            return False
        if setup == 'AG Short' and paradigm == 'AG-TARGET':
            return False
        if setup in ('Skew Charm', 'AG Short'): return True
        if setup == 'DD Exhaustion' and align != 0: return True
        return False

v12 = [t for t in raw if passes_v12fix(t)]
print(f"V12-fix eligible: {len(v12)}")

# 3. For each v12 trade, compute deployed V13 GEX/DD features
print("Computing deployed V13 features (GEX above, DD near)...", flush=True)
import json

def get_v13_features(signal_ts, spot):
    """Returns (gex_above, dd_near) at nearest snapshot to signal_ts."""
    cur.execute("""
    SELECT columns, rows FROM chain_snapshots
    WHERE ts <= %s AND ts >= %s - interval '3 minutes'
      AND spot IS NOT NULL
    ORDER BY ts DESC LIMIT 1
    """, (signal_ts, signal_ts))
    row = cur.fetchone()
    gex_above = 0.0
    if row:
        cols, rows = row
        # find column indices
        try:
            strike_idx = cols.index('Strike')
            # first Gamma/Open Int are call side
            c_oi_idx = cols.index('Open Int')
            c_g_idx = cols.index('Gamma')
            # second occurrences are put side
            p_g_idx = cols.index('Gamma', c_g_idx + 1)
            p_oi_idx = cols.index('Open Int', c_oi_idx + 1)
            max_gex = 0
            for r in rows:
                s = r[strike_idx]
                if s is None or float(s) <= float(spot): continue
                c_g = float(r[c_g_idx] or 0); c_oi = float(r[c_oi_idx] or 0)
                p_g = float(r[p_g_idx] or 0); p_oi = float(r[p_oi_idx] or 0)
                net_gex = c_g * c_oi - p_g * p_oi
                if net_gex > max_gex:
                    max_gex = net_gex
            gex_above = max_gex
        except Exception:
            pass
    # DD near
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
    """, (signal_ts, signal_ts, float(spot)))
    dd = cur.fetchone()
    dd_near = float(dd[0]) if dd and dd[0] else 0.0
    return gex_above, dd_near

# 4. For each v12 trade, compute vanna cliff/peak features
def get_vanna_features(signal_ts, spot):
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
    """, (signal_ts, signal_ts))
    pts = cur.fetchall()
    if not pts: return None, None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None, None
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

enriched = []
for i, t in enumerate(v12):
    gex_above, dd_near = get_v13_features(t[1], t[5])
    c_side, p_side = get_vanna_features(t[1], t[5])
    enriched.append({'t': t, 'gex_above': gex_above, 'dd_near': dd_near,
                     'cliff': c_side, 'peak': p_side})
    if (i+1) % 50 == 0: print(f"  ...{i+1}/{len(v12)}", flush=True)

# 5. Apply deployed V13 block logic
def v13_block(r):
    t = r['t']; setup = t[2]; dirx = t[11]
    if dirx in ('long', 'bullish'): return False
    if setup not in ('Skew Charm', 'DD Exhaustion'): return False
    if r['gex_above'] >= 75: return True
    if r['dd_near'] >= 3_000_000_000: return True
    return False

# 6. Apply vanna rules (V14 candidate — ON TOP of V13)
def vanna_block(r):
    t = r['t']; setup = t[2]; dirx = t[11]; c = r['cliff']; p = r['peak']
    if c is None: return False
    if dirx in ('short','bearish'):
        if setup == 'DD Exhaustion' and c == 'A': return True
        if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
        if setup == 'AG Short' and c == 'B' and p == 'A': return True
    if dirx in ('long','bullish'):
        if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
    return False

def pnl(rs): return sum(float(r['t'][7] or 0) for r in rs)
def wl(rs):
    w = sum(1 for r in rs if r['t'][6]=='WIN')
    l = sum(1 for r in rs if r['t'][6]=='LOSS')
    return w, l

baseline = pnl(enriched)
v13_blocked = [r for r in enriched if v13_block(r)]
v13_kept = [r for r in enriched if not v13_block(r)]
v13_pnl = pnl(v13_kept)

vanna_only_blocked = [r for r in enriched if vanna_block(r)]
vanna_only_kept = [r for r in enriched if not vanna_block(r)]
vanna_only_pnl = pnl(vanna_only_kept)

# Combined: block if EITHER v13 or vanna block
combo_blocked = [r for r in enriched if v13_block(r) or vanna_block(r)]
combo_kept = [r for r in enriched if not (v13_block(r) or vanna_block(r))]
combo_pnl = pnl(combo_kept)

# Overlap analysis
both_block = [r for r in enriched if v13_block(r) and vanna_block(r)]
v13_only = [r for r in enriched if v13_block(r) and not vanna_block(r)]
vanna_only = [r for r in enriched if vanna_block(r) and not v13_block(r)]

print()
print("=" * 75)
print(f"COMBINED BACKTEST: {START} to {END}")
print("=" * 75)
print(f"V12-fix baseline:        {baseline:+8.1f} pts   ({len(enriched)} trades)")
print(f"V13 deployed (GEX+DD):   {v13_pnl:+8.1f} pts   (blocks {len(v13_blocked)}, Δ={v13_pnl-baseline:+.1f})")
print(f"Vanna rules alone:       {vanna_only_pnl:+8.1f} pts   (blocks {len(vanna_only_blocked)}, Δ={vanna_only_pnl-baseline:+.1f})")
print(f"V13 + Vanna combined:    {combo_pnl:+8.1f} pts   (blocks {len(combo_blocked)}, Δ={combo_pnl-baseline:+.1f})")
print()
print("=" * 75)
print("OVERLAP ANALYSIS")
print("=" * 75)
print(f"V13-blocked total:       {len(v13_blocked):>3} trades, pnl={pnl(v13_blocked):+.1f}")
print(f"Vanna-blocked total:     {len(vanna_only_blocked):>3} trades, pnl={pnl(vanna_only_blocked):+.1f}")
print(f"BOTH block (overlap):    {len(both_block):>3} trades, pnl={pnl(both_block):+.1f}")
print(f"V13 only (no vanna):     {len(v13_only):>3} trades, pnl={pnl(v13_only):+.1f}")
print(f"Vanna only (no V13):     {len(vanna_only):>3} trades, pnl={pnl(vanna_only):+.1f}")
print(f"Combined block:          {len(combo_blocked):>3} trades, pnl={pnl(combo_blocked):+.1f}")

# Overlap rate
if len(v13_blocked) and len(vanna_only_blocked):
    overlap_pct = 100.0 * len(both_block) / min(len(v13_blocked), len(vanna_only_blocked))
    print(f"\nOverlap rate: {overlap_pct:.1f}% of smaller set")

# How much does vanna ADD on top of V13?
extra_from_vanna = combo_pnl - v13_pnl
print(f"\nVanna's INCREMENTAL edge on top of V13: {extra_from_vanna:+.1f} pts")
print(f"Vanna adds {len(vanna_only)} new blocks (not caught by V13)")

# Monthly breakdown
print()
print("=" * 75)
print("MONTHLY BREAKDOWN")
print("=" * 75)
from collections import defaultdict
by_month = defaultdict(list)
for r in enriched:
    k = (r['t'][14].year, r['t'][14].month)
    by_month[k].append(r)
print(f"{'Month':<10}{'Base':>9}{'V13':>9}{'V13+Van':>10}{'V13Δ':>8}{'VanΔ':>8}{'CombΔ':>8}")
for k in sorted(by_month.keys()):
    rs = by_month[k]
    b = pnl(rs)
    v13_rs = [r for r in rs if not v13_block(r)]
    combo_rs = [r for r in rs if not (v13_block(r) or vanna_block(r))]
    v13_p = pnl(v13_rs); combo_p = pnl(combo_rs)
    print(f"{k[0]}-{k[1]:02d}   {b:>+9.1f}{v13_p:>+9.1f}{combo_p:>+10.1f}{v13_p-b:>+8.1f}{combo_p-v13_p:>+8.1f}{combo_p-b:>+8.1f}")
