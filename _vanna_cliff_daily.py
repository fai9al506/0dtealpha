"""Daily breakdown: what vanna-cliff filter blocks and how blocked trades resulted."""
import psycopg2
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl, greek_alignment,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-11' AND '2026-04-16'
  AND direction IN ('short','bearish')
  AND setup_name IN ('Skew Charm','DD Exhaustion','AG Short')
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""")
all_t = cur.fetchall()

v12 = []
for t in all_t:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m, d = t
    if setup == 'Skew Charm' and grade not in ('A+', 'A', 'B'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and ((h == 14 and m >= 30) or h == 15): continue
    if setup == 'DD Exhaustion' and align == 0: continue
    v12.append(t)

def get_cliff(ts, spot):
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
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None
    near.sort()
    crossings = []
    for i in range(1, len(near)):
        s0, v0 = near[i-1]; s1, v1 = near[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0: crossings.append(s0 + (-v0/(v1-v0))*(s1-s0))
    if not crossings: return None
    return min(crossings, key=lambda s: abs(s - float(spot)))

print("Computing cliffs...", flush=True)
enriched = []
for i, t in enumerate(v12):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m, d = t
    cliff = get_cliff(ts, spot)
    cliff_side = None
    if cliff is not None:
        cliff_side = 'ABOVE' if cliff > float(spot) else 'BELOW'
    enriched.append({
        'id': tid, 'd': d, 'setup': setup, 'spot': float(spot),
        'cliff': cliff, 'side': cliff_side, 'outcome': outcome,
        'pnl': float(pnl) if pnl else 0, 'h': h, 'm': m
    })
    if (i+1) % 50 == 0: print(f"  ...{i+1}/{len(v12)}", flush=True)

# DEFINE FILTER: block DD Exhaustion when cliff is ABOVE
def blocked(r):
    return r['setup'] == 'DD Exhaustion' and r['side'] == 'ABOVE'

blocked_trades = [r for r in enriched if blocked(r)]
kept_trades = [r for r in enriched if not blocked(r)]

print()
print("=" * 80)
print("FILTER: Block DD Exhaustion shorts when vanna cliff is ABOVE spot")
print("=" * 80)
print()
print(f"Total V12-fix shorts (Feb 11 - Apr 16): {len(enriched)}")
print(f"Blocked: {len(blocked_trades)}")
print(f"Kept:    {len(kept_trades)}")

# Aggregate
tot_pnl = sum(r['pnl'] for r in enriched)
kept_pnl = sum(r['pnl'] for r in kept_trades)
blk_pnl = sum(r['pnl'] for r in blocked_trades)
blk_w = sum(1 for r in blocked_trades if r['outcome'] == 'WIN')
blk_l = sum(1 for r in blocked_trades if r['outcome'] == 'LOSS')
blk_e = sum(1 for r in blocked_trades if r['outcome'] == 'EXPIRED')

print()
print(f"BEFORE filter total short PnL: {tot_pnl:+.1f} pts")
print(f"AFTER  filter total short PnL: {kept_pnl:+.1f} pts")
print(f"DELTA (improvement):           {kept_pnl - tot_pnl:+.1f} pts")
print()
print(f"What the {len(blocked_trades)} BLOCKED DD-above trades actually did:")
print(f"  WIN={blk_w}  LOSS={blk_l}  EXPIRED={blk_e}")
print(f"  Combined PnL = {blk_pnl:+.1f} pts (this is what we SAVE by blocking)")
print(f"  WR on decided = {100*blk_w/max(1,blk_w+blk_l):.1f}%")

# Per-day breakdown
print()
print("=" * 80)
print("DAILY IMPACT (only days with DD-above blocks):")
print("=" * 80)
print(f"{'Date':<12}{'Blk_n':>6}{'Blk_W':>6}{'Blk_L':>6}{'Blk_E':>6}{'BlkPnL':>9}{'Kept_n':>8}{'KeptPnL':>9}{'BeforeDay':>10}{'AfterDay':>9}")
daily_before = defaultdict(float)
daily_after = defaultdict(float)
for r in enriched:
    daily_before[str(r['d'])] += r['pnl']
    if not blocked(r):
        daily_after[str(r['d'])] += r['pnl']

daily_blk = defaultdict(list)
daily_kept = defaultdict(list)
for r in enriched:
    if blocked(r): daily_blk[str(r['d'])].append(r)
    else: daily_kept[str(r['d'])].append(r)

tot_before = 0; tot_after = 0
for d in sorted(set(list(daily_blk.keys()) + list(daily_kept.keys()))):
    blk = daily_blk.get(d, [])
    kept = daily_kept.get(d, [])
    if not blk: continue  # only show days with blocks
    bw = sum(1 for r in blk if r['outcome']=='WIN')
    bl = sum(1 for r in blk if r['outcome']=='LOSS')
    be = sum(1 for r in blk if r['outcome']=='EXPIRED')
    bpnl = sum(r['pnl'] for r in blk)
    kpnl = sum(r['pnl'] for r in kept)
    before = bpnl + kpnl
    after = kpnl
    print(f"{d:<12}{len(blk):>6}{bw:>6}{bl:>6}{be:>6}{bpnl:>+9.1f}{len(kept):>8}{kpnl:>+9.1f}{before:>+10.1f}{after:>+9.1f}")
    tot_before += before
    tot_after += after

print()
print(f"TOTALS (days with blocks only): Before={tot_before:+.1f}  After={tot_after:+.1f}  Saved={tot_after-tot_before:+.1f}")

# Worst single blocked loser / best blocked winner
print()
print("WORST blocked trades (what we saved from):")
for r in sorted(blocked_trades, key=lambda x: x['pnl'])[:5]:
    print(f"  {r['d']} {r['h']:02d}:{r['m']:02d} DD {r['outcome']} pnl={r['pnl']:+.1f}  spot={r['spot']:.1f} cliff={r['cliff']:.1f} (+{r['cliff']-r['spot']:.1f})")
print("BEST blocked trades (opportunities missed):")
for r in sorted(blocked_trades, key=lambda x: -x['pnl'])[:5]:
    print(f"  {r['d']} {r['h']:02d}:{r['m']:02d} DD {r['outcome']} pnl={r['pnl']:+.1f}  spot={r['spot']:.1f} cliff={r['cliff']:.1f} (+{r['cliff']-r['spot']:.1f})")
