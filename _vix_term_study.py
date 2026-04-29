"""VIX term structure compression: effect on V12-fix short outcomes.

For each short signal:
  - Pull VIX + VIX3M at signal time (nearest chain_snapshot)
  - Compute overvix at signal time
  - Compute overvix direction vs day open (compressing vs expanding)
  - Bucket trades and compute PnL per regime
"""
import psycopg2
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# 1. V12-fix shorts
cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl, greek_alignment,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       vix, overvix,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-30' AND '2026-04-16'
  AND direction IN ('short','bearish')
  AND setup_name IN ('Skew Charm','DD Exhaustion','AG Short')
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""")
all_t = cur.fetchall()
trades = []
for t in all_t:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m, vix_sig, ovx_sig, d = t
    if setup == 'Skew Charm' and grade not in ('A+', 'A', 'B'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and ((h == 14 and m >= 30) or h == 15): continue
    if setup == 'DD Exhaustion' and align == 0: continue
    trades.append(t)
print(f"V12-fix eligible shorts: {len(trades)}")

# 2. Day-open overvix (first snapshot of day)
cur.execute("""
WITH first AS (
  SELECT DISTINCT ON ((ts AT TIME ZONE 'America/New_York')::date)
    (ts AT TIME ZONE 'America/New_York')::date as d, vix, vix3m
  FROM chain_snapshots
  WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-30' AND '2026-04-16'
    AND vix IS NOT NULL AND vix3m IS NOT NULL
  ORDER BY (ts AT TIME ZONE 'America/New_York')::date, ts
)
SELECT d, vix, vix3m FROM first
""")
open_ovx = {}
for d, vix, v3m in cur.fetchall():
    open_ovx[d] = float(vix) - float(v3m)

# 3. Process: compute regime for each trade
results = []
for t in trades:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m, vix_sig, ovx_sig, d = t
    if d not in open_ovx: continue
    ov_open = open_ovx[d]
    ov_now = float(ovx_sig) if ovx_sig is not None else None
    if ov_now is None: continue
    # Compression: overvix dropping since open (getting more negative)
    compress = ov_now - ov_open  # negative = compressing, positive = expanding
    results.append({
        'id': tid, 'setup': setup, 'outcome': outcome,
        'pnl': float(pnl) if pnl else 0,
        'vix': float(vix_sig) if vix_sig else None,
        'ov_open': ov_open, 'ov_now': ov_now, 'compress': compress,
    })
print(f"Processed: {len(results)}")

def fmt(name, rs):
    if not rs: return f"  {name:<18} (empty)"
    w = sum(1 for r in rs if r['outcome'] == 'WIN')
    l = sum(1 for r in rs if r['outcome'] == 'LOSS')
    pnl = sum(r['pnl'] for r in rs)
    wr = 100.0 * w / max(1, w + l)
    return f"  {name:<18} n={len(rs):>3}  W={w:>3} L={l:>3}  WR={wr:>5.1f}%  pnl={pnl:+7.1f}  avg={pnl/len(rs):+.2f}"

# Cut 1: compressing vs expanding
print()
print("=== Overvix DIRECTION since day open ===")
comp = [r for r in results if r['compress'] < -0.1]
exp_ = [r for r in results if r['compress'] > 0.1]
flat = [r for r in results if abs(r['compress']) <= 0.1]
print(fmt('compressing', comp))
print(fmt('expanding', exp_))
print(fmt('flat', flat))

# Cut 2: overvix LEVEL at signal time
print()
print("=== Overvix LEVEL at signal time ===")
levels = [(-10, -2, 'ov<-2'), (-2, -0.5, '-2 to -0.5'),
          (-0.5, 0.5, 'neutral'), (0.5, 2, '+0.5 to +2'), (2, 10, 'ov>+2')]
for lo, hi, label in levels:
    rs = [r for r in results if lo <= r['ov_now'] < hi]
    print(fmt(label, rs))

# Cut 3: crossed regimes
print()
print("=== Level x Direction matrix ===")
for dir_name, dir_filter in [('COMPRESS', lambda r: r['compress'] < -0.1),
                              ('EXPAND', lambda r: r['compress'] > 0.1)]:
    for lo, hi, label in levels:
        rs = [r for r in results if lo <= r['ov_now'] < hi and dir_filter(r)]
        if rs: print(fmt(f"{dir_name}+{label}", rs))

# Cut 4: pure VIX level (regardless of term structure)
print()
print("=== Raw VIX level at signal time ===")
for lo, hi, label in [(0,16,'<16'),(16,18,'16-18'),(18,20,'18-20'),(20,25,'20-25'),(25,100,'>25')]:
    rs = [r for r in results if r['vix'] is not None and lo <= r['vix'] < hi]
    print(fmt(label, rs))

# Cut 5: Apollo/Yahya's "VIX crushing" = compressing AND VIX dropping hard intraday
# Proxy: overvix declining and overvix_now < overvix_open by 0.5+
print()
print("=== Sharp compression (overvix dropping >= 0.5 since open) vs stable ===")
sharp = [r for r in results if r['compress'] < -0.5]
stable = [r for r in results if abs(r['compress']) < 0.3]
surge = [r for r in results if r['compress'] > 0.5]
print(fmt('sharp compress', sharp))
print(fmt('stable', stable))
print(fmt('sharp expansion', surge))
