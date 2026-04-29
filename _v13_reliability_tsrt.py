"""V13 reliability deep-dive + TSRT-specific impact analysis."""
import pickle, bisect
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from collections import defaultdict
import os
import random

os.environ['DATABASE_URL'] = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
eng = create_engine(os.environ['DATABASE_URL'])

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

print('Loading DD...', flush=True)
with eng.connect() as c:
    dd_raw = c.execute(text("""
        SELECT ts_utc, CAST(strike AS FLOAT), CAST(value AS FLOAT)
        FROM volland_exposure_points
        WHERE ts_utc >= '2026-03-01' AND ts_utc <= '2026-04-16'
          AND ticker='SPX' AND greek='deltaDecay' AND expiration_option='TODAY'
    """)).fetchall()

dd_by_ts = defaultdict(list)
for ts,k,v in dd_raw: dd_by_ts[ts].append((k,v))
dd_ts_sorted = sorted(dd_by_ts.keys())

def find_nearest(t, arr, max_sec=300):
    i = bisect.bisect_left(arr, t)
    c = []
    if i>0: c.append(arr[i-1])
    if i<len(arr): c.append(arr[i])
    if not c: return None
    b = min(c, key=lambda x: abs((x-t).total_seconds()))
    return b if abs((b-t).total_seconds()) <= max_sec else None

for r in R:
    if r['setup'] not in ('Skew Charm','DD Exhaustion'):
        r['dd_max_abs'] = 0
        continue
    cm = find_nearest(r['ts'], dd_ts_sorted)
    if not cm:
        r['dd_max_abs'] = 0
        continue
    near = [(k,v) for k,v in dd_by_ts[cm] if abs(k - r['spot']) <= 10]
    r['dd_max_abs'] = max((abs(v) for _,v in near), default=0)

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for x in arr if x['out']=='WIN')
    return len(arr), w, sum(x['pnl'] for x in arr)

def v13_blocks(r):
    """V13 = GEX>=70 OR DD>=3B applied to SC/DD shorts only. AG unaffected."""
    if r['setup'] not in ('Skew Charm','DD Exhaustion'): return False
    return r['plus_above_v']>=70 or r['dd_max_abs']>=3e9

# ══════════════════════════════════════════════════════════════════
# SECTION 1: FULL V12-fix vs V13 (all shorts)
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 1: V12-fix vs V13 (ALL shorts: SC+DD+AG)')
print('='*75)
all_sorts = R
v12_pnl = sum(r['pnl'] for r in all_sorts)
v13_arr = [r for r in all_sorts if not v13_blocks(r)]
v13_pnl = sum(r['pnl'] for r in v13_arr)
print(f"V12-fix: {len(all_sorts)}t  PnL {v12_pnl:+.1f}  WR={sum(1 for r in all_sorts if r['out']=='WIN')/len(all_sorts)*100:.1f}%")
print(f"V13:     {len(v13_arr)}t  PnL {v13_pnl:+.1f}  WR={sum(1 for r in v13_arr if r['out']=='WIN')/max(len(v13_arr),1)*100:.1f}%")
print(f"V13 DELTA over V12-fix: {v13_pnl-v12_pnl:+.1f} pts ({(v13_pnl/v12_pnl-1)*100:+.1f}%)")

# ══════════════════════════════════════════════════════════════════
# SECTION 2: TSRT-SPECIFIC (SC shorts only — what's live on TSRT)
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 2: TSRT IMPACT (SC shorts only — what real trader runs)')
print('='*75)
sc_shorts = [r for r in R if r['setup']=='Skew Charm']
n_sc,w_sc,p_sc = stats(sc_shorts)
blk = [r for r in sc_shorts if v13_blocks(r)]
kept = [r for r in sc_shorts if not v13_blocks(r)]
bn,bw,bp = stats(blk); kn,kw,kp = stats(kept)
print(f"SC shorts baseline (V12-fix): {n_sc}t {w_sc}W={w_sc/n_sc*100:.0f}% WR {p_sc:+.1f} pts")
print(f"  V13 blocks: {bn}t ({bw}W / {bn-bw}L = {bw/max(bn,1)*100:.0f}% WR) avoided {-bp:+.1f} pts")
print(f"  V13 keeps:  {kn}t ({kw}W / {kn-kw}L = {kw/max(kn,1)*100:.0f}% WR) {kp:+.1f} pts")
print(f"  TSRT SC shorts delta (V12-fix -> V13): {-bp:+.1f} pts")
print(f"  At 1 MES ($5/pt): {-bp*5:+.2f} USD over 46 days = {-bp*5/46:+.2f}/day")

# ══════════════════════════════════════════════════════════════════
# SECTION 3: V13 BLOCKED WR — wins killed vs losers saved
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 3: V13 BLOCKED TRADES (SC+DD) — wins vs losses')
print('='*75)
v13_blk = [r for r in R if v13_blocks(r)]
blk_wins = [r for r in v13_blk if r['out']=='WIN']
blk_losses = [r for r in v13_blk if r['out']!='WIN']
w_sum = sum(r['pnl'] for r in blk_wins)
l_sum = sum(r['pnl'] for r in blk_losses)
print(f"Total V13 blocks: {len(v13_blk)} (SC+DD only)")
print(f"  Winners killed:  {len(blk_wins)}  total PnL = {w_sum:+.1f} (this is the COST of filter)")
print(f"  Losers avoided:  {len(blk_losses)} total PnL = {l_sum:+.1f} (this is the SAVE)")
print(f"  Net save: {-l_sum - w_sum:+.1f} pts = winners killed ({w_sum:+.1f}) subtracted from losers saved ({-l_sum:+.1f})")
print(f"  Reward-to-cost ratio: {-l_sum/max(w_sum,0.01):.1f}x (save $ per $ cost)")

# ══════════════════════════════════════════════════════════════════
# SECTION 4: BOOTSTRAP — is the edge real?
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 4: BOOTSTRAP RESAMPLING — confidence in the edge')
print('='*75)
random.seed(42)
scdd = [r for r in R if r['setup'] in ('Skew Charm','DD Exhaustion')]
N = len(scdd)
improvements = []
for _ in range(2000):
    sample = [scdd[random.randint(0, N-1)] for _ in range(N)]
    base = sum(r['pnl'] for r in sample)
    v13 = sum(r['pnl'] for r in sample if not v13_blocks(r))
    improvements.append(v13 - base)
improvements.sort()
p05 = improvements[int(len(improvements)*0.05)]
p50 = improvements[len(improvements)//2]
p95 = improvements[int(len(improvements)*0.95)]
p_neg = sum(1 for x in improvements if x <= 0) / len(improvements)
print(f"2000 bootstrap resamples of V13 improvement (SC+DD):")
print(f"  p5:  {p05:+.1f} pts")
print(f"  p50: {p50:+.1f} pts (median)")
print(f"  p95: {p95:+.1f} pts")
print(f"  P(improvement <= 0): {p_neg*100:.1f}%")
print(f"  Observed actual improvement: +212.5 pts")

# ══════════════════════════════════════════════════════════════════
# SECTION 5: PER-DAY EDGE CONSISTENCY
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 5: PER-DAY V13 EDGE (how many days does V13 HELP vs HURT?)')
print('='*75)
by_date = defaultdict(list)
for r in scdd: by_date[r['date']].append(r)
days_help = days_hurt = days_neutral = 0
total_help = total_hurt = 0.0
day_deltas = []
for d in sorted(by_date.keys()):
    arr = by_date[d]
    delta = -sum(r['pnl'] for r in arr if v13_blocks(r))
    day_deltas.append((d, delta))
    if delta > 0.5: days_help += 1; total_help += delta
    elif delta < -0.5: days_hurt += 1; total_hurt += delta
    else: days_neutral += 1
print(f"Days V13 HELPS (delta > +0.5 pt): {days_help}  total save {total_help:+.1f}")
print(f"Days V13 HURTS (delta < -0.5 pt): {days_hurt}  total cost {total_hurt:+.1f}")
print(f"Days V13 NEUTRAL:                 {days_neutral}")
print(f"Net: {total_help+total_hurt:+.1f} pts")
print('\nAll days with |delta| > 5:')
for d,delta in day_deltas:
    if abs(delta) >= 5:
        direction = "HELP" if delta>0 else "HURT"
        print(f"  {d} ({d.strftime('%a')}): {delta:+.1f}  [{direction}]")

# ══════════════════════════════════════════════════════════════════
# SECTION 6: FALSE POSITIVE DETAIL
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 6: FALSE POSITIVES (V13 killed winners — what were they?)')
print('='*75)
fps = sorted([r for r in scdd if v13_blocks(r) and r['out']=='WIN'], key=lambda x: x['pnl'], reverse=True)
print(f"{len(fps)} false positives total:")
for r in fps:
    reason = []
    if r['plus_above_v']>=70: reason.append(f"GEX+above={r['plus_above_v']:+.0f}M")
    if r['dd_max_abs']>=3e9: reason.append(f"DD={r['dd_max_abs']/1e9:.2f}B")
    print(f"  {r['ts_et'].strftime('%m-%d %H:%M')} {r['setup']:<15} {r['grade'] or '-':<4} spot={r['spot']:.0f} pnl={r['pnl']:+.1f}  ({' AND '.join(reason)})")

# ══════════════════════════════════════════════════════════════════
# SECTION 7: MONTHLY PROJECTION
# ══════════════════════════════════════════════════════════════════
print('\n'+'='*75)
print('SECTION 7: MONTHLY REAL-MONEY PROJECTION (TSRT 1 MES, SC only)')
print('='*75)
# 46 calendar days in sample. Trading days ~33.
# SC-only delta = bp from SC portion of V13 blocks
sc_blk = [r for r in sc_shorts if v13_blocks(r)]
sc_blk_pnl = sum(r['pnl'] for r in sc_blk)
trading_days = 33  # rough estimate Mar 1 - Apr 15
sc_month_improvement_pts = -sc_blk_pnl * (21/trading_days)  # scale to 21-day month
print(f"SC-only V13 blocks: {len(sc_blk)}, avoided PnL {-sc_blk_pnl:+.1f}")
print(f"Per trading day: {-sc_blk_pnl/trading_days:+.2f} pts")
print(f"Projected 21-day month: {sc_month_improvement_pts:+.1f} pts = {sc_month_improvement_pts*5:+.2f} USD at 1 MES")
print(f"\nFor context:")
print(f"  Apr 15 alone would have saved: ~27 pts on TSRT = ~$135 on 1 MES")
print(f"  Monthly TSRT infrastructure cost: ~$524")
print(f"  Expected V13 TSRT improvement covers: {sc_month_improvement_pts*5/524*100:.0f}% of infra cost/month")
