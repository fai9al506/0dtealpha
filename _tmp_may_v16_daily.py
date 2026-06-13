"""Daily V16-passing portal PnL across May 2026 vs today."""
import os, sys, psycopg2
from collections import defaultdict
from statistics import mean, median, stdev
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# V16 mirrors real_trader whitelist + V14 filter rules. Simplified approximation
# in SQL — pull all trades, apply V16 filter logic in Python (mirrors JS).
cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm, greek_alignment,
           vix, overvix, vanna_cliff_side, vanna_peak_side,
           v13_gex_above, v13_dd_near, vanna_regime,
           outcome_result, outcome_pnl, ts
    FROM setup_log
    WHERE ts >= '2026-05-01' AND ts < '2026-05-20'
      AND outcome_result IS NOT NULL AND outcome_result != 'EXPIRED'
    ORDER BY id
""")
rows = cur.fetchall()

V16_ALLOWED = {'Skew Charm', 'AG Short', 'Vanna Pivot Bounce', 'ES Absorption', 'DD Exhaustion'}

def is_long(d):
    return d in ('long', 'bullish')

def get_et_dow(ts):
    """Returns (weekday, day_of_month) in ET."""
    from datetime import timezone, timedelta
    et = ts.astimezone(timezone(timedelta(hours=-4)))  # rough ET
    return et.weekday(), et.day

def passes_v16(r):
    (lid, sn, dir_, grade, para, align, vix, overvix,
     cliff, peak, gex_above, dd_near, vanna_reg, res, pnl, ts) = r
    align = align or 0
    vix = vix or 0
    overvix = overvix if overvix is not None else -99

    if sn not in V16_ALLOWED:
        return False
    # DD long admit path (V16.1)
    if sn == 'DD Exhaustion' and is_long(dir_):
        if para == 'SIDIAL-EXTREME': return False
        if align < 0: return False
        if align >= 3: return False
        if vix >= 22: return False
        if para in ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return False
        if grade == 'C': return False
        return True
    # SC grade gate
    if sn == 'Skew Charm' and grade in ('C', 'LOG'): return False
    # ES Abs PURE
    if sn == 'ES Absorption':
        if grade not in ('A', 'A+'): return False
        if para in ('AG-TARGET', 'AG-LIS'): return False
        # V16 R10: ES Abs bearish hr>=14
        from datetime import timezone, timedelta
        et = ts.astimezone(timezone(timedelta(hours=-4)))
        if not is_long(dir_) and et.hour >= 14: return False
        if is_long(dir_) and align < 0: return False
        if not is_long(dir_) and align > 0: return False
        return True
    # VPB
    if sn == 'Vanna Pivot Bounce':
        if not is_long(dir_): return False
        if vanna_reg != 'bullish': return False
        return True
    # V13 Vanna (with V13.2 narrowing)
    if cliff:
        if not is_long(dir_):
            if sn == 'DD Exhaustion' and cliff == 'A' and peak == 'B': return False
            if sn == 'Skew Charm' and cliff == 'A' and peak == 'B': return False
            if sn == 'AG Short' and cliff == 'B' and peak == 'A': return False
        # V13.2: SC LONG cliff=A peak=B REMOVED
    # V13 bullish-structure block (SC/DD shorts)
    if not is_long(dir_) and sn in ('Skew Charm', 'DD Exhaustion'):
        if (gex_above or 0) >= 75: return False
        if (dd_near or 0) >= 3_000_000_000: return False
    # GEX-LIS block on SC/DD shorts
    if sn in ('Skew Charm','DD Exhaustion') and not is_long(dir_) and para == 'GEX-LIS':
        return False
    # AG Short paradigm + OpEx Friday
    if sn == 'AG Short':
        if para == 'AG-TARGET': return False
        from datetime import timezone, timedelta
        et = ts.astimezone(timezone(timedelta(hours=-4)))
        if et.weekday() == 4 and 15 <= et.day <= 21: return False
    # SC long V16 rules
    if sn == 'Skew Charm' and is_long(dir_):
        if para == 'SIDIAL-EXTREME': return False
        if align == 3 and para in ('GEX-LIS','AG-LIS','AG-PURE','BOFA-MESSY'): return False
        if para == 'GEX-LIS': return False
        from datetime import timezone, timedelta
        et = ts.astimezone(timezone(timedelta(hours=-4)))
        if et.weekday() == 4 and 15 <= et.day <= 21: return False
        return True
    # SC short — V14 admits all (with grade gate already applied)
    if sn == 'Skew Charm' and not is_long(dir_):
        return True
    # AG Short
    if sn == 'AG Short' and not is_long(dir_):
        return True
    # DD short — V16 has whitelist blocking shorts (S145 audit)
    if sn == 'DD Exhaustion' and not is_long(dir_):
        return False  # DD shorts blocked at dispatch
    return False

# Group by ET date
from datetime import timezone, timedelta
ET = timezone(timedelta(hours=-4))

daily = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0, 'l': 0})
for r in rows:
    if not passes_v16(r):
        continue
    ts = r[15]
    pnl = float(r[14])
    date_et = ts.astimezone(ET).date()
    daily[date_et]['pnl'] += pnl
    daily[date_et]['n'] += 1
    if pnl > 0:
        daily[date_et]['w'] += 1
    else:
        daily[date_et]['l'] += 1

print(f"{'Date':<12} {'n':<4} {'W/L':<7} {'WR':<6} {'PnL pts':<9} {'$MES':<8} {'$ES':<8}")
print("-" * 56)
pnls = []
counts = []
wrs = []
for d in sorted(daily.keys()):
    x = daily[d]
    if x['n'] == 0: continue
    wr = x['w']/x['n']*100
    pnls.append(x['pnl'])
    counts.append(x['n'])
    wrs.append(wr)
    print(f"{d.isoformat():<12} {x['n']:<4} {x['w']}W/{x['l']}L  {wr:5.1f}% {x['pnl']:+7.1f}  ${x['pnl']*5:+6.0f}  ${x['pnl']*50:+7.0f}")

if pnls:
    print("-" * 56)
    print(f"\n=== May 1-19 V16 backtest summary ===")
    print(f"  Trading days w/ ≥1 V16 trade: {len(pnls)}")
    print(f"  Mean daily PnL:    {mean(pnls):+6.1f} pts = ${mean(pnls)*5:+5.0f} MES = ${mean(pnls)*50:+6.0f} ES")
    print(f"  Median daily PnL:  {median(pnls):+6.1f} pts = ${median(pnls)*5:+5.0f} MES = ${median(pnls)*50:+6.0f} ES")
    if len(pnls) > 1:
        print(f"  StdDev daily PnL:  {stdev(pnls):6.1f} pts (variance is HIGH)")
    print(f"  Worst day:         {min(pnls):+6.1f} pts")
    print(f"  Best day:          {max(pnls):+6.1f} pts")
    print(f"  Mean trades/day:   {mean(counts):.1f}")
    print(f"  Mean WR:           {mean(wrs):.1f}%")
    print(f"  Total May PnL:     {sum(pnls):+.1f} pts = ${sum(pnls)*5:+.0f} MES = ${sum(pnls)*50:+.0f} ES")

# Today comparison
today_et = max(daily.keys()) if daily else None
if today_et:
    t = daily[today_et]
    print(f"\n=== Today ({today_et}) vs May ===")
    print(f"  Today: {t['n']}t, {t['w']}W/{t['l']}L, {t['w']/t['n']*100:.1f}% WR, +{t['pnl']:.1f} pts")
    pct = sum(1 for p in pnls if p <= t['pnl']) / len(pnls) * 100
    print(f"  Today ranks: {pct:.0f}th percentile of May daily distribution")

cur.close(); conn.close()
