"""
Trend-day momentum dip-buy LONG backtest (Discord-pro inspired).
Walks SPX chain_snapshots 30s/2min spot. Validation-protocol compliant:
- raw UTC -> ET via zoneinfo (DST-safe)
- benchmarks: dip-buy vs buy-at-open (same exit), regime-gated vs all-days
- era split: Feb-Mar (mixed) vs Apr-May (bull grind)
"""
import os, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

# ---- params (sensible defaults, NOT optimized) ----
WIN_START = dtime(9, 30)
WIN_END   = dtime(11, 30)   # entry window (pros: done by noon)
DIP_PTS    = 8.0            # pullback from session high to qualify as a dip
CONFIRM_PTS= 4.0            # bounce off local low = bottom-wick confirmation
TARGET     = 10.0
STOP       = 8.0
EXIT_CUTOFF= dtime(16, 0)

# ---- pull full spot series ----
cur.execute("""
  select ts, spot from chain_snapshots
  where ts::date >= '2026-02-02' and spot is not null
  order by ts
""")
rows = cur.fetchall()
days = defaultdict(list)   # et_date -> list[(et_dt, spot)]
for ts, spot in rows:
    et = ts.astimezone(ET)
    days[et.date()].append((et, float(spot)))

daylist = sorted(days)
# daily close (last spot) for regime gate
close = {d: days[d][-1][1] for d in daylist}

def sma_prev(d, n):
    idx = daylist.index(d)
    if idx < n: return None
    prev = daylist[idx-n:idx]
    return sum(close[p] for p in prev)/n

def session_open(d):
    # first spot at/after 9:30 ET
    for et, sp in days[d]:
        if et.time() >= WIN_START:
            return sp
    return days[d][0][1]

def walk_exit(series_after, entry):
    """series_after: list[(et,spot)] strictly after entry. Returns (pnl, result)."""
    for et, sp in series_after:
        if et.time() > EXIT_CUTOFF:
            break
        if sp <= entry - STOP:
            return (-STOP, "LOSS")
        if sp >= entry + TARGET:
            return (TARGET, "WIN")
    # expire at last available spot in session
    last = None
    for et, sp in series_after:
        if et.time() > EXIT_CUTOFF: break
        last = sp
    if last is None: return (0.0, "EXPIRED")
    return (round(last-entry,2), "EXPIRED")

def find_dip_entry(d):
    """Return (entry_spot, entry_et, idx) for first dip-buy trigger in window, else None."""
    s = days[d]
    sess_high = -1e9
    in_dip = False
    local_low = 1e9
    for i,(et,sp) in enumerate(s):
        if et.time() < WIN_START:
            continue
        if et.time() > WIN_END:
            break
        sess_high = max(sess_high, sp)
        if not in_dip:
            if sp <= sess_high - DIP_PTS:
                in_dip = True
                local_low = sp
        else:
            local_low = min(local_low, sp)
            if sp >= local_low + CONFIRM_PTS:
                return (sp, et, i)
    return None

def run(regime_gate):
    trades=[]
    for d in daylist:
        if regime_gate:
            sma = sma_prev(d,3)
            if sma is None: continue
            if session_open(d) < sma:   # not an uptrend regime
                continue
        ent = find_dip_entry(d)
        if not ent: continue
        entry, eet, idx = ent
        pnl,res = walk_exit(days[d][idx+1:], entry)
        trades.append((d,entry,pnl,res))
    return trades

def buyopen(regime_gate):
    """Benchmark: buy at first snapshot in window, same exit."""
    trades=[]
    for d in daylist:
        if regime_gate:
            sma=sma_prev(d,3)
            if sma is None: continue
            if session_open(d)<sma: continue
        s=days[d]
        ent=None
        for i,(et,sp) in enumerate(s):
            if et.time()>=WIN_START:
                ent=(sp,i); break
        if not ent: continue
        entry,idx=ent
        pnl,res=walk_exit(s[idx+1:],entry)
        trades.append((d,entry,pnl,res))
    return trades

def summ(trades, label):
    if not trades:
        print(f"{label:<34} n=0"); return
    n=len(trades); w=sum(1 for t in trades if t[3]=="WIN")
    tot=sum(t[2] for t in trades); avg=tot/n
    # max drawdown on cumulative
    cum=0; peak=0; dd=0
    for t in sorted(trades):
        cum+=t[2]; peak=max(peak,cum); dd=min(dd,cum-peak)
    print(f"{label:<34} n={n:<4} WR={100*w/n:>5.1f}% avgP={avg:>5.2f} totP={tot:>7.1f} maxDD={dd:>6.1f}")

def era(trades, lo, hi):
    return [t for t in trades if lo<=t[0].isoformat()<=hi]

print("=== TREND-DAY MOMENTUM DIP-BUY LONG ===")
print(f"params: window {WIN_START}-{WIN_END} ET, dip>={DIP_PTS}, confirm>={CONFIRM_PTS}, T={TARGET}/S={STOP}\n")

dip_all   = run(False)
dip_gated = run(True)
bo_gated  = buyopen(True)
bo_all    = buyopen(False)

for lbl,(lo,hi) in [("FEB-MAR (mixed)",("2026-02-01","2026-03-31")),
                    ("APR-MAY (bull grind)",("2026-04-01","2026-05-31")),
                    ("JUN (live era)",("2026-06-01","2026-06-30")),
                    ("FULL",("2026-02-01","2026-06-30"))]:
    print(f"--- {lbl} ---")
    summ(era(dip_gated,lo,hi), "  dip-buy + uptrend gate")
    summ(era(dip_all,lo,hi),   "  dip-buy (all days, no gate)")
    summ(era(bo_gated,lo,hi),  "  BENCH buy-at-open + gate")
    summ(era(bo_all,lo,hi),    "  BENCH buy-at-open all days")
    print()
conn.close()
