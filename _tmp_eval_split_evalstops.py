"""Eval split backtest with EXACT eval per-setup stops + trail (from eval_trader.py).
Re-walks each historical signal on the SPX 2-min path with eval stop/trail logic.
Caveat: chain-walk vs MES 5pt range bars differs ~2.35pt (S55, chain tags trail early)."""
import os, psycopg2, statistics
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')

# eval config (exact)
STOPS = {"AG Short":12,"ES Absorption":8,"DD Exhaustion":12,"Skew Charm":14,
         "VIX Divergence":8,"Vanna Pivot Bounce":8,"Paradigm Reversal":12}
TARGETS = {"Vanna Pivot Bounce":10,"Paradigm Reversal":10}   # else trail-only
QTY = {"DD Exhaustion":2}   # default 3
DEF_QTY = 3
TRAIL = {
    "DD Exhaustion":  {"mode":"continuous","activation":20,"gap":5},
    "AG Short":       {"mode":"hybrid","be_trigger":10,"activation":12,"gap":5},
    "Skew Charm":     {"mode":"hybrid","be_trigger":10,"activation":10,"gap":3},
    "VIX Divergence": {"mode":"continuous","activation":10,"gap":8},
    "ES Absorption":  {"mode":"hybrid","be_trigger":5,"activation":8,"gap":3},
}
NO_NEW_AFTER = dtime(15,30); FLATTEN = dtime(15,50)

conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()
cur.execute("select ts,spot from chain_snapshots where ts::date>='2026-02-01' and spot is not null order by ts")
days = defaultdict(list)
for ts,sp in cur.fetchall():
    et=ts.astimezone(ET); days[et.date()].append((et,float(sp)))
cur.execute("""select id, ts, setup_name, direction, spot from setup_log
  where ts::date>='2026-02-01' and outcome_pnl is not null and spot is not null
    and setup_name in ('Skew Charm','AG Short','DD Exhaustion','ES Absorption','VIX Divergence','Vanna Pivot Bounce')
  order by ts""")
signals=cur.fetchall(); conn.close()

def walk(setup, direction, entry, et0, path):
    is_long = direction in ('long','bullish')
    stop_pts = STOPS.get(setup,12)
    stop = entry - stop_pts if is_long else entry + stop_pts
    tgt_pts = TARGETS.get(setup)
    target = (entry+tgt_pts if is_long else entry-tgt_pts) if tgt_pts else None
    tp = TRAIL.get(setup)
    max_fav = 0.0; last = entry
    for et,sp in path:
        if et <= et0: continue
        if et.time() > FLATTEN: break
        last = sp
        profit = (sp-entry) if is_long else (entry-sp)
        max_fav = max(max_fav, profit)
        # update trailing stop (tighten only)
        if tp:
            new_stop=None
            if tp["mode"]=="continuous":
                if max_fav>=tp["activation"]:
                    lock=max_fav-tp["gap"]; new_stop=(entry+lock) if is_long else (entry-lock)
            else:  # hybrid
                if max_fav>=tp["activation"]:
                    lock=max_fav-tp["gap"]; new_stop=(entry+lock) if is_long else (entry-lock)
                elif max_fav>=tp["be_trigger"]:
                    new_stop=entry
            if new_stop is not None:
                if (is_long and new_stop>stop) or (not is_long and new_stop<stop): stop=new_stop
        # adverse-first: check stop, then target
        if (is_long and sp<=stop) or (not is_long and sp>=stop):
            return (stop-entry) if is_long else (entry-stop)
        if target is not None and ((is_long and sp>=target) or (not is_long and sp<=target)):
            return tgt_pts
    return (last-entry) if is_long else (entry-last)

# build per-day per-direction RAW points (qty=1); qty applied in sim
dayL=defaultdict(float); dayS=defaultdict(float); alld=set()
ntr=defaultdict(int)
for sid, ts, setup, direction, spot in signals:
    et=ts.astimezone(ET); d=et.date()
    if et.time()>NO_NEW_AFTER or et.time()<dtime(9,30): continue
    if d not in days: continue
    pts = walk(setup, direction, float(spot), et, days[d])
    alld.add(d)
    if direction in ('long','bullish'): dayL[d]+=pts
    else: dayS[d]+=pts
    ntr[('L' if direction in ('long','bullish') else 'S')]+=1
days_s=sorted(alld)

# E2T 25K TCP official rules
START=25000.0; TR=1500.0; TGT=1750.0; DLL=550.0   # EOD trailing DD, profit goal, daily loss limit
MIN_DAYS=10
def sim(series, qty, daily_floor):
    """daily_floor in $ (e.g. -550 hard rule, or -200 protective)."""
    dpt=qty*5.0
    bal=START;peak=START;maxdd=0.0;busted=None;hit=None
    worst_day=0.0
    for i,d in enumerate(days_s):
        usd=series.get(d,0.0)*dpt
        usd=max(usd, daily_floor)          # daily loss limit (stop trading)
        worst_day=min(worst_day, usd)
        bal+=usd; peak=max(peak,bal); floor=min(peak-TR,START)
        maxdd=min(maxdd,bal-peak)
        if bal<floor and busted is None: busted=(d,bal)
        if (bal-START)>=TGT and hit is None and (i+1)>=MIN_DAYS: hit=i+1
        elif (bal-START)>=TGT and hit is None: hit=max(i+1,MIN_DAYS)  # min 10 trading days
    return dict(final=bal-START,maxdd=maxdd,busted=busted,hit=hit,worst_day=worst_day)

dayC={d:dayL.get(d,0)+dayS.get(d,0) for d in days_s}
Lv=[dayL.get(d,0) for d in days_s];Sv=[dayS.get(d,0) for d in days_s];n=len(days_s)
mL=sum(Lv)/n;mS=sum(Sv)/n;cov=sum((Lv[i]-mL)*(Sv[i]-mS) for i in range(n))/n
a=statistics.pstdev(Lv);b=statistics.pstdev(Sv);corr=cov/(a*b) if a and b else 0
print(f"EVAL-STOPS re-run. {n} days, {ntr['L']} long + {ntr['S']} short signals. L-vs-S corr={corr:+.2f}")
print(f"Raw pts/day stream totals: LONG {sum(Lv):.0f}p  SHORT {sum(Sv):.0f}p")
print(f"Rules: start $25k | goal +$1750 | EOD trail $1500 | daily loss $550 | min 10 days\n")
for qty in (1,3):
    for floor in (-550.0, -200.0):
        ftag=f"daily floor ${floor:.0f}"
        print(f"===== {qty} MES (${qty*5}/pt), {ftag} =====")
        print(f"{'account':<10}{'final$':>9}{'maxDD$':>9}{'DDok?':>7}{'worstDay$':>10}{'DLLok?':>7}{'PASS(+1750)':>13}{'BUST':>16}")
        for name,series in [('LONG',dayL),('SHORT',dayS),('COMBINED(net)',dayC)]:
            r=sim(series,qty,floor)
            ddok='OK' if abs(r['maxdd'])<TR else 'BUST'
            dllok='OK' if abs(r['worst_day'])<=DLL+0.5 else 'OVER'
            bs=f"{r['busted'][0]}" if r['busted'] else "no"
            passd=f"day {r['hit']}" if r['hit'] else "no"
            print(f"{name:<10}{r['final']:>9.0f}{r['maxdd']:>9.0f}{ddok:>7}{r['worst_day']:>10.0f}{dllok:>7}{passd:>13}{bs:>16}")
        print()

# ================= REGIME ROBUSTNESS =================
print("\n\n################ REGIME ROBUSTNESS ################")
# monthly per-stream
from collections import defaultdict as _dd
bym=_dd(list)
for d in days_s: bym[d.strftime('%Y-%m')].append(d)
print("\n--- Monthly per-stream (raw pts, worst day pts, within-month maxDD pts) ---")
for name,series in [('LONG',dayL),('SHORT',dayS)]:
    print(f" {name}:")
    for m in sorted(bym):
        if m<'2026-03': continue
        ds=bym[m]; tot=sum(series.get(d,0) for d in ds)
        worst=min((series.get(d,0) for d in ds), default=0)
        cum=0;pk=0;mdd=0
        for d in ds:
            cum+=series.get(d,0);pk=max(pk,cum);mdd=min(mdd,cum-pk)
        print(f"   {m}: n={len(ds):>2}  pts={tot:>6.0f}  worstDay={worst:>5.0f}p  maxDD={mdd:>5.0f}p")

# rolling-start: begin eval on each day, run forward to PASS(+1750,>=10d) or BUST(-1500 trail)
def roll(series, qty, start_idx, floor=-200.0):
    bal=START;peak=START;days=0;mindd=0
    for i in range(start_idx, len(days_s)):
        usd=max(series.get(days_s[i],0)*qty*5.0, floor)
        bal+=usd; peak=max(peak,bal); tfloor=min(peak-TR,START)
        mindd=min(mindd,bal-peak); days+=1
        if bal<tfloor: return ('BUST',days,mindd)
        if (bal-START)>=TGT and days>=MIN_DAYS: return ('PASS',days,mindd)
    return ('INCOMPLETE',days,mindd)

print("\n--- ROLLING START (begin on each Mar-May day, -$200 floor) ---")
mar_start=[i for i,d in enumerate(days_s) if d.isoformat()>='2026-03-01']
for qty in (1,3):
    for name,series in [('LONG',dayL),('SHORT',dayS)]:
        res=[roll(series,qty,i) for i in mar_start]
        resolved=[r for r in res if r[0] in ('PASS','BUST')]
        passes=[r[1] for r in res if r[0]=='PASS']
        busts=sum(1 for r in res if r[0]=='BUST')
        inc=sum(1 for r in res if r[0]=='INCOMPLETE')
        worstdd=min((r[2] for r in res), default=0)*qty*5.0  # already $ since usd applied? no-> mindd is in $
        worstdd=min((r[2] for r in res), default=0)
        if passes:
            import statistics as st
            print(f" {qty}MES {name:<6}: starts={len(res)} PASS={len(passes)} BUST={busts} incomplete={inc} | days-to-pass min/med/max={min(passes)}/{int(st.median(passes))}/{max(passes)} | worstDD=${worstdd:.0f}")
        else:
            print(f" {qty}MES {name:<6}: starts={len(res)} PASS=0 BUST={busts} incomplete={inc} worstDD=${worstdd:.0f}")
