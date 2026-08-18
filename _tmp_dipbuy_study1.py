# -*- coding: utf-8 -*-
"""Dip-Buy v1 reconstruction on clean 1-min SPX + baseline validation + SL/TP grid.
Faithful to dipbuy_detector._detect (v1): dip 8pt off session high, confirm +4pt
bounce, one/day, window 9:30-11:30 ET, enter at confirm price. Exit T/S/EOD(16:00)."""
import os, pickle, psycopg2
from collections import defaultdict
from datetime import time as dtime
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

DIP=8.0; CONFIRM=4.0; WIN_S=dtime(9,30); WIN_E=dtime(11,30); EOD=dtime(16,0)

cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d,
  (ts AT TIME ZONE 'America/New_York') et, bar_open,bar_high,bar_low,bar_close
  FROM spx_ohlc_1m WHERE bar_open IS NOT NULL ORDER BY ts""")
days=defaultdict(list)
for d,et,o,h,l,c in cur.fetchall():
    days[str(d)].append((et,float(o),float(h),float(l),float(c)))
daylist=sorted(days)
prev_close={}
for i,d in enumerate(daylist):
    prev_close[d]=days[daylist[i-1]][-1][4] if i>0 else None

def reconstruct(d):
    # close-based (mimics live 30s spot point-sampling; 1-min close = closest proxy)
    bars=days[d]; sess_high=None; in_dip=False; local_low=None; local_low_ts=None; sh_ts=None
    for et,o,h,l,c in bars:
        t=et.time()
        if t<WIN_S or t>WIN_E:
            if t>WIN_E: break
            continue
        if sess_high is None or c>sess_high: sess_high=c; sh_ts=et
        if not in_dip:
            if c<=sess_high-DIP:
                in_dip=True; local_low=c; local_low_ts=et
        else:
            if c<local_low: local_low=c; local_low_ts=et
            elif c>=local_low+CONFIRM:
                entry=c
                return dict(date=d, entry_ts=et, entry=entry, sess_high=sess_high,
                            local_low=local_low, dip_depth=sess_high-local_low,
                            mins=(et.hour*60+et.minute)-(9*60+30), sh_ts=sh_ts,
                            local_low_ts=local_low_ts, prior_close=prev_close[d],
                            vs_prior_close=(entry-prev_close[d]) if prev_close[d] else None)
    return None

def sim_outcome(sig, T, S):
    # close-based outcome (point-sampling, matches live spot-checking)
    bars=days[sig['date']]; e=sig['entry']; et0=sig['entry_ts']; mfe=0.0; mae=0.0
    for et,o,h,l,c in bars:
        if et<=et0: continue
        mfe=max(mfe,c-e); mae=min(mae,c-e)
        if c<=e-S: return (round(c-e,2),"stop",mfe,mae)
        if c>=e+T: return (round(c-e,2),"target",mfe,mae)
        if et.time()>=EOD: return (round(c-e,2),"eod",mfe,mae)
    return (round(bars[-1][4]-e,2),"eod",mfe,mae)

sigs=[s for s in (reconstruct(d) for d in daylist) if s]
print(f"reconstructed v1 signals: {len(sigs)} over {daylist[0]}..{daylist[-1]}")
for s in sigs:
    pnl,reason,mfe,mae=sim_outcome(s,10,8); s.update(pnl=pnl,reason=reason,mfe=mfe,mae=mae,win=(pnl>0))
n=len(sigs); wins=sum(s['win'] for s in sigs); tot=sum(s['pnl'] for s in sigs)
print(f"BASELINE T10/S8: n={n} WR={wins/n*100:.1f}% PnL={tot:+.1f} avg={tot/n:+.2f}")

cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York'), outcome_result, ROUND(outcome_pnl::numeric,1), ROUND(spot::numeric,1)
  FROM setup_log WHERE setup_name='Dip-Buy' AND outcome_result IS NOT NULL ORDER BY ts""")
live={str(r[0]):(r[1],float(r[2]),float(r[3])) for r in cur.fetchall()}
print("\nVALIDATION vs live:")
match=0; comp=0
for s in sigs:
    if s['date'] in live:
        comp+=1; lo,lp,le=live[s['date']]
        if (lo=="WIN")==(s['pnl']>0): match+=1
print(f"  direction match: {match}/{comp} (recon vs live on overlapping dates)")

def maxdd(seq):
    cum=peak=dd=0
    for x in seq: cum+=x; peak=max(peak,cum); dd=min(dd,cum-peak)
    return dd
print("\nSL/TP GRID (n=%d, 1-min sim). cell = WR%% / PnL / expectancy"%n)
Ts=[6,8,10,12,15,20]; Ss=[6,8,10,12,15]
print(f"  {'T/S':<6}"+"".join(f"{('S'+str(s)):>17}" for s in Ss))
best=None
for T in Ts:
    cells=[]
    for S in Ss:
        rs=[sim_outcome(sig,T,S) for sig in sigs]; pnls=[r[0] for r in rs]
        w=sum(1 for p in pnls if p>0); tot=sum(pnls)
        cells.append(f"{w/n*100:>3.0f}%/{tot:>5.0f}/{tot/n:>4.1f}")
        if best is None or tot>best[1]: best=((T,S),tot,w/n*100,maxdd(pnls))
    print(f"  T{T:<5}"+"".join(f"{c:>17}" for c in cells))
print(f"\nbest PnL: T{best[0][0]}/S{best[0][1]} = PnL {best[1]:.0f}, WR {best[2]:.0f}%, maxDD {best[3]:.0f}")
pickle.dump((sigs,days,daylist),open("_tmp_dipbuy_sigs.pkl","wb"))
print(f"saved {len(sigs)} signals")
conn.close()
