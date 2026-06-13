"""ACCURATE, LOOK-AHEAD-FREE hybrid core:
 - Trail on the SMOOTH SPX path (chain_snapshots) = a real causal trailing stop (no peeking).
   * before activation: stop at entry_SPX -/+ stop_pts
   * after SPX favorable >= activation: trailing stop = SPX_peak -/+ gap
 - EXECUTE at the real ES price (1-min ESM26) at the exit minute.
 This captures 'don't exit on ES wicks' (case 3) WITHOUT look-ahead and WITHOUT the
 untestable 2pt ride. Loss side uses SPX stop (its cost is included honestly).
"""
import os, sys, json, re, psycopg2
import pandas as pd
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')
# parse 1-min ES
esb={}; cur_d=None
rr=re.compile(r'^\|\s*(\d{2}:\d{2})\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|')
for ln in open(r'G:/My Drive/temp/ESM26_1min_OHLC_2026-05-16_to_06-12.md',encoding='utf-8'):
    h=re.match(r'^###\s*(\d{4}-\d{2}-\d{2})',ln)
    if h: cur_d=h.group(1); esb[cur_d]={}; continue
    m=rr.match(ln)
    if m and cur_d: esb[cur_d][m.group(1)]=(float(m.group(2)),float(m.group(3)),float(m.group(4)),float(m.group(5)))
def es_at(dt):
    d=dt.strftime('%Y-%m-%d'); t=dt.strftime('%H:%M')
    day=esb.get(d,{})
    if t in day: return day[t][3]  # close of that minute
    # nearest earlier minute same day
    ts=sorted(day.keys()); best=None
    for k in ts:
        if k<=t: best=k
        else: break
    return day[best][3] if best else None
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor();cur2=conn.cursor()
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.direction,
   COALESCE(sl.trail_activation,8) act, COALESCE(sl.trail_gap,5) gap, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
trades=cur.fetchall()
def spx_path(et):
    cur2.execute("""SELECT (ts AT TIME ZONE 'America/New_York'), spot FROM chain_snapshots
       WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')>=%s
       AND (ts AT TIME ZONE 'America/New_York')::date=%s ORDER BY ts""",(et,et.date()))
    return cur2.fetchall()
def sim(et,entry_spx,entry_es,il,sp,act,gap,audit=False):
    path=spx_path(et)
    if not path or entry_es is None: return None
    trailed=False; peak=entry_spx; exit_t=None
    for tt,s in path:
        s=float(s); adv=(entry_spx-s) if il else (s-entry_spx); fav=(s-entry_spx) if il else (entry_spx-s)
        if not trailed:
            if adv>=sp: exit_t=tt; break
            if fav>=act: trailed=True; peak=s
        else:
            peak=max(peak,s) if il else min(peak,s)
            pull=(peak-s) if il else (s-peak)
            if pull>=gap: exit_t=tt; break
    if exit_t is None: exit_t=path[-1][0]
    ex=es_at(exit_t)
    if ex is None: return None
    pnl=(ex-entry_es) if il else (entry_es-ex)
    if audit: print(f"     entry_spx={entry_spx} entry_es={entry_es} exit_t={exit_t} es_exit={ex} pnl={pnl:+.1f} (sp{sp}/act{act}/gap{gap})")
    return pnl
res=[]; audited=0
for sid,et,spot,direction,act,gap,state in trades:
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None or spot is None: continue
    il=direction in ('long','bullish'); broker=(float(e)-float(f)) if il else (float(f)-float(e))
    sp=float(st.get('stop_pts') or 14); p=float(port.get(sid,0))
    hy=sim(et,float(spot),float(f),il,sp,float(act),float(gap))
    if hy is None: continue
    res.append({'id':sid,'day':str(et)[:10],'broker':broker,'portal':p,'hybrid':hy})
# AUDIT 4 sample winners
print("=== AUDIT sample trades (verify exit is causal & sane) ===")
for sid,et,spot,direction,act,gap,state in trades:
    if sid not in [3038,3905,3900,3629]: continue
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); il=direction in ('long','bullish'); sp=float(st.get('stop_pts') or 14)
    print(f"  #{sid} {str(et)[:16]} {direction} portal={port.get(sid):+.1f}")
    sim(et,float(spot),float(f),il,sp,float(act),float(gap),audit=True)
def tot(k): return (sum(r[k] for r in res if r['day']<='2026-06-04')*5, sum(r[k] for r in res if r['day']>='2026-06-05')*5)
print(f"\ntrades={len(res)}\n{'measure':32}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
for k,lab in [('broker','ACTUAL broker'),('portal','portal (SPX sim)'),('hybrid','HYBRID core (SPX-trail/ES-exec)')]:
    w,l=tot(k); print(f"{lab:32}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
conn.close()
