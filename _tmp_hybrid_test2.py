"""Faithful HYBRID test on real 1-min ES.
 Phase 0 (not yet in profit): normal stop at -stop_pts (loss side UNCHANGED).
 Once ES favorable reaches activation -> trail engaged -> HYBRID:
   hold to SPX-exit (entry+duration) ignoring MES wicks (catastrophe 20 only),
   then ride ES with 2pt trailing stop. (captures case 2 + case 3)
"""
import os, sys, json, re, psycopg2
import pandas as pd
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')
CAT=20.0; RIDE=2.0
bars={}; cur_d=None
rr=re.compile(r'^\|\s*(\d{2}:\d{2})\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|')
for ln in open(r'G:/My Drive/temp/ESM26_1min_OHLC_2026-05-16_to_06-12.md',encoding='utf-8'):
    h=re.match(r'^###\s*(\d{4}-\d{2}-\d{2})',ln)
    if h: cur_d=h.group(1); bars[cur_d]=[]; continue
    m=rr.match(ln)
    if m and cur_d: bars[cur_d].append((m.group(1),float(m.group(2)),float(m.group(3)),float(m.group(4)),float(m.group(5))))
def bars_from(dt):
    d=dt.strftime('%Y-%m-%d'); out=[]
    for tm,o,h,l,c in bars.get(d,[]):
        hh,mm=tm.split(':'); bdt=datetime(dt.year,dt.month,dt.day,int(hh),int(mm))
        if bdt>=dt.replace(second=0,microsecond=0): out.append((bdt,o,h,l,c))
    return out
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor()
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.direction,
   COALESCE(sl.trail_activation,8) act, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
def sim(et,d_min,il,portal,sp,act):
    seq=bars_from(et)
    if not seq: return None
    entry=seq[0][1]; spx_exit=et+timedelta(minutes=max(d_min,1))
    trailed=False; peak=entry
    for k,(bdt,o,h,l,c) in enumerate(seq):
        fav=(h-entry) if il else (entry-l)
        adv=(entry-l) if il else (h-entry)
        if not trailed:
            if adv>=sp: return -sp            # normal stop (loss side unchanged)
            if fav>=act: trailed=True; peak=(h if il else l)  # trail engaged
        if trailed:
            peak=max(peak,h) if il else min(peak,l)
            if adv>=CAT: return -CAT
            if bdt>=spx_exit:                  # past SPX-exit -> ride 2pt
                pull=(peak-l) if il else (h-peak)
                if pull>=RIDE:
                    px=(peak-RIDE) if il else (peak+RIDE)
                    return (px-entry) if il else (entry-px)
    # ran out of bars
    if trailed:
        px=(peak-RIDE) if il else (peak+RIDE); return (px-entry) if il else (entry-px)
    px=seq[-1][4]; return (px-entry) if il else (entry-px)
res=[]
for sid,et,direction,act,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    il=direction in ('long','bullish'); broker=(float(e)-float(f)) if il else (float(f)-float(e))
    sp=float(st.get('stop_pts') or 14); p=float(port.get(sid,0)); d=float(dur.get(sid,30) or 30)
    hy=sim(et,d,il,p,sp,float(act))
    if hy is None: continue
    res.append({'day':str(et)[:10],'broker':broker,'portal':p,'hybrid':hy})
def tot(k): return (sum(r[k] for r in res if r['day']<='2026-06-04')*5, sum(r[k] for r in res if r['day']>='2026-06-05')*5)
print(f"trades={len(res)}\n{'measure':28}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
for k,lab in [('broker','ACTUAL broker'),('portal','portal (SPX)'),('hybrid','HYBRID faithful (1-min ES)')]:
    w,l=tot(k); print(f"{lab:28}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
conn.close()
