"""CONSERVATIVE hybrid test (adverse-first within each 1-min bar -> honest/pessimistic).
Test ride-stop widths 2/3/5pt to show how sensitive it is to sub-bar resolution."""
import os, sys, json, re, psycopg2
import pandas as pd
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')
CAT=20.0
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
rows=[]
for sid,et,direction,act,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    il=direction in ('long','bullish'); broker=(float(e)-float(f)) if il else (float(f)-float(e))
    rows.append({'id':sid,'et':et,'il':il,'broker':broker,'sp':float(st.get('stop_pts') or 14),
                 'p':float(port.get(sid,0)),'d':float(dur.get(sid,30) or 30),'act':float(act),'day':str(et)[:10]})
def sim(r,RIDE):
    seq=bars_from(r['et'])
    if not seq: return None
    entry=seq[0][1]; spx_exit=r['et']+timedelta(minutes=max(r['d'],1)); il=r['il']
    trailed=False; peak=entry
    for bdt,o,h,l,c in seq:
        adv=(entry-l) if il else (h-entry)
        if not trailed:
            if adv>=r['sp']: return -r['sp']          # normal stop first (conservative)
            fav=(h-entry) if il else (entry-l)
            if fav>=r['act']: trailed=True; peak=(h if il else l)
            continue
        # trailed: adverse-FIRST -> check ride-stop vs low/high BEFORE new peak
        if adv>=CAT: return -CAT
        pull=(peak-l) if il else (h-peak)
        if pull>=RIDE and bdt>=spx_exit:           # only ride-exit after SPX-exit window
            px=(peak-RIDE) if il else (peak+RIDE); return (px-entry) if il else (entry-px)
        # before SPX-exit, ignore MES wicks (hold); update peak
        peak=max(peak,h) if il else min(peak,l)
    if trailed:
        px=(peak-RIDE) if il else (peak+RIDE); return (px-entry) if il else (entry-px)
    px=seq[-1][4]; return (px-entry) if il else (entry-px)
print(f"{'scenario':30}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
bw=sum(r['broker'] for r in rows if r['day']<='2026-06-04')*5; bl=sum(r['broker'] for r in rows if r['day']>='2026-06-05')*5
print(f"{'ACTUAL broker':30}{bw:>+9.0f}{bl:>+9.0f}{bw+bl:>+9.0f}")
pw=sum(r['p'] for r in rows if r['day']<='2026-06-04')*5; pl=sum(r['p'] for r in rows if r['day']>='2026-06-05')*5
print(f"{'portal (SPX)':30}{pw:>+9.0f}{pl:>+9.0f}{pw+pl:>+9.0f}")
for RIDE in [2,3,5]:
    w=l=0
    for r in rows:
        hy=sim(r,RIDE)
        if hy is None: continue
        if r['day']<='2026-06-04': w+=hy*5
        else: l+=hy*5
    print(f"{'HYBRID conservative ride='+str(RIDE)+'pt':30}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
conn.close()
