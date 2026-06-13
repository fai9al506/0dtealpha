"""Test the MES trailing-ride on 1-min OHLC, bracketing the intra-bar ordering:
 - DIP-FIRST (conservative floor): each bar, test stop on low BEFORE lifting peak with high.
 - RIP-FIRST (optimistic ceiling): lift peak with high BEFORE testing stop on low.
The gap between them = the part that genuinely needs tick data.
Pure ES trail: normal stop -sp until +activation, then trail at peak-/+gap. No look-ahead.
"""
import os, sys, json, re, psycopg2
import pandas as pd
from datetime import datetime
sys.stdout.reconfigure(encoding='utf-8')
esb={}; cur_d=None
rr=re.compile(r'^\|\s*(\d{2}:\d{2})\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|')
for ln in open(r'G:/My Drive/temp/ESM26_1min_OHLC_2026-05-16_to_06-12.md',encoding='utf-8'):
    h=re.match(r'^###\s*(\d{4}-\d{2}-\d{2})',ln)
    if h: cur_d=h.group(1); esb[cur_d]=[]; continue
    m=rr.match(ln)
    if m and cur_d: esb[cur_d].append((m.group(1),float(m.group(2)),float(m.group(3)),float(m.group(4)),float(m.group(5))))
def seq_from(et):
    d=et.strftime('%Y-%m-%d'); t=et.strftime('%H:%M'); out=[]
    for tm,o,h,l,c in esb.get(d,[]):
        if tm>=t: out.append((o,h,l,c))
    return out
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
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
    rows.append({'et':et,'il':il,'broker':broker,'sp':float(st.get('stop_pts') or 14),
                 'act':float(act),'day':str(et)[:10]})
def sim(r,gap,rip_first):
    seq=seq_from(r['et'])
    if not seq: return None
    entry=seq[0][0]; il=r['il']; trailed=False; peak=entry
    for o,h,l,c in seq:
        adv=(entry-l) if il else (h-entry)
        if not trailed:
            if adv>=r['sp']: return -r['sp']
            fav=(h-entry) if il else (entry-l)
            if fav>=r['act']: trailed=True; peak=(h if il else l)
            continue
        if rip_first:
            peak=max(peak,h) if il else min(peak,l)
            pull=(peak-l) if il else (h-peak)
            if pull>=gap: px=(peak-gap) if il else (peak+gap); return (px-entry) if il else (entry-px)
        else:  # dip-first (conservative)
            pull=(peak-l) if il else (h-peak)
            if pull>=gap: px=(peak-gap) if il else (peak+gap); return (px-entry) if il else (entry-px)
            peak=max(peak,h) if il else min(peak,l)
    px=seq[-1][3]; return (px-entry) if il else (entry-px)
bw=sum(r['broker'] for r in rows if r['day']<='2026-06-04')*5; bl=sum(r['broker'] for r in rows if r['day']>='2026-06-05')*5
print(f"{'scenario':34}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
print(f"{'ACTUAL broker':34}{bw:>+9.0f}{bl:>+9.0f}{bw+bl:>+9.0f}")
for gap in [2,3,5]:
    for rip,lab in [(False,'DIP-first (floor)'),(True,'RIP-first (ceiling)')]:
        w=l=0
        for r in rows:
            hy=sim(r,gap,rip)
            if hy is None: continue
            if r['day']<='2026-06-04': w+=hy*5
            else: l+=hy*5
        print(f"{('trail gap='+str(gap)+'pt '+lab):34}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
conn.close()
