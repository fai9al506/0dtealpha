"""Stress-test the user's HYBRID trail exit on REAL 1-min ESM26 data.
Rule:
 - Hold from entry to the SPX-exit moment (entry+portal Duration); during the hold,
   IGNORE MES wicks (case 3) — only a WIDE catastrophe stop (20pt) can exit.
 - At the SPX-exit moment:
     * if portal was a WIN (SPX trail/target, profit) -> ride ES with a 2pt trailing
       stop from its running peak (case 2: capture MES over-run). Exit on 2pt pullback.
     * if portal was a LOSS (SPX hit stop) -> exit at ES now.
 - entry_ES taken from the 1-min ES bar (avoids stale recorded fills).
Compare: actual broker vs portal vs HYBRID. Also a 'case3-only' variant (no ride).
"""
import os, sys, json, re, psycopg2
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')
NY=ZoneInfo("America/New_York")
CAT=20.0; RIDE_STOP=2.0

# ---- parse 1-min ES md ----
bars={}  # date(str) -> list of (HH:MM, o,h,l,c)
cur_d=None
rowre=re.compile(r'^\|\s*(\d{2}:\d{2})\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|')
with open(r'G:/My Drive/temp/ESM26_1min_OHLC_2026-05-16_to_06-12.md',encoding='utf-8') as fh:
    for ln in fh:
        h=re.match(r'^###\s*(\d{4}-\d{2}-\d{2})',ln)
        if h: cur_d=h.group(1); bars[cur_d]=[]; continue
        m=rowre.match(ln)
        if m and cur_d:
            bars[cur_d].append((m.group(1),float(m.group(2)),float(m.group(3)),float(m.group(4)),float(m.group(5))))
# index by datetime for fast forward-walk
series={}  # date -> dict minute_index list sorted
for d,lst in bars.items():
    series[d]=lst  # already in order

def bars_from(dt):
    """return list of (minute_dt, o,h,l,c) from dt forward within same day."""
    d=dt.strftime('%Y-%m-%d'); out=[]
    if d not in series: return out
    for tm,o,h,l,c in series[d]:
        hh,mm=tm.split(':'); bdt=datetime(dt.year,dt.month,dt.day,int(hh),int(mm))
        if bdt>=dt.replace(second=0,microsecond=0): out.append((bdt,o,h,l,c))
    return out

df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor()
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
def sim(entry_dt,d_min,is_long,portal):
    seq=bars_from(entry_dt)
    if not seq: return None,None
    entry=seq[0][1]  # open of entry minute
    exit_idx=None; peak=entry
    # phase 1: hold to SPX-exit (entry+d_min); catastrophe only
    spx_exit_dt=(entry_dt+timedelta(minutes=max(d_min,1)))
    i=0
    for bdt,o,h,l,c in seq:
        adv=(entry-l) if is_long else (h-entry)
        if adv>=CAT:  # catastrophe
            return -CAT, entry
        if bdt>=spx_exit_dt: break
        i+=1
    # at SPX-exit
    if portal<=0:  # loss -> exit at ES now (close of this bar)
        px=seq[min(i,len(seq)-1)][4]
        return ((px-entry) if is_long else (entry-px)), entry
    # win -> ride ES with 2pt trailing stop from peak
    peak = seq[min(i,len(seq)-1)][4]
    for bdt,o,h,l,c in seq[i:]:
        peak = max(peak,h) if is_long else min(peak,l)
        # 2pt pullback from peak?
        pull = (peak-l) if is_long else (h-peak)
        if pull>=RIDE_STOP:
            px = (peak-RIDE_STOP) if is_long else (peak+RIDE_STOP)
            return ((px-entry) if is_long else (entry-px)), entry
    # EOD: exit at last close
    px=seq[-1][4]
    return ((px-entry) if is_long else (entry-px)), entry
res=[]
for sid,et,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    il=direction in ('long','bullish'); broker=(float(e)-float(f)) if il else (float(f)-float(e))
    p=float(port.get(sid,0)); d=float(dur.get(sid,30) or 30)
    hy,entry=sim(et,d,il,p)
    if hy is None: continue
    res.append({'day':str(et)[:10],'broker':broker,'portal':p,'hybrid':hy})
def tot(k):
    return (sum(r[k] for r in res if r['day']<='2026-06-04')*5, sum(r[k] for r in res if r['day']>='2026-06-05')*5)
print(f"trades simulated: {len(res)}\n")
print(f"{'measure':28}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
for k,lab in [('broker','ACTUAL broker'),('portal','portal (SPX)'),('hybrid','HYBRID (1-min ES test)')]:
    w,l=tot(k); print(f"{lab:28}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
conn.close()
