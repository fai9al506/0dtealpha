"""HONEST REPLAY of the SPX-based-exit fix against the REAL ES path.
For each post-V16 placed trade:
  - entry = actual ES fill price, entry time = signal time
  - exit time = entry + portal Duration (the SPX-logic-decided hold)
  - exit price = REAL ES price at that moment (vps_es_range_bars) -> honest basis, not portal proxy
  - catastrophe stop = 20 ES pts: if ES adverse excursion >= 20 before exit, capped at -20
Compare: actual broker vs portal vs REPLAY. Then apply scheme B (confirmed-only).
"""
import os, sys, json, psycopg2, bisect
import pandas as pd
from datetime import timedelta
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor();cur2=conn.cursor()
CAT_STOP=20.0  # wide catastrophe stop (ES pts)

cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]; sb_t=[x[0] for x in sb]
def basket_at(t):
    i=bisect.bisect_right(sb_t,t)-1; return sb[i][1] if i>=0 else None

def es_path(ts,exit_ts):
    cur2.execute("""SELECT bar_high,bar_low,bar_close,ts_start FROM vps_es_range_bars
       WHERE symbol='@ES' AND range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start""",(ts,exit_ts))
    return cur2.fetchall()

cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
rows=cur.fetchall()
T=[]; no_es=0
for sid,ts,et,direction,state in rows:
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None: continue
    f=float(f); is_long=direction in ('long','bullish')
    broker=((float(e)-f) if is_long else (f-float(e))) if e is not None else 0.0
    p=float(port.get(sid,0)); d=float(dur.get(sid,30) or 30)
    # cap exit at 16:00 ET -> compute exit_ts in UTC
    exit_ts=ts+timedelta(minutes=max(d,3))
    bars=es_path(ts,exit_ts)
    if not bars:
        replay=p; no_es+=1   # fall back to portal if no ES bars
    else:
        # catastrophe check + exit at last bar close
        hit=False; rep=None
        for hi,lo,cl,_ in bars:
            adverse = (f-float(lo)) if is_long else (float(hi)-f)
            if adverse>=CAT_STOP:
                rep=-CAT_STOP; hit=True; break
        if not hit:
            last_close=float(bars[-1][2])
            rep=(last_close-f) if is_long else (f-last_close)
        replay=rep
    b=basket_at(et.replace(tzinfo=None))
    al='no_data' if b is None else ('neutral' if abs(b)<0.15 else ('confirm' if (b>0)==is_long else 'contradict'))
    T.append({'day':str(et)[:10],'broker':broker,'portal':p,'replay':replay,'al':al})

def split(key,filt=lambda t:True):
    w=sum(t[key] for t in T if t['day']<='2026-06-04' and filt(t))*5
    l=sum(t[key] for t in T if t['day']>='2026-06-05' and filt(t))*5
    return w,l
print(f"post-V16 trades={len(T)} (no ES bars, fell back to portal: {no_es})\n")
print(f"{'measure':34}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
for k,lab in [('broker','ACTUAL broker (baseline)'),('portal','portal (SPX smooth, optimistic)'),
              ('replay','REPLAY (SPX-exit on real ES path)')]:
    w,l=split(k); print(f"{lab:34}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
print()
# scheme B on replay (confirmed + no_data only)
wB,lB=split('replay', lambda t: t['al'] in ('confirm','no_data'))
print(f"{'REPLAY + Scheme B (0/0/1)':34}{wB:>+9.0f}{lB:>+9.0f}{wB+lB:>+9.0f}")
# commission haircut estimate
nB=sum(1 for t in T if t['al'] in ('confirm','no_data'))
nAll=len(T)
print(f"\nTrades: all={nAll} (comm ~${nAll}), schemeB={nB} (comm ~${nB})")
print(f"NET est: replay-all {split('replay')[0]+split('replay')[1]-nAll:+.0f} | replay-B {wB+lB-nB:+.0f}")
conn.close()
