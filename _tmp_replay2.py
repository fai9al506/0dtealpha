"""CLEAN replay: use consistent ES-MARKET prices (from vps_es_range_bars) for both
entry and exit, ignoring possibly-stale recorded fill_price. Isolates the exit-timing
fix. entry_ES = first bar at/after signal; exit_ES = last bar at signal+duration;
catastrophe stop 20 vs entry_ES. Validate entry_ES sane vs SPX (basis 3-15)."""
import os, sys, json, psycopg2, bisect
import pandas as pd
from datetime import timedelta
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor();cur2=conn.cursor()
CAT=20.0
cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]; sb_t=[x[0] for x in sb]
def basket_at(t):
    i=bisect.bisect_right(sb_t,t)-1; return sb[i][1] if i>=0 else None
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
T=[]; bad_fill=0; no_es=0
for sid,ts,et,spot,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    is_long=direction in ('long','bullish')
    broker=((float(e)-float(f)) if is_long else (float(f)-float(e))) if (f is not None and e is not None) else 0.0
    p=float(port.get(sid,0)); d=float(dur.get(sid,30) or 30)
    exit_ts=ts+timedelta(minutes=max(d,3))
    cur2.execute("""SELECT bar_high,bar_low,bar_close,bar_open FROM vps_es_range_bars
       WHERE symbol='@ES' AND range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start""",(ts,exit_ts))
    bars=cur2.fetchall()
    if not bars: no_es+=1; replay=p; entry_es=None
    else:
        entry_es=float(bars[0][3])  # bar_open of first bar = ES market at entry
        # data-quality check: recorded fill vs ES market
        if f is not None and abs(float(f)-entry_es)>25: bad_fill+=1
        hit=False; replay=None
        for hi,lo,cl,op in bars:
            adv=(entry_es-float(lo)) if is_long else (float(hi)-entry_es)
            if adv>=CAT: replay=-CAT; hit=True; break
        if not hit:
            ex=float(bars[-1][2])
            replay=(ex-entry_es) if is_long else (entry_es-ex)
    b=basket_at(et.replace(tzinfo=None))
    al='no_data' if b is None else ('neutral' if abs(b)<0.15 else ('confirm' if (b>0)==is_long else 'contradict'))
    T.append({'day':str(et)[:10],'broker':broker,'portal':p,'replay':replay,'al':al})
print(f"trades={len(T)}  bad_fill(>25pt off ES)={bad_fill}  no_es={no_es}\n")
def split(key,filt=lambda t:True):
    return (sum(t[key] for t in T if t['day']<='2026-06-04' and filt(t))*5,
            sum(t[key] for t in T if t['day']>='2026-06-05' and filt(t))*5)
print(f"{'measure':36}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
for k,lab in [('broker','ACTUAL broker'),('portal','portal (SPX, optimistic)'),('replay','REPLAY (clean ES path)')]:
    w,l=split(k); print(f"{lab:36}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
wB,lB=split('replay', lambda t:t['al'] in ('confirm','no_data'))
print(f"{'REPLAY + Scheme B (0/0/1)':36}{wB:>+9.0f}{lB:>+9.0f}{wB+lB:>+9.0f}")
# sanity: any |replay|>25 (impossible-ish)?
big=[t for t in T if abs(t['replay'])>22]
print(f"\nsanity: {len(big)} trades with |replay|>22pt (should be ~0 given CAT=20)")
conn.close()
