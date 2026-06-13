"""ACCURATE replay of the proposed design:
  - exit decision on SPX (smooth): if SPX crosses entry_SPX -/+ stop_pts -> LOSS,
    executed at the REAL ES price at that moment.
  - wide ES catastrophe stop 20 (insurance): if ES adverse>=20 first -> -20.
  - else trade survives to its portal exit -> portal P&L (incl target/trail).
Walks chain (SPX, 30s) + vps_es_range_bars (ES) per trade. entry_ES = ES market at entry.
"""
import os, sys, json, psycopg2, bisect
import pandas as pd
from datetime import timedelta
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor();cur2=conn.cursor();cur3=conn.cursor()
CAT=20.0
cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]; sb_t=[x[0] for x in sb]
def basket_at(t):
    i=bisect.bisect_right(sb_t,t)-1; return sb[i][1] if i>=0 else None
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
T=[]
for sid,ts,et,spot,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    is_long=direction in ('long','bullish')
    broker=((float(e)-float(f)) if is_long else (float(f)-float(e))) if (f is not None and e is not None) else 0.0
    p=float(port.get(sid,0)); d=float(dur.get(sid,30) or 30); sp=float(st.get('stop_pts') or 14)
    exit_ts=ts+timedelta(minutes=max(d,3))
    entry_spx=float(spot) if spot else None
    # ES bars in window (with timestamps)
    cur2.execute("""SELECT ts_start,bar_high,bar_low,bar_open,bar_close FROM vps_es_range_bars
       WHERE symbol='@ES' AND range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start""",(ts,exit_ts))
    bars=cur2.fetchall()
    # SPX series in window
    cur3.execute("""SELECT ts,spot FROM chain_snapshots WHERE spot IS NOT NULL AND ts>=%s AND ts<=%s ORDER BY ts""",(ts,exit_ts))
    spxs=cur3.fetchall()
    if not bars or entry_spx is None:
        T.append({'day':str(et)[:10],'broker':broker,'portal':p,'replay':p,
                  'al':'nd'}); continue
    entry_es=float(bars[0][3])
    # 1) find SPX-stop crossing time
    t_stop=None
    for tt,s in spxs:
        adv=(entry_spx-float(s)) if is_long else (float(s)-entry_spx)
        if adv>=sp: t_stop=tt; break
    # 2) find ES catastrophe time
    t_cat=None
    for tt,hi,lo,op,cl in bars:
        adv=(entry_es-float(lo)) if is_long else (float(hi)-entry_es)
        if adv>=CAT: t_cat=tt; break
    # decide earliest event
    def es_at(tt):
        prev=bars[0]
        for b in bars:
            if b[0]<=tt: prev=b
            else: break
        return float(prev[4])
    events=[]
    if t_stop is not None: events.append((t_stop,'stop'))
    if t_cat is not None: events.append((t_cat,'cat'))
    if events:
        events.sort(); etime,ekind=events[0]
        if ekind=='cat': replay=-CAT
        else:  # SPX stop hit -> real ES at that time
            ex=es_at(etime); replay=(ex-entry_es) if is_long else (entry_es-ex)
    else:
        replay=p   # survived -> portal outcome (incl trail/target)
    b=basket_at(et.replace(tzinfo=None))
    al='nd' if b is None else ('neu' if abs(b)<0.15 else ('conf' if (b>0)==is_long else 'contra'))
    T.append({'day':str(et)[:10],'broker':broker,'portal':p,'replay':replay,'al':al})
def split(key,filt=lambda t:True):
    return (sum(t[key] for t in T if t['day']<='2026-06-04' and filt(t))*5,
            sum(t[key] for t in T if t['day']>='2026-06-05' and filt(t))*5)
print(f"trades={len(T)}\n")
print(f"{'measure':40}{'WIN':>9}{'LOSS':>9}{'TOTAL':>9}")
for k,lab in [('broker','ACTUAL broker (baseline)'),('portal','portal (optimistic ceiling)'),
              ('replay','ACCURATE REPLAY (SPX-stop + ES exec)')]:
    w,l=split(k); print(f"{lab:40}{w:>+9.0f}{l:>+9.0f}{w+l:>+9.0f}")
wB,lB=split('replay',lambda t:t['al'] in ('conf','nd'))
print(f"{'ACCURATE REPLAY + Scheme B (0/0/1)':40}{wB:>+9.0f}{lB:>+9.0f}{wB+lB:>+9.0f}")
big=[t for t in T if t['replay']< -22]
print(f"\nsanity: losses worse than -22 (should be ~0): {len(big)}")
conn.close()
