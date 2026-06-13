"""Audit the replay: where does replay differ a lot from portal in the loss window?
Likely bug: exiting at 'ES close at entry+duration' over/under-captures vs portal's
actual exit mechanism. Dump per-trade and inspect the biggest divergences."""
import os, sys, json, psycopg2
import pandas as pd
from datetime import timedelta
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
res=dict(zip(df['ID'],df['Result']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor();cur2=conn.cursor()
CAT_STOP=20.0
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-06-05' ORDER BY sl.ts""")
out=[]
for sid,ts,et,setup,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None: continue
    f=float(f); is_long=direction in ('long','bullish')
    broker=((float(e)-f) if is_long else (f-float(e))) if e is not None else 0.0
    p=float(port.get(sid,0)); d=float(dur.get(sid,30) or 30)
    exit_ts=ts+timedelta(minutes=max(d,3))
    cur2.execute("""SELECT bar_high,bar_low,bar_close FROM vps_es_range_bars
       WHERE symbol='@ES' AND range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start""",(ts,exit_ts))
    bars=cur2.fetchall()
    if not bars: replay=p; tag='no_es'
    else:
        hit=False; replay=None
        for hi,lo,cl in bars:
            adv=(f-float(lo)) if is_long else (float(hi)-f)
            if adv>=CAT_STOP: replay=-CAT_STOP; hit=True; break
        if not hit:
            replay=(float(bars[-1][2])-f) if is_long else (f-float(bars[-1][2]))
        tag=f'{len(bars)}bars{" CAT" if hit else ""}'
    out.append({'id':sid,'day':str(et)[:10],'et':str(et)[11:16],'setup':setup[:11],'dir':direction,
                'res':res.get(sid),'dur':d,'portal':p,'broker':broker,'replay':replay,'diff':replay-p,'tag':tag})
o=pd.DataFrame(out)
print(f"loss-window: portal={o['portal'].sum()*5:+.0f}  broker={o['broker'].sum()*5:+.0f}  replay={o['replay'].sum()*5:+.0f}")
print("\nBiggest replay-vs-portal divergences (where my replay disagrees with portal):")
o['absd']=o['diff'].abs()
for _,r in o.sort_values('absd',ascending=False).head(14).iterrows():
    print(f"  #{r['id']} {r['day']} {r['et']} {r['setup']:11} {r['dir']:7} {str(r['res']):5} dur={r['dur']:>4.0f}m "
          f"portal={r['portal']:>+6.1f} broker={r['broker']:>+6.1f} replay={r['replay']:>+6.1f} diff={r['diff']:>+6.1f} [{r['tag']}]")
conn.close()
