"""AUDIT the claims before trusting them.
1) Reconcile per-lid GROSS total vs tsrt_daily_stmt NET (commission/FIFO gap).
2) Loss-window scheme-B: list every confirmed/no_data trade Jun5-12 (is +29 real or 3 trades?).
3) After-fix gain: how many trades drive it (broad or 1-2 outliers?).
"""
import os, sys, json, psycopg2, bisect
import pandas as pd
from datetime import timedelta
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor();cur2=conn.cursor()
cur.execute("SELECT sum(net),sum(gross),sum(n_trades) FROM tsrt_daily_stmt WHERE day>='2026-05-19'")
net,gross,nrt=cur.fetchone()
print(f"1) tsrt_daily_stmt post-V16: NET={float(net):+.0f}  GROSS={float(gross):+.0f}  RTs={nrt}")
cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]; sb_t=[x[0] for x in sb]
def basket_at(t):
    i=bisect.bisect_right(sb_t,t)-1; return sb[i][1] if i>=0 else None
def spx_mae(ts,d,entry,is_long):
    if entry is None: return None
    cur2.execute("SELECT min(spot),max(spot) FROM chain_snapshots WHERE spot IS NOT NULL AND ts>=%s AND ts<=%s",
                 (ts,ts+timedelta(minutes=max(d,3)+2)))
    lo,hi=cur2.fetchone()
    return None if lo is None else ((entry-float(lo)) if is_long else (float(hi)-entry))
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.setup_name, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
T=[]
for sid,ts,et,spot,setup,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    is_long=direction in ('long','bullish')
    broker=(float(e)-float(f)) if is_long else (float(f)-float(e))
    p=float(port.get(sid,0)); cr=str(st.get('close_reason')); sp=float(st.get('stop_pts') or 14)
    if cr=='outcome_close_trail_market_exit': fixed=p; ftag='trail_fix'
    elif cr=='stop_filled':
        m=spx_mae(ts,float(dur.get(sid,30) or 30),float(spot) if spot else None,is_long)
        if m is not None and m<sp: fixed=p; ftag='stop_survives'
        else: fixed=broker; ftag=''
    else: fixed=broker; ftag=''
    b=basket_at(et.replace(tzinfo=None))
    al='no_data' if b is None else ('neutral' if abs(b)<0.15 else ('confirm' if (b>0)==is_long else 'contradict'))
    T.append({'id':sid,'day':str(et)[:10],'et':str(et)[11:16],'setup':setup,'dir':direction,
              'broker':broker,'fixed':fixed,'al':al,'ftag':ftag,'cr':cr})
bro=sum(t['broker'] for t in T)*5
print(f"   my per-lid GROSS = {bro:+.0f} vs tsrt NET {float(net):+.0f}: gap ~{bro-float(net):+.0f} (={nrt} RTs * ~$1 comm + FIFO/multi-lid)")
print("\n2) Scheme B (confirm+no_data) trades in LOSS window Jun5-12:")
lb=[t for t in T if t['day']>='2026-06-05' and t['al'] in ('confirm','no_data')]
print(f"   {len(lb)} trades. broker=${sum(t['broker'] for t in lb)*5:+.0f}  fixed=${sum(t['fixed'] for t in lb)*5:+.0f}")
for t in lb:
    print(f"   #{t['id']} {t['day']} {t['et']} {t['setup'][:11]:11} {t['dir']:7} {t['al']:8} brk={t['broker']*5:>+6.0f} fix={t['fixed']*5:>+6.0f} {t['ftag']}")
dr=[t for t in T if t['day']>='2026-06-05' and t['al'] in ('contradict','neutral')]
print(f"   DROPPED (contradict+neutral) Jun5-12: {len(dr)} trades, broker=${sum(t['broker'] for t in dr)*5:+.0f}")
print("\n3) After-fix gain drivers:")
chg=[t for t in T if abs(t['fixed']-t['broker'])>0.01]
gain=sum(t['fixed']-t['broker'] for t in chg)*5
print(f"   {len(chg)} trades changed, total swing ${gain:+.0f}")
chg.sort(key=lambda t:-(t['fixed']-t['broker']))
for t in chg[:8]:
    print(f"   #{t['id']} {t['day']} {t['setup'][:11]:11} {t['ftag']:13} {t['broker']*5:>+6.0f}->{t['fixed']*5:>+6.0f} (Δ${(t['fixed']-t['broker'])*5:+.0f})")
print(f"   top-3 = ${sum((t['fixed']-t['broker']) for t in chg[:3])*5:+.0f} of ${gain:+.0f}")
conn.close()
