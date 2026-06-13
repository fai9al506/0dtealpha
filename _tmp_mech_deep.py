"""Deep dive on the two leak mechanisms the user named:
 A) Stops: trades stopped on broker (MES) that were NOT losses in portal (SPX).
    -> count, cost, and MES-range vs SPX-range in the trade window (MES vol vs tighter SL?).
 B) Trail under-capture: winners where broker << portal (e.g. #3905 portal+25 broker+6.5).
    -> which exit path, why didn't it ride the portal.
Pull full state for #3905 and #3900. Compare SPX vs MES paths.
"""
import os, sys, json, psycopg2
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
df['Date']=pd.to_datetime(df['Date']); df['month']=df['Date'].dt.strftime('%Y-%m')
jun=df[df['month']=='2026-06'].copy()
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()
ids=tuple(int(x) for x in jun['ID'].tolist())
cur.execute("""SELECT sl.id, sl.direction, sl.outcome_pnl, sl.spot, sl.ts AT TIME ZONE 'America/New_York', rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id WHERE sl.id IN %s""",(ids,))
P={}
for sid,direction,opnl,spot,et,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    bpts=None
    if f is not None and e is not None:
        f,e=float(f),float(e); bpts=(e-f) if direction in ('long','bullish') else (f-e)
    P[sid]={'dir':direction,'portal':float(opnl) if opnl is not None else None,'bpts':bpts,
            'cr':str(st.get('close_reason')),'stop_pts':st.get('stop_pts'),'fill':f,'exit':e,
            'et':str(et),'st':st}

port=dict(zip(jun['ID'], jun['P&L']))

# A) stopped on broker but portal was NOT a (full) loss
print("=== A) broker STOP_FILLED but portal positive-or-better ===")
A=[(i,P[i]) for i in P if P[i]['cr']=='stop_filled' and P[i]['bpts'] is not None and port.get(i,0) > P[i]['bpts']+2]
A.sort(key=lambda x: port.get(x[0],0)-x[1]['bpts'])
costA=0.0
print(f"{'ID':6} {'setup-dir':20} {'portal':>7} {'broker':>7} {'stop_pts':>8} {'lossgap':>8}")
for i,p in A:
    row=jun[jun['ID']==i].iloc[0]
    gap=p['bpts']-port.get(i,0); costA+=gap
    print(f"#{i:5} {row['Setup'][:11]+'-'+p['dir'][:4]:20} {port.get(i,0):>+7.1f} {p['bpts']:>+7.1f} {str(p['stop_pts']):>8} {gap:>+8.1f}")
print(f"  -> {len(A)} trades, broker-vs-portal cost = {costA:+.1f} pts (${costA*5:+.0f})  [stopped on MES, portal didn't]")

# B) winners under-captured: portal>0 and broker < portal-4
print("\n=== B) WINNERS under-captured (portal>+4, broker < portal-4) ===")
B=[(i,P[i]) for i in P if port.get(i,0)>4 and P[i]['bpts'] is not None and P[i]['bpts'] < port.get(i,0)-4]
B.sort(key=lambda x: x[1]['bpts']-port.get(x[0],0))
costB=0.0
print(f"{'ID':6} {'setup-dir':20} {'portal':>7} {'broker':>7} {'cr':22} {'undercap':>8}")
for i,p in B:
    row=jun[jun['ID']==i].iloc[0]
    gap=p['bpts']-port.get(i,0); costB+=gap
    print(f"#{i:5} {row['Setup'][:11]+'-'+p['dir'][:4]:20} {port.get(i,0):>+7.1f} {p['bpts']:>+7.1f} {p['cr']:22} {gap:>+8.1f}")
print(f"  -> {len(B)} winners, under-capture cost = {costB:+.1f} pts (${costB*5:+.0f})")

# detail on #3905 and #3900
for tid in [3905,3900]:
    if tid in P:
        print(f"\n=== #{tid} full state ===")
        st=P[tid]['st']
        for k in ['direction','fill_price','stop_pts','target_pts','target_price','current_stop',
                  'stop_fill_price','close_fill_price','trail_active','trail_only','be_triggered',
                  'max_favorable','close_reason']:
            print(f"   {k}: {st.get(k)}")

# SPX path during #3905 window vs the MES fills
print("\n=== SPX path 12:30-13:10 Jun 11 (around #3905 short fill 7310.75) ===")
cur.execute("""SELECT to_char(ts AT TIME ZONE 'America/New_York','HH24:MI:SS'), spot FROM chain_snapshots
   WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-11'
     AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '12:30' AND '13:15' ORDER BY ts""")
print("  "+"  ".join(f"{t[11:]}={s:.2f}" if False else f"{t}={float(s):.1f}" for t,s in cur.fetchall()))
conn.close()
