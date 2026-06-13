"""Load the authoritative V16 portal trade log (Excel), verify monthly totals,
then join by ID to broker fills (real_trade_orders) to get per-trade portal-vs-broker gap.
"""
import os, sys, json, psycopg2
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
XL=r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx'
df=pd.read_excel(XL, sheet_name='trade_log_2026-06-13')
df['Date']=pd.to_datetime(df['Date'])
df['month']=df['Date'].dt.strftime('%Y-%m')
print("rows:", len(df), "date span:", df['Date'].min().date(), "->", df['Date'].max().date())

print("\n=== monthly P&L from Excel (verify vs user: Feb152.6 Mar1499.3 Apr985.7 May769.4 Jun-12.3) ===")
print(df.groupby('month')['P&L'].agg(['sum','count']).round(1).to_string())

# June rows
jun=df[df['month']=='2026-06'].copy()
print(f"\nJune V16 portal rows: {len(jun)}, P&L sum {jun['P&L'].sum():.1f}")
print("June by Setup x Direction (portal):")
print(jun.groupby(['Setup','Direction'])['P&L'].agg(['sum','count','mean']).round(1).to_string())

# Join to broker fills by ID
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()
ids=tuple(int(x) for x in jun['ID'].tolist())
cur.execute("""SELECT sl.id, sl.direction, sl.outcome_pnl, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.id IN %s""",(ids,))
brk={}
for sid,direction,opnl,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    bpts=None
    if f is not None and e is not None:
        f,e=float(f),float(e); bpts=(e-f) if direction in ('long','bullish') else (f-e)
    brk[sid]={'bpts':bpts,'cr':str(st.get('close_reason')),'stop_pts':st.get('stop_pts'),
              'fill':f if f is not None else None,
              'stop_fill':st.get('stop_fill_price'),'close_fill':st.get('close_fill_price'),
              'target_price':st.get('target_price'),'current_stop':st.get('current_stop'),
              'dir':direction}

jun['placed']=jun['ID'].apply(lambda i: i in brk)
print(f"\nJune V16 portal trades that were PLACED on broker: {jun['placed'].sum()} / {len(jun)}")
placed=jun[jun['placed']].copy()
placed['bpts']=placed['ID'].apply(lambda i: brk[i]['bpts'])
placed['cr']=placed['ID'].apply(lambda i: brk[i]['cr'])
placed=placed[placed['bpts'].notna()].copy()
placed['gap']=placed['bpts']-placed['P&L']  # broker minus portal (pts)
print(f"\nPLACED June trades w/ broker fill: {len(placed)}")
print(f"  portal pts sum = {placed['P&L'].sum():+.1f}")
print(f"  broker pts sum = {placed['bpts'].sum():+.1f}")
print(f"  total gap (broker-portal) = {placed['gap'].sum():+.1f} pts  (${placed['gap'].sum()*5:+.0f})")

# bucket the gap by close_reason
print("\n=== gap by broker close_reason ===")
g=placed.groupby('cr').agg(n=('ID','count'),portal=('P&L','sum'),broker=('bpts','sum'),gap=('gap','sum')).round(1)
print(g.to_string())

# worst gaps trade by trade
print("\n=== 15 worst capture gaps (broker much worse than portal) ===")
w=placed.sort_values('gap').head(15)
for _,r in w.iterrows():
    b=brk[r['ID']]
    print(f"  #{r['ID']} {r['Date'].date()} {r['Time (ET)']} {r['Setup'][:12]:12} {r['Direction']:7} "
          f"portal={r['P&L']:>+6.1f} broker={r['bpts']:>+6.1f} gap={r['gap']:>+6.1f}  cr={b['cr']:14} "
          f"stop_pts={b['stop_pts']} fill={b['fill']} stopfill={b['stop_fill']} closefill={b['close_fill']}")
conn.close()
