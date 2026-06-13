"""A) Trail-bug (trail_market_exit) total cost since post-V16 (May19+).
   B) Price-space check for #3900: is fill_price SPX or ES? + swing highs both spaces.
"""
import os, sys, json, psycopg2
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'], df['P&L']))
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()

# A) all trail_market_exit since May19, broker vs portal
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
tot=0.0; n=0
print("=== A) trail_market_exit trades since May19 (the bug) ===")
print(f"{'ID':6} {'date':11} {'setup-dir':18} {'portal':>7} {'broker':>7} {'lost':>7}")
for sid,et,setup,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    if str(st.get('close_reason'))!='outcome_close_trail_market_exit': continue
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    f,e=float(f),float(e); b=(e-f) if direction in ('long','bullish') else (f-e)
    p=float(port.get(sid,0)); lost=b-p; tot+=lost; n+=1
    print(f"#{sid:5} {str(et)[:10]:11} {setup[:11]+'-'+direction[:3]:18} {p:>+7.1f} {b:>+7.1f} {lost:>+7.1f}")
print(f"  -> {n} trades, bug cost (broker-portal) = {tot:+.1f} pts = ${tot*5:+.0f} since May19")

# B) #3900 price space
print("\n=== B) #3900 (Jun 11 12:07 SC short) price-space check ===")
cur.execute("""SELECT rto.state FROM real_trade_orders rto WHERE rto.setup_log_id=3900""")
st=cur.fetchone()[0]; st=st if isinstance(st,dict) else json.loads(st)
print(f"  state fill_price={st.get('fill_price')}  signal_es_price={st.get('signal_es_price')}  "
      f"current_stop={st.get('current_stop')}  stop_fill={st.get('stop_fill_price')}")
print(f"  setup_log.spot (SPX at signal):")
cur.execute("SELECT spot,(ts AT TIME ZONE 'America/New_York') FROM setup_log WHERE id=3900")
print("   ", cur.fetchone())
# SPX around 12:00-12:20
cur.execute("""SELECT to_char(ts AT TIME ZONE 'America/New_York','HH24:MI:SS'), spot FROM chain_snapshots
   WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-11'
     AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '12:00' AND '12:25' ORDER BY ts""")
print("  SPX 12:00-12:25:", "  ".join(f"{t}={float(s):.1f}" for t,s in cur.fetchall()))
# ES range bars around then
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='vps_es_range_bars' ORDER BY ordinal_position""")
print("  vps_es_range_bars cols:", [r[0] for r in cur.fetchall()])
conn.close()
