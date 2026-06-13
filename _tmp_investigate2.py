"""A) per-day PORTAL vs BROKER (capture) — loss window. Did portal win while broker lost?
   F) exit mechanism by month — did the safety-SL get hit more in June (S131 leak)?
   Also capture ratio by month.
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')
def rp(state,direction):
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: return None
    f,e=float(f),float(e)
    return (e-f) if direction in ('long','bullish') else (f-e)

# A) per-day portal (placed outcome_pnl) vs broker pts
print("=== A) PER-DAY portal-sim vs broker (PLACED V16 trades) ===")
print(f"{'day':12} {'n':>3} {'portal_pts':>10} {'broker_pts':>10} {'capture%':>9}")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction, sl.outcome_pnl, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19'
   ORDER BY sl.ts""",(WL,))
byday=defaultdict(lambda:{'n':0,'portal':0.0,'broker':0.0})
for d,direction,opnl,state in cur.fetchall():
    ds=str(d); a=byday[ds]; a['n']+=1
    if opnl is not None: a['portal']+=float(opnl)
    b=rp(state,direction)
    if b is not None: a['broker']+=b
for ds in sorted(byday):
    a=byday[ds]
    cap = (a['broker']/a['portal']*100) if a['portal']!=0 else 0
    flag = '  <LOSS-WINDOW' if ds>='2026-06-05' else ''
    print(f"{ds:12} {a['n']:>3} {a['portal']:>10.1f} {a['broker']:>10.1f} {cap:>8.0f}%{flag}")

# F) exit mechanism by month (close_reason in state JSON)
print("\n=== F) exit mechanism (close_reason from state) by month ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-04-15'
   ORDER BY sl.ts""",(WL,))
mech=defaultdict(lambda: defaultdict(int))
mech_pts=defaultdict(lambda: defaultdict(float))
for d,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    m=str(d)[:7]
    cr=str(st.get('close_reason'))
    mech[m][cr]+=1
    b=rp(state,direction)
    if b is not None: mech_pts[m][cr]+=b
for m in sorted(mech):
    print(f"\n  {m}:")
    for cr,n in sorted(mech[m].items(), key=lambda x:-x[1]):
        print(f"     {cr:28} n={n:>3}  pts={mech_pts[m][cr]:>+7.1f}")

# capture ratio by month (broker/portal) on placed trades
print("\n=== capture ratio by month (placed trades) ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction, sl.outcome_pnl, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-04-15'
   ORDER BY sl.ts""",(WL,))
mo=defaultdict(lambda:{'portal':0.0,'broker':0.0,'n':0})
for d,direction,opnl,state in cur.fetchall():
    m=str(d)[:7]; mo[m]['n']+=1
    if opnl is not None: mo[m]['portal']+=float(opnl)
    b=rp(state,direction)
    if b is not None: mo[m]['broker']+=b
for m in sorted(mo):
    a=mo[m]; cap=(a['broker']/a['portal']*100) if a['portal']!=0 else 0
    print(f"  {m}: portal={a['portal']:>+8.1f} broker={a['broker']:>+8.1f} capture={cap:>6.0f}% (n={a['n']})")
conn.close()
