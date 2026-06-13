"""What if the fixes had been live post-V16? Counterfactual on placed trades.
Fix A: trail_market_exit trades ride the portal exit instead (use portal pts).
Fix B: size-down (halve) when after 2 consecutive stops OR VIX-at-open >= 19.
Show LOSS window (Jun5-12), WIN era (May19-Jun4), and totals. $ at per-lid pts*5.
"""
import os, sys, json, psycopg2
import pandas as pd
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
df['Date']=pd.to_datetime(df['Date']); df['m']=df['Date'].dt.strftime('%Y-%m')
port=dict(zip(df['ID'], df['P&L']))
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.direction, sl.vix, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19'
     AND sl.id = ANY(%s) ORDER BY sl.ts""",(list(port.keys()),))
rows=[]
for sid,et,direction,vix,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    f,e=float(f),float(e); b=(e-f) if direction in ('long','bullish') else (f-e)
    rows.append({'id':sid,'day':str(et)[:10],'et':str(et)[11:16],'broker':b,
                 'portal':float(port.get(sid,0)),'cr':str(st.get('close_reason')),
                 'stop':str(st.get('close_reason'))=='stop_filled','vix':float(vix) if vix else None})
firstvix={}
for r in rows: firstvix.setdefault(r['day'], r['vix'])

def total(rows, fn, lo='2026-06-05', hi='2026-06-04'):
    win=loss=0.0
    byday=defaultdict(list)
    for r in rows: byday[r['day']].append(r)
    for day,trs in byday.items():
        consec=0
        for r in trs:
            v=fn(r,consec)*5
            if day>=lo: loss+=v
            else: win+=v
            consec=consec+1 if r['stop'] else 0
    return win,loss

def base(r,c): return r['broker']
def fixA(r,c): return r['portal'] if r['cr']=='outcome_close_trail_market_exit' else r['broker']
def fixB(r,c):
    m=0.5 if (c>=2 or (firstvix.get(r['day']) or 0)>=19) else 1.0
    return r['broker']*m
def fixAB(r,c):
    bp = r['portal'] if r['cr']=='outcome_close_trail_market_exit' else r['broker']
    m=0.5 if (c>=2 or (firstvix.get(r['day']) or 0)>=19) else 1.0
    return bp*m

print("Per-lid broker $ (size = as traded). WIN era = May19-Jun04, LOSS window = Jun05-12\n")
for name,fn in [("BASELINE (actual broker)",base),
                ("+ Fix A (trail bug -> ride portal)",fixA),
                ("+ Fix B (size-down hi-vol/streak)",fixB),
                ("+ Fix A & B combined",fixAB)]:
    w,l=total(rows,fn)
    print(f"{name:38} WIN-era {w:>+8.0f}$   LOSS-window {l:>+8.0f}$   TOTAL {w+l:>+8.0f}$")

# how many trades each fix touched in loss window
nA=sum(1 for r in rows if r['day']>='2026-06-05' and r['cr']=='outcome_close_trail_market_exit')
print(f"\nFix A touches {nA} loss-window trades (trail_market_exit).")
conn.close()
