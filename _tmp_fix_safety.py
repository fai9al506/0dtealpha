"""Safety check: do the real-time fixes HURT the winning sub-period (May19-Jun04)?
Split era into WIN (<=2026-06-04) and LOSS (>=2026-06-05). Apply each fix to each.
A good fix: ~neutral on WIN sub-period, big save on LOSS sub-period.
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.direction, sl.vix, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19'
   ORDER BY sl.ts""",(WL,))
rows=[]
for et,direction,vix,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    f,e=float(f),float(e)
    pts=(e-f) if direction in ('long','bullish') else (f-e)
    rows.append({'day':str(et)[:10],'et':str(et)[11:16],'pts':pts,
                 'stop':str(st.get('close_reason'))=='stop_filled','vix':float(vix) if vix else None})
firstvix={}
for r in rows: firstvix.setdefault(r['day'], r['vix'])
cur.execute("""SELECT (ts AT TIME ZONE 'America/New_York')::date d, to_char(ts AT TIME ZONE 'America/New_York','HH24:MI') t, spot
   FROM chain_snapshots WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY ts""")
ss=defaultdict(list)
for d,t,s in cur.fetchall(): ss[str(d)].append((t,float(s)))
def rsf(day,et):
    p=[s for t,s in ss.get(day,[]) if t<=et]; return (max(p)-min(p)) if p else 0

def split(multfn):
    win=loss=0.0
    byday=defaultdict(list)
    for r in rows: byday[r['day']].append(r)
    for day,trs in byday.items():
        consec=0
        for r in trs:
            m=multfn(r,consec)
            v=r['pts']*m*5
            if day<='2026-06-04': win+=v
            else: loss+=v
            consec=consec+1 if r['stop'] else 0
    return win,loss

base_w,base_l=split(lambda r,c:1.0)
print(f"BASELINE: WIN-era(May19-Jun04)={base_w:+.0f}$  LOSS-era(Jun05-12)={base_l:+.0f}$\n")
print(f"{'fix':42} {'WIN-era Δ':>11} {'LOSS-era Δ':>11}")
def t(name,fn):
    w,l=split(fn); print(f"{name:42} {w-base_w:>+11.0f} {l-base_l:>+11.0f}")

t("P1 vix1>=19 halve day",       lambda r,c: 0.5 if (firstvix.get(r['day']) or 0)>=19 else 1.0)
t("P2 consec-stop>=2 halve",     lambda r,c: 0.5 if c>=2 else 1.0)
t("P4 range_so_far>=60 halve",   lambda r,c: 0.5 if rsf(r['day'],r['et'])>=60 else 1.0)
t("P4 range_so_far>=80 halve",   lambda r,c: 0.5 if rsf(r['day'],r['et'])>=80 else 1.0)
t("COMBO vix>=19 + consec>=2",   lambda r,c: (0.5 if (firstvix.get(r['day']) or 0)>=19 else 1.0)*(0.5 if c>=2 else 1.0))
t("COMBO range>=80 + consec>=2", lambda r,c: (0.5 if rsf(r['day'],r['et'])>=80 else 1.0)*(0.5 if c>=2 else 1.0))
conn.close()
