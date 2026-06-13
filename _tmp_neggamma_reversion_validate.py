"""Decisive validation of the -gamma late-day REVERSION (TS GEX regime).
Kill the beta trap: does it reverse on BOTH up-days and down-days (real reversion),
or only down-days (dip-buy beta)? + per-month robustness. Day-level.
"""
import psycopg2, json
from collections import defaultdict
from math import sqrt
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,args=()):
    global conn,cur
    try: cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()
DP=defaultdict(list)
for d,t,spot in q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date,
    (ts AT TIME ZONE 'America/New_York')::time, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}' AND spot IS NOT NULL ORDER BY ts"""):
    DP[d].append((str(t)[:8],float(spot)))
def at_after(day,h):
    for t,sp in DP.get(day,[]):
        if t>=h: return sp
    return None
def last_before(day,h):
    prev=None
    for t,sp in DP.get(day,[]):
        if t<=h: prev=sp
        else: break
    return prev
rows=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d,(ts AT TIME ZONE 'America/New_York')::time t, rows, spot
    FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '14:30' AND '15:30' AND spot IS NOT NULL ORDER BY ts""")
best={}
for d,t,rr,spot in rows:
    sec=abs((int(t.hour)*3600+int(t.minute)*60)-15*3600)
    if d not in best or sec<best[d][0]: best[d]=(sec,rr,spot)
net_ts={}
for d,(_,rr,spot) in best.items():
    rr=rr if isinstance(rr,list) else json.loads(rr); tg=0.0
    for row in rr:
        try: s=float(row[iS])
        except: continue
        tg+=float(row[iCG] or 0)*float(row[iCOI] or 0)-float(row[iPG] or 0)*float(row[iPOI] or 0)
    net_ts[d]=tg
days=[]
for d in sorted(DP):
    if d not in net_ts: continue
    arr=DP[d]; op=at_after(d,'09:35'); mid=last_before(d,'15:30'); close=arr[-1][1]
    if None in (op,mid): continue
    days.append({'d':d,'m':str(d)[:7],'net':net_ts[d],'ret_day':mid-op,'ret_close':close-mid})
# -gamma = lowest tercile of net TS GEX
s=sorted(days,key=lambda x:x['net']); t=len(s)//3
neg=[x for x in s[:t] if abs(x['ret_day'])>2]
def z(p,n): return (p-0.5)/sqrt(0.25/n) if n else 0
print(f"-gamma (low TS GEX tercile) days with move>2pt: n={len(neg)}\n")
print("=== BETA-TRAP CHECK: reversion on UP-days vs DOWN-days ===")
up=[x for x in neg if x['ret_day']>0]; dn=[x for x in neg if x['ret_day']<0]
for lab,g in [('UP-days (day rose)  -> expect close DOWN if real',up),
              ('DOWN-days (day fell)-> expect close UP if real',dn)]:
    if not g: print(f"  {lab}: n=0"); continue
    rev=sum(1 for x in g if (x['ret_close']>0)!=(x['ret_day']>0))
    fade=sum(-x['ret_close'] if x['ret_day']>0 else x['ret_close'] for x in g)  # fade pnl
    print(f"  {lab}: n={len(g)}  P(reverse)={rev/len(g)*100:.0f}% (z={z(rev/len(g),len(g)):+.1f})  fade avg={fade/len(g):+.2f}p")
print("  (REAL reversion = both up & down reverse ~equally; BETA = only down-days reverse)")
print("\n=== PER-MONTH (-gamma reversion, fade the day move last 30min) ===")
bym=defaultdict(list)
for x in neg: bym[x['m']].append(x)
for m in sorted(bym):
    g=bym[m]; rev=sum(1 for x in g if (x['ret_close']>0)!=(x['ret_day']>0))
    fade=sum(-x['ret_close'] if x['ret_day']>0 else x['ret_close'] for x in g)
    print(f"  {m}: n={len(g)}  P(reverse)={rev/len(g)*100:.0f}%  fade total={fade:+.1f}p")
conn.close()
