"""ROBUSTNESS: does the charm x gamma directional edge hold PER MONTH (esp Mar/Apr,
the non-bull months), or is it all-May like the last finding? Also report effective
independent sample (distinct days) per cell, and a DAY-LEVEL test (one obs/day) to
defeat intraday autocorrelation."""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,args):
    global conn,cur
    try: cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()
DP=defaultdict(list)
for d,ts,spot in q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date, ts, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}' AND spot IS NOT NULL ORDER BY ts""",()):
    DP[d].append((ts,float(spot)))
def fwd(day,ts,sec):
    for t2,sp in DP.get(day,[]):
        if (t2-ts).total_seconds()>=sec: return sp
    return DP[day][-1][1] if DP.get(day) else None
def aggcharm(ts):
    r=q("""SELECT payload->'statistics'->>'aggregatedCharm' FROM volland_snapshots
        WHERE ts BETWEEN %s-interval '4 min' AND %s+interval '2 min'
        AND payload->'statistics'->>'aggregatedCharm' IS NOT NULL
        ORDER BY abs(extract(epoch FROM(ts-%s))) LIMIT 1""",(ts,ts,ts))
    if not r or r[0][0] in (None,''): return None
    try: return float(str(r[0][0]).replace('$','').replace(',',''))
    except: return None
snaps=q(f"""SELECT ts,(ts AT TIME ZONE 'America/New_York') t, spot, rows FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:45' AND '15:00'
    AND spot IS NOT NULL ORDER BY ts""",())
recs=[]; last={}
for ts,t,spot,rows in snaps:
    d=t.date(); lf=last.get(d)
    if lf is not None and (t-lf).total_seconds()<30*60: continue
    last[d]=t
    rows=rows if isinstance(rows,list) else json.loads(rows)
    gex=[]
    for rr in rows:
        try: s=float(rr[iS])
        except: continue
        if not (spot-60<=s<=spot+60): continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    tg=sum(v for _,v in gex); ch=aggcharm(ts); f60=fwd(d,ts,3600)
    if ch is None or f60 is None: continue
    recs.append({'day':d,'month':str(d)[:7],'r60':f60-spot,
                 'cell':('chP' if ch>0 else 'chN')+('_gP' if tg>=0 else '_gN')})
def wr(rs):
    if not rs: return None,0,0
    up=sum(1 for r in rs if r['r60']>0); return up/len(rs)*100,len(rs),len(set(r['day'] for r in rs))

print("=== PER-MONTH: P(up r60) for the two diagonal signals ===")
print(f"{'month':9s} {'LONG chP_gP':>22s} {'SHORT chN_gN':>22s}")
for m in sorted(set(r['month'] for r in recs)):
    L=[r for r in recs if r['month']==m and r['cell']=='chP_gP']
    S=[r for r in recs if r['month']==m and r['cell']=='chN_gN']
    lw,ln,ld=wr(L); sw,sn,sd=wr(S)
    ls=f"{lw:.0f}%up n={ln}({ld}d)" if lw is not None else "n=0"
    ss=f"{sw:.0f}%up n={sn}({sd}d)" if sw is not None else "n=0"
    print(f"{m:9s} {ls:>22s} {ss:>22s}")

print("\n=== FULL-WINDOW by cell: P(up r60), n, distinct-days ===")
for cell in ['chP_gP','chP_gN','chN_gP','chN_gN']:
    rs=[r for r in recs if r['cell']==cell]; w,n,dd=wr(rs)
    print(f"  {cell}: P(up)={w:.0f}%  n={n}  days={dd}  avg={sum(r['r60'] for r in rs)/len(rs):+.1f}p")

print("\n=== DAY-LEVEL test (one obs/day per cell = kills intraday autocorrelation) ===")
# per day, per cell: mean r60 sign
daycell=defaultdict(list)
for r in recs: daycell[(r['day'],r['cell'])].append(r['r60'])
for cell in ['chP_gP','chN_gN']:
    days=[(d,sum(v)/len(v)) for (d,c),v in daycell.items() if c==cell]
    if not days: print(f"  {cell}: n=0"); continue
    up=sum(1 for _,m in days if m>0)
    print(f"  {cell}: {len(days)} days, P(day mean up)={up/len(days)*100:.0f}%  "
          f"mean-of-day-means={sum(m for _,m in days)/len(days):+.1f}p")
conn.close()
