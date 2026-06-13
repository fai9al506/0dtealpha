"""Apply the +gamma/-gamma TS-GEX regime filter to CLEAN single-version GEX Long sets:
 A) v3.1 & v3.2 PORTAL OVERLAY (app/gex_long_v3.py, Volland-gamma graded, re-simulated)
 B) v3.1 & v3.2 on TS GEX (correct source, generated from scratch)
Report all / +gamma / -gamma: trades, distinct days, WR, TOTAL pts, avg, maxDD.
Regime = sign of net TS GEX at noon. outcome in POINTS.
"""
import psycopg2, json
from collections import defaultdict
from sqlalchemy import create_engine, text
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-03"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,args=()):
    global conn,cur
    try: cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()

# ---- daily TS-GEX regime (sign of net TS GEX at noon) ----
rows=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d,(ts AT TIME ZONE 'America/New_York')::time t, rows, spot
    FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '11:30' AND '12:30' AND spot IS NOT NULL ORDER BY ts""")
best={}
for d,t,rr,spot in rows:
    sec=abs((int(t.hour)*3600+int(t.minute)*60)-12*3600)
    if d not in best or sec<best[d][0]: best[d]=(sec,rr,spot)
regime={}
for d,(_,rr,spot) in best.items():
    rr=rr if isinstance(rr,list) else json.loads(rr); tg=0.0
    for row in rr:
        try: s=float(row[iS])
        except: continue
        tg+=float(row[iCG] or 0)*float(row[iCOI] or 0)-float(row[iPG] or 0)*float(row[iPOI] or 0)
    regime[d]='pos' if tg>=0 else 'neg'

def rpt(rows):
    # rows: list of (date,'WIN'/...,pnl)
    if not rows: return None
    rows=sorted(rows)
    n=len(rows); days=len(set(x[0] for x in rows)); w=sum(1 for x in rows if x[1]=='WIN'); tot=sum(x[2] for x in rows)
    eq=0;peak=0;dd=0
    for x in rows: eq+=x[2]; peak=max(peak,eq); dd=min(dd,eq-peak)
    return n,days,w/n*100,tot,tot/n,dd
def show(title,sig):
    # sig: list of (date,res,pnl)
    pos=[x for x in sig if regime.get(x[0])=='pos']; neg=[x for x in sig if regime.get(x[0])=='neg']
    print(f"  {title}")
    for lab,s in [('ALL',sig),('+gamma ONLY (filter)',pos),('-gamma (removed)',neg)]:
        r=rpt(s)
        if not r: print(f"     {lab:22s} n=0"); continue
        print(f"     {lab:22s} {r[0]:3d} tr / {r[1]:2d} d | WR {r[2]:3.0f}% | TOTAL {r[3]:+7.1f}p | avg {r[4]:+5.2f}p | maxDD {r[5]:6.1f}p")
    print()

# ====== VERSION A: portal overlay (Volland gamma) ======
print("="*92)
print("VERSION A — v3.1 / v3.2 PORTAL OVERLAY (app/gex_long_v3.py, Volland-gamma graded)")
print("="*92)
from app.gex_long_v3 import _build_cache
engine=create_engine(DB)
overlay=_build_cache(engine)  # lid -> {pass,pass_v32,result,pnl,...}
# lid -> date
liddate={}
with engine.begin() as cx:
    for lid,d in cx.execute(text(f"""SELECT id,(ts AT TIME ZONE 'America/New_York')::date
        FROM setup_log WHERE setup_name='GEX Long' AND grade!='LOG'
        AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'""")):
        liddate[lid]=d
v31=[]; v32=[]
for lid,o in overlay.items():
    d=liddate.get(lid)
    if d is None or o.get('result') is None: continue
    if o.get('pass'): v31.append((d,o['result'],o['pnl']))
    if o.get('pass_v32'): v32.append((d,o['result'],o['pnl']))
show("v3.1 overlay (verdict ABC + align>=0 + hr<15):",v31)
show("v3.2 overlay (+ bull-paradigm substitute):",v32)

# ====== VERSION B: TS GEX from scratch ======
print("="*92)
print("VERSION B — v3.1 / v3.2 on TS GEX (correct source, generated from scratch)")
print("="*92)
BULL={"BofA-LIS","GEX-TARGET","SIDIAL-MESSY","BOFA-PURE"}
SL,TFLOOR,TACT,TGAP=14.0,20.0,15.0,5.0
def stat_near(ts):
    r=q("""SELECT payload->'statistics'->>'paradigm',payload->'statistics'->>'aggregatedCharm'
        FROM volland_snapshots WHERE ts BETWEEN %s-interval '4 min' AND %s+interval '2 min'
        AND payload->'statistics'->>'paradigm' IS NOT NULL ORDER BY abs(extract(epoch FROM(ts-%s))) LIMIT 1""",(ts,ts,ts))
    return (r[0][0],r[0][1]) if r else (None,None)
def charm_near(ts,lo,hi):
    r=q("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
        AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    if not r: return []
    return [(float(s),float(v)) for s,v in q("""SELECT strike,value FROM volland_exposure_points
        WHERE ts_utc=%s AND greek='charm' AND strike BETWEEN %s AND %s""",(r[0][0],lo,hi))]
def vanna_near(ts):
    r=q("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
        AND greek='vanna' AND expiration_option='ALL' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    if not r: return None
    r2=q("SELECT COALESCE(SUM(value),0) FROM volland_exposure_points WHERE ts_utc=%s AND greek='vanna' AND expiration_option='ALL'",(r[0][0],))
    return float(r2[0][0]) if r2 else None
def classify(f):
    if not f['CORE_R3']:return 'BAD'
    if f['R_VETO']:return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']):return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']):return 'A'
    if f['CORE_R2'] or f['R5_align']:return 'B'
    return 'C'
DP=defaultdict(list)
for d,ts,spot in q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date, ts, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time<'16:00' AND spot IS NOT NULL ORDER BY ts"""):
    DP[d].append((ts,float(spot)))
def sim(day,ts,entry,target):
    path=[sp for (t2,sp) in DP.get(day,[]) if t2>=ts]
    if not path: return None
    s=entry-SL;mfe=0;ta=False;tstop=s
    for sp in path:
        mfe=max(mfe,sp-entry);stop=tstop if ta else s
        if sp<=stop:return ('WIN' if stop-entry>0 else 'LOSS',stop-entry)
        if sp>=target:return ('WIN',target-entry)
        if not ta and mfe>=TACT:ta=True;tstop=entry+(mfe-TGAP)
        elif ta:
            nt=entry+(mfe-TGAP)
            if nt>tstop:tstop=nt
    return ('EXPIRED',path[-1]-entry)
snaps=q(f"""SELECT ts,(ts AT TIME ZONE 'America/New_York') t, spot, rows FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '14:59' AND spot IS NOT NULL ORDER BY ts""")
b_v31=[]; b_v32=[]; last_fire={}; last_eval={}
for ts,t,spot,rows in snaps:
    d=t.date(); le=last_eval.get(d)
    if le is not None and (t-le).total_seconds()<120: continue
    last_eval[d]=t; lf=last_fire.get(d)
    if lf is not None and (t-lf).total_seconds()<15*60: continue
    rows=rows if isinstance(rows,list) else json.loads(rows)
    gex=[]
    for rr in rows:
        try: s=float(rr[iS])
        except: continue
        if not (spot-50<=s<=spot+50): continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    charm=charm_near(ts,spot-50,spot+50)
    if not charm: continue
    ga=[(s,v) for s,v in gex if s>spot];gb=[(s,v) for s,v in gex if s<spot];ca=[(s,v) for s,v in charm if s>spot]
    sga=max(ga,key=lambda x:abs(x[1])) if ga else (None,0);sgb=max(gb,key=lambda x:abs(x[1])) if gb else (None,0)
    nca=[(s,v) for s,v in ca if v<0];bcm=min(nca,key=lambda x:x[1])[0] if nca else None
    tg=sum(v for _,v in gex);tc=sum(v for _,v in charm);acpp=sum(1 for _,v in ca if v>0)/max(len(ca),1)*100
    R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
    f={'CORE_R3':sga[1]>0,'CORE_R2':sgb[1]<0,'R5_align':R5,'R_charm_bullish':tc<0,
       'R_gex_regime_pos':tg>=0,'R_VETO':(acpp>=80) and (not R5),'gex_magnet_strike':sga[0]}
    if classify(f) not in ('A++','A','B'): continue
    if t.hour>=15: continue
    para,agg=stat_near(ts);cv=None
    if agg not in (None,''):
        try:cv=float(str(agg).replace('$','').replace(',',''))
        except:cv=None
    vv=vanna_near(ts);mpg=f['gex_magnet_strike'];align=0
    if cv is not None:align+=1 if cv>0 else -1
    if vv is not None:align+=1 if vv>0 else -1
    if mpg:align+=1 if spot<=mpg else -1
    is_bull=para in BULL
    pass31=(align>=0); pass32=(align>=0) or is_bull
    if not pass32: continue
    s=sim(d,ts,spot,max(mpg or 0,spot+TFLOOR))
    if not s: continue
    last_fire[d]=t; res,pnl=s
    if pass31: b_v31.append((d,res,pnl))
    if pass32: b_v32.append((d,res,pnl))
show("v3.1 on TS GEX (align>=0):",b_v31)
show("v3.2 on TS GEX (align>=0 or bull-paradigm):",b_v32)
conn.close()
