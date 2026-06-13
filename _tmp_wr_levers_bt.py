"""WR is below 50% — find the lever. Re-run gate-removed (TS GEX) but break out
WR by GRADE and by EXIT config. Fetch path once/signal, run 3 exit variants in memory.
Goal: which (grade, exit) combo clears 50% WR while staying profitable?"""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"; COOLDOWN_MIN=15
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
conn=psycopg2.connect(DB);cur=conn.cursor()
def stat_near(ts):
    cur.execute("""SELECT payload->'statistics'->>'paradigm',payload->'statistics'->>'aggregatedCharm'
        FROM volland_snapshots WHERE ts BETWEEN %s-interval '4 min' AND %s+interval '2 min'
        AND payload->'statistics'->>'paradigm' IS NOT NULL ORDER BY abs(extract(epoch FROM(ts-%s))) LIMIT 1""",(ts,ts,ts))
    r=cur.fetchone();return (r[0],r[1]) if r else (None,None)
def charm_near(ts,lo,hi):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
        AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    r=cur.fetchone()
    if not r:return []
    cur.execute("""SELECT strike,value FROM volland_exposure_points WHERE ts_utc=%s AND greek='charm'
        AND strike BETWEEN %s AND %s""",(r[0],lo,hi))
    return [(float(s),float(v)) for s,v in cur.fetchall()]
def vanna_near(ts):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
        AND greek='vanna' AND expiration_option='ALL' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    r=cur.fetchone()
    if not r:return None
    cur.execute("""SELECT COALESCE(SUM(value),0) FROM volland_exposure_points WHERE ts_utc=%s
        AND greek='vanna' AND expiration_option='ALL'""",(r[0],))
    v=cur.fetchone();return float(v[0]) if v else None
def classify(f):
    if not f['CORE_R3']:return 'BAD'
    if f['R_VETO']:return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']):return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']):return 'A'
    if f['CORE_R2'] or f['R5_align']:return 'B'
    return 'C'
def get_path(ts):
    cur.execute("""SELECT spot FROM chain_snapshots WHERE ts>=%s
        AND (ts AT TIME ZONE 'America/New_York')::date=(%s AT TIME ZONE 'America/New_York')::date
        AND (ts AT TIME ZONE 'America/New_York')::time<'16:00' AND spot IS NOT NULL ORDER BY ts""",(ts,ts))
    return [float(r[0]) for r in cur.fetchall()]
def sim(path,entry,target,sl_pts,trail_act,trail_gap):
    sl=entry-sl_pts;mfe=0;ta=False;tstop=sl
    for sp in path:
        mfe=max(mfe,sp-entry);stop=tstop if ta else sl
        if sp<=stop:return 'WIN' if stop-entry>0 else 'LOSS',stop-entry
        if sp>=target:return 'WIN',target-entry
        if trail_act and not ta and mfe>=trail_act:ta=True;tstop=entry+(mfe-trail_gap)
        elif trail_act and ta:
            nt=entry+(mfe-trail_gap)
            if nt>tstop:tstop=nt
    return 'EXPIRED',path[-1]-entry
cur.execute(f"""SELECT ts,(ts AT TIME ZONE 'America/New_York') t,spot,rows FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '14:59'
    AND spot IS NOT NULL ORDER BY ts""")
snaps=cur.fetchall()
sigs=[];last_fire={};last_eval={}
for ts,t,spot,rows in snaps:
    d=t.date();le=last_eval.get(d)
    if le is not None and (t-le).total_seconds()<120:continue
    last_eval[d]=t;lf=last_fire.get(d)
    if lf is not None and (t-lf).total_seconds()<COOLDOWN_MIN*60:continue
    rows=rows if isinstance(rows,list) else json.loads(rows)
    gex=[]
    for rr in rows:
        try:s=float(rr[iS])
        except:continue
        if not(spot-50<=s<=spot+50):continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex:continue
    charm=charm_near(ts,spot-50,spot+50)
    if not charm:continue
    ga=[(s,v) for s,v in gex if s>spot];gb=[(s,v) for s,v in gex if s<spot];ca=[(s,v) for s,v in charm if s>spot]
    sga=max(ga,key=lambda x:abs(x[1])) if ga else (None,0);sgb=max(gb,key=lambda x:abs(x[1])) if gb else (None,0)
    nca=[(s,v) for s,v in ca if v<0];bcm=min(nca,key=lambda x:x[1])[0] if nca else None
    tg=sum(v for _,v in gex);tc=sum(v for _,v in charm);acpp=sum(1 for _,v in ca if v>0)/max(len(ca),1)*100
    R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
    f={'CORE_R3':sga[1]>0,'CORE_R2':sgb[1]<0,'R5_align':R5,'R_charm_bullish':tc<0,
       'R_gex_regime_pos':tg>=0,'R_VETO':(acpp>=80) and (not R5),'gex_magnet_strike':sga[0]}
    g=classify(f)
    if g not in ('A++','A','B'):continue
    para,agg=stat_near(ts);cv=None
    if agg not in (None,''):
        try:cv=float(str(agg).replace('$','').replace(',',''))
        except:cv=None
    vv=vanna_near(ts);mpg=f['gex_magnet_strike'];align=0
    if cv is not None:align+=1 if cv>0 else -1
    if vv is not None:align+=1 if vv>0 else -1
    if mpg:align+=1 if spot<=mpg else -1
    is_bull=para in {"BofA-LIS","GEX-TARGET","SIDIAL-MESSY","BOFA-PURE"}
    if not((align>=0) or is_bull):continue
    path=get_path(ts)
    if not path:continue
    last_fire[d]=t
    variants={
      'A:tgt=max(mag,+20) SL14 trail15/5': sim(path,spot,max(mpg or 0,spot+20),14,15,5),
      'B:tgt=+10 SL8 notrail':             sim(path,spot,spot+10,8,None,None),
      'C:tgt=magnet SL14 trail15/5':       sim(path,spot,(mpg or spot+20),14,15,5),
      'D:tgt=+15 SL10 trail12/5':          sim(path,spot,spot+15,10,12,5),
    }
    sigs.append({'grade':g,'align':align,'is_gex':'GEX' in (para or '').upper(),'var':variants})
print(f"signals: {len(sigs)}\n")
def block(filt,label):
    sub=[s for s in sigs if filt(s)]
    print(f"--- {label}  (n={len(sub)}) ---")
    if not sub:return
    for vk in ['A:tgt=max(mag,+20) SL14 trail15/5','B:tgt=+10 SL8 notrail','C:tgt=magnet SL14 trail15/5','D:tgt=+15 SL10 trail12/5']:
        n=len(sub);w=sum(1 for s in sub if s['var'][vk][0]=='WIN');p=sum(s['var'][vk][1] for s in sub)
        print(f"   {vk:36s} WR={w/n*100:4.0f}%  PnL={p:+7.1f}p  avg={p/n:+5.1f}")
print("===== ALL gate-removed signals =====")
block(lambda s:True,"all grades")
block(lambda s:s['grade'] in ('A++','A'),"A++/A only (drop B)")
block(lambda s:s['grade']=='A++',"A++ only")
block(lambda s:s['grade'] in ('A++','A') and s['align']>=1,"A++/A AND align>=1")
print("\n===== non-GEX only (gate-removal adds) =====")
block(lambda s:not s['is_gex'],"non-GEX all grades")
block(lambda s:not s['is_gex'] and s['grade'] in ('A++','A'),"non-GEX A++/A only")
block(lambda s:not s['is_gex'] and s['grade'] in ('A++','A') and s['align']>=1,"non-GEX A++/A AND align>=1")
conn.close()
