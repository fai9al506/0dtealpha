"""IS THERE AN EDGE? Two tests:
 1) Cooldown sensitivity (15/30/60 min) — kill same-move double-counting.
 2) Null benchmark — long at SAME cadence/exit but WITHOUT the GEX-structure filter.
    If structure doesn't beat unconditional-long (beta), there is NO edge.
Exit = variant C (target=magnet, SL14, trail15/5) for BOTH signal and null.
One sim pass: evaluate 15-min-spaced slots; re-space to 30/60 in post.
"""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _connect():
    return psycopg2.connect(DB, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
conn=_connect();cur=conn.cursor()
def q(sql,args):
    """Execute with one reconnect on dropped SSL."""
    global conn,cur
    try:
        cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_connect();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()
def stat_near(ts):
    rows=q("""SELECT payload->'statistics'->>'paradigm',payload->'statistics'->>'aggregatedCharm'
        FROM volland_snapshots WHERE ts BETWEEN %s-interval '4 min' AND %s+interval '2 min'
        AND payload->'statistics'->>'paradigm' IS NOT NULL ORDER BY abs(extract(epoch FROM(ts-%s))) LIMIT 1""",(ts,ts,ts))
    return (rows[0][0],rows[0][1]) if rows else (None,None)
def charm_near(ts,lo,hi):
    rows=q("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
        AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    if not rows:return []
    rows2=q("""SELECT strike,value FROM volland_exposure_points WHERE ts_utc=%s AND greek='charm'
        AND strike BETWEEN %s AND %s""",(rows[0][0],lo,hi))
    return [(float(s),float(v)) for s,v in rows2]
def vanna_near(ts):
    rows=q("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
        AND greek='vanna' AND expiration_option='ALL' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    if not rows:return None
    rows2=q("""SELECT COALESCE(SUM(value),0) FROM volland_exposure_points WHERE ts_utc=%s
        AND greek='vanna' AND expiration_option='ALL'""",(rows[0][0],))
    return float(rows2[0][0]) if rows2 else None
def classify(f):
    if not f['CORE_R3']:return 'BAD'
    if f['R_VETO']:return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']):return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']):return 'A'
    if f['CORE_R2'] or f['R5_align']:return 'B'
    return 'C'
# Pre-cache each day's full spot path (09:35-16:00) ONCE — eliminates per-slot path queries.
_DAYPATHS={}
def _load_daypaths():
    rows=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d, ts, spot FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
        AND (ts AT TIME ZONE 'America/New_York')::time < '16:00' AND spot IS NOT NULL ORDER BY ts""",())
    for d,ts,spot in rows:
        _DAYPATHS.setdefault(d,[]).append((ts,float(spot)))
def get_path(day,ts):
    return [sp for (t2,sp) in _DAYPATHS.get(day,[]) if t2>=ts]
def sim(path,entry,target,sl=14,ta_act=15,ta_gap=5):
    s=entry-sl;mfe=0;ta=False;tstop=s
    for sp in path:
        mfe=max(mfe,sp-entry);stop=tstop if ta else s
        if sp<=stop:return ('WIN' if stop-entry>0 else 'LOSS',stop-entry)
        if sp>=target:return ('WIN',target-entry)
        if not ta and mfe>=ta_act:ta=True;tstop=entry+(mfe-ta_gap)
        elif ta:
            nt=entry+(mfe-ta_gap)
            if nt>tstop:tstop=nt
    return ('EXPIRED',path[-1]-entry)

print("loading day paths...",flush=True)
_load_daypaths()
print(f"  cached {len(_DAYPATHS)} days",flush=True)
snaps=q(f"""SELECT ts,(ts AT TIME ZONE 'America/New_York') t,spot,rows FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '14:59'
    AND spot IS NOT NULL ORDER BY ts""",())
# evaluate 15-min-spaced slots; store both null (uncond long) + signal (if qualifies)
slots=[];last_eval={}
for ts,t,spot,rows in snaps:
    d=t.date();le=last_eval.get(d)
    if le is not None and (t-le).total_seconds()<15*60:continue
    last_eval[d]=t
    rows=rows if isinstance(rows,list) else json.loads(rows)
    gex=[]
    for rr in rows:
        try:s=float(rr[iS])
        except:continue
        if not(spot-50<=s<=spot+50):continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    ga=[(x,v) for x,v in gex if x>spot]
    sga=max(ga,key=lambda z:abs(z[1])) if ga else (None,0)
    mpg=sga[0]
    if not mpg:continue
    path=get_path(d,ts)
    if not path:continue
    null_res=sim(path,spot,mpg)  # unconditional long, magnet target
    # signal qualification
    qualifies=False;grade=None;align=None;para=None
    charm=charm_near(ts,spot-50,spot+50)
    if charm and gex:
        gb=[(x,v) for x,v in gex if x<spot];ca=[(x,v) for x,v in charm if x>spot]
        sgb=max(gb,key=lambda z:abs(z[1])) if gb else (None,0)
        nca=[(x,v) for x,v in ca if v<0];bcm=min(nca,key=lambda z:z[1])[0] if nca else None
        tg=sum(v for _,v in gex);tc=sum(v for _,v in charm);acpp=sum(1 for _,v in ca if v>0)/max(len(ca),1)*100
        R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
        f={'CORE_R3':sga[1]>0,'CORE_R2':sgb[1]<0,'R5_align':R5,'R_charm_bullish':tc<0,
           'R_gex_regime_pos':tg>=0,'R_VETO':(acpp>=80) and (not R5)}
        g=classify(f)
        if g in ('A++','A','B'):
            para,agg=stat_near(ts);cv=None
            if agg not in (None,''):
                try:cv=float(str(agg).replace('$','').replace(',',''))
                except:cv=None
            vv=vanna_near(ts);al=0
            if cv is not None:al+=1 if cv>0 else -1
            if vv is not None:al+=1 if vv>0 else -1
            al+=1 if spot<=mpg else -1
            is_bull=para in {"BofA-LIS","GEX-TARGET","SIDIAL-MESSY","BOFA-PURE"}
            if (al>=0) or is_bull:
                sig_res=sim(path,spot,mpg)
                qualifies=True;grade=g;align=al
    slots.append({'ts':ts,'t':t,'day':t.date(),'month':str(t)[:7],
                  'null':null_res,'qualifies':qualifies,'grade':grade,'align':align,
                  'is_gex':'GEX' in (para or '').upper() if para else None})
print(f"15-min slots evaluated: {len(slots)}  (qualifying signals: {sum(1 for s in slots if s['qualifies'])})\n")

def respace(items,cd_min):
    out=[];last={}
    for s in sorted(items,key=lambda z:z['ts']):
        d=s['day'];lf=last.get(d)
        if lf is not None and (s['t']-lf).total_seconds()<cd_min*60:continue
        last[d]=s['t'];out.append(s)
    return out
def summ(items,key,label):
    rec=[(s[key][0],s[key][1]) for s in items]
    if not rec:print(f"  {label:22s} n=0");return
    n=len(rec);w=sum(1 for r,_ in rec if r=='WIN');p=sum(x for _,x in rec)
    mo=3.33
    print(f"  {label:22s} n={n:4d}  WR={w/n*100:4.0f}%  PnL={p:+8.1f}p  avg={p/n:+5.2f}  ~{p/mo:+6.0f}p/mo")

for cd in [15,30,60]:
    sel=respace(slots,cd)
    sig=[s for s in sel if s['qualifies']]
    sig_ng=[s for s in sig if s['is_gex']==False]
    print(f"===== COOLDOWN {cd} min  (slots after respace: {len(sel)}) =====")
    summ(sel,'null',  "NULL long-everything")
    summ(sig,'null',  "SIGNAL all-paradigm")   # use null key = same magnet sim
    summ(sig_ng,'null',"SIGNAL non-GEX only")
    print()

# per-month, signal non-GEX at 60-min cooldown
print("===== PER-MONTH (SIGNAL non-GEX, 60-min cooldown) =====")
sel=respace(slots,60); sig_ng=[s for s in sel if s['qualifies'] and s['is_gex']==False]
bym=defaultdict(list)
for s in sig_ng:bym[s['month']].append(s)
for m in sorted(bym):
    rec=[(x['null'][0],x['null'][1]) for x in bym[m]]
    n=len(rec);w=sum(1 for r,_ in rec if r=='WIN');p=sum(x for _,x in rec)
    print(f"  {m}  n={n:3d}  WR={w/n*100:4.0f}%  PnL={p:+7.1f}p")
# beta context
days=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d, min(spot), max(spot),
    (array_agg(spot ORDER BY ts))[1] first_spot, (array_agg(spot ORDER BY ts DESC))[1] last_spot
    FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND spot IS NOT NULL GROUP BY 1 ORDER BY 1""",())
if days:
    print(f"\nBETA CONTEXT: SPX {days[0][3]:.0f} (Feb23) -> {days[-1][4]:.0f} ({END}) = {days[-1][4]-days[0][3]:+.0f} pts over {len(days)} days")
conn.close()
