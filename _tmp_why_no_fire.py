"""WHY did GEX Long v3 reject every signal since May 18 (strong uptrend)?
For each raw graded GEX Long signal May 18 -> Jun 5, compute the v3 classifier
features on TS GEX (live source) and show which gate failed:
  CORE_R3 false / R_VETO true / verdict not ABC / align<0 / hour>=15
Tally the dominant rejection reason.
"""
import psycopg2, json
from collections import Counter
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,a=()):
    global conn,cur
    try: cur.execute(sql,a);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,a);return cur.fetchall()
def charm_near(ts,lo,hi):
    r=q("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '6 min' AND %s
        AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    if not r: return []
    return [(float(s),float(v)) for s,v in q("""SELECT strike,value FROM volland_exposure_points
        WHERE ts_utc=%s AND greek='charm' AND strike BETWEEN %s AND %s""",(r[0][0],lo,hi))]
def vanna_near(ts):
    r=q("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '6 min' AND %s
        AND greek='vanna' AND expiration_option='ALL' ORDER BY ts_utc DESC LIMIT 1""",(ts,ts))
    if not r: return None
    r2=q("SELECT COALESCE(SUM(value),0) FROM volland_exposure_points WHERE ts_utc=%s AND greek='vanna' AND expiration_option='ALL'",(r[0][0],))
    return float(r2[0][0]) if r2 else None

# chain snapshot nearest each signal
sigs=q("""SELECT id, ts, (ts AT TIME ZONE 'America/New_York') t, spot, grade, paradigm,
    (SELECT cs.rows FROM chain_snapshots cs WHERE cs.ts BETWEEN sl.ts - interval '90 sec' AND sl.ts + interval '90 sec'
       ORDER BY abs(extract(epoch FROM (cs.ts - sl.ts))) LIMIT 1) AS rows
    FROM setup_log sl WHERE setup_name='GEX Long' AND grade!='LOG'
    AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-05-18' AND '2026-06-05'
    ORDER BY ts""")
print(f"raw graded GEX Long signals May 18 - Jun 5: {len(sigs)}\n")
reasons=Counter(); accepted=0
print(f"{'date/time':17s} {'spot':>7s} {'magnet':>6s} {'R3':3s} {'R2':3s} {'R5':3s} {'VETO':4s} {'verdict':7s} {'align':>5s} {'hr':>2s}  REJECT-REASON")
for lid,ts,t,spot,grade,para,rows in sigs:
    if not rows or not spot:
        reasons['no_chain_data']+=1; continue
    rows=rows if isinstance(rows,list) else json.loads(rows)
    spot=float(spot)
    gex=[]
    for rr in rows:
        try: s=float(rr[iS])
        except: continue
        if not (spot-50<=s<=spot+50): continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    charm=charm_near(ts,spot-50,spot+50)
    if not gex or not charm:
        reasons['no_features']+=1; continue
    ga=[(s,v) for s,v in gex if s>spot];gb=[(s,v) for s,v in gex if s<spot];ca=[(s,v) for s,v in charm if s>spot]
    sga=max(ga,key=lambda x:abs(x[1])) if ga else (None,0);sgb=max(gb,key=lambda x:abs(x[1])) if gb else (None,0)
    nca=[(s,v) for s,v in ca if v<0];bcm=min(nca,key=lambda x:x[1])[0] if nca else None
    tg=sum(v for _,v in gex);tc=sum(v for _,v in charm);acpp=sum(1 for _,v in ca if v>0)/max(len(ca),1)*100
    R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
    CORE_R3=sga[1]>0; CORE_R2=sgb[1]<0; R_VETO=(acpp>=80) and (not R5)
    # verdict
    if not CORE_R3: verdict='BAD'
    elif R_VETO: verdict='BAD'
    elif CORE_R2 and R5 and (tc<0 or tg>=0): verdict='A++'
    elif CORE_R2 and (R5 or tc<0): verdict='A'
    elif CORE_R2 or R5: verdict='B'
    else: verdict='C'
    # align
    cv=None
    aggc=q("""SELECT payload->'statistics'->>'aggregatedCharm' FROM volland_snapshots
        WHERE ts BETWEEN %s-interval '4 min' AND %s+interval '2 min'
        AND payload->'statistics'->>'aggregatedCharm' IS NOT NULL ORDER BY abs(extract(epoch FROM(ts-%s))) LIMIT 1""",(ts,ts,ts))
    if aggc and aggc[0][0] not in (None,''):
        try: cv=float(str(aggc[0][0]).replace('$','').replace(',',''))
        except: cv=None
    vv=vanna_near(ts); align=0
    if cv is not None: align+= 1 if cv>0 else -1
    if vv is not None: align+= 1 if vv>0 else -1
    if sga[0]: align+= 1 if spot<=sga[0] else -1
    hr=t.hour
    # determine reject reason (first failing gate in order)
    reason='ACCEPTED'
    if not CORE_R3: reason='CORE_R3=False (no +GEX magnet above)'
    elif R_VETO: reason=f'R_VETO (charm wall: {acpp:.0f}% charm-above positive)'
    elif verdict not in ('A++','A','B'): reason='verdict=C'
    elif align<0: reason=f'align<0 ({align})'
    elif hr>=15: reason='hour>=15'
    else: accepted+=1
    reasons[reason.split(' (')[0].split('=')[0] if reason!='ACCEPTED' else 'ACCEPTED']+=1
    print(f"{str(t)[:16]:17s} {spot:7.0f} {str(int(sga[0])) if sga[0] else '-':>6s} "
          f"{str(CORE_R3)[0]:3s} {str(CORE_R2)[0]:3s} {str(R5)[0]:3s} {str(R_VETO)[0]:4s} {verdict:7s} {align:+5d} {hr:2d}  {reason}")

print(f"\n=== REJECTION TALLY ({len(sigs)} signals) ===")
for r,c in reasons.most_common(): print(f"  {c:3d}  {r}")
print(f"\nACCEPTED by v3 filter: {accepted}")
conn.close()
