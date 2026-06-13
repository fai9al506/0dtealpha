"""Today's GEX Long signals (gate removed, TS GEX) with entry/exit, MFE, MAE.
Mirrors the live detector source + v3.2 route + v3.1 exit sim."""
import psycopg2, json

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
cur = psycopg2.connect(DB).cursor()
iS, iCOI, iCG, iPG, iPOI = 10, 1, 3, 17, 19
SL, TFLOOR, TACT, TGAP = 14.0, 20.0, 15.0, 5.0
COOLDOWN_MIN = 15
BULL = {"BofA-LIS", "GEX-TARGET", "SIDIAL-MESSY", "BOFA-PURE"}

def stat_near(ts):
    cur.execute("""SELECT payload->'statistics'->>'paradigm', payload->'statistics'->>'aggregatedCharm'
                   FROM volland_snapshots WHERE ts BETWEEN %s - interval '4 min' AND %s + interval '2 min'
                     AND payload->'statistics'->>'paradigm' IS NOT NULL
                   ORDER BY abs(extract(epoch FROM (ts-%s))) LIMIT 1""", (ts, ts, ts))
    r = cur.fetchone(); return (r[0], r[1]) if r else (None, None)

def charm_near(ts, lo, hi):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
                   AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""", (ts, ts))
    r = cur.fetchone()
    if not r: return []
    cur.execute("""SELECT strike,value FROM volland_exposure_points WHERE ts_utc=%s AND greek='charm'
                   AND strike BETWEEN %s AND %s""", (r[0], lo, hi))
    return [(float(s), float(v)) for s, v in cur.fetchall()]

def vanna_all_near(ts):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points WHERE ts_utc BETWEEN %s-interval '5 min' AND %s
                   AND greek='vanna' AND expiration_option='ALL' ORDER BY ts_utc DESC LIMIT 1""", (ts, ts))
    r = cur.fetchone()
    if not r: return None
    cur.execute("""SELECT COALESCE(SUM(value),0) FROM volland_exposure_points WHERE ts_utc=%s
                   AND greek='vanna' AND expiration_option='ALL'""", (r[0],))
    v = cur.fetchone(); return float(v[0]) if v else None

def classify(f):
    if not f['CORE_R3']: return 'BAD'
    if f['R_VETO']: return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']): return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']): return 'A'
    if f['CORE_R2'] or f['R5_align']: return 'B'
    return 'C'

def simulate(entry_ts, entry, target):
    cur.execute("""SELECT (ts AT TIME ZONE 'America/New_York')::time, spot FROM chain_snapshots
                   WHERE ts >= %s AND (ts AT TIME ZONE 'America/New_York')::date=(%s AT TIME ZONE 'America/New_York')::date
                     AND (ts AT TIME ZONE 'America/New_York')::time < '16:00' AND spot IS NOT NULL ORDER BY ts""",
                (entry_ts, entry_ts))
    path = [(t, float(s)) for t, s in cur.fetchall()]
    if not path: return None
    sl = entry-SL; mfe=0.0; mae=0.0; ta=False; ts=sl
    for t, sp in path:
        mfe = max(mfe, sp-entry); mae = min(mae, sp-entry)
        stop = ts if ta else sl
        if sp <= stop: return (('WIN' if stop-entry>0 else 'LOSS'), stop-entry, mfe, mae, t, 'trail' if ta else 'stop')
        if sp >= target: return ('WIN', target-entry, mfe, mae, t, 'target')
        if not ta and mfe>=TACT: ta=True; ts=entry+(mfe-TGAP)
        elif ta:
            nt=entry+(mfe-TGAP)
            if nt>ts: ts=nt
    return ('EXPIRED', path[-1][1]-entry, mfe, mae, path[-1][0], 'eod')

cur.execute("""SELECT ts, (ts AT TIME ZONE 'America/New_York')::time, spot, rows FROM chain_snapshots
               WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-02'
                 AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '14:59'
                 AND spot IS NOT NULL ORDER BY ts""")
snaps = cur.fetchall()
last_fire=None; last_eval=None; out=[]
for ts, et, spot, rows in snaps:
    if last_eval is not None and (et.hour*3600+et.minute*60+et.second)-(last_eval.hour*3600+last_eval.minute*60+last_eval.second) < 120:
        continue
    last_eval=et
    if last_fire is not None and (et.hour*3600+et.minute*60+et.second)-(last_fire.hour*3600+last_fire.minute*60+last_fire.second) < COOLDOWN_MIN*60:
        continue
    rows = rows if isinstance(rows, list) else json.loads(rows)
    gex=[]
    for rr in rows:
        try: s=float(rr[iS])
        except: continue
        if not (spot-50<=s<=spot+50): continue
        gex.append((s, float(rr[iCG] or 0)*float(rr[iCOI] or 0) - float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    charm = charm_near(ts, spot-50, spot+50)
    if not charm: continue
    ga=[(s,v) for s,v in gex if s>spot]; gb=[(s,v) for s,v in gex if s<spot]
    ca=[(s,v) for s,v in charm if s>spot]
    sga=max(ga,key=lambda x:abs(x[1])) if ga else (None,0)
    sgb=max(gb,key=lambda x:abs(x[1])) if gb else (None,0)
    nca=[(s,v) for s,v in ca if v<0]; bcm=min(nca,key=lambda x:x[1])[0] if nca else None
    tg=sum(v for _,v in gex); tc=sum(v for _,v in charm)
    acpp=sum(1 for _,v in ca if v>0)/max(len(ca),1)*100
    R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
    f={'CORE_R3':sga[1]>0,'CORE_R2':sgb[1]<0,'R5_align':R5,'R_charm_bullish':tc<0,
       'R_gex_regime_pos':tg>=0,'R_VETO':(acpp>=80) and (not R5),'gex_magnet_strike':sga[0]}
    v=classify(f)
    if v not in ('A++','A','B'): continue
    para, agg = stat_near(ts)
    cv=None
    if agg not in (None,''):
        try: cv=float(str(agg).replace('$','').replace(',',''))
        except: cv=None
    vv=vanna_all_near(ts); mpg=f['gex_magnet_strike']; align=0
    if cv is not None: align+=1 if cv>0 else -1
    if vv is not None: align+=1 if vv>0 else -1
    if mpg: align+=1 if spot<=mpg else -1
    is_bull = para in BULL
    if not ((align>=0) or is_bull): continue
    tgt=max(mpg or 0, spot+TFLOOR)
    sim=simulate(ts, spot, tgt)
    if not sim: continue
    last_fire=et
    res,pnl,mfe,mae,xt,xr=sim
    out.append((str(et)[:8], para, v, align, spot, mpg, tgt, res, pnl, mfe, mae, str(xt)[:8], xr))

print(f"{'entry':9s} {'paradigm':11s} {'g':3s} {'al':>3s} {'entry$':>8s} {'mag':>6s} {'tgt$':>8s} "
      f"{'result':7s} {'pnl':>7s} {'MFE':>6s} {'MAE':>6s} {'exit':9s} {'why':6s}")
for r in out:
    et,para,v,al,sp,mpg,tgt,res,pnl,mfe,mae,xt,xr = r
    print(f"{et:9s} {para:11s} {v:3s} {al:+3d} {sp:8.1f} {str(int(mpg)) if mpg else '-':>6s} {tgt:8.1f} "
          f"{res:7s} {pnl:+7.1f} {mfe:+6.1f} {mae:+6.1f} {xt:9s} {xr:6s}")
# align>=0 subset (the recommended filter)
ag=[r for r in out if r[3]>=0]
n=len(ag); w=sum(1 for r in ag if r[7]=='WIN'); p=sum(r[8] for r in ag)
print(f"\nALL generated: {len(out)} | align>=0 subset: {n}t  {w}W  {p:+.1f}p (~${p*5:+,.0f}@1MES)")
