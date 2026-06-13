"""
BACKTEST v2 (CORRECTED SOURCE): GEX Long v3 with paradigm gate REMOVED, using
TS GEX (TS Gamma Exposure from chain_snapshots: C_Gamma*C_OI - P_Gamma*P_OI) as
the structure source — the LIVE detector source, NOT Volland gamma.

Signal = v3 classifier verdict in {A++,A,B} (TS GEX + Volland charm)
         AND hour_et < 15
         AND (align>=0 OR paradigm in BULL_PARADIGMS)   [v3.2 route]
Exit    = SL14 / target=max(magnet, entry+20) / trail act15 gap5, walk chain spot to 16:00.
Cooldown= 15 min/day (mirrors S191). Sample chain snaps >=2 min apart.
Split outcomes by paradigm: GEX-* (current fires) vs non-GEX (gate-removal adds).
"""
import psycopg2, json
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START, END = "2026-02-23", "2026-06-02"
COOLDOWN_MIN = 15
iS, iCOI, iCG, iPG, iPOI = 10, 1, 3, 17, 19
SL, TFLOOR, TACT, TGAP = 14.0, 20.0, 15.0, 5.0
BULL_PARADIGMS = {"BofA-LIS", "GEX-TARGET", "SIDIAL-MESSY", "BOFA-PURE"}

conn = psycopg2.connect(DB); cur = conn.cursor()

# Pre-pull paradigm + aggregatedCharm + vanna_all timeline (volland) per day for joins.
def stat_near(ts):
    cur.execute("""SELECT payload->'statistics'->>'paradigm',
                          payload->'statistics'->>'aggregatedCharm'
                   FROM volland_snapshots
                   WHERE ts BETWEEN %s - interval '4 min' AND %s + interval '2 min'
                     AND payload->'statistics'->>'paradigm' IS NOT NULL
                   ORDER BY abs(extract(epoch FROM (ts - %s))) LIMIT 1""", (ts, ts, ts))
    r = cur.fetchone()
    return (r[0], r[1]) if r else (None, None)

def charm_near(ts, lo, hi):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s AND greek='charm'
                   ORDER BY ts_utc DESC LIMIT 1""", (ts, ts))
    r = cur.fetchone()
    if not r: return []
    cur.execute("""SELECT strike, value FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='charm' AND strike BETWEEN %s AND %s""",
                (r[0], lo, hi))
    return [(float(s), float(v)) for s, v in cur.fetchall()]

def vanna_all_near(ts):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s
                     AND greek='vanna' AND expiration_option='ALL'
                   ORDER BY ts_utc DESC LIMIT 1""", (ts, ts))
    r = cur.fetchone()
    if not r: return None
    cur.execute("""SELECT COALESCE(SUM(value),0) FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='vanna' AND expiration_option='ALL'""", (r[0],))
    v = cur.fetchone()
    return float(v[0]) if v else None

def classify(f):
    if not f['CORE_R3']: return 'BAD'
    if f['R_VETO']: return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']): return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']): return 'A'
    if f['CORE_R2'] or f['R5_align']: return 'B'
    return 'C'

def simulate(entry_ts, entry, target):
    cur.execute("""SELECT spot FROM chain_snapshots
                   WHERE ts >= %s
                     AND (ts AT TIME ZONE 'America/New_York')::date=(%s AT TIME ZONE 'America/New_York')::date
                     AND (ts AT TIME ZONE 'America/New_York')::time < '16:00'
                     AND spot IS NOT NULL ORDER BY ts""", (entry_ts, entry_ts))
    path = [float(r[0]) for r in cur.fetchall()]
    if not path: return None
    sl = entry - SL; mf = 0.0; ta = False; ts = sl
    for sp in path:
        mf = max(mf, sp - entry)
        stop = ts if ta else sl
        if sp <= stop: return ('WIN' if stop-entry>0 else 'LOSS'), stop-entry, mf
        if sp >= target: return 'WIN', target-entry, mf
        if not ta and mf >= TACT: ta = True; ts = entry + (mf-TGAP)
        elif ta:
            nt = entry + (mf-TGAP)
            if nt > ts: ts = nt
    return 'EXPIRED', path[-1]-entry, mf

# Iterate chain snapshots (sampled >=2min) in window, market hours, hour<15.
cur.execute(f"""SELECT ts, (ts AT TIME ZONE 'America/New_York') AS t_et, spot, rows
                FROM chain_snapshots
                WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
                  AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '14:59'
                  AND spot IS NOT NULL ORDER BY ts""")
snaps = cur.fetchall()
print(f"chain snapshots in window: {len(snaps)}")

signals = []
last_fire = {}
last_eval = {}
for ts, t_et, spot, rows in snaps:
    day = t_et.date()
    # sample >=2min apart to cut cost
    le = last_eval.get(day)
    if le is not None and (t_et - le).total_seconds() < 120:
        continue
    last_eval[day] = t_et
    lf = last_fire.get(day)
    if lf is not None and (t_et - lf).total_seconds() < COOLDOWN_MIN*60:
        continue
    rows = rows if isinstance(rows, list) else json.loads(rows)
    gex = []
    for rr in rows:
        try: s = float(rr[iS])
        except Exception: continue
        if not (spot-50 <= s <= spot+50): continue
        gex.append((s, float(rr[iCG] or 0)*float(rr[iCOI] or 0) - float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    charm = charm_near(ts, spot-50, spot+50)
    if not charm: continue
    gb=[(s,v) for s,v in gex if s<spot]; ga=[(s,v) for s,v in gex if s>spot]
    ca=[(s,v) for s,v in charm if s>spot]
    sgb=max(gb,key=lambda x:abs(x[1])) if gb else (None,0)
    sga=max(ga,key=lambda x:abs(x[1])) if ga else (None,0)
    nca=[(s,v) for s,v in ca if v<0]; bcm=min(nca,key=lambda x:x[1])[0] if nca else None
    tg=sum(v for _,v in gex); tc=sum(v for _,v in charm)
    acpp=(sum(1 for _,v in ca if v>0)/max(len(ca),1)*100)
    R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
    f={'CORE_R3':sga[1]>0,'CORE_R2':sgb[1]<0,'R5_align':R5,'R_charm_bullish':tc<0,
       'R_gex_regime_pos':tg>=0,'R_VETO':(acpp>=80) and (not R5),'gex_magnet_strike':sga[0]}
    v=classify(f)
    if v not in ('A++','A','B'): continue
    paradigm, agg = stat_near(ts)
    # alignment (long)
    charm_v=None
    if agg not in (None,''):
        try: charm_v=float(str(agg).replace('$','').replace(',',''))
        except Exception: charm_v=None
    vanna_v=vanna_all_near(ts); mpg=f['gex_magnet_strike']
    align=0
    if charm_v is not None: align += 1 if charm_v>0 else -1
    if vanna_v is not None: align += 1 if vanna_v>0 else -1
    if mpg: align += 1 if spot<=mpg else -1
    is_bull = paradigm in BULL_PARADIGMS
    if not ((align>=0) or is_bull): continue
    target=max(mpg or 0, spot+TFLOOR)
    sim=simulate(ts, spot, target)
    if not sim: continue
    last_fire[day]=t_et
    res,pnl,mf=sim
    signals.append({'day':day,'t_et':t_et,'paradigm':paradigm or '?','verdict':v,
                    'align':align,'is_gex':'GEX' in (paradigm or '').upper(),
                    'is_bull':is_bull,'result':res,'pnl':pnl,'mf':mf})

print(f"total signals (TS GEX, gate removed): {len(signals)}\n")
def stats(sigs,label):
    if not sigs: print(f"{label:34s} n=0"); return
    n=len(sigs); w=sum(1 for s in sigs if s['result']=='WIN'); p=sum(s['pnl'] for s in sigs)
    ss=sorted(sigs,key=lambda s:s['t_et']); eq=peak=mdd=0
    for s in ss: eq+=s['pnl']; peak=max(peak,eq); mdd=min(mdd,eq-peak)
    gw=sum(s['pnl'] for s in sigs if s['pnl']>0); gl=sum(s['pnl'] for s in sigs if s['pnl']<0)
    pf=(gw/abs(gl)) if gl<0 else 99
    print(f"{label:34s} n={n:3d} WR={w/n*100:4.0f}% PnL={p:+7.1f}p avg={p/n:+5.1f} PF={pf:4.2f} MaxDD={mdd:6.1f} (~${p*5:+,.0f})")

gx=[s for s in signals if s['is_gex']]; ng=[s for s in signals if not s['is_gex']]
print("="*104)
stats(gx,"GEX-* (system already fires)")
stats(ng,"non-GEX (gate-removal ADDS)")
print("-"*104)
byp=defaultdict(list)
for s in ng: byp[s['paradigm']].append(s)
for p in sorted(byp,key=lambda x:-len(byp[x])): stats(byp[p],f"   {p}")
print("-"*104)
stats([s for s in ng if s['is_bull']],"non-GEX via BULL_PARADIGMS route")
stats([s for s in ng if not s['is_bull']],"non-GEX via align>=0 route only")
print("="*104)
stats(gx,"CURRENT  (gate ON)")
stats(signals,"PROPOSED (gate OFF)")
print("="*104)
print("TODAY 2026-06-02 signals (TS GEX):")
for s in [s for s in signals if str(s['day'])=='2026-06-02']:
    print(f"   {str(s['t_et'])[11:19]} {s['paradigm']:12s} {s['verdict']:3s} align={s['align']:+d} "
          f"bull={s['is_bull']} -> {s['result']:7s} {s['pnl']:+.1f}p (mfe {s['mf']:+.1f})")
conn.close()
