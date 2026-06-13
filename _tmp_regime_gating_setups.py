"""ACTIONABLE backtest: split each existing setup's historical outcomes by TS-GEX
regime (+gamma low-vol vs -gamma high-vol). If mean-reversion setups do better in
+gamma and worse in -gamma, that's a shippable gating rule.

Regime = sign/tercile of net TS GEX (all strikes, C_Gamma*C_OI - P_Gamma*P_OI) at
the daily snapshot nearest 12:00 ET. Day-level (regime sign stable intraday).
outcome_pnl is in POINTS.
"""
import psycopg2, json
from collections import defaultdict
from math import sqrt
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

# daily regime: net TS GEX nearest 12:00 ET + daily range (vol anchor recheck)
rows=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d,(ts AT TIME ZONE 'America/New_York')::time t, rows, spot
    FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '11:30' AND '12:30' AND spot IS NOT NULL ORDER BY ts""")
best={}
for d,t,rr,spot in rows:
    sec=abs((int(t.hour)*3600+int(t.minute)*60)-12*3600)
    if d not in best or sec<best[d][0]: best[d]=(sec,rr,spot)
net_ts={}
for d,(_,rr,spot) in best.items():
    rr=rr if isinstance(rr,list) else json.loads(rr); tg=0.0
    for row in rr:
        try: s=float(row[iS])
        except: continue
        tg+=float(row[iCG] or 0)*float(row[iCOI] or 0)-float(row[iPG] or 0)*float(row[iPOI] or 0)
    net_ts[d]=tg
# daily range for vol-anchor recheck
rng={}
for d,lo,hi in q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date, min(spot), max(spot)
    FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND spot IS NOT NULL GROUP BY 1"""):
    rng[d]=float(hi)-float(lo)
# tercile thresholds
vals=sorted(net_ts.values()); n=len(vals); lo_thr=vals[n//3]; hi_thr=vals[2*n//3]
def regime(d):
    v=net_ts.get(d)
    if v is None: return None
    if v>=hi_thr: return 'pos'    # +gamma (highest tercile = low vol, validated)
    if v<=lo_thr: return 'neg'    # -gamma (lowest tercile = high vol)
    return 'mid'
# vol anchor recheck at 12:00
hi_d=[d for d in net_ts if regime(d)=='pos']; lo_d=[d for d in net_ts if regime(d)=='neg']
print(f"VOL ANCHOR @12:00: +gamma range={sum(rng[d] for d in hi_d if d in rng)/len([d for d in hi_d if d in rng]):.1f}pt  "
      f"-gamma range={sum(rng[d] for d in lo_d if d in rng)/len([d for d in lo_d if d in rng]):.1f}pt  (n={len(hi_d)}/{len(lo_d)})\n")

# setups + outcomes
sigs=q(f"""SELECT setup_name, (ts AT TIME ZONE 'America/New_York')::date d, direction,
    outcome_result, outcome_pnl FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND outcome_result IS NOT NULL AND outcome_pnl IS NOT NULL AND grade != 'LOG'""")
bucket=defaultdict(lambda:defaultdict(list))
for name,d,dirn,res,pnl in sigs:
    r=regime(d)
    if r in ('pos','neg'):
        bucket[name][r].append((res,float(pnl)))
def z(p,nn): return (p-0.5)/sqrt(0.25/nn) if nn else 0
def stat(rows):
    if not rows: return None
    n=len(rows); w=sum(1 for r,_ in rows if r=='WIN'); p=sum(x for _,x in rows)
    return n,w/n*100,p,p/n
print(f"{'setup':18s} {'+gamma (low-vol)':28s} {'-gamma (high-vol)':28s}  signal")
print("-"*90)
for name in sorted(bucket):
    P=stat(bucket[name]['pos']); N=stat(bucket[name]['neg'])
    ps=f"n={P[0]:3d} WR={P[1]:3.0f}% avg={P[3]:+5.1f}p" if P else "n=0"
    ns=f"n={N[0]:3d} WR={N[1]:3.0f}% avg={N[3]:+5.1f}p" if N else "n=0"
    sig=""
    if P and N and P[0]>=8 and N[0]>=8:
        d_wr=P[1]-N[1]; d_avg=P[3]-N[3]
        if d_avg>=3: sig=f"<= +gamma BETTER by {d_avg:+.1f}p/tr"
        elif d_avg<=-3: sig=f"<= -gamma BETTER by {-d_avg:+.1f}p/tr"
    print(f"{name:18s} {ps:28s} {ns:28s}  {sig}")

print("\n=== AGGREGATE: mean-reversion setups (SC/DD/ES Abs/BofA) by regime ===")
mr=('Skew Charm','DD Exhaustion','ES Absorption','BofA Scalp','SB Absorption','SB2 Absorption')
for r in ('pos','neg'):
    allr=[x for name in mr for x in bucket.get(name,{}).get(r,[])]
    s=stat(allr)
    if s: print(f"  {('+gamma' if r=='pos' else '-gamma')}: n={s[0]} WR={s[1]:.0f}% avg={s[3]:+.1f}p total={s[2]:+.1f}p")
conn.close()
