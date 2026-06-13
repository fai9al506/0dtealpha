"""DETAILED regime-filter report. Regime = SIGN of net TS GEX (all strikes,
C_Gamma*C_OI - P_Gamma*P_OI) at the daily snapshot nearest 12:00 ET.
  +gamma = net TS GEX >= 0 (low-vol/pin regime)
  -gamma = net TS GEX <  0 (high-vol regime)
For each setup: trades, distinct days, WR, TOTAL points, avg/trade, max drawdown,
best/worst trade. Then the GEX Long FILTER before/after. outcome_pnl is POINTS.
This is a FILTER on existing setups, not a new setup.
"""
import psycopg2, json
from collections import defaultdict
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
def regime(d):
    v=net_ts.get(d)
    if v is None: return None
    return 'pos' if v>=0 else 'neg'
npos=sum(1 for d in net_ts if net_ts[d]>=0); nneg=len(net_ts)-npos
print(f"Regime defined for {len(net_ts)} trading days: +gamma={npos} days, -gamma={nneg} days")
print(f"(+gamma = net TS GEX>=0 at noon = low-vol/pin; -gamma = net TS GEX<0 = high-vol)\n")

sigs=q(f"""SELECT setup_name,(ts AT TIME ZONE 'America/New_York')::date d, ts,
    outcome_result, outcome_pnl FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND outcome_result IS NOT NULL AND outcome_pnl IS NOT NULL AND grade!='LOG'
    ORDER BY ts""")
data=defaultdict(lambda:defaultdict(list))  # setup -> regime -> [(ts,date,res,pnl)]
for name,d,ts,res,pnl in sigs:
    r=regime(d)
    if r: data[name][r].append((ts,d,res,float(pnl)))

def report(rows):
    if not rows: return None
    rows=sorted(rows,key=lambda x:x[0])
    n=len(rows); days=len(set(x[1] for x in rows))
    w=sum(1 for x in rows if x[2]=='WIN'); tot=sum(x[3] for x in rows)
    eq=0;peak=0;dd=0
    for x in rows: eq+=x[3]; peak=max(peak,eq); dd=min(dd,eq-peak)
    best=max(x[3] for x in rows); worst=min(x[3] for x in rows)
    return dict(n=n,days=days,wr=w/n*100,tot=tot,avg=tot/n,dd=dd,best=best,worst=worst)

def line(label,r):
    if not r: print(f"   {label:16s} n=0"); return
    print(f"   {label:16s} {r['n']:4d} trades / {r['days']:2d} days | WR {r['wr']:3.0f}% | "
          f"TOTAL {r['tot']:+7.1f}p | avg {r['avg']:+5.2f}p | maxDD {r['dd']:6.1f}p | "
          f"best {r['best']:+.0f} worst {r['worst']:+.0f}")

KEY=['GEX Long','Skew Charm','ES Absorption','AG Short','DD Exhaustion']
for name in KEY:
    print(f"=== {name} ===")
    allrows=data[name]['pos']+data[name]['neg']
    line("ALL (current)", report(allrows))
    line("+gamma days", report(data[name]['pos']))
    line("-gamma days", report(data[name]['neg']))
    print()

print("="*96)
print("GEX LONG — FILTER IMPACT (this is the actionable gate: block GEX Long on -gamma days)")
print("="*96)
allr=report(data['GEX Long']['pos']+data['GEX Long']['neg'])
posr=report(data['GEX Long']['pos']); negr=report(data['GEX Long']['neg'])
line("CURRENT (no filter)", allr)
line("AFTER FILTER (+gamma only)", posr)
line("REMOVED (-gamma, the bleed)", negr)
if allr and posr and negr:
    print(f"\n   => Filter removes {negr['n']} trades that netted {negr['tot']:+.1f}p (WR {negr['wr']:.0f}%).")
    print(f"   => Kept book improves from {allr['tot']:+.1f}p ({allr['wr']:.0f}% WR, maxDD {allr['dd']:.0f}p)")
    print(f"      to {posr['tot']:+.1f}p ({posr['wr']:.0f}% WR, maxDD {posr['dd']:.0f}p) on {posr['n']} trades / {posr['days']} days.")
    print(f"   => Net swing from gating: {posr['tot']-allr['tot']:+.1f}p saved (the -gamma bleed avoided).")
conn.close()
