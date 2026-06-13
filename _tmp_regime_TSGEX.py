"""Gamma-regime test with regime = TS GEX (TS Gamma Exposure, full 0DTE chain, ALL
strikes: C_Gamma*C_OI - P_Gamma*P_OI), measured ~15:00 ET per day.
Step 1: anchor sign empirically via realized range (don't assume).
Step 2: momentum-into-close vs mean-reversion by regime, day-level, with z-scores.
Horizons: last-30min (Baltussen) and PM (12:00->close). Also tercile + sign splits.
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
def at_after(day,hhmm):
    for t,sp in DP.get(day,[]):
        if t>=hhmm: return sp
    return None
def last_before(day,hhmm):
    prev=None
    for t,sp in DP.get(day,[]):
        if t<=hhmm: prev=sp
        else: break
    return prev

# regime = net TS GEX over the FULL 0DTE chain (all strikes), nearest 15:00 ET per day
rows=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d,
    (ts AT TIME ZONE 'America/New_York')::time t, rows, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '14:30' AND '15:30' AND spot IS NOT NULL ORDER BY ts""")
best={}
for d,t,rr,spot in rows:
    sec=abs((int(t.hour)*3600+int(t.minute)*60)-15*3600)
    if d not in best or sec<best[d][0]: best[d]=(sec,rr,spot)
net_ts={}
for d,(_,rr,spot) in best.items():
    rr=rr if isinstance(rr,list) else json.loads(rr)
    tg=0.0
    for row in rr:
        try: s=float(row[iS])
        except: continue
        tg+=float(row[iCG] or 0)*float(row[iCOI] or 0)-float(row[iPG] or 0)*float(row[iPOI] or 0)
    net_ts[d]=tg

days=[]
for d in sorted(DP):
    if d not in net_ts: continue
    arr=DP[d]
    op=at_after(d,'09:35'); noon=last_before(d,'12:00'); mid=last_before(d,'15:30'); close=arr[-1][1]
    if None in (op,noon,mid): continue
    hi=max(s for _,s in arr); lo=min(s for _,s in arr)
    days.append({'d':d,'net':net_ts[d],'rng':hi-lo,
                 'ret_day':mid-op,'ret_close':close-mid,'ret_am':noon-op,'ret_pm':close-noon})
print(f"days: {len(days)}")
nets=sorted(d['net'] for d in days)
npos=sum(1 for x in days if x['net']>=0); nneg=len(days)-npos
print(f"net TS GEX: min={nets[0]:.3g}  median={nets[len(nets)//2]:.3g}  max={nets[-1]:.3g}  (pos days={npos}, neg days={nneg})")

s=sorted(days,key=lambda x:x['net']); t=len(s)//3
low=s[:t]; high=s[-t:]
def meanrng(g): return sum(x['rng'] for x in g)/len(g)
print(f"\n=== STEP 1 ANCHOR: which tercile is low-vol (+gamma)? ===")
print(f"  HIGH net-TSGEX tercile (n={len(high)}): mean range = {meanrng(high):.1f} pts")
print(f"  LOW  net-TSGEX tercile (n={len(low)}):  mean range = {meanrng(low):.1f} pts")
pos_is_high = meanrng(high)<meanrng(low)
print(f"  => +gamma (low-vol) regime = {'HIGH' if pos_is_high else 'LOW'} net-TSGEX tercile")

def z(p,n): return (p-0.5)/sqrt(0.25/n) if n else 0
def test(g,label,dk,ck):
    g=[r for r in g if abs(r[dk])>2]
    if not g: print(f"  {label}: n=0"); return
    cont=sum(1 for r in g if (r[ck]>0)==(r[dk]>0)); n=len(g); p=cont/n
    pnl=sum(r[ck] if r[dk]>0 else -r[ck] for r in g)
    print(f"  {label}: n={n}  P(continue)={p*100:.0f}% (z={z(p,n):+.1f})  WITH-trend avg={pnl/n:+.2f}p  FADE avg={-pnl/n:+.2f}p")

posreg,negreg = (high,low) if pos_is_high else (low,high)
# also sign-based split (net>=0 vs net<0)
sign_pos=[x for x in days if x['net']>=0]; sign_neg=[x for x in days if x['net']<0]

print(f"\n=== STEP 2a TERCILE: LAST-30-MIN (open->15:30 predicts 15:30->close) ===")
test(posreg,"+gamma (low-vol) ",'ret_day','ret_close')
test(negreg,"-gamma (high-vol)",'ret_day','ret_close')
print(f"\n=== STEP 2b TERCILE: PM (open->12:00 predicts 12:00->close) ===")
test(posreg,"+gamma (low-vol) ",'ret_am','ret_pm')
test(negreg,"-gamma (high-vol)",'ret_am','ret_pm')
print(f"\n=== STEP 2c SIGN-SPLIT (net TS GEX >=0 vs <0): last-30-min ===")
test(sign_pos,"net>=0",'ret_day','ret_close')
test(sign_neg,"net<0 ",'ret_day','ret_close')
print(f"\n=== SIGN-SPLIT: PM ===")
test(sign_pos,"net>=0",'ret_am','ret_pm')
test(sign_neg,"net<0 ",'ret_am','ret_pm')
print("\n(|z|>1.6 ~ 5% one-sided significance; below that = not significant at n~20-35)")
conn.close()
