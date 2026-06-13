"""Gamma-regime test with the CORRECT regime input: full-expiry dealer gamma from
Volland gamma exposure (expiration_option='ALL'), measured ~15:00 ET per day.

Step 1 (ANCHOR the sign): the +gamma regime must show LOWER realized daily range
(SqueezeMetrics validated vol result). Use that to confirm which sign = +gamma.
Step 2: test momentum-into-close vs mean-reversion-into-close by regime, day-level,
with z-scores. Two horizons: last-30min (Baltussen) and PM (12:00->close).
"""
import psycopg2
from collections import defaultdict
from math import sqrt
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,args=()):
    global conn,cur
    try: cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()

# spot path per day (with ET time strings) to derive open/12:00/15:30/close + range
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

# Full-expiry gamma regime per day from Volland ALL, nearest 15:00 ET
net_all={}
rows=q(f"""SELECT (ts_utc AT TIME ZONE 'America/New_York')::date d,
    (ts_utc AT TIME ZONE 'America/New_York')::time t, ts_utc
    FROM volland_exposure_points
    WHERE greek='gamma' AND expiration_option='ALL'
    AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts_utc AT TIME ZONE 'America/New_York')::time BETWEEN '14:30' AND '15:30'
    ORDER BY ts_utc""")
# pick one ts_utc per day (closest to 15:00)
best={}
for d,t,ts in rows:
    sec=abs((int(t.hour)*3600+int(t.minute)*60)-15*3600)
    if d not in best or sec<best[d][0]: best[d]=(sec,ts)
for d,(_,ts) in best.items():
    r=q("SELECT COALESCE(SUM(value),0) FROM volland_exposure_points WHERE ts_utc=%s AND greek='gamma' AND expiration_option='ALL'",(ts,))
    net_all[d]=float(r[0][0]) if r else 0.0

days=[]
for d in sorted(DP):
    if d not in net_all: continue
    arr=DP[d]
    op=at_after(d,'09:35'); noon=last_before(d,'12:00'); mid=last_before(d,'15:30'); close=arr[-1][1]
    if None in (op,noon,mid): continue
    hi=max(s for _,s in arr); lo=min(s for _,s in arr)
    days.append({'d':d,'net':net_all[d],'rng':hi-lo,
                 'ret_day':mid-op,'ret_close':close-mid,'ret_am':noon-op,'ret_pm':close-noon})
print(f"days with full-expiry gamma regime: {len(days)}")
nets=sorted(d['net'] for d in days)
print(f"net Volland gamma ALL: min={nets[0]:.2e}  median={nets[len(nets)//2]:.2e}  max={nets[-1]:.2e}")

# tercile split on net gamma
s=sorted(days,key=lambda x:x['net']); n=len(s); t=n//3
low=s[:t]; high=s[-t:]   # low net (more negative) vs high net (more positive)
def meanrng(g): return sum(x['rng'] for x in g)/len(g)
print(f"\n=== STEP 1 ANCHOR (validated vol result): +gamma should have LOWER range ===")
print(f"  HIGH net-gamma tercile (n={len(high)}): mean daily range = {meanrng(high):.1f} pts")
print(f"  LOW  net-gamma tercile (n={len(low)}):  mean daily range = {meanrng(low):.1f} pts")
print(f"  => {'CONFIRMS +gamma=low vol (high tercile=+gamma regime)' if meanrng(high)<meanrng(low) else 'INVERTED: low tercile behaves as +gamma — Volland sign is flipped'}")

# decide which tercile is the +gamma (low-vol) regime
posreg, negreg = (high,low) if meanrng(high)<meanrng(low) else (low,high)

def z(p,n): return (p-0.5)/sqrt(0.25/n) if n else 0
def test(g,label,day_key,close_key):
    g=[r for r in g if abs(r[day_key])>2]
    if not g: print(f"  {label}: n=0"); return
    cont=sum(1 for r in g if (r[close_key]>0)==(r[day_key]>0)); n=len(g)
    p=cont/n
    pnl_mom=sum(r[close_key] if r[day_key]>0 else -r[close_key] for r in g)
    print(f"  {label}: n={n}  P(continue)={p*100:.0f}% (z={z(p,n):+.1f})  "
          f"WITH-trend avg={pnl_mom/n:+.2f}p  FADE avg={-pnl_mom/n:+.2f}p")

print(f"\n=== STEP 2a: LAST-30-MIN (Baltussen) — open->15:30 predicts 15:30->close ===")
test(posreg,"+gamma regime",'ret_day','ret_close')
test(negreg,"-gamma regime",'ret_day','ret_close')
print(f"\n=== STEP 2b: PM horizon — open->12:00 predicts 12:00->close ===")
test(posreg,"+gamma regime",'ret_am','ret_pm')
test(negreg,"-gamma regime",'ret_am','ret_pm')
print("\n(z>1.6 ~ 5% one-sided; |z|<1.6 = not significant at this sample)")
conn.close()
