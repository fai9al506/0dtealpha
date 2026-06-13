"""Test ALL proposed fixes vs broker truth. Loss window + full post-V16 era.
Fixes:
 F1 vol-regime size-down (halve on range>=120)
 F2 vol-regime size-down (halve on VIX>=19)
 F3 stand-aside on high-vol days (range>=120 -> skip)
 F4 multi-day breaker (after 2 consecutive <= -250 days, half-size next days until a green day)
 F5 tighter daily breaker -200 (approx via intraday cum, cap day at -200)
Report each fix's era P&L and what it gives up on winners.
"""
import os, sys, psycopg2, json
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# day meta (range, vix) + net + intraday cum
cur.execute("""
  WITH d AS (SELECT (ts AT TIME ZONE 'America/New_York')::date dd, spot, vix,
                    ts AT TIME ZONE 'America/New_York' et FROM chain_snapshots
             WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::date>='2026-05-19')
  SELECT dd, max(spot)-min(spot), avg(vix) FROM d GROUP BY dd""")
meta={str(dd):{'rng':float(r),'vix':float(v) if v else 0} for dd,r,v in cur.fetchall()}

cur.execute("""SELECT day, net, trades FROM tsrt_daily_stmt WHERE day>='2026-05-19' ORDER BY day""")
days=[]
for day,net,trades in cur.fetchall():
    t=trades if isinstance(trades,list) else (json.loads(trades) if trades else [])
    days.append((str(day), float(net or 0), t))

def cap_day_at(trades, cap):
    """simulate halting once intraday cum (by exit_et) <= cap."""
    cum=0.0; halted=False
    for it in sorted(trades, key=lambda x:x.get('exit_et') or ''):
        if halted: continue
        cum+=float(it.get('usd') or 0)
        if cum<=cap: halted=True
    return cum

LOSS_START='2026-06-05'
def report(name, fn):
    era=loss=0.0; detail=[]
    for ds,net,t in days:
        v=fn(ds,net,t); era+=v
        if ds>=LOSS_START: loss+=v
        detail.append((ds,net,v))
    base_era=sum(n for _,n,_ in days); base_loss=sum(n for ds,n,_ in days if ds>=LOSS_START)
    print(f"\n{name}")
    print(f"  ERA: base {base_era:+.0f} -> fix {era:+.0f}  (delta {era-base_era:+.0f})")
    print(f"  LOSS WINDOW: base {base_loss:+.0f} -> fix {loss:+.0f}  (delta {loss-base_loss:+.0f})")
    # show days where fix changed a WINNER (cost) vs LOSER (saved)
    cost=sum(v-n for ds,n,v in detail if n>0 and abs(v-n)>1)
    save=sum(v-n for ds,n,v in detail if n<0 and abs(v-n)>1)
    print(f"  gave-up-on-winners {cost:+.0f} | saved-on-losers {save:+.0f}")

base_era=sum(n for _,n,_ in days)
print(f"BASELINE era (May19-Jun12) = {base_era:+.0f}  | loss window = {sum(n for ds,n,_ in days if ds>=LOSS_START):+.0f}")

report("F1 halve on range>=120", lambda ds,net,t: net*0.5 if meta.get(ds,{}).get('rng',0)>=120 else net)
report("F2 halve on VIX>=19",    lambda ds,net,t: net*0.5 if meta.get(ds,{}).get('vix',0)>=19 else net)
report("F3 stand-aside range>=120", lambda ds,net,t: 0.0 if meta.get(ds,{}).get('rng',0)>=120 else net)

# F4 multi-day breaker
def f4():
    out={}; streak=0
    for ds,net,t in days:
        size=0.5 if streak>=2 else 1.0
        out[ds]=net*size
        # update streak on ORIGINAL outcome
        if net<=-250: streak+=1
        elif net>0: streak=0
    return out
o4=f4()
report("F4 half-size after 2 consec <=-250 days", lambda ds,net,t: o4[ds])

# F5 tighter daily breaker -200
report("F5 daily breaker -200 (intraday cap)", lambda ds,net,t: cap_day_at(t,-200) if t else net)
# F5b -150
report("F5b daily breaker -150 (intraday cap)", lambda ds,net,t: cap_day_at(t,-150) if t else net)

# combo: F1 + F5b
def combo(ds,net,t):
    v = cap_day_at(t,-150) if t else net
    if meta.get(ds,{}).get('rng',0)>=120: v*=0.5
    return v
report("COMBO F1(halve hivol)+F5b(-150 breaker)", combo)
conn.close()
