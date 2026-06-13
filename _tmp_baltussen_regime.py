"""Test the PEER-REVIEWED edge (Baltussen/Da JFE 2021) on our data:
In the NEGATIVE-gamma regime, does the rest-of-day return predict the last-30-min
(into close) return? + the +gamma mean-reversion counterpart. DAY-LEVEL (70 obs,
no intraday autocorrelation). Regime = sign of net GEX (gamma flip) measured ~15:00.
"""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,args):
    global conn,cur
    try: cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()

# full-day spot path to 16:05
DP=defaultdict(list)
for d,ts,t,spot in q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date, ts,
    (ts AT TIME ZONE 'America/New_York')::time, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}' AND spot IS NOT NULL ORDER BY ts""",()):
    DP[d].append((str(t)[:8],float(spot)))
def at_or_after(day,hhmm):
    for t,sp in DP.get(day,[]):
        if t>=hhmm: return sp
    return None
def last_before(day,hhmm):
    prev=None
    for t,sp in DP.get(day,[]):
        if t<=hhmm: prev=sp
        else: break
    return prev

# net GEX regime per day, measured ~15:00 (decision time for the close trade)
regime={}
rows=q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date d, ts, rows,
    (ts AT TIME ZONE 'America/New_York')::time t, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '14:45' AND '15:15'
    AND spot IS NOT NULL ORDER BY ts""",())
seen=set()
for d,ts,rr,t,spot in rows:
    if d in seen: continue
    seen.add(d)
    rr=rr if isinstance(rr,list) else json.loads(rr)
    tg=0
    for row in rr:
        try: s=float(row[iS])
        except: continue
        if not (spot-60<=s<=spot+60): continue
        tg+=float(row[iCG] or 0)*float(row[iCOI] or 0)-float(row[iPG] or 0)*float(row[iPOI] or 0)
    regime[d]=tg

days=[]
for d in sorted(DP):
    if d not in regime: continue
    op=at_or_after(d,'09:35'); mid=last_before(d,'15:30'); close=DP[d][-1][1]
    if op is None or mid is None: continue
    ret_day=mid-op            # rest-of-day (open -> 15:30)
    ret_close=close-mid       # last 30 min (15:30 -> close)
    days.append({'d':d,'reg':'neg' if regime[d]<0 else 'pos','ret_day':ret_day,'ret_close':ret_close})
print(f"days: {len(days)}  (neg-gamma {sum(1 for x in days if x['reg']=='neg')}, pos-gamma {sum(1 for x in days if x['reg']=='pos')})\n")

def momentum_test(rs,label):
    rs=[r for r in rs if abs(r['ret_day'])>2]  # need a real intraday move to predict from
    if not rs: print(f"  {label}: n=0"); return
    # momentum = close continues day direction
    cont=sum(1 for r in rs if (r['ret_close']>0)==(r['ret_day']>0))
    n=len(rs)
    # avg close-return if you trade IN the day's direction
    pnl=sum(r['ret_close'] if r['ret_day']>0 else -r['ret_close'] for r in rs)
    print(f"  {label}: n={n}  P(close CONTINUES day dir)={cont/n*100:.0f}%  "
          f"avg close-ret trading WITH day dir={pnl/n:+.2f}p  total={pnl:+.1f}p")

print("=== BALTUSSEN intraday momentum into close (trade last 30min WITH rest-of-day dir) ===")
momentum_test([r for r in days if r['reg']=='neg'], "NEG-gamma (edge expected HERE)")
momentum_test([r for r in days if r['reg']=='pos'], "POS-gamma (expect weak/revert)")
momentum_test(days, "ALL days (baseline)")

print("\n=== POS-gamma mean-reversion (does last 30min REVERSE the day move?) ===")
def revert_test(rs,label):
    rs=[r for r in rs if abs(r['ret_day'])>2]
    if not rs: print(f"  {label}: n=0"); return
    rev=sum(1 for r in rs if (r['ret_close']>0)!=(r['ret_day']>0))
    pnl=sum(-r['ret_close'] if r['ret_day']>0 else r['ret_close'] for r in rs)  # fade
    print(f"  {label}: n={len(rs)}  P(close REVERSES day dir)={rev/len(rs)*100:.0f}%  "
          f"avg close-ret FADING day dir={pnl/len(rs):+.2f}p")
revert_test([r for r in days if r['reg']=='pos'], "POS-gamma fade")
revert_test([r for r in days if r['reg']=='neg'], "NEG-gamma fade (control)")
conn.close()
