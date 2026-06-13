"""Audit: SHORTS taken with spot 0-15pt above a dominant negative DD floor >=2B.
Check date/setup distribution to rule out single-day artifact. Compare vs existing
v13 SC/DD >=3B block (which already exists)."""
import os, psycopg2, bisect
from datetime import date
from collections import defaultdict
from zoneinfo import ZoneInfo
UTC = ZoneInfo("UTC"); ET = ZoneInfo("America/New_York")
START, END = date(2026,4,1), date(2026,6,1)
c=psycopg2.connect(os.environ["DATABASE_URL"]);cur=c.cursor()
cur.execute("""SELECT ts_utc, strike::numeric, value::numeric, current_price::numeric
  FROM volland_exposure_points WHERE greek='deltaDecay' AND expiration_option='TODAY'
  AND ticker='SPX' AND ts_utc::date BETWEEN %s AND %s ORDER BY ts_utc""",(START,END))
snaps=defaultdict(list); spot_at={}
for ts,s,v,cp in cur.fetchall():
    snaps[ts].append((float(s),float(v)))
    if cp is not None: spot_at[ts]=float(cp)
sts=sorted(snaps); aware=[t if t.tzinfo else t.replace(tzinfo=UTC) for t in sts]
def to_a(t): return t if t.tzinfo else t.replace(tzinfo=UTC)
def nsnap(ts):
    ts=to_a(ts); i=bisect.bisect_left(aware,ts); cand=[]
    if i<len(aware): cand.append(sts[i])
    if i>0: cand.append(sts[i-1])
    if not cand: return None
    b=min(cand,key=lambda t:abs((to_a(t)-ts).total_seconds()))
    return b if abs((to_a(b)-ts).total_seconds())<=240 else None
def feat(ts,spot):
    sn=nsnap(ts)
    if sn is None: return None
    sp=spot if spot is not None else spot_at.get(sn)
    if sp is None: return None
    negs=sorted([p for p in snaps[sn] if p[1]<0],key=lambda p:p[1])
    if not negs: return None
    ds,dv=negs[0]; second=abs(negs[1][1]) if len(negs)>=2 else 1.0
    return dict(ds=ds,dv=dv,ratio=abs(dv)/max(second,1.0),dist=sp-ds)

cur.execute("""SELECT id,ts,setup_name,direction,grade,outcome_result,outcome_pnl
  FROM setup_log WHERE ts::date BETWEEN %s AND %s
  AND outcome_result IN ('WIN','LOSS','EXPIRED') AND outcome_pnl IS NOT NULL
  ORDER BY ts""",(START,END))
SHORT={"short","bearish"}
by_date=defaultdict(lambda:{"n":0,"w":0,"pts":0.0}); by_setup=defaultdict(lambda:{"n":0,"w":0,"pts":0.0})
tot={"n":0,"w":0,"pts":0.0}
for sid,ts,setup,dir_,grade,res,pnl in cur.fetchall():
    if (dir_ or "").lower() not in SHORT: continue
    f=feat(ts,None)
    if not f: continue
    if not (f["ratio"]>=2.0 and 0<f["dist"]<=15 and abs(f["dv"])>=2e9): continue
    d=to_a(ts).astimezone(ET).date().isoformat()
    for b in (by_date[d],by_setup[setup],tot):
        b["n"]+=1; b["w"]+= (res=="WIN"); b["pts"]+=float(pnl)
print(f"SHORTS above >=2B neg DD floor (0-15pt, ratio>=2): n={tot['n']} WR={100*tot['w']/max(tot['n'],1):.0f}% pts={tot['pts']:+.1f}")
print("\nBy DATE:")
for d in sorted(by_date): s=by_date[d]; print(f"  {d}  n={s['n']:<2} W={s['w']} pts={s['pts']:+6.1f}")
print("\nBy SETUP:")
for k in sorted(by_setup,key=lambda x:by_setup[x]['pts']): s=by_setup[k]; print(f"  {k:<18} n={s['n']:<2} W={s['w']} pts={s['pts']:+6.1f}")
cur.close();c.close()
