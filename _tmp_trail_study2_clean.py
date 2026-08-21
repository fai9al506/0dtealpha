# -*- coding: utf-8 -*-
"""Phase 2: detect & remove replay-burst bars (cross-check vs real SPX), then
re-validate mes_walk vs DB mes_sim. Clean bars are the basis for optimization."""
import os, sys, json, pickle, bisect, psycopg2
from datetime import timedelta
from collections import defaultdict
sys.path.insert(0,'.')
from app.mes_sim_backfill import mes_walk, _DEFAULT_PARAMS
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

# 1) SPX 2-min snapshots: epoch -> spot (per UTC ts)
cur.execute("""SELECT extract(epoch from ts), spot FROM chain_snapshots
               WHERE spot IS NOT NULL AND ts >= '2026-03-22' ORDER BY ts""")
spx=[(float(e),float(s)) for e,s in cur.fetchall()]
spx_t=[e for e,_ in spx]; spx_v=[s for _,s in spx]
def spx_near(epoch):
    i=bisect.bisect_left(spx_t,epoch)
    best=None
    for j in (i-1,i):
        if 0<=j<len(spx_t) and abs(spx_t[j]-epoch)<=180:
            if best is None or abs(spx_t[j]-epoch)<abs(spx_t[best]-epoch): best=j
    return spx_v[best] if best is not None else None

# 2) all ES 5pt bars with epoch + ET date
cur.execute("""SELECT extract(epoch from ts_start), ts_start, ts_end, bar_open,bar_high,bar_low,bar_close,
                      date(ts_start AT TIME ZONE 'America/New_York')
               FROM vps_es_range_bars WHERE range_pts=5 ORDER BY ts_start""")
raw=[]
for e,ts,te,o,h,l,cl,d in cur.fetchall():
    if o is None: continue
    raw.append((float(e),ts,te,float(o),float(h),float(l),float(cl),str(d)))
print(f"raw 5pt bars: {len(raw)}")

# 3) day median basis (ES_close - SPX_near) over bars with an SPX match
by_day=defaultdict(list)
for b in raw: by_day[b[7]].append(b)
basis_by_day={}
for d,bs in by_day.items():
    diffs=[]
    for e,ts,te,o,h,l,cl,_ in bs:
        s=spx_near(e)
        if s is not None: diffs.append(cl-s)
    if diffs:
        diffs.sort(); basis_by_day[d]=diffs[len(diffs)//2]

# 4) flag bad bars: |bar_close - (spx_near + median_basis)| > 20  (also check high/low extremes)
clean=[]; nbad=0; bad_by_day=defaultdict(int)
for e,ts,te,o,h,l,cl,d in raw:
    s=spx_near(e); mb=basis_by_day.get(d)
    if s is None or mb is None:
        clean.append((ts,te,o,h,l,cl)); continue   # keep if can't check
    exp=s+mb
    # use the bar's own close & the mid; flag if close OR both extremes are >20 off expected
    if abs(cl-exp)>20 or min(abs(h-exp),abs(l-exp))>20:
        nbad+=1; bad_by_day[d]+=1; continue
    clean.append((ts,te,o,h,l,cl))
print(f"flagged bad (replay/spike) bars: {nbad} ({nbad/len(raw)*100:.1f}%)")
top=sorted(bad_by_day.items(),key=lambda x:-x[1])[:12]
print("worst days by bad-bar count:", top)

# index clean bars by epoch for slicing
clean_t=[b[0].timestamp() for b in clean]
def bars_after(ts, max_min):
    e0=ts.timestamp(); e1=e0+max_min*60
    i=bisect.bisect_left(clean_t,e0); out=[]
    j=i
    while j<len(clean) and clean_t[j]<=e1:
        out.append(clean[j]); j+=1
    return out

# 5) re-validate vs DB mes_sim
univ=pickle.load(open("_tmp_trail_universe.pkl","rb"))
ids=[u['id'] for u in univ]
cur.execute("SELECT id, mes_sim_outcome_pnl FROM setup_log WHERE id=ANY(%s)",(ids,))
dbm={r[0]:r[1] for r in cur.fetchall()}
def live_params(u):
    df=_DEFAULT_PARAMS.get(u['setup'],{"sl":14,"be_trigger":None,"be_lock":0,"trail_act":10,"trail_gap":5})
    return (float(u['tsl']) if u['tsl'] is not None else df['sl'],df['be_trigger'],df['be_lock'],
            float(u['tact']) if u['tact'] is not None else df['trail_act'],
            float(u['tgap']) if u['tgap'] is not None else df['trail_gap'])
diffs=[];
for u in univ:
    if dbm.get(u['id']) is None: continue
    sl,bt,bl,ta,tg=live_params(u); mm=max(u['elapsed']+30,60)
    r=mes_walk(bars_after(u['ts'],mm),u['entry'],u['is_long'],sl,bt,bl,ta,tg,mm)
    u['mes_clean']=round(r['pnl'],2)
    diffs.append(abs(u['mes_clean']-float(dbm[u['id']])))
import statistics
print(f"\nAFTER CLEANING: mean abs diff my_mes vs DB_mes = {statistics.mean(diffs):.2f} (was 2.28, fat tails)")
print(f"  n={len(diffs)}  max diff={max(diffs):.1f}  >10pt outliers={sum(1 for d in diffs if d>10)}")

# save clean bars + univ
pickle.dump(clean,open("_tmp_clean_bars.pkl","wb"))
pickle.dump(univ,open("_tmp_trail_universe.pkl","wb"))
print("saved clean bars + universe")
conn.close()
