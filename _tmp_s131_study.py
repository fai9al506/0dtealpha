"""S131 study: Policy A (exit on MES crossing trail = current/broker) vs
Policy B (exit on SPX crossing trail, fill at MES; origin SL = disaster stop).
Validation: Policy A sim must reproduce broker actuals (Gate 2)."""
import os, psycopg, json, bisect
from datetime import date, timedelta
from zoneinfo import ZoneInfo
from app.mes_sim_backfill import mes_walk, _DEFAULT_PARAMS
ET=ZoneInfo("America/New_York"); UTC=ZoneInfo("UTC")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()

PARAMS={"Skew Charm":(14,10,5),"AG Short":(12,12,5),"DD Exhaustion":(12,20,5)}

def spx_b_walk(esbars, spots, entry_es, entry_spx, is_long, sl, act, gap):
    """Policy B: disaster SL on MES (fixed origin), profit-trail on SPX, fill at MES."""
    sl_mes = entry_es - sl if is_long else entry_es + sl
    # disaster stop time/price from ES bars (fixed SL, adverse-first)
    t_dis=p_dis=None
    for ts_s,ts_e,o,h,l,c in esbars:
        if (is_long and l<=sl_mes) or ((not is_long) and h>=sl_mes):
            t_dis=ts_s; p_dis=sl_mes; break
    # SPX trail exit
    maxfav=0.0; t_tr=None
    for ts,spot in spots:
        fav=(spot-entry_spx) if is_long else (entry_spx-spot)
        if fav>maxfav: maxfav=fav
        if maxfav>=act:
            trail=entry_spx+(maxfav-gap) if is_long else entry_spx-(maxfav-gap)
            crossed=(is_long and spot<=trail) or ((not is_long) and spot>=trail)
            if crossed: t_tr=ts; break
    # earliest exit
    cand=[]
    if t_dis: cand.append((t_dis,"disaster",p_dis))
    if t_tr:  cand.append((t_tr,"spx_trail",None))
    if not cand:
        c=esbars[-1][5]; return (c-entry_es) if is_long else (entry_es-c),"eod"
    cand.sort(key=lambda x:x[0]); t,reason,px=cand[0]
    if reason=="disaster":
        return (px-entry_es) if is_long else (entry_es-px),"disaster"
    # fill at MES price nearest t_tr
    fill=None
    for ts_s,ts_e,o,h,l,c in esbars:
        if ts_s<=t_tr: fill=c
        else: break
    if fill is None: fill=esbars[0][0] if False else entry_es
    return (fill-entry_es) if is_long else (entry_es-fill),"spx_trail"

cur.execute("""SELECT s.id,s.ts,s.setup_name,s.direction,r.state
  FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.setup_name IN ('Skew Charm','AG Short','DD Exhaustion')
    AND (r.state->>'fill_price') IS NOT NULL AND s.ts::date>='2026-04-15'
  ORDER BY s.ts""")
rows=cur.fetchall()
results=[]
for lid,ts,name,dirn,state in rows:
    st=state if isinstance(state,dict) else json.loads(state)
    entry=st.get("fill_price"); 
    exit_=st.get("close_fill_price") or st.get("stop_fill_price")
    if not entry or not exit_: continue
    is_long=str(dirn).lower() in ("long","bullish")
    broker=(exit_-entry) if is_long else (entry-exit_)
    sl,act,gap=PARAMS[name]
    end=ts+timedelta(minutes=150)
    cur.execute("SELECT ts_start,ts_end,bar_open,bar_high,bar_low,bar_close FROM vps_es_range_bars WHERE range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start",(ts,end))
    esbars=[(r[0],r[1],float(r[2]),float(r[3]),float(r[4]),float(r[5])) for r in cur.fetchall()]
    if not esbars: continue
    cur.execute("SELECT ts,spot FROM chain_snapshots WHERE ts>=%s AND ts<=%s AND spot IS NOT NULL ORDER BY ts",(ts,end))
    spots=[(r[0],float(r[1])) for r in cur.fetchall()]
    entry_spx=spots[0][1] if spots else entry
    A=mes_walk(esbars,entry,is_long,sl,None,0,act,gap,150)
    a_pnl=A["pnl"]
    b_pnl,b_reason=spx_b_walk(esbars,spots,entry,entry_spx,is_long,sl,act,gap)
    results.append((lid,ts.astimezone(ET),name,is_long,broker,a_pnl,b_pnl,b_reason))

# Gate 2: A-sim vs broker
import statistics
diffs=[abs(a-bk) for _,_,_,_,bk,a,b,_ in results]
print(f"n={len(results)}  Gate-2 |A_sim - broker| mean={statistics.mean(diffs):.2f}pt median={statistics.median(diffs):.2f}pt (S55 baseline 2.35)")
tb=sum(bk for *_,bk,a,b,_ in results); ta=sum(a for *_,bk,a,b,_ in results); tB=sum(b for *_,bk,a,b,_ in results)
print(f"\nTOTALS (pts):  broker {tb:+.0f}  | PolicyA-sim {ta:+.0f}  | PolicyB(SPX-timing) {tB:+.0f}")
print(f"  in $@1MES:   broker ${tb*5:+.0f} | A ${ta*5:+.0f} | B ${tB*5:+.0f}   ->  B-vs-A delta {(tB-ta)*5:+.0f}")
# where B differs from A
better=[r for r in results if r[6]-r[5]>1]; worse=[r for r in results if r[6]-r[5]<-1]
print(f"\nB beats A: {len(better)} trades (+{sum(r[6]-r[5] for r in better):.0f}pt)")
print(f"B worse than A: {len(worse)} trades ({sum(r[6]-r[5] for r in worse):.0f}pt)")
print(f"\nBiggest B-improvements:")
for r in sorted(results,key=lambda x:-(x[6]-x[5]))[:8]:
    print(f"  {r[1].date()} lid{r[0]:>5} {r[2]:<12} A{r[5]:+6.1f} B{r[6]:+6.1f} (Δ{r[6]-r[5]:+5.1f}) {r[7]}")
print(f"Biggest B-regressions:")
for r in sorted(results,key=lambda x:(x[6]-x[5]))[:6]:
    print(f"  {r[1].date()} lid{r[0]:>5} {r[2]:<12} A{r[5]:+6.1f} B{r[6]:+6.1f} (Δ{r[6]-r[5]:+5.1f}) {r[7]}")
