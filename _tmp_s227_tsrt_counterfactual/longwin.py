"""Long-window policy comparison from SB-basket data start (2026-03-16)."""
import os, sys, json
sys.path.insert(0,"G:/My Drive/Python/MyProject/GitHub/0dtealpha")
os.environ.setdefault("DATABASE_URL",os.environ["DATABASE_URL"])
from engine import *
from sqlalchemy import text
from app.live_filter import passes_v16, load_gaps, COLS, ET
import statistics, math

DISPATCH={"Skew Charm","AG Short","Vanna Pivot Bounce","VIX Divergence","DD Exhaustion","GEX Long","ES Absorption"}
DEAD=0.15
try: MESFILL={int(k):v for k,v in json.load(open("mesfill_cache.json")).items()}
except Exception: MESFILL={}
print(f"[mes gap-fill loaded: {len(MESFILL)}]", file=sys.stderr)

with conn() as c:
    gaps=load_gaps(c)
    rows=c.execute(text(f"""SELECT {COLS}, spot, outcome_stop_level, outcome_target_level,
        trail_sl, outcome_pnl, mes_sim_outcome_pnl
        FROM setup_log WHERE ts>='2026-03-16' ORDER BY ts""")).mappings().all()
    # recompute basket_pct for ALL rows from semi_basket (stamped only from June)
    sb=c.execute(text("SELECT et, basket_pct, n_names FROM semi_basket ORDER BY et")).fetchall()
sbmap={}
for et,bp,nn in sb:
    if bp is not None and (nn or 0)>=4: sbmap[et.replace(second=0,microsecond=0)]=float(bp)

def basket_at(et_naive):
    """latest semi_basket <= signal time, within 10 min (mirrors _compute_basket_pct)."""
    t=et_naive.replace(second=0,microsecond=0)
    for k in range(0,11):
        v=sbmap.get(t-timedelta(minutes=k))
        if v is not None: return v
    return None

# build candidate set: V16 base (basket-free) + dispatchable setup
cands=[]
for r in rows:
    if r['setup_name'] not in DISPATCH: continue
    if r['ts'] is None or r['spot'] is None: continue
    l=dict(r); l['basket_pct']=None            # force basket-free V16 base
    if not passes_v16(l,gaps): continue
    et=r['ts'].astimezone(ET).replace(tzinfo=None)
    cands.append({"id":r['id'],"et":et,"setup":r['setup_name'],
        "il": r['direction'] in ('long','bullish'),"spot":float(r['spot']),
        "sl":float(r['outcome_stop_level']) if r['outcome_stop_level'] is not None else None,
        "tl":float(r['outcome_target_level']) if r['outcome_target_level'] is not None else None,
        "tsl":float(r['trail_sl']) if r['trail_sl'] is not None else None,
        "mes": float(r['mes_sim_outcome_pnl']) if r['mes_sim_outcome_pnl'] is not None
               else MESFILL.get(r['id']),
        "bp": basket_at(et)})
print(f"V16 base candidates (basket-free) since 2026-03-16: {len(cands)}", file=sys.stderr)
print(f"  with basket data: {sum(1 for x in cands if x['bp'] is not None)}", file=sys.stderr)
print(f"  with MES outcome: {sum(1 for x in cands if x['mes'] is not None)}", file=sys.stderr)
json.dump({"n":len(cands)},open("_lw_meta.json","w"))
import pickle; pickle.dump(cands,open("cands.pkl","wb"))
