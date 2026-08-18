import os, json, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()

# All real TSRT trades with a broker exit, joined to sim outcomes
cur.execute("""
  SELECT rto.setup_log_id, sl.setup_name, sl.direction,
         to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM-DD') d,
         rto.state, sl.outcome_pnl, sl.mes_sim_outcome_pnl
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id = rto.setup_log_id
  WHERE sl.ts AT TIME ZONE 'America/New_York' >= '2026-06-13'
  ORDER BY sl.ts
""")
rows = cur.fetchall()

def broker_pts(state, direction):
    st = state if isinstance(state, dict) else json.loads(state)
    fp = st.get("fill_price")
    exitp = (st.get("stop_fill_price_pre_fifo_reconcile") or st.get("stop_fill_price")
             or st.get("close_fill_price_pre_fifo_reconcile") or st.get("close_fill_price"))
    if fp is None or exitp is None: return None
    d = 1 if str(direction).lower() in ("long", "bullish") else -1
    return (float(exitp) - float(fp)) * d

recs = []
for lid, sn, dirn, d, state, chain, mes in rows:
    bp = broker_pts(state, dirn)
    if bp is None: continue
    recs.append(dict(lid=lid, sn=sn, d=d, brk=bp,
                     chain=(float(chain) if chain is not None else None),
                     mes=(float(mes) if mes is not None else None)))

print(f"Real TSRT trades with broker exit fills (post-S217, Jun13+): {len(recs)}")

# Accuracy: mean abs error of each sim vs broker, on trades where that sim exists
def mae(key):
    e = [abs(r['brk'] - r[key]) for r in recs if r[key] is not None]
    return (sum(e)/len(e), len(e)) if e else (None, 0)
cm, cn = mae('chain'); mm, mn = mae('mes')
print(f"\nPREDICTOR ACCURACY vs REAL BROKER (mean abs error, pts):")
print(f"  chain-sim: MAE={cm:.2f}pt  (n={cn})")
print(f"  MES-sim  : MAE={mm:.2f}pt  (n={mn})")

# Only where BOTH exist (apples-to-apples)
both = [r for r in recs if r['chain'] is not None and r['mes'] is not None]
if both:
    ec = sum(abs(r['brk']-r['chain']) for r in both)/len(both)
    em = sum(abs(r['brk']-r['mes']) for r in both)/len(both)
    sb = sum(r['brk'] for r in both); sc = sum(r['chain'] for r in both); sm = sum(r['mes'] for r in both)
    print(f"\nMATCHED (both sims present, n={len(both)}):")
    print(f"  chain MAE={ec:.2f}pt   MES MAE={em:.2f}pt   -> {'MES' if em<ec else 'CHAIN'} closer to broker")
    print(f"  SUM broker={sb:+.1f}pt   chain={sc:+.1f}pt   MES={sm:+.1f}pt")

# Day-level: broker vs chain vs mes summed per day (both-present trades)
print("\nPER-DAY (matched trades): broker / chain / mes  [pts]")
from collections import defaultdict
dd = defaultdict(lambda: [0.0,0.0,0.0,0])
for lid, sn, dirn, d, state, chain, mes in rows:
    bp = broker_pts(state, dirn)
    if bp is None or chain is None or mes is None: continue
    dd[d][0]+=bp; dd[d][1]+=float(chain); dd[d][2]+=float(mes); dd[d][3]+=1
for d in sorted(dd):
    b,ch,m,n = dd[d]
    print(f"  {d}: n={n:2d}  broker={b:+6.1f}  chain={ch:+6.1f}  mes={m:+6.1f}")
