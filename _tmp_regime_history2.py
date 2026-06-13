"""GATE part 2 — reconcile the 'months of profit then bleed'.

Q1: Was the 3-4mo profit REAL money or portal-sim? (real_trade_orders by month)
Q2: chain-sim vs mes-sim vs broker by month -> how much did sim HIDE? (Apr15+)
Q3: Long/short MIX by month -> did the book tilt long?
Q4: March(hivol-down, WON) vs June(hivol-down, LOST): trend persistence + side.
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
WL = ('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')
MES=5.0
def rp(state, direction):
    if isinstance(state,str): state=json.loads(state)
    f=state.get('entry_fill_price') or state.get('fill_price')
    e=state.get('stop_fill_price') or state.get('close_fill_price')
    if f is None or e is None: return None
    f,e=float(f),float(e)
    return (e-f) if direction in ('long','bullish') else (f-e)

# Q1: real trades by month
print("=== Q1: REAL placed trades & broker pts by month ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s ORDER BY sl.ts""",(WL,))
rm=defaultdict(lambda:{'n':0,'pts':0.0,'w':0})
for d,direction,state in cur.fetchall():
    m=str(d)[:7]; p=rp(state,direction)
    if p is None: continue
    rm[m]['n']+=1; rm[m]['pts']+=p
    if p>0: rm[m]['w']+=1
for m in sorted(rm):
    a=rm[m]; print(f"  {m}: real_trades={a['n']:>3} WR={a['w']/a['n']*100:>3.0f}% broker_pts={a['pts']:>+7.1f} ${a['pts']*MES:>+7.0f}")

# Q2: chain vs mes vs broker by month (only where all exist)
print("\n=== Q2: chain-sim vs mes-sim vs broker by month (real trades) ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction,
                      sl.outcome_pnl, sl.mes_sim_outcome_pnl, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s ORDER BY sl.ts""",(WL,))
cm=defaultdict(lambda:{'chain':0.0,'mes':0.0,'brk':0.0,'n':0,'mes_n':0})
for d,direction,opnl,mpnl,state in cur.fetchall():
    m=str(d)[:7]; a=cm[m]; a['n']+=1
    if opnl is not None: a['chain']+=float(opnl)
    if mpnl is not None: a['mes']+=float(mpnl); a['mes_n']+=1
    p=rp(state,direction)
    if p is not None: a['brk']+=p
for m in sorted(cm):
    a=cm[m]
    print(f"  {m}: chain={a['chain']:>+7.1f}  mes={a['mes']:>+7.1f}(n={a['mes_n']})  broker={a['brk']:>+7.1f}  chain-broker_gap={a['chain']-a['brk']:>+7.1f}")

# Q3: long/short count mix by month (portal signals, WL)
print("\n=== Q3: long vs short MIX by month (portal signals) ===")
cur.execute(f"""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d,
   sum(CASE WHEN direction IN ('long','bullish') THEN 1 ELSE 0 END) longs,
   sum(CASE WHEN direction IN ('short','bearish') THEN 1 ELSE 0 END) shorts
   FROM setup_log sl WHERE setup_name IN %s AND outcome_pnl IS NOT NULL
   GROUP BY d""",(WL,))
mix=defaultdict(lambda:[0,0])
for d,lo,sh in cur.fetchall():
    m=str(d)[:7]; mix[m][0]+=int(lo or 0); mix[m][1]+=int(sh or 0)
for m in sorted(mix):
    lo,sh=mix[m]; tot=lo+sh
    print(f"  {m}: longs={lo:>4} ({lo/tot*100:>3.0f}%)  shorts={sh:>4} ({sh/tot*100:>3.0f}%)")

# Q4: trend persistence — close position within day range (0=closed at low,1=at high)
print("\n=== Q4: March vs June daily trend character (close-position-in-range) ===")
cur.execute("""
  WITH d AS (SELECT (ts AT TIME ZONE 'America/New_York')::date dd, spot,
                    ts AT TIME ZONE 'America/New_York' et FROM chain_snapshots
             WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::date>='2026-03-01')
  SELECT dd, min(spot) lo, max(spot) hi,
         (array_agg(spot ORDER BY et))[1] op, (array_agg(spot ORDER BY et DESC))[1] cl
  FROM d GROUP BY dd ORDER BY dd""")
import statistics
mtrend=defaultdict(list)
for dd,lo,hi,op,cl in cur.fetchall():
    lo,hi,op,cl=float(lo),float(hi),float(op),float(cl)
    rng=hi-lo
    if rng<1: continue
    pos=(cl-lo)/rng   # where close sits in range
    net=cl-op
    mtrend[str(dd)[:7]].append((abs(net)/rng, rng, net))  # directional efficiency
for m in sorted(mtrend):
    effs=[x[0] for x in mtrend[m]]; rngs=[x[1] for x in mtrend[m]]
    downdays=sum(1 for x in mtrend[m] if x[2]<=-25); updays=sum(1 for x in mtrend[m] if x[2]>=25)
    print(f"  {m}: days={len(effs)} avg_dir_efficiency={statistics.mean(effs):.2f} "
          f"avg_range={statistics.mean(rngs):.0f} down={downdays} up={updays}  (eff~1=clean trend, ~0=chop/reversal)")
conn.close()
