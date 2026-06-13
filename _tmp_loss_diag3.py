"""Loss diagnosis part 3 — bug vs regime.

1) Portal-sim (chain) vs MES-sim vs broker for loss-window longs.
   If portal sim ALSO loses -> signal/regime. If portal wins but broker loses -> execution bug.
2) SPX daily direction (open->close) + VIX for each loss day -> regime check.
3) Any GEX Long real trades leaked? (v4/v6 supposed to be paused.)
4) Skew Charm fire-rate: loss window vs win era (over-firing bug?).
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# 1) portal outcome_pnl (chain sim) vs mes_sim vs broker, loss-window longs
print("=== 1) portal chain-sim vs MES-sim vs broker (loss window, by setup/dir) ===")
cur.execute("""
   SELECT sl.setup_name, sl.direction,
          sl.outcome_result, sl.outcome_pnl, sl.mes_sim_outcome_pnl, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-06-05'
   ORDER BY sl.ts
""")
def real_pts(state, direction):
    if isinstance(state,str): state=json.loads(state)
    fill=state.get('entry_fill_price') or state.get('fill_price')
    ex=state.get('stop_fill_price') or state.get('close_fill_price')
    if fill is None or ex is None: return None
    fill,ex=float(fill),float(ex)
    return (ex-fill) if direction=='long' else (fill-ex)
agg=defaultdict(lambda:{'n':0,'chain':0.0,'mes':0.0,'broker':0.0,'chain_n':0,'mes_n':0,'brk_n':0})
for setup,direction,res,opnl,mpnl,state in cur.fetchall():
    k=(setup,direction); a=agg[k]; a['n']+=1
    if opnl is not None: a['chain']+=float(opnl); a['chain_n']+=1
    if mpnl is not None: a['mes']+=float(mpnl); a['mes_n']+=1
    rp=real_pts(state,direction)
    if rp is not None: a['broker']+=rp; a['brk_n']+=1
print(f"{'setup':16} {'dir':6} {'n':>3} {'chain_pts':>9} {'mes_pts':>8} {'broker_pts':>10}")
for k in sorted(agg):
    a=agg[k]
    print(f"{k[0]:16} {k[1]:6} {a['n']:>3} {a['chain']:>9.1f} {a['mes']:>8.1f} {a['broker']:>10.1f}")

# 2) SPX direction + VIX per loss day (from chain_snapshots spot @open vs @close)
print("\n=== 2) SPX open->close + intraday range per day (chain_snapshots) ===")
cur.execute("""
  WITH d AS (
    SELECT (ts AT TIME ZONE 'America/New_York')::date AS dd,
           ts AT TIME ZONE 'America/New_York' AS et, spot
    FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date >= '2026-06-03'
      AND spot IS NOT NULL )
  SELECT dd,
    (array_agg(spot ORDER BY et))[1] AS open_spot,
    (array_agg(spot ORDER BY et DESC))[1] AS close_spot,
    min(spot) AS lo, max(spot) AS hi
  FROM d GROUP BY dd ORDER BY dd
""")
print(f"{'day':12} {'open':>8} {'close':>8} {'chg':>7} {'range':>7}")
for dd,o,c,lo,hi in cur.fetchall():
    o,c,lo,hi=float(o),float(c),float(lo),float(hi)
    print(f"{str(dd):12} {o:>8.0f} {c:>8.0f} {c-o:>+7.0f} {hi-lo:>7.0f}")

# 3) any GEX Long real trades? (should be paused)
print("\n=== 3) GEX Long real trades since Jun 5 (should be 0 / paused) ===")
cur.execute("""SELECT count(*), min(sl.ts), max(sl.ts) FROM setup_log sl
   JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name ILIKE '%GEX Long%'
     AND (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-06-05'""")
print("GEX Long real trades:", cur.fetchone())

# 4) Skew Charm fire rate (real placed) per day
print("\n=== 4) Skew Charm long real placements per day ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d, count(*)
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name='Skew Charm' AND sl.direction='long'
     AND (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-05-19'
   GROUP BY d ORDER BY d""")
for d,n in cur.fetchall():
    print(f"  {str(d)}  {n}")
conn.close()
