# -*- coding: utf-8 -*-
"""(1) Capture rate post-V16 May vs June (am I wrong that chain-sim is 'over-optimistic'?)
   (2) Full dump of the +25.3 trade (Jun 11 12:38 SC short): portal SPX vs broker MES."""
import os, json, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

# ---- (1) capture rate: broker gross pts / chain-sim pts, by era window ----
cur.execute("SELECT day, gross, trades FROM tsrt_daily_stmt ORDER BY day")
brk={}
for day,g,tr in cur.fetchall():
    js=tr if isinstance(tr,list) else json.loads(tr or '[]')
    brk[str(day)]=sum(t.get('pts',0) for t in js)
cur.execute("""SELECT date(sl.ts AT TIME ZONE 'America/New_York') d,
  ROUND(SUM(sl.outcome_pnl)::numeric,1) chain
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE sl.outcome_result IS NOT NULL AND date(sl.ts AT TIME ZONE 'America/New_York')>='2026-05-15'
  GROUP BY d""")
chain={str(d):float(c or 0) for d,c in cur.fetchall()}
def window(lo,hi):
    ch=sum(v for d,v in chain.items() if lo<=d<=hi)
    bk=sum(v for d,v in brk.items() if lo<=d<=hi)
    return ch,bk
for lbl,lo,hi in [("May pre-V16.1 (15-18)","2026-05-15","2026-05-18"),
                  ("May post-V16.1 (19-31)","2026-05-19","2026-05-31"),
                  ("June (all)","2026-06-01","2026-06-30")]:
    ch,bk=window(lo,hi)
    cap = bk/ch*100 if ch else float('nan')
    print(f"{lbl:<26} chain={ch:>8.1f}  brokerGROSS={bk:>8.1f}  capture={cap:>6.1f}%")

# ---- (2) the +25.3 trade ----
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name, direction, grade, paradigm,
         spot, lis, target, outcome_pnl, outcome_result, outcome_max_profit, outcome_max_loss, outcome_target_level, outcome_stop_level, outcome_first_event, outcome_elapsed_min,
         mes_sim_outcome_pnl, mes_sim_outcome_result, mes_sim_max_fav, abs_es_price,
         trail_sl, trail_activation, trail_gap, exit_price
  FROM setup_log
  WHERE date(ts AT TIME ZONE 'America/New_York')='2026-06-11'
    AND setup_name='Skew Charm' AND direction='short'
    AND to_char(ts AT TIME ZONE 'America/New_York','HH24:MI')='12:38'
""")
cols=[d[0] for d in cur.description]
row=cur.fetchone()
print("\n=== PORTAL setup_log row (the +25.3 trade) ===")
if row:
    for c,v in zip(cols,row): print(f"  {c:<24}{v}")
    sid=row[0]
else:
    print("  not found at 12:38 — listing all Jun11 SC shorts:")
    cur.execute("""SELECT id, to_char(ts AT TIME ZONE 'America/New_York','HH24:MI'), grade, paradigm, outcome_pnl, max_fav
      FROM setup_log WHERE date(ts AT TIME ZONE 'America/New_York')='2026-06-11'
      AND setup_name='Skew Charm' AND direction='short' ORDER BY ts""")
    for r in cur.fetchall(): print("   ",r)
    sid=None

# SPX 30s path from 12:35 to 13:00 ET on Jun 11
cur.execute("""
  SELECT to_char(ts AT TIME ZONE 'America/New_York','HH24:MI:SS') et, ROUND(spot::numeric,2)
  FROM chain_snapshots
  WHERE date(ts AT TIME ZONE 'America/New_York')='2026-06-11'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '12:36' AND '13:05'
    AND spot IS NOT NULL ORDER BY ts""")
print("\n=== SPX 30s path 12:36-13:05 ET (portal sees this) ===")
sp=cur.fetchall()
print("  "+"  ".join(f"{t[11:]}={s}" for t,s in sp))
if sp:
    entry_spx=float(sp[0][1])
    lows=min(float(s) for _,s in sp)
    print(f"  entry~{entry_spx}  min_spx={lows}  max_drop_from_entry={entry_spx-lows:.1f} pts (short MFE if held)")

# MES range bars same window
cur.execute("""
  SELECT to_char(ts_start AT TIME ZONE 'America/New_York','HH24:MI:SS') st,
         ROUND(high::numeric,2), ROUND(low::numeric,2), ROUND(close::numeric,2)
  FROM vps_es_range_bars
  WHERE date(ts_start AT TIME ZONE 'America/New_York')='2026-06-11'
    AND (ts_start AT TIME ZONE 'America/New_York')::time BETWEEN '12:36' AND '13:05'
  ORDER BY ts_start""")
print("\n=== MES 5pt range bars 12:36-13:05 ET (broker fills here) ===")
for st,h,l,c in cur.fetchall():
    print(f"  {st}  H={h} L={l} C={c}")
print("\n=== BROKER round-trip (TSRT) at 12:38 ===")
cur.execute("SELECT trades FROM tsrt_daily_stmt WHERE day='2026-06-11'")
js=cur.fetchone()[0]; js=js if isinstance(js,list) else json.loads(js)
for t in js:
    if t.get('entry_et')=='12:38' or (t.get('dir')=='SHORT' and t.get('entry_et','')>='12:35' and t.get('entry_et','')<='12:50'):
        print("  ",json.dumps(t,default=str))
conn.close()
