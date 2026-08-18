# -*- coding: utf-8 -*-
import os, json, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
# state + outcomes
cur.execute("""SELECT sl.id, to_char(sl.ts AT TIME ZONE 'America/New_York','HH24:MI:SS') t, sl.setup_name, sl.direction,
   sl.spot, round(sl.outcome_pnl::numeric,1), sl.outcome_result, round(sl.mes_sim_outcome_pnl::numeric,1), sl.outcome_max_profit,
   round(sl.exit_price::numeric,2), sl.trail_sl, sl.trail_activation, sl.trail_gap
   FROM setup_log sl WHERE sl.id=4329""")
print("setup_log 4329:", cur.fetchone())
cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id=4329")
st=cur.fetchone()[0]; s=st if isinstance(st,dict) else json.loads(st)
print("\nreal_trade_orders state keys/vals:")
for k in ['direction','fill_price','signal_es_price','current_stop','target_price','stop_pts','target_pts',
          'trail_active','trail_only','be_triggered','max_favorable','initial_realign_done','close_fill_price',
          'stop_fill_price','close_reason','status','ts_placed']:
    if k in s: print(f"  {k}: {s[k]}")
# SPX path around the trade (entry 11:48 -> exit)
cur.execute("""SELECT to_char(ts AT TIME ZONE 'America/New_York','HH24:MI') t, round(spot::numeric,1)
   FROM chain_snapshots WHERE date(ts AT TIME ZONE 'America/New_York')='2026-06-24'
   AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '11:45' AND '12:35' ORDER BY ts""")
rows=cur.fetchall()
print(f"\nSPX path 11:45-12:35 (entry spot was signal): {len(rows)} snaps")
print("  "+"  ".join(f"{t}:{sp}" for t,sp in rows[::3]))
print(f"  SPX min/max in window: {min(r[1] for r in rows)} / {max(r[1] for r in rows)}")
c.close()
