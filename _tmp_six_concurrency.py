# -*- coding: utf-8 -*-
import os, json, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()
ids=[4263,4268,4271,4272,4273,4274]
print("=== signal time / skip / outcome for the 6 ===")
cur.execute("""SELECT id, to_char(ts AT TIME ZONE 'America/New_York','HH24:MI:SS') t,
  setup_name, real_trade_skip_reason, round(spot::numeric,1),
  to_char(date_trunc('minute',(ts + (outcome_elapsed_min||' min')::interval)) AT TIME ZONE 'America/New_York','HH24:MI') exit_t,
  outcome_elapsed_min, outcome_result
  FROM setup_log WHERE id=ANY(%s) ORDER BY ts""",(ids,))
for r in cur.fetchall(): print(r)

print("\n=== real_trade_orders state (entry/exit/status) ===")
cur.execute("SELECT setup_log_id, state FROM real_trade_orders WHERE setup_log_id=ANY(%s) ORDER BY setup_log_id",(ids,))
for sid,st in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st)
    keys=['status','entry_time','entry_fill_price','exit_time','close_fill_price','stop_fill_price','direction','qty']
    print(sid, {k:s.get(k) for k in keys if k in s})
conn.close()
