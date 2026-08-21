# -*- coding: utf-8 -*-
import os, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()
TODAY='2026-06-23'
print("=== ALL signals today (setup_log) ===")
cur.execute("""
 SELECT sl.id, to_char(sl.ts AT TIME ZONE 'America/New_York','HH24:MI') t,
        sl.setup_name, sl.direction, sl.grade, sl.paradigm,
        round(sl.spot::numeric,1) spot, round(sl.basket_pct::numeric,3) bpct,
        sl.live_pass lp, sl.real_trade_skip_reason skip,
        round(sl.outcome_pnl::numeric,1) pnl, sl.outcome_result res,
        round(sl.mes_sim_outcome_pnl::numeric,1) mpnl
 FROM setup_log sl
 WHERE date(sl.ts AT TIME ZONE 'America/New_York')=%s ORDER BY sl.ts
""",(TODAY,))
rows=cur.fetchall()
print(f"{'id':>5} {'t':>5} {'setup':<13}{'dir':<6}{'grd':<4}{'paradigm':<14}{'spot':>8}{'bpct':>7} {'lp':<2}{'skip':<20}{'pnl':>6}{'res':>9}{'mpnl':>6}")
for r in rows:
    i,t,nm,d,g,par,sp,bp,lp,sk,pnl,res,mp=r
    print(f"{i:>5} {t:>5} {str(nm):<13}{str(d):<6}{str(g):<4}{str(par)[:13]:<14}{(sp or 0):>8}{(bp if bp is not None else 0):>7} {str(lp):<2}{str(sk)[:19]:<20}{(pnl if pnl is not None else 0):>6}{str(res):>9}{(mp if mp is not None else 0):>6}")

print("\n=== Placed real today ===")
cur.execute("""
 SELECT rto.setup_log_id, to_char(sl.ts AT TIME ZONE 'America/New_York','HH24:MI') t, sl.setup_name, sl.direction
 FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
 WHERE date(sl.ts AT TIME ZONE 'America/New_York')=%s
 GROUP BY rto.setup_log_id,t,sl.setup_name,sl.direction ORDER BY t""",(TODAY,))
pl=cur.fetchall()
for r in pl: print(r)
print(f"placed distinct: {len(pl)}")

print("\n=== semi_basket today ===")
cur.execute("""SELECT to_char(et AT TIME ZONE 'America/New_York','HH24:MI') t, round(basket_pct::numeric,3), n_names
 FROM semi_basket WHERE date(et AT TIME ZONE 'America/New_York')=%s ORDER BY et""",(TODAY,))
bk=cur.fetchall(); print(f"rows: {len(bk)}")
samp = bk[:4]+[('...','...','...')]+bk[-4:] if len(bk)>8 else bk
for r in samp: print(r)
conn.close()
