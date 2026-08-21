# -*- coding: utf-8 -*-
"""Broker-truth (TSRT) daily statement: the REAL money. Contrast vs portal sim."""
import os
import psycopg2

DB = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DB)
cur = conn.cursor()

# what columns exist?
cur.execute("""SELECT column_name FROM information_schema.columns
               WHERE table_name='tsrt_daily_stmt' ORDER BY ordinal_position""")
cols = [r[0] for r in cur.fetchall()]
print("tsrt_daily_stmt columns:", cols)

cur.execute("""SELECT day, gross, comm, net, n_trades, n_wins
               FROM tsrt_daily_stmt ORDER BY day""")
rows = cur.fetchall()
print(f"\nTSRT broker-truth rows: {len(rows)}  (era starts 2026-05-19, post-V16.1)")
print(f"{'day':<12}{'gross':>9}{'comm':>8}{'net':>9}{'trades':>8}{'wins':>6}")
import collections
by_month = collections.defaultdict(lambda: [0.0,0.0,0,0,0])  # net, gross, trades, wins, days
for day, gross, comm, net, nt, nw in rows:
    m = str(day)[:7]
    g=float(gross or 0); c=float(comm or 0); nn=float(net or 0)
    by_month[m][0]+=nn; by_month[m][1]+=g; by_month[m][2]+=(nt or 0)
    by_month[m][3]+=(nw or 0); by_month[m][4]+=1
    print(f"{str(day):<12}{g:>9.2f}{c:>8.2f}{nn:>9.2f}{(nt or 0):>8}{(nw or 0):>6}")

print(f"\n{'month':<10}{'net$':>10}{'gross$':>10}{'trades':>8}{'wins':>6}{'days':>6}{'WR%':>7}")
total_net=0
for m in sorted(by_month):
    nn,g,nt,nw,dys = by_month[m]
    total_net+=nn
    wr = nw/nt*100 if nt else 0
    print(f"{m:<10}{nn:>10.2f}{g:>10.2f}{nt:>8.0f}{nw:>6.0f}{dys:>6}{wr:>7.1f}")
print(f"{'TOTAL':<10}{total_net:>10.2f}")

# June day-by-day net only, sorted
print("\n--- JUNE broker-truth daily net ---")
jun = [(str(d),float(n or 0)) for d,_,_,n,_,_ in rows if str(d)[:7]=='2026-06']
for d,n in jun:
    flag = " <== RED" if n<0 else ""
    print(f"  {d}: ${n:>8.2f}{flag}")
jn = sum(n for _,n in jun)
red = sum(n for _,n in jun if n<0)
print(f"  June net: ${jn:.2f} | sum of red days: ${red:.2f} | red days: {sum(1 for _,n in jun if n<0)}/{len(jun)}")
conn.close()
