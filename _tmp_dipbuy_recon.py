"""Reconcile backtest vs live Dip-Buy on Jun 1-3 (Gate 2)."""
import os, psycopg2
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

DIP, CONF, T, S = 8.0, 4.0, 10.0, 8.0
WS, WE = dtime(9,30), dtime(11,30)

for day in ("2026-06-01","2026-06-02","2026-06-03"):
    cur.execute("""select ts, spot from chain_snapshots
                   where (ts at time zone 'America/New_York')::date = %s and spot is not null
                   order by ts""", (day,))
    s = [(ts.astimezone(ET), float(sp)) for ts,sp in cur.fetchall()]
    # snapshot cadence
    gaps = [(s[i+1][0]-s[i][0]).total_seconds() for i in range(len(s)-1)]
    med = sorted(gaps)[len(gaps)//2] if gaps else None
    sess_high=-1e9; in_dip=False; lo=1e9; entry=None
    for et,sp in s:
        if et.time()<WS: continue
        if et.time()>WE: break
        sess_high=max(sess_high,sp)
        if not in_dip:
            if sp<=sess_high-DIP: in_dip=True; lo=sp
        else:
            lo=min(lo,sp)
            if sp>=lo+CONF: entry=(et,sp,sess_high,lo); break
    print(f"{day}: snaps={len(s)} median_gap={med}s", end="  ")
    if entry:
        et,sp,hi,lo2 = entry
        # walk exit
        res=None
        after=[x for x in s if x[0]>et]
        for et2,sp2 in after:
            if et2.time()>dtime(16,0): break
            if sp2<=sp-S: res=("LOSS",-S,et2); break
            if sp2>=sp+T: res=("WIN",T,et2); break
        print(f"BT entry {et.time()} @{sp:.2f} (hi={hi:.2f} lo={lo2:.2f}) -> {res[0] if res else 'EXPIRED'} at {res[2].time() if res else '-'}")
    else:
        print("BT: no entry")

print()
cur.execute("""select id, ts at time zone 'America/New_York', spot, outcome_result, outcome_pnl,
                      outcome_stop_level, outcome_target_level, abs_details->>'sess_high', abs_details->>'dip_low'
               from setup_log where setup_name='Dip-Buy' order by ts""")
for r in cur.fetchall():
    print("LIVE", r)
conn.close()
