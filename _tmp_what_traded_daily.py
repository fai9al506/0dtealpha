# -*- coding: utf-8 -*-
"""Per-day truth: what filter governed placement (BASE vs SB), + BASE portal vs TSRT broker.
Ground truth of 'what we traded' = real_trade_orders placed setup_log_ids each day.
Classify each placed id: is it in the BASE-V16 set? the V16-SB set?"""
import os, json, pandas as pd, psycopg2
from collections import defaultdict

BASE=r"C:/Users/Faisal/Downloads/trade_log_2026-06-22 (1).csv"
SB  =r"C:/Users/Faisal/Downloads/trade_log_2026-06-22.csv"
def ids(p):
    d=pd.read_csv(p,encoding='utf-8-sig'); d.columns=[c.strip() for c in d.columns]
    d['Date']=pd.to_datetime(d['Date'])
    j=d[d['Date'].dt.to_period('M').astype(str)=='2026-06']
    return set(j['ID']), dict(zip(d['ID'], pd.to_numeric(d['P&L'],errors='coerce')))
base_ids, base_pnl = ids(BASE)
sb_ids,   sb_pnl   = ids(SB)

conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()
# placed signals per day (distinct setup_log_id) in June
cur.execute("""
  SELECT date(sl.ts AT TIME ZONE 'America/New_York') d, rto.setup_log_id
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-06'
  GROUP BY d, rto.setup_log_id ORDER BY d""")
placed=defaultdict(list)
for d,sid in cur.fetchall(): placed[str(d)].append(sid)

# broker net per day
cur.execute("SELECT day,net FROM tsrt_daily_stmt WHERE to_char(day,'YYYY-MM')='2026-06'")
brk={str(d):float(n or 0) for d,n in cur.fetchall()}

print("Per-day: of the trades the bot PLACED, how many are in the BASE-V16 set vs the V16-SB set?")
print("(if placed matches BASE but not SB => BASE filter was live that day)\n")
print(f"{'date':<12}{'placed':>7}{'in_BASE':>8}{'in_SB':>7}{'base_simP':>10}{'sb_simP':>9}{'brokerNET$':>11}  governing")
for d in sorted(placed):
    pl=placed[d]; n=len(pl)
    inb=sum(1 for x in pl if x in base_ids)
    ins=sum(1 for x in pl if x in sb_ids)
    bsim=sum(base_pnl.get(x,0) for x in pl if x in base_pnl and pd.notna(base_pnl.get(x)))
    ssim=sum(sb_pnl.get(x,0) for x in pl if x in sb_ids and pd.notna(sb_pnl.get(x)))
    gov = 'SB' if (n and ins/n>0.85) else ('BASE' if (n and inb/n>0.85) else 'mixed/looser')
    print(f"{d:<12}{n:>7}{inb:>8}{ins:>7}{bsim:>10.1f}{ssim:>9.1f}{brk.get(d,0):>11.1f}  {gov}")

# totals
allpl=[x for v in placed.values() for x in v]
print(f"\nJune placed distinct: {len(allpl)}")
print(f"  in BASE-V16 set: {sum(1 for x in allpl if x in base_ids)}")
print(f"  in V16-SB set:   {sum(1 for x in allpl if x in sb_ids)}")
print(f"  in NEITHER (looser-than-base / pre-tightening): {sum(1 for x in allpl if x not in base_ids and x not in sb_ids)}")
print(f"\nBASE portal sim of PLACED set: {sum(base_pnl.get(x,0) for x in allpl if x in base_pnl and pd.notna(base_pnl.get(x))):.1f} pts")
print(f"Broker actual June net: ${sum(brk.values()):.1f}")
conn.close()
