"""E2T eval: split long/short into two accounts vs one combined account.
Concern = drawdown (EOD trailing $1,500). Sim three independent accounts.
Caveat: uses portal outcome_pnl (points); eval uses tighter stops -> directional, not exact."""
import os, psycopg2
from collections import defaultdict

conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()
cur.execute("""
  select ts::date d,
    case when direction in ('bullish','long') then 'L' else 'S' end dir,
    sum(outcome_pnl) pts
  from setup_log
  where ts::date>='2026-02-01' and outcome_pnl is not null
    and setup_name in ('Skew Charm','AG Short','DD Exhaustion','ES Absorption','VIX Divergence','Vanna Pivot Bounce')
  group by 1,2 order by 1
""")
dayL=defaultdict(float); dayS=defaultdict(float); alldays=set()
for d,dir,pts in cur.fetchall():
    alldays.add(d)
    if dir=='L': dayL[d]=float(pts)
    else: dayS[d]=float(pts)
conn.close()
days=sorted(alldays)

# --- E2T params ---
START=25000.0; TRAIL=1500.0; TARGET=1500.0  # advance milestone
DPT=10.0  # $/point at 2 MES (sensitivity below)
DAILY_FLOOR=-200.0   # trial self-imposed stop (config daily_loss_floor)
DAILY_CAP=525.0      # e2t_daily_pnl_cap

def sim(daily_pts, dpt=DPT, floor=DAILY_FLOOR, cap=DAILY_CAP, use_floor=True):
    bal=START; peak=START; trail_floor=min(peak-TRAIL, START)
    maxdd=0.0; busted=None; hit_target=None
    eq=[bal]
    for i,d in enumerate(days):
        dd_usd=daily_pts.get(d,0.0)*dpt
        if use_floor:
            dd_usd=max(dd_usd, floor)      # daily loss floor (stop trading)
            dd_usd=min(dd_usd, cap)        # daily profit cap
        bal+=dd_usd
        peak=max(peak,bal)
        trail_floor=min(peak-TRAIL, START)
        dd=bal-peak
        maxdd=min(maxdd,dd)
        if bal < trail_floor and busted is None:
            busted=(d, bal)
        if (bal-START)>=TARGET and hit_target is None:
            hit_target=(d, i+1)
        eq.append(bal)
    return dict(final=bal-START, maxdd=maxdd, busted=busted, hit_target=hit_target,
                trail_used=abs(maxdd), headroom=TRAIL-abs(maxdd))

dayC={d:dayL.get(d,0)+dayS.get(d,0) for d in days}

# correlation of daily L vs S
import statistics
Lv=[dayL.get(d,0) for d in days]; Sv=[dayS.get(d,0) for d in days]
n=len(days); mL=sum(Lv)/n; mS=sum(Sv)/n
cov=sum((Lv[i]-mL)*(Sv[i]-mS) for i in range(n))/n
sdL=statistics.pstdev(Lv); sdS=statistics.pstdev(Sv)
corr=cov/(sdL*sdS) if sdL and sdS else 0

print(f"Feb-May: {n} trading days. Daily L vs S correlation = {corr:+.2f}")
print(f"(negative = anti-correlated => COMBINING smooths drawdown)\n")

for qty,dpt in [(2,10.0),(3,15.0)]:
    print(f"================= qty={qty} MES (${dpt:.0f}/pt) =================")
    for use_floor in (True, False):
        tag = "WITH -$200 daily floor" if use_floor else "RAW (no daily floor)"
        print(f"--- {tag} ---")
        print(f"{'account':<10}{'finalP&L':>10}{'maxDD$':>9}{'trailUsed':>10}{'headroom':>9}{'BUST?':>16}{'hit+1500':>12}")
        for name,series in [('LONG',dayL),('SHORT',dayS),('COMBINED',dayC)]:
            r=sim(series,dpt=dpt,use_floor=use_floor)
            bust = f"{r['busted'][0]} ${r['busted'][1]:.0f}" if r['busted'] else "no"
            tgt = f"{r['hit_target'][1]}d" if r['hit_target'] else "never"
            print(f"{name:<10}{r['final']:>10.0f}{r['maxdd']:>9.0f}{r['trail_used']:>10.0f}{r['headroom']:>9.0f}{bust:>16}{tgt:>12}")
        print()
