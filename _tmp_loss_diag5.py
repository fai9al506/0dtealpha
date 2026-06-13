"""Loss diagnosis part 5 — validate vol-regime sizing OUT of the loss window.

Memory warns: June-only fixes overfit. Test halve-on-high-vol across the FULL
post-V16 era (May 19 - Jun 12) AND earlier, to see if it kills winning high-vol
days too. Also pull VIX per day to use the validated VIX>=19 threshold.
"""
import os, sys, psycopg2, json
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# range per day (chain spot hi-lo) for all of post-V16
cur.execute("""
  WITH d AS (SELECT (ts AT TIME ZONE 'America/New_York')::date dd, spot,
                    ts AT TIME ZONE 'America/New_York' et FROM chain_snapshots
             WHERE (ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' AND spot IS NOT NULL)
  SELECT dd, max(spot)-min(spot) rng,
         (array_agg(spot ORDER BY et))[1]-(array_agg(spot ORDER BY et DESC))[1] AS open_minus_close
  FROM d GROUP BY dd ORDER BY dd""")
meta={}
for dd,rng,omc in cur.fetchall():
    meta[str(dd)]={'rng':float(rng),'omc':float(omc)}  # omc>0 means down day

# VIX per day if available
try:
    cur.execute("""SELECT (ts AT TIME ZONE 'America/New_York')::date dd, avg(vix)
                   FROM chain_snapshots WHERE vix IS NOT NULL
                   AND (ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' GROUP BY dd""")
    for dd,v in cur.fetchall():
        if str(dd) in meta: meta[str(dd)]['vix']=float(v) if v else None
except Exception as e:
    print("vix col missing:", e)

cur.execute("""SELECT day,net,n_trades,n_wins FROM tsrt_daily_stmt WHERE day>='2026-05-19' ORDER BY day""")
days=cur.fetchall()

print(f"{'day':12} {'net':>8} {'range':>6} {'down?':>6} {'vix':>6}")
for day,net,n,w in days:
    ds=str(day); m=meta.get(ds,{})
    print(f"{ds:12} {float(net or 0):>+8.1f} {m.get('rng',0):>6.0f} "
          f"{('DOWN' if m.get('omc',0)>15 else ('up' if m.get('omc',0)<-15 else 'flat')):>6} {m.get('vix') or 0:>6.1f}")

# sweep range thresholds: halve days with range>=thr, compute total era P&L
print("\n=== Halve-size sweep across FULL era (May19-Jun12) ===")
for thr in [100,110,120,130,140,150]:
    base=half=0.0; nhalf=0; saved_on_losers=0.0; lost_on_winners=0.0
    for day,net,n,w in days:
        ds=str(day); net=float(net or 0); r=meta.get(ds,{}).get('rng',0)
        base+=net
        if r>=thr:
            half+=net*0.5; nhalf+=1
            if net<0: saved_on_losers += -net*0.5
            else: lost_on_winners += net*0.5
        else: half+=net
    print(f"  range>={thr}: base={base:+.0f} halved={half:+.0f} delta={half-base:+.0f} "
          f"(days_halved={nhalf}, saved_on_losers=+{saved_on_losers:.0f}, gave_up_on_winners=-{lost_on_winners:.0f})")

# same but VIX threshold if available
if any('vix' in m for m in meta.values()):
    print("\n=== Halve-size sweep by VIX threshold ===")
    for thr in [17,18,19,20,21,22]:
        base=half=0.0; nhalf=0
        for day,net,n,w in days:
            ds=str(day); net=float(net or 0); v=meta.get(ds,{}).get('vix') or 0
            base+=net
            if v>=thr: half+=net*0.5; nhalf+=1
            else: half+=net
        print(f"  vix>={thr}: base={base:+.0f} halved={half:+.0f} delta={half-base:+.0f} (days={nhalf})")
conn.close()
