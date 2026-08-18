# -*- coding: utf-8 -*-
"""Rally-exhaustion lever test: do V16-placed LONGS that fire while price is
X pts BELOW the session high (a rollover) lose systematically ACROSS ALL ERAS?
Judged on mes_sim (execution truth) restricted to mes-covered rows; chain shown for context.
Era-stability is the kill criterion (every prior 'down-day' idea was June-overfit)."""
import os, psycopg2, pandas as pd, numpy as np
conn=psycopg2.connect(os.environ["DATABASE_URL"])

# A) V16-placeable LONGS, full history
longs = pd.read_sql("""
  SELECT id, ts AT TIME ZONE 'America/New_York' AS et,
         setup_name, paradigm, grade, vix, spot,
         outcome_pnl, outcome_result,
         mes_sim_outcome_pnl, mes_sim_outcome_result
  FROM setup_log
  WHERE live_pass = true AND direction='long'
    AND ts AT TIME ZONE 'America/New_York' >= '2026-02-01'
  ORDER BY ts
""", conn)
longs['et']=pd.to_datetime(longs['et'])
longs['d']=longs['et'].dt.date
longs['month']=longs['et'].dt.to_period('M').astype(str)

# B) intraday spot path from chain_snapshots (dense), ET
spots = pd.read_sql("""
  SELECT ts AT TIME ZONE 'America/New_York' AS et, spot
  FROM chain_snapshots
  WHERE ts AT TIME ZONE 'America/New_York' >= '2026-02-01' AND spot IS NOT NULL
  ORDER BY ts
""", conn)
conn.close()
spots['et']=pd.to_datetime(spots['et']); spots['d']=spots['et'].dt.date

# session-high-so-far at each long's fire time
def sess_high(row):
    s=spots[(spots['d']==row['d']) & (spots['et']<=row['et'])]
    if len(s)==0: return np.nan, np.nan
    hi=s['spot'].max()
    hi_t=s.loc[s['spot'].idxmax(),'et']
    return hi, (row['et']-hi_t).total_seconds()/60.0
longs[['sess_high','min_since_high']]=longs.apply(lambda r: pd.Series(sess_high(r)), axis=1)
longs['pullback']=longs['sess_high']-longs['spot']   # pts below session high

def block(df,label):
    n=len(df)
    if n==0: print(f"{label:<26} n=0"); return
    # mes-covered subset
    m=df[df['mes_sim_outcome_pnl'].notna()]
    cwr=(df['outcome_result']=='WIN').mean()*100 if n else 0
    csum=df['outcome_pnl'].sum()
    mn=len(m); mwr=(m['mes_sim_outcome_result']=='WIN').mean()*100 if mn else 0
    msum=m['mes_sim_outcome_pnl'].sum()
    print(f"{label:<26} n={n:<4} chain {cwr:4.0f}%/{csum:+7.0f}p | mes(n={mn:<4}) {mwr:4.0f}%/{msum:+7.0f}p")

g=longs.dropna(subset=['pullback'])
print("=== ALL V16 longs by pullback-from-session-high (Feb-Jun) ===")
for lo,hi in [(-999,5),(5,15),(15,25),(25,999)]:
    block(g[(g['pullback']>=lo)&(g['pullback']<hi)], f"pullback {lo}..{hi}")

print("\n=== THE LEVER: longs with pullback>=15 AND high>=10min ago (rollover) ===")
roll=g[(g['pullback']>=15)&(g['min_since_high']>=10)]
block(roll,"rollover longs (block?)")
keep=g[~((g['pullback']>=15)&(g['min_since_high']>=10))]
block(keep,"kept longs")

print("\n=== ERA-STABILITY of the rollover bucket (the kill test) ===")
for mo in sorted(g['month'].unique()):
    block(roll[roll['month']==mo], f"  rollover {mo}")
print("  -- kept by month --")
for mo in sorted(g['month'].unique()):
    block(keep[keep['month']==mo], f"  kept     {mo}")

print(f"\nToday's 6 placed longs pullback check:")
tod=longs[longs['d']==pd.to_datetime('2026-06-23').date()]
for _,r in tod.iterrows():
    print(f"  {r['et'].strftime('%H:%M')} {r['setup_name']:<14} spot={r['spot']:.0f} high={r['sess_high'] if pd.notna(r['sess_high']) else 0:.0f} pullback={r['pullback'] if pd.notna(r['pullback']) else 0:+.0f} minSinceHi={r['min_since_high'] if pd.notna(r['min_since_high']) else 0:.0f}")
