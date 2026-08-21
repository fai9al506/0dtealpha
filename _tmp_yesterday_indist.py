# -*- coding: utf-8 -*-
"""Day-by-day BASE(V16) vs SB(basket confirm-only, fail-open), Mar16-Jun24, mes-truth.
Then place YESTERDAY (2026-06-23) in the daily distribution: normal down-day or outlier?
Question: do other days refund it (equity rises) -> SB nets ~1.4x base?"""
import os, psycopg2, pandas as pd, numpy as np
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption','DD Exhaustion')
DEAD=0.15
df=pd.read_sql(f"""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name, direction,
   basket_pct, outcome_pnl cpnl, mes_sim_outcome_pnl mpnl, outcome_result res
   FROM setup_log WHERE live_pass=true AND setup_name IN {WL}
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-16' AND outcome_result IS NOT NULL
   ORDER BY ts""",c)
# basket series for fade-day join where basket_pct null on row (pre-June)
bk=pd.read_sql("SELECT et AT TIME ZONE 'America/New_York' bt, basket_pct bp FROM semi_basket ORDER BY et",c)
c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); bk['bt']=pd.to_datetime(bk['bt']).dt.tz_localize(None)
df['d']=df['et'].dt.date
# truth pnl: mes where present else chain
df['pnl']=df['mpnl'].fillna(df['cpnl']).astype(float)
df['long']=df['direction'].isin(['long','bullish'])
# fill basket from series (most recent <=signal) when row col missing
df=df.sort_values('et'); bk=bk.sort_values('bt')
merged=pd.merge_asof(df,bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min'))
merged['bp']=merged['basket_pct'].astype(float).fillna(merged['bp'])
def confirm(bp,islong):
    if pd.isna(bp): return True   # fail-open
    if abs(bp)<DEAD: return False # neutral -> skip
    return (bp>0)==islong
merged['take_sb']=merged.apply(lambda r:confirm(r['bp'],r['long']),axis=1)

base_day=merged.groupby('d')['pnl'].sum()
sb_day  =merged[merged['take_sb']].groupby('d')['pnl'].sum().reindex(base_day.index).fillna(0)
day=pd.DataFrame({'BASE':base_day,'SB':sb_day}); day['delta']=day['SB']-day['BASE']
day=day.round(1)

print(f"3-mo totals (mes-truth pts):  BASE={day['BASE'].sum():.0f}   SB={day['SB'].sum():.0f}   ratio={day['SB'].sum()/day['BASE'].sum():.2f}x")
print(f"trading days={len(day)}   BASE down-days={ (day['BASE']<0).sum() }   SB down-days={ (day['SB']<0).sum() }")
print(f"\nDaily BASE distribution: mean={day['BASE'].mean():.1f}  std={day['BASE'].std():.1f}  min={day['BASE'].min():.1f}  p10={day['BASE'].quantile(.1):.1f}  p90={day['BASE'].quantile(.9):.1f}")
print(f"Daily SB   distribution: mean={day['SB'].mean():.1f}  std={day['SB'].std():.1f}  min={day['SB'].min():.1f}  p10={day['SB'].quantile(.1):.1f}  p90={day['SB'].quantile(.9):.1f}")

print("\n=== 10 WORST SB days (mes-truth pts) — is yesterday among normal down-days? ===")
print(day.sort_values('SB').head(10).to_string())

ystr=pd.to_datetime('2026-06-23').date()
if ystr in day.index:
    y=day.loc[ystr]
    pct=(day['SB']<y['SB']).mean()*100
    print(f"\nYESTERDAY 2026-06-23:  BASE={y['BASE']:.1f}  SB={y['SB']:.1f}  -> percentile {pct:.0f}% (this % of days were WORSE)")
else:
    # compute yesterday directly even if not live_pass-stamped
    c2=psycopg2.connect(os.environ["DATABASE_URL"]);c2.autocommit=True
    yq=pd.read_sql(f"""SELECT direction,basket_pct bp,COALESCE(mes_sim_outcome_pnl,outcome_pnl) pnl
       FROM setup_log WHERE setup_name IN {WL} AND (ts AT TIME ZONE 'America/New_York')::date='2026-06-23'
       AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL""",c2); c2.close()
    yq['long']=yq['direction'].isin(['long','bullish'])
    base_y=yq['pnl'].astype(float).sum()
    sb_y=yq[yq.apply(lambda r:confirm(r['bp'],r['long']),axis=1)]['pnl'].astype(float).sum()
    pct=(day['SB']<sb_y).mean()*100
    print(f"\nYESTERDAY 2026-06-23 (not in live_pass set; computed direct):  BASE={base_y:.1f}  SB={sb_y:.1f}")
    print(f"   -> {pct:.0f}% of the 3-mo SB days were WORSE than yesterday")

print("\n=== After each of BASE's 8 worst days: did the NEXT 5 days refund? ===")
s=day['SB']
worst=day.sort_values('SB').head(8).index
for w in worst:
    pos=list(day.index).index(w)
    nxt=s.iloc[pos+1:pos+6].sum()
    print(f"  {w}: SB={s.loc[w]:+.1f}  -> next 5 days {nxt:+.1f}")
