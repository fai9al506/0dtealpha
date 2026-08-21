# -*- coding: utf-8 -*-
"""Is SB net-positive CONDITIONAL on the 'tech green-from-open but SPX longs fade' divergence regime?
chain $ (exec~=chain now), full V16 long set, 15min dedup. Semi mult 2x conf/0.5x fight/1x neu by basket-at-entry."""
import os, psycopg2, pandas as pd, numpy as np
from datetime import timedelta
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
df=pd.read_sql("""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name nm, direction dir,
   basket_pct, outcome_pnl cpnl, outcome_result res
   FROM setup_log WHERE live_pass=true AND direction in ('long','bullish')
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-16' AND outcome_pnl IS NOT NULL ORDER BY ts""",c)
bk=pd.read_sql("SELECT et AT TIME ZONE 'America/New_York' bt, basket_pct bp FROM semi_basket ORDER BY et",c); c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); bk['bt']=pd.to_datetime(bk['bt']).dt.tz_localize(None)
# dedup 15min per setup (longs only here)
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=r['nm']
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep)
df=df.sort_values('et'); bk=bk.sort_values('bt')
m=pd.merge_asof(df,bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min'))
m['b']=m['basket_pct'].astype(float).fillna(m['bp'])   # basket at entry (open-anchored)
m['d']=m['et'].dt.date; m['cpnl']=m['cpnl'].astype(float)*5  # $ @1MES
def mult(b):
    if pd.isna(b) or abs(b)<0.15: return 1.0
    return 2.0 if b>0 else 0.5   # for LONGS: b>0=confirm 2x, b<0=fight 0.5x
m['semi']=m['cpnl']*m['b'].apply(mult)
m['conf']= m['b']>0.15

# per-trade regime buckets
print("=== LONG trades by basket-at-entry (chain $, @1MES) ===")
for lab,mask in [("CONFIRMED (b>+.15)",m['conf']),
                 ("contradicted (b<-.15)",m['b']<-0.15),
                 ("neutral/nodata",(m['b'].abs()<=0.15)|m['b'].isna())]:
    s=m[mask]; w=(s['res']=='WIN').mean()*100 if len(s) else 0
    los=s[s['res']!='WIN']
    print(f"  {lab:<24} n={len(s):<4} WR={w:4.0f}%  base=${s['cpnl'].sum():+7.0f}  semi=${s['semi'].sum():+7.0f}  | losers n={len(los)} base=${los['cpnl'].sum():+7.0f} semi=${los['semi'].sum():+7.0f}")

# DAY-level regime: confirm-regime days where the long-book LOST (the 'yesterday' divergence tail)
day=m.groupby('d').agg(base=('cpnl','sum'), semi=('semi','sum'),
    avgb=('b','mean'), n=('id','size')).reset_index()
day['regime']=np.where(day['avgb']>0.15,'confirm',np.where(day['avgb']<-0.15,'contradict','neutral'))
print("\n=== DAY-level: long-book base vs semi, split by regime AND base outcome ===")
for reg in ['confirm','contradict','neutral']:
    sub=day[day['regime']==reg]
    bleed=sub[sub['base']<0]   # days the longs lost
    print(f"\n {reg.upper()} regime: {len(sub)} days, base=${sub['base'].sum():+.0f}, semi=${sub['semi'].sum():+.0f} (d {sub['semi'].sum()-sub['base'].sum():+.0f})")
    print(f"   of which long-book LOST: {len(bleed)} days, base=${bleed['base'].sum():+.0f}, semi=${bleed['semi'].sum():+.0f}  -> SB d on bleed days {bleed['semi'].sum()-bleed['base'].sum():+.0f}")

# the specific tail: confirm-regime + base lost = 'tech green, SPX faded' = yesterday
tail=day[(day['regime']=='confirm')&(day['base']<0)]
resc=day[(day['regime']=='contradict')&(day['base']<0)]
print(f"\n>>> DIVERGENCE TAIL (tech-green-from-open + longs lost): {len(tail)} days, SB makes it {tail['semi'].sum()-tail['base'].sum():+.0f} (NEGATIVE = SB hurts)")
print(f">>> JUNE-RESCUE case (tech-red + longs lost): {len(resc)} days, SB makes it {resc['semi'].sum()-resc['base'].sum():+.0f} (POSITIVE = SB saves)")
print("\nWorst confirm-regime bleed days:")
print(tail.sort_values('base').head(6)[['d','base','semi','avgb','n']].to_string(index=False))
