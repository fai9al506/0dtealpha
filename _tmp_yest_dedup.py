# -*- coding: utf-8 -*-
import os, psycopg2, pandas as pd, numpy as np
from datetime import timedelta
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption','DD Exhaustion')
DEAD=0.15
df=pd.read_sql(f"""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name nm, direction dir,
   basket_pct, outcome_pnl cpnl, mes_sim_outcome_pnl mpnl, outcome_result res
   FROM setup_log WHERE live_pass=true AND setup_name IN {WL}
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-16' AND outcome_result IS NOT NULL ORDER BY ts""",c)
bk=pd.read_sql("SELECT et AT TIME ZONE 'America/New_York' bt, basket_pct bp FROM semi_basket ORDER BY et",c); c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); bk['bt']=pd.to_datetime(bk['bt']).dt.tz_localize(None)
df['long']=df['dir'].isin(['long','bullish']); df['pnl']=df['mpnl'].fillna(df['cpnl']).astype(float)
# 15-min per (setup, side) dedup
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=(r['nm'],'L' if r['long'] else 'S')
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep); df['d']=df['et'].dt.date
df=df.sort_values('et'); bk=bk.sort_values('bt')
m=pd.merge_asof(df,bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min'))
m['bp']=m['basket_pct'].astype(float).fillna(m['bp'])
def cls(bp,il):
    if pd.isna(bp): return 'neutral'  # no_data
    if abs(bp)<DEAD: return 'neutral'
    return 'conf' if (bp>0)==il else 'unconf'
m['k']=m.apply(lambda r:('nodata' if pd.isna(r['bp']) else cls(r['bp'],r['long'])),axis=1)
# SB gate = take conf + nodata(fail-open)
m['take_sb']=m['k'].isin(['conf','nodata'])
# sizing scheme A: 2x conf, 0.5x unconf, 1x neutral/nodata
def mult(k): return 2 if k=='conf' else (0.5 if k=='unconf' else 1)
m['szpnl']=m['pnl']*m['k'].apply(mult); m['szcap']=m['k'].apply(mult)

base=m.groupby('d')['pnl'].sum()
sb  =m[m['take_sb']].groupby('d')['pnl'].sum().reindex(base.index).fillna(0)
sz  =m.groupby('d')['szpnl'].sum().reindex(base.index).fillna(0)
day=pd.DataFrame({'BASE':base,'SBgate':sb,'SBsize':sz}).round(1)

bt,gt,zt=day['BASE'].sum(),day['SBgate'].sum(),day['SBsize'].sum()
avgcap=m['szcap'].mean()
print(f"3-mo mes-truth pts (deduped):")
print(f"  BASE   total={bt:.0f}  (1.00x, 1.0 unit/trade)")
print(f"  SBgate total={gt:.0f}  ({gt/bt:.2f}x)   [confirm-only/fail-open]")
print(f"  SBsize total={zt:.0f}  ({zt/bt:.2f}x raw, {(zt/avgcap)/bt:.2f}x capital-normalized @ {avgcap:.2f}u/trade)")
print(f"\ntrading days={len(day)}  BASE std={day['BASE'].std():.0f}  mean={day['BASE'].mean():.0f}  min={day['BASE'].min():.0f}")
print("\n=== 8 WORST BASE days (deduped, mes-truth) ===")
print(day.sort_values('BASE').head(8).to_string())
y=pd.to_datetime('2026-06-23').date()
if y in day.index:
    yv=day.loc[y]; pct=(day['BASE']<yv['BASE']).mean()*100; z=(yv['BASE']-day['BASE'].mean())/day['BASE'].std()
    print(f"\nYESTERDAY: BASE={yv['BASE']:.1f} SBgate={yv['SBgate']:.1f} SBsize={yv['SBsize']:.1f}")
    print(f"  -> {pct:.0f}% of days worse; z-score={z:.1f} std from mean")

# CHAIN-based ratios (the likely source of the remembered 1.4x)
baseC=m.groupby('d')['cpnl'].sum()
m['szC']=m['cpnl']*m['k'].apply(mult)
sbC=m[m['take_sb']].groupby('d')['cpnl'].sum().reindex(baseC.index).fillna(0)
szC=m.groupby('d')['szC'].sum()
btC,gtC,ztC=baseC.sum(),sbC.sum(),szC.sum()
print(f"\n=== CHAIN-sim ratios (overstates; the 1.4x source) ===")
print(f"  BASE chain={btC:.0f}  SBgate={gtC:.0f} ({gtC/btC:.2f}x)  SBsize={ztC:.0f} ({ztC/btC:.2f}x raw)")
# OOS window only (May-Jun) chain, the darkmate-style cut
oos=m[m['d']>=pd.to_datetime('2026-05-01').date()]
bo=oos.groupby('d')['cpnl'].sum().sum(); zo=(oos['cpnl']*oos['k'].apply(mult)).sum()
print(f"  OOS(May-Jun) chain: BASE={bo:.0f}  SBsize={zo:.0f} ({zo/bo:.2f}x raw)")
