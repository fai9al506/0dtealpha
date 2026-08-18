# -*- coding: utf-8 -*-
"""Compare BASE V16 (what was actually traded early June) vs V16-SB (last week's fix)."""
import pandas as pd, numpy as np
pd.set_option('display.width',220)

BASE=r"C:/Users/Faisal/Downloads/trade_log_2026-06-22 (1).csv"   # bigger = base (looser)
SB  =r"C:/Users/Faisal/Downloads/trade_log_2026-06-22.csv"        # smaller = V16-SB

def load(p,label):
    d=pd.read_csv(p,encoding='utf-8-sig'); d.columns=[c.strip() for c in d.columns]
    d['Date']=pd.to_datetime(d['Date']); d['Month']=d['Date'].dt.to_period('M').astype(str)
    d['P&L']=pd.to_numeric(d['P&L'],errors='coerce')
    d['ver']=label
    return d

b=load(BASE,'BASE'); s=load(SB,'SB')
print("rows  BASE:",len(b)," SB:",len(s))
print("BASE date range:",b['Date'].min().date(),"->",b['Date'].max().date())
print("SB   date range:",s['Date'].min().date(),"->",s['Date'].max().date())
print("\nBASE setups:",b['Setup'].value_counts().to_dict())
print("SB   setups:",s['Setup'].value_counts().to_dict())

def monthly(d,label):
    r=d[d['P&L'].notna()].groupby('Month').agg(
        n=('P&L','size'),
        wins=('Result',lambda x:(x=='WIN').sum()),
        pnl=('P&L','sum')).round(1)
    r['WR%']=(r['wins']/r['n']*100).round(1)
    r['ver']=label
    return r
print("\n=== MONTHLY  BASE ==="); print(monthly(b,'BASE'))
print("\n=== MONTHLY  SB   ==="); print(monthly(s,'SB'))

# June daily for both
print("\n=== JUNE DAILY P&L: BASE vs SB ===")
bd=b[b['Month']=='2026-06'].groupby('Date')['P&L'].sum()
sd=s[s['Month']=='2026-06'].groupby('Date')['P&L'].sum()
j=pd.DataFrame({'BASE':bd,'SB':sd}).fillna(0).round(1)
j['diff']=(j['SB']-j['BASE']).round(1)
print(j.to_string())
print(f"\nJune totals -> BASE: {bd.sum():.1f}   SB: {sd.sum():.1f}")

# Are the two files identical IDs or does SB drop trades?
bids=set(b['ID']); sids=set(s['ID'])
print(f"\nIDs only in BASE (dropped by SB): {len(bids-sids)}")
print(f"IDs only in SB  (added vs BASE): {len(sids-bids)}")
print(f"IDs in both: {len(bids&sids)}")
# P&L of the trades SB DROPS (june)
dropped=b[b['ID'].isin(bids-sids) & (b['Month']=='2026-06')]
print(f"\nJune trades SB DROPS: n={len(dropped)}  sim P&L={dropped['P&L'].sum():.1f}  WR={ (dropped['Result']=='WIN').mean()*100:.0f}%")
