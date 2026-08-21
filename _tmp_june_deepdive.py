# -*- coding: utf-8 -*-
"""Deep dive: why was V16 green daily for 3 months but not in June?
Source: user's portal export trade_log_2026-06-22.csv (V16-filtered set, chain-sim P&L)."""
import pandas as pd, numpy as np
pd.set_option('display.width', 200); pd.set_option('display.max_columns', 40)

CSV = r"C:/Users/Faisal/Downloads/trade_log_2026-06-22.csv"
df = pd.read_csv(CSV, encoding='utf-8-sig')
df.columns = [c.strip() for c in df.columns]
df['Date'] = pd.to_datetime(df['Date'])
df['Month'] = df['Date'].dt.to_period('M').astype(str)
df['P&L'] = pd.to_numeric(df['P&L'], errors='coerce')
df['VIX'] = pd.to_numeric(df['VIX'], errors='coerce')
df['Alignment'] = pd.to_numeric(df['Alignment'], errors='coerce')
df['Overvix'] = pd.to_numeric(df['Overvix'], errors='coerce')
df['hour'] = pd.to_datetime(df['Time (ET)'], format='%H:%M:%S', errors='coerce').dt.hour

print("="*90)
print("DATA PROFILE")
print("="*90)
print("rows:", len(df), "| date range:", df['Date'].min().date(), "->", df['Date'].max().date())
print("setups:", df['Setup'].value_counts().to_dict())
print("results:", df['Result'].value_counts().to_dict())
na_pnl = df['P&L'].isna().sum()
print("rows with NaN P&L (unresolved/open):", na_pnl)
df = df[df['P&L'].notna()].copy()
print("resolved rows used:", len(df))

def stats(g):
    n = len(g)
    wins = (g['Result']=='WIN').sum()
    wr = wins/n*100 if n else 0
    pnl = g['P&L'].sum()
    return pd.Series({'n':n,'WR%':round(wr,1),'PnL':round(pnl,1),'avg':round(pnl/n,2) if n else 0})

print("\n" + "="*90); print("MONTHLY (all V16 trades)"); print("="*90)
print(df.groupby('Month').apply(stats, include_groups=False))

print("\n" + "="*90); print("MONTHLY x DIRECTION"); print("="*90)
print(df.groupby(['Month','Direction']).apply(stats, include_groups=False))

print("\n" + "="*90); print("MONTHLY x SETUP"); print("="*90)
print(df.groupby(['Month','Setup']).apply(stats, include_groups=False))

# Daily P&L: count green vs red days per month
print("\n" + "="*90); print("DAILY P&L -> green/red day counts per month"); print("="*90)
daily = df.groupby(['Month','Date'])['P&L'].sum().reset_index()
daily['green'] = daily['P&L']>0
dd = daily.groupby('Month').agg(days=('Date','count'), green_days=('green','sum'),
                                 month_pnl=('P&L','sum'),
                                 worst_day=('P&L','min'), best_day=('P&L','max'))
dd['red_days']=dd['days']-dd['green_days']
dd['green%']=round(dd['green_days']/dd['days']*100,0)
print(dd[['days','green_days','red_days','green%','month_pnl','worst_day','best_day']])

print("\n" + "="*90); print("JUNE DAILY DETAIL"); print("="*90)
jun = daily[daily['Month']=='2026-06'].copy()
print(jun[['Date','P&L']].to_string(index=False))

# Running cumulative + max drawdown over whole period
df_sorted = df.sort_values(['Date','Time (ET)'])
cum = df_sorted['P&L'].cumsum()
peak = cum.cummax()
dd_curve = cum-peak
print("\nAll-time cum PnL:", round(cum.iloc[-1],1), "| max drawdown:", round(dd_curve.min(),1), "pts")
