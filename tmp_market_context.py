import os, pandas as pd, numpy as np
from sqlalchemy import create_engine, text
import pytz
from datetime import time as dtime, date as dt_date

NY = pytz.timezone("US/Eastern")
engine = create_engine(os.environ["DATABASE_URL"])

q = text("""
    SELECT ts, spot FROM chain_snapshots
    WHERE spot IS NOT NULL AND spot > 0
    AND ts >= '2026-01-21' AND ts <= '2026-03-08'
    ORDER BY ts
""")
with engine.connect() as conn:
    df = pd.read_sql(q, conn)
df['ts'] = pd.to_datetime(df['ts'], utc=True)
df['et'] = df['ts'].dt.tz_convert(NY)
df['date'] = df['et'].dt.date
df['time'] = df['et'].dt.time

days = []
for dt, grp in df.groupby('date'):
    mkt = grp[(grp['time'] >= dtime(9,30)) & (grp['time'] <= dtime(16,0))]
    if len(mkt) < 5:
        continue
    days.append({'date': dt, 'open': mkt.iloc[0]['spot'], 'close': mkt.iloc[-1]['spot'],
                 'high': mkt['spot'].max(), 'low': mkt['spot'].min(),
                 'change': mkt.iloc[-1]['spot'] - mkt.iloc[0]['spot']})

days_df = pd.DataFrame(days)
first_open = days_df.iloc[0]['open']
last_close = days_df.iloc[-1]['close']
total = last_close - first_open
up = (days_df['change'] > 0).sum()
dn = (days_df['change'] < 0).sum()

print("MARKET CONTEXT: SPX Jan 21 - Mar 8 2026")
print("=" * 80)
print(f"Start: {first_open:.1f}  End: {last_close:.1f}  Move: {total:+.1f} pts ({total/first_open*100:+.2f}%)")
print(f"Period High: {days_df['high'].max():.1f}  Period Low: {days_df['low'].min():.1f}")
print(f"Up Days: {up}  Down Days: {dn}  Avg Daily: {days_df['change'].mean():+.1f} pts")
print()

days_df['date_ts'] = pd.to_datetime(days_df['date'])
days_df['week'] = days_df['date_ts'].dt.isocalendar().week.astype(int)
print("WEEKLY TREND:")
for wk, wg in days_df.groupby('week'):
    wo = wg.iloc[0]['open']
    wc = wg.iloc[-1]['close']
    wch = wc - wo
    d = "UP" if wch > 10 else "DOWN" if wch < -10 else "FLAT"
    print(f"  Wk{wk}: {str(wg.iloc[0]['date']):>10} - {str(wg.iloc[-1]['date']):>10}  {wo:.0f}->{wc:.0f} = {wch:+.0f} {d}")

print()
print("GEX LONG TRADES vs DAILY MARKET:")
print(f" {'#':>2} {'Date':>12} {'Spot':>8} {'DayOpen':>8} {'DayClose':>8} {'DayChg':>7} {'Mkt':>5} {'Result':>6} {'PnL':>6}")
print("-" * 80)

trades = [
    ("2026-01-23", 6930.8, "LOSS", -8.0),
    ("2026-01-28", 6974.6, "EXP", +8.2),
    ("2026-02-02", 6929.3, "WIN", +10.0),
    ("2026-02-03", 6886.3, "WIN", +10.0),
    ("2026-02-05", 6807.2, "WIN", +10.0),
    ("2026-02-09", 6927.7, "WIN", +10.0),
    ("2026-02-17", 6842.9, "EXP", -4.8),
    ("2026-02-26", 6889.6, "WIN", +10.0),
    ("2026-02-26", 6883.8, "LOSS", -8.0),
    ("2026-03-02", 6854.7, "WIN", +10.0),
    ("2026-03-02", 6875.7, "WIN", +10.0),
    ("2026-03-02", 6874.1, "WIN", +10.0),
]

wins_up, wins_dn, losses_up, losses_dn = 0, 0, 0, 0
for i, (td, spot, outcome, pnl) in enumerate(trades):
    td_obj = dt_date.fromisoformat(td)
    day = days_df[days_df['date'] == td_obj]
    if len(day) > 0:
        d = day.iloc[0]
        mkt = "UP" if d['change'] > 5 else "DN" if d['change'] < -5 else "FLAT"
        print(f"{i+1:2d} {td:>12} {spot:8.1f} {d['open']:8.1f} {d['close']:8.1f} {d['change']:+7.1f} {mkt:>5} {outcome:>6} {pnl:+6.1f}")
        if pnl > 0:
            if d['change'] < -5: wins_dn += 1
            else: wins_up += 1
        elif pnl < 0:
            if d['change'] < -5: losses_dn += 1
            else: losses_up += 1

print()
print(f"Wins on UP/FLAT days: {wins_up}")
print(f"Wins on DOWN days:    {wins_dn}")
print(f"Losses on UP/FLAT: {losses_up}")
print(f"Losses on DOWN:    {losses_dn}")
print()
print(f"BOTTOM LINE:")
print(f"SPX dropped {total:+.0f} pts ({total/first_open*100:+.1f}%) during backtest period.")
print(f"GEX Long (a LONG setup) made +67.4 pts at 80% WR AGAINST the trend.")
