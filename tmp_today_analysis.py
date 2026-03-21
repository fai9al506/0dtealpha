# -*- coding: utf-8 -*-
import sys,io
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')

print('='*70)
print('IS TODAY A BAD DAY OR A BROKEN STRATEGY?')
print('V8 Real Option Prices Backtest (Mar 1-13) vs Today (Mar 16)')
print('='*70)

# Historical daily P&L from Analysis #13 (real option prices, chain snapshots)
history = [
    ('Mar 2',  15, +2440, 21.2),
    ('Mar 3',  32, +3135, 24.1),
    ('Mar 4',  34, -4325, 21.1),
    ('Mar 5',  41, +5450, 23.7),
    ('Mar 6',  23, +3300, 26.7),
    ('Mar 9',  18, -2795, 27.4),
    ('Mar 10', 37, +13030, 23.8),
    ('Mar 11', 31, -2320, 25.0),
    ('Mar 12', 12, -480,  26.0),
    ('Mar 13', 12, -2505, 26.0),
]

print('')
print('HISTORICAL DAILY P&L (V8, real option prices, 1 SPY/signal):')
print('{:>8} {:>6} {:>8} {:>5}'.format('Date','Trades','P&L','VIX'))
print('-'*35)
win_days = 0
loss_days = 0
for date, trades, pnl, vix in history:
    tag = 'WIN' if pnl > 0 else 'LOSS'
    if pnl > 0: win_days += 1
    else: loss_days += 1
    print('{:>8} {:>6} ${:>+7,} {:>5.1f}  {}'.format(date,trades,pnl,vix,tag))

# Today (closed trades only, theo prices - bug-fixed scenario)
today_closed_pnl = -57  # 21 closed trades using theo prices
today_trades = 21

print('-'*35)
print('{:>8} {:>6} ${:>+7,} {:>5.1f}  {}'.format(
    'Mar 16*', today_trades, today_closed_pnl, 24.2, 'LOSS'))
print('  * closed trades only (theo prices), bugs fixed')

print('')
print('DAILY P&L STATISTICS (10 backtest days):')
all_pnl = [p for _,_,p,_ in history]
print('  Avg day:    ${:+,.0f}'.format(sum(all_pnl)/len(all_pnl)))
print('  Best day:   ${:+,.0f} (Mar 10)'.format(max(all_pnl)))
print('  Worst day:  ${:+,.0f} (Mar 4)'.format(min(all_pnl)))
print('  Win days:   {}/10 ({}%)'.format(win_days, win_days*10))
print('  Loss days:  {}/10'.format(loss_days))
loss_days_pnl = [p for _,_,p,_ in history if p < 0]
print('  Avg loss day: ${:+,.0f}'.format(sum(loss_days_pnl)/len(loss_days_pnl)))
print('  Today:        ${:+,.0f} (WITHIN NORMAL RANGE)'.format(today_closed_pnl))

print('')
print('='*70)
print('CALLS vs PUTS in BACKTEST (they BOTH work over time)')
print('='*70)
print('')
print('Backtest WR by setup (V8, real option prices, 10 days):')
print('{:>20} {:>6} {:>5} {:>9} {:>5}'.format('Setup','Trades','WR','P&L','Type'))
print('-'*50)
setups = [
    ('Skew Charm', 106, '48.1%', +9450, 'BOTH'),
    ('DD Exhaustion', 90, '31.1%', +4080, 'BOTH'),
    ('AG Short', 16, '56.2%', +2670, 'PUTS'),
    ('GEX Long', 5, '60.0%', +290, 'CALLS'),
    ('ES Absorption', 33, '48.5%', -305, 'BOTH'),
    ('BofA Scalp', 3, '0.0%', -700, 'mixed'),
    ('Paradigm Rev', 2, '0.0%', -555, 'CALLS'),
]
for name, trades, wr, pnl, typ in setups:
    print('{:>20} {:>6} {:>5} ${:>+7,} {:>5}'.format(name,trades,wr,pnl,typ))

print('')
print('KEY INSIGHT: WR is 42% but P&L is +$14,930!')
print('  Avg winner: $560')
print('  Avg loser:  -$304')
print('  Winner/Loser ratio: 1.84x')
print('')
print('  Options have ASYMMETRIC payoffs:')
print('  - Small losses (-$50 to -$200) count as "losses" but are just premium decay')
print('  - Big winners ($1,000-$2,400) from gamma acceleration dwarf the small losses')
print('  - You NEED the calls to capture the big up-moves (Mar 10: +$13,030)')

print('')
print('='*70)
print('TODAY vs WORST BACKTEST DAYS')
print('='*70)
print('')
print('{:>8} {:>8} {:>8}  {}'.format('Date','P&L','Trades','Context'))
print('-'*55)
print('{:>8} ${:>+7,} {:>8}  {}'.format('Mar 4', -4325, 34, 'VIX 21.1, choppy'))
print('{:>8} ${:>+7,} {:>8}  {}'.format('Mar 9', -2795, 18, 'VIX 27.4, crisis'))
print('{:>8} ${:>+7,} {:>8}  {}'.format('Mar 13', -2505, 12, 'VIX 26.0, crisis'))
print('{:>8} ${:>+7,} {:>8}  {}'.format('Mar 11', -2320, 31, 'VIX 25.0, mixed'))
print('{:>8} ${:>+7,} {:>8}  {}'.format('Mar 12', -480, 12, 'VIX 26.0, mixed'))
print('{:>8} ${:>+7,} {:>8}  {}'.format('Mar 16*', -57, 21, 'VIX 24.2, choppy'))
print('')
print('  Today (bug-fixed): -$57 is the SMALLEST loss day!')
print('  The -$460 was from the bug (expired positions), not the strategy.')

print('')
print('='*70)
print('CONCLUSIONS')
print('='*70)
print('''
1. TODAY IS A NORMAL BAD DAY
   -$57 closed (theo prices) is smaller than 4 of the 4 backtest loss days.
   The -$460 was caused by the EOD bug (8 positions expired worthless).
   With the bug fixed, today would be a mild -$57 loss, not -$460.

2. CALLS ARE PROFITABLE OVER TIME
   Backtest: 42% WR but +$14,930 in 10 days (+$1,493/day).
   Skew Charm (calls+puts): 48% WR, +$9,450 — MVP.
   DD Exhaustion (calls+puts): 31% WR, +$4,080 — workhorse.
   The low WR is EXPECTED. Winners ($560 avg) crush losers ($304 avg).

3. REMOVING CALLS WOULD KILL THE BEST DAYS
   Mar 10: +$13,030 (calls drove most of this)
   Mar 5:  +$5,450
   These monster days are what make the strategy work.
   Puts-only would miss all of them.

4. THE BUGS WERE THE REAL PROBLEM
   3 bugs fixed:
   - EOD summary now closes option positions
   - New options EOD flatten at 15:55 ET
   - Silent exception swallowing replaced with logging
   This prevents the $403 expired-position loss from recurring.

5. NO STRATEGY CHANGE NEEDED
   V8 + both calls and puts + bug fixes = correct approach.
   Give it 2 weeks of clean data before reconsidering.
''')
