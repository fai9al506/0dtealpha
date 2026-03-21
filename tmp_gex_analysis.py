import requests, json, sys, io, time
from collections import defaultdict
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

for attempt in range(8):
    try:
        r = requests.get('https://0dtealpha.com/api/debug/gex-analysis', timeout=30)
        data = r.json()
        if 'setup_outcomes' in data and 'error' not in data:
            break
        print(f"Attempt {attempt+1}: {str(data.get('error','no outcomes'))[:100]}")
        time.sleep(15)
    except Exception as e:
        print(f"Attempt {attempt+1}: {e}")
        time.sleep(15)

if 'setup_outcomes' not in data:
    print(f"Failed. Keys: {list(data.keys())}")
    if 'error' in data:
        print(data['error'][:500])
    sys.exit(1)

volland = {v['date']: v for v in data.get('volland_days', [])}
paradigm_all = data.get('paradigm_all', [])
outcomes = data.get('setup_outcomes', [])

# Build paradigm map per day (dominant paradigm = most snapshots)
paradigm_by_day = defaultdict(lambda: defaultdict(int))
for p in paradigm_all:
    paradigm_by_day[p['date']][p['paradigm']] += p['count']

day_paradigm = {}
for d, counts in paradigm_by_day.items():
    dominant = max(counts, key=counts.get)
    day_paradigm[d] = dominant

# GEX environment: GEX paradigm variants = positive GEX, AG variants = negative GEX
def classify_gex(paradigm):
    if not paradigm:
        return 'UNKNOWN'
    p = paradigm.upper()
    if 'AG' in p or 'ANTI' in p:
        return 'NEG'  # Anti-Gamma = negative GEX
    elif 'GEX' in p or 'SIDAL' in p:
        return 'POS'  # GEX/Sidal = positive GEX
    return 'MIXED'

all_dates = sorted(set([o['date'] for o in outcomes]))
print(f"Data: {len(all_dates)} trading days, {len(outcomes)} setup outcomes")
print()

# Per-day summary
day_data = {}
for d in all_dates:
    paradigm = day_paradigm.get(d, volland.get(d, {}).get('paradigm', '?'))
    gex_env = classify_gex(paradigm)
    day_outcomes = [o for o in outcomes if o['date'] == d]

    longs = [o for o in day_outcomes if o['direction'] in ('long', 'bullish')]
    shorts = [o for o in day_outcomes if o['direction'] in ('short', 'bearish')]

    long_pnl = sum(o['pnl'] for o in longs)
    short_pnl = sum(o['pnl'] for o in shorts)
    total_pnl = sum(o['pnl'] for o in day_outcomes)
    wins = sum(1 for o in day_outcomes if o['result'] == 'WIN')
    losses = sum(1 for o in day_outcomes if o['result'] == 'LOSS')
    long_wins = sum(1 for o in longs if o['result'] == 'WIN')
    long_losses = sum(1 for o in longs if o['result'] == 'LOSS')

    day_data[d] = {
        'paradigm': paradigm, 'gex_env': gex_env,
        'trades': len(day_outcomes), 'longs': len(longs), 'shorts': len(shorts),
        'wins': wins, 'losses': losses,
        'long_wins': long_wins, 'long_losses': long_losses,
        'total_pnl': total_pnl, 'long_pnl': long_pnl, 'short_pnl': short_pnl
    }

# Print per-day table
print("=" * 130)
print("PER-DAY: GEX ENVIRONMENT vs OUTCOMES")
print("=" * 130)
print(f"{'Date':<12} {'Paradigm':<16} {'Env':<6} {'Trades':<7} {'L':<4} {'S':<4} {'W':<4} {'Loss':<5} {'WR':<6} {'Total':>8} {'LongPnL':>8} {'ShortPnL':>8}")
print("-" * 130)

for d in sorted(day_data.keys()):
    dd = day_data[d]
    wr = f"{dd['wins']/(dd['wins']+dd['losses'])*100:.0f}%" if (dd['wins']+dd['losses']) > 0 else '--'
    print(f"{d:<12} {dd['paradigm']:<16} {dd['gex_env']:<6} {dd['trades']:<7} {dd['longs']:<4} {dd['shorts']:<4} {dd['wins']:<4} {dd['losses']:<5} {wr:<6} {dd['total_pnl']:>+8.1f} {dd['long_pnl']:>+8.1f} {dd['short_pnl']:>+8.1f}")

# Aggregate by GEX environment
print("\n" + "=" * 100)
print("AGGREGATE: POSITIVE GEX (GEX/Sidal) vs NEGATIVE GEX (AG/Anti-Gamma)")
print("=" * 100)

for env_label, env_code in [('POSITIVE GEX (GEX/Sidal paradigm)', 'POS'),
                              ('NEGATIVE GEX (AG/Anti-Gamma paradigm)', 'NEG'),
                              ('MIXED/UNKNOWN', 'MIXED')]:
    days = {d: dd for d, dd in day_data.items() if dd['gex_env'] == env_code}
    if not days:
        unknown_days = {d: dd for d, dd in day_data.items() if dd['gex_env'] == 'UNKNOWN'}
        if env_code == 'MIXED' and unknown_days:
            days.update(unknown_days)
        if not days:
            continue

    total_trades = sum(dd['trades'] for dd in days.values())
    total_wins = sum(dd['wins'] for dd in days.values())
    total_losses = sum(dd['losses'] for dd in days.values())
    total_pnl = sum(dd['total_pnl'] for dd in days.values())
    long_pnl = sum(dd['long_pnl'] for dd in days.values())
    short_pnl = sum(dd['short_pnl'] for dd in days.values())
    long_wins = sum(dd['long_wins'] for dd in days.values())
    long_losses = sum(dd['long_losses'] for dd in days.values())

    wr = f"{total_wins/(total_wins+total_losses)*100:.0f}%" if (total_wins+total_losses) > 0 else '--'
    long_wr = f"{long_wins/(long_wins+long_losses)*100:.0f}%" if (long_wins+long_losses) > 0 else '--'
    avg_pnl = total_pnl / len(days) if days else 0

    print(f"\n{env_label}:")
    print(f"  Days: {len(days)} | Trades: {total_trades} | Avg trades/day: {total_trades/len(days):.1f}")
    print(f"  Wins: {total_wins} | Losses: {total_losses} | WR: {wr}")
    print(f"  Total P&L: {total_pnl:+.1f} pts | Avg/day: {avg_pnl:+.1f} pts")
    print(f"  LONGS: WR={long_wr} ({long_wins}W/{long_losses}L) | P&L={long_pnl:+.1f}")
    print(f"  SHORTS: P&L={short_pnl:+.1f}")

# Detailed paradigm breakdown
print("\n" + "=" * 100)
print("BY EXACT PARADIGM")
print("=" * 100)
print(f"{'Paradigm':<20} {'Days':<6} {'Trades':<7} {'WR':<6} {'PnL':>8} {'Avg/Day':>8} {'LongWR':<7} {'LongPnL':>8}")
print("-" * 80)

by_paradigm = defaultdict(lambda: {'days': set(), 'wins': 0, 'losses': 0, 'pnl': 0, 'lw': 0, 'll': 0, 'lpnl': 0})
for d, dd in day_data.items():
    p = dd['paradigm']
    by_paradigm[p]['days'].add(d)
    by_paradigm[p]['wins'] += dd['wins']
    by_paradigm[p]['losses'] += dd['losses']
    by_paradigm[p]['pnl'] += dd['total_pnl']
    by_paradigm[p]['lw'] += dd['long_wins']
    by_paradigm[p]['ll'] += dd['long_losses']
    by_paradigm[p]['lpnl'] += dd['long_pnl']

for p in sorted(by_paradigm.keys(), key=lambda x: by_paradigm[x]['pnl'], reverse=True):
    pd = by_paradigm[p]
    nd = len(pd['days'])
    wr = f"{pd['wins']/(pd['wins']+pd['losses'])*100:.0f}%" if (pd['wins']+pd['losses']) > 0 else '--'
    lwr = f"{pd['lw']/(pd['lw']+pd['ll'])*100:.0f}%" if (pd['lw']+pd['ll']) > 0 else '--'
    avg = pd['pnl'] / nd if nd else 0
    print(f"{p:<20} {nd:<6} {pd['wins']+pd['losses']:<7} {wr:<6} {pd['pnl']:>+8.1f} {avg:>+8.1f} {lwr:<7} {pd['lpnl']:>+8.1f}")

print("\nDone.")
