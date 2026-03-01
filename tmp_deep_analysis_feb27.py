"""Deep analysis of Feb 27 trading day."""
import os, sys, json, psycopg
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from psycopg.rows import dict_row
from datetime import datetime
import pytz

NY = pytz.timezone("America/New_York")
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row)

# ── 1. All trades with outcomes ──
trades = c.execute("""
    SELECT id, setup_name, direction, grade, score,
           outcome_result, outcome_pnl, outcome_first_event,
           outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
           spot, lis, target, abs_es_price, abs_vol_ratio,
           paradigm, vix, comments,
           ts AT TIME ZONE 'America/New_York' as ts_et
    FROM setup_log
    WHERE ts::date = '2026-02-27'
    ORDER BY id
""").fetchall()

print(f"{'='*80}")
print(f"  DEEP TRADE ANALYSIS — Feb 27, 2026")
print(f"{'='*80}")
print(f"\nTotal signals: {len(trades)}")

# ── 2. Per-setup breakdown ──
setups = {}
for t in trades:
    name = t['setup_name']
    if name not in setups:
        setups[name] = {'trades': [], 'wins': 0, 'losses': 0, 'expired': 0, 'open': 0, 'pnl': 0}
    setups[name]['trades'].append(t)
    res = t['outcome_result']
    pnl = float(t['outcome_pnl'] or 0)
    if res == 'WIN': setups[name]['wins'] += 1
    elif res == 'LOSS': setups[name]['losses'] += 1
    elif res == 'EXPIRED': setups[name]['expired'] += 1
    else: setups[name]['open'] += 1
    setups[name]['pnl'] += pnl

print(f"\n{'─'*80}")
print(f"  PER-SETUP BREAKDOWN")
print(f"{'─'*80}")
for name, s in sorted(setups.items(), key=lambda x: -x[1]['pnl']):
    total = len(s['trades'])
    wr = s['wins'] / (s['wins'] + s['losses']) * 100 if (s['wins'] + s['losses']) > 0 else 0
    print(f"\n  {name}")
    print(f"    Trades: {total} | {s['wins']}W/{s['losses']}L/{s['expired']}E/{s['open']}O | WR: {wr:.0f}% | PnL: {s['pnl']:+.1f} pts")

# ── 3. Detailed trade log ──
print(f"\n{'─'*80}")
print(f"  DETAILED TRADE LOG")
print(f"{'─'*80}")
total_pnl = 0
for t in trades:
    res = t['outcome_result'] or 'OPEN'
    pnl = float(t['outcome_pnl'] or 0)
    total_pnl += pnl
    mp = t['outcome_max_profit']
    ml = t['outcome_max_loss']
    elapsed = t['outcome_elapsed_min']
    time_str = t['ts_et'].strftime('%H:%M') if t['ts_et'] else '?'
    dir_arrow = '^' if t['direction'].lower() in ('long', 'bullish') else 'v'

    # Entry price
    entry = t['abs_es_price'] or t['spot']

    print(f"  #{t['id']:3d} {time_str} {t['setup_name']:20s} {dir_arrow} {t['grade']:3s} {t['score']:5.1f} | "
          f"entry={entry} | {res:7s} pnl={pnl:+6.1f} | maxP={mp or 0:+6.1f} maxL={ml or 0:+6.1f} | "
          f"dur={elapsed or '--':>4} | para={t['paradigm'] or '?'}")

print(f"\n  {'─'*40}")
print(f"  TOTAL P&L: {total_pnl:+.1f} pts")

# ── 4. ES Absorption deep dive (using range bars) ──
print(f"\n{'─'*80}")
print(f"  ES ABSORPTION DEEP DIVE")
print(f"{'─'*80}")

abs_trades = [t for t in trades if t['setup_name'] == 'ES Absorption']
bars = c.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
           cumulative_delta, ts_start, ts_end
    FROM es_range_bars
    WHERE trade_date = '2026-02-27' AND source = 'rithmic' AND status = 'closed'
    ORDER BY bar_idx ASC
""").fetchall()

for t in abs_trades:
    es_entry = float(t['abs_es_price'])
    is_long = t['direction'].lower() in ('long', 'bullish')
    ts = t['ts_et']

    # Find signal bar
    signal_idx = None
    for b in bars:
        bs = b['ts_start']
        if hasattr(bs, 'tzinfo') and bs.tzinfo is None:
            bs = NY.localize(bs)
        elif bs.tzinfo is None:
            bs = NY.localize(bs)
        ts_aware = ts if ts.tzinfo else NY.localize(ts)
        if bs <= ts_aware:
            signal_idx = b['bar_idx']
        else:
            break

    if signal_idx is None:
        continue

    # Walk bars: compute T1, T2, max profit, max loss, time to 10pt
    hit_10pt = False
    max_profit = 0
    max_loss = 0
    trail_peak = 0
    trail_stop = es_entry - 12 if is_long else es_entry + 12
    trail_exit_pnl = None
    bars_to_10pt = 0
    bars_to_exit = 0

    for b in bars:
        if b['bar_idx'] <= signal_idx:
            continue
        bh = float(b['bar_high'])
        bl = float(b['bar_low'])

        ph = (bh - es_entry) if is_long else (es_entry - bl)
        pl = (bl - es_entry) if is_long else (es_entry - bh)

        if ph > max_profit: max_profit = ph
        if pl < max_loss: max_loss = pl

        if not hit_10pt:
            bars_to_10pt += 1
            ten_pt = es_entry + 10 if is_long else es_entry - 10
            if (is_long and bh >= ten_pt) or (not is_long and bl <= ten_pt):
                hit_10pt = True

        if trail_exit_pnl is None:
            bars_to_exit += 1
            if ph > trail_peak: trail_peak = ph
            if trail_peak >= 10:
                lock = max(trail_peak - 5, 0)
                ns = es_entry + lock if is_long else es_entry - lock
                if is_long and ns > trail_stop: trail_stop = ns
                elif not is_long and ns < trail_stop: trail_stop = ns
            if (is_long and bl <= trail_stop) or (not is_long and bh >= trail_stop):
                trail_exit_pnl = round((trail_stop - es_entry) if is_long else (es_entry - trail_stop), 2)

    avg_pnl = round((10.0 + (trail_exit_pnl or 0)) / 2, 1) if hit_10pt else (trail_exit_pnl or 0)
    dir_arrow = '^' if is_long else 'v'
    pattern = ''
    if t['comments']:
        p = t['comments'].split('|')[0].replace('Pattern:', '').strip()
        pattern = p[:30]

    print(f"  #{t['id']} {t['ts_et'].strftime('%H:%M')} {dir_arrow} @ {es_entry} vol={t['abs_vol_ratio']}x | {pattern}")
    print(f"    T1: {'HIT' if hit_10pt else 'MISS'} ({bars_to_10pt} bars) | T2: trail_peak={trail_peak:.1f} exit={trail_exit_pnl} ({bars_to_exit} bars)")
    print(f"    maxP={max_profit:.1f} maxL={max_loss:.1f} | avg_pnl={avg_pnl:+.1f}")
    print(f"    Left on table: {max_profit - 10:.1f} pts beyond T1")
    print()

# ── 5. Timing analysis ──
print(f"\n{'─'*80}")
print(f"  TIMING ANALYSIS")
print(f"{'─'*80}")

hourly = {}
for t in trades:
    hour = t['ts_et'].hour if t['ts_et'] else 0
    if hour not in hourly:
        hourly[hour] = {'count': 0, 'wins': 0, 'losses': 0, 'pnl': 0}
    hourly[hour]['count'] += 1
    res = t['outcome_result']
    pnl = float(t['outcome_pnl'] or 0)
    hourly[hour]['pnl'] += pnl
    if res == 'WIN': hourly[hour]['wins'] += 1
    elif res == 'LOSS': hourly[hour]['losses'] += 1

for h in sorted(hourly):
    s = hourly[h]
    decided = s['wins'] + s['losses']
    wr = s['wins'] / decided * 100 if decided > 0 else 0
    print(f"  {h:02d}:00  trades={s['count']:2d}  {s['wins']}W/{s['losses']}L  WR={wr:5.1f}%  PnL={s['pnl']:+6.1f}")

# ── 6. Paradigm analysis ──
print(f"\n{'─'*80}")
print(f"  PARADIGM ANALYSIS")
print(f"{'─'*80}")

para_stats = {}
for t in trades:
    p = t['paradigm'] or 'Unknown'
    if p not in para_stats:
        para_stats[p] = {'count': 0, 'wins': 0, 'losses': 0, 'pnl': 0}
    para_stats[p]['count'] += 1
    pnl = float(t['outcome_pnl'] or 0)
    para_stats[p]['pnl'] += pnl
    if t['outcome_result'] == 'WIN': para_stats[p]['wins'] += 1
    elif t['outcome_result'] == 'LOSS': para_stats[p]['losses'] += 1

for p, s in sorted(para_stats.items(), key=lambda x: -x[1]['pnl']):
    decided = s['wins'] + s['losses']
    wr = s['wins'] / decided * 100 if decided > 0 else 0
    print(f"  {p:20s}  trades={s['count']:2d}  {s['wins']}W/{s['losses']}L  WR={wr:5.1f}%  PnL={s['pnl']:+6.1f}")

# ── 7. Direction analysis ──
print(f"\n{'─'*80}")
print(f"  DIRECTION ANALYSIS")
print(f"{'─'*80}")

for d in ('long/bullish', 'short/bearish'):
    dtrades = [t for t in trades if t['direction'].lower() in d.split('/')]
    wins = sum(1 for t in dtrades if t['outcome_result'] == 'WIN')
    losses = sum(1 for t in dtrades if t['outcome_result'] == 'LOSS')
    pnl = sum(float(t['outcome_pnl'] or 0) for t in dtrades)
    decided = wins + losses
    wr = wins / decided * 100 if decided > 0 else 0
    print(f"  {d.split('/')[0].upper():6s}  trades={len(dtrades):2d}  {wins}W/{losses}L  WR={wr:5.1f}%  PnL={pnl:+6.1f}")

# ── 8. DD Exhaustion pattern (the big loser today) ──
dd_trades = [t for t in trades if t['setup_name'] == 'DD Exhaustion']
if dd_trades:
    print(f"\n{'─'*80}")
    print(f"  DD EXHAUSTION ANALYSIS ({len(dd_trades)} trades)")
    print(f"{'─'*80}")
    for t in dd_trades:
        time_str = t['ts_et'].strftime('%H:%M')
        res = t['outcome_result'] or 'OPEN'
        pnl = float(t['outcome_pnl'] or 0)
        mp = float(t['outcome_max_profit'] or 0)
        ml = float(t['outcome_max_loss'] or 0)
        print(f"  #{t['id']} {time_str} {t['direction']:8s} {t['grade']:3s} | {res:7s} pnl={pnl:+6.1f} maxP={mp:+6.1f} maxL={ml:+6.1f} | para={t['paradigm']}")

# ── 9. Cumulative P&L curve ──
print(f"\n{'─'*80}")
print(f"  CUMULATIVE P&L CURVE")
print(f"{'─'*80}")
cum = 0
for t in trades:
    pnl = float(t['outcome_pnl'] or 0)
    cum += pnl
    time_str = t['ts_et'].strftime('%H:%M')
    bar = '+' * int(max(cum, 0) / 2) if cum >= 0 else '-' * int(abs(cum) / 2)
    print(f"  {time_str} #{t['id']:3d} {t['setup_name'][:6]:6s} {pnl:+5.1f} -> cum={cum:+6.1f}  {bar}")

# ── 10. ES price action context ──
print(f"\n{'─'*80}")
print(f"  ES PRICE ACTION CONTEXT")
print(f"{'─'*80}")
if bars:
    opens = [float(b['bar_open']) for b in bars]
    highs = [float(b['bar_high']) for b in bars]
    lows = [float(b['bar_low']) for b in bars]
    closes = [float(b['bar_close']) for b in bars]

    day_open = opens[0]
    day_high = max(highs)
    day_low = min(lows)
    day_close = closes[-1]
    day_range = day_high - day_low

    print(f"  Open:  {day_open:.2f}")
    print(f"  High:  {day_high:.2f}")
    print(f"  Low:   {day_low:.2f}")
    print(f"  Close: {day_close:.2f}")
    print(f"  Range: {day_range:.2f} pts")
    print(f"  Direction: {'UP' if day_close > day_open else 'DOWN'} ({day_close - day_open:+.2f})")
    print(f"  Bars: {len(bars)}")

    # Identify key moves
    print(f"\n  Key price zones where trades fired:")
    for t in trades:
        entry = float(t['abs_es_price'] or t['spot'] or 0)
        pct_from_low = (entry - day_low) / day_range * 100 if day_range > 0 else 0
        dir_arrow = '^' if t['direction'].lower() in ('long', 'bullish') else 'v'
        print(f"    #{t['id']:3d} {t['ts_et'].strftime('%H:%M')} {dir_arrow} @ {entry:.1f} ({pct_from_low:.0f}% of range)")

# ── 11. What-if: proposed filters ──
print(f"\n{'─'*80}")
print(f"  WHAT-IF: PROPOSED FILTERS")
print(f"{'─'*80}")

# Filter 1: DD 14:00 cutoff
dd_after_14 = [t for t in dd_trades if t['ts_et'] and t['ts_et'].hour >= 14]
dd_after_14_pnl = sum(float(t['outcome_pnl'] or 0) for t in dd_after_14)
print(f"\n  F1: DD cutoff 14:00 ET")
print(f"    Trades blocked: {len(dd_after_14)}")
print(f"    PnL saved: {-dd_after_14_pnl:+.1f} pts")

# Filter 2: Block BOFA-PURE paradigm on DD
dd_bofa_pure = [t for t in dd_trades if t['paradigm'] and 'BOFA' in t['paradigm'].upper() and 'PURE' in t['paradigm'].upper()]
dd_bofa_pnl = sum(float(t['outcome_pnl'] or 0) for t in dd_bofa_pure)
print(f"\n  F2: Block DD in BofA-PURE paradigm")
print(f"    Trades blocked: {len(dd_bofa_pure)}")
print(f"    PnL saved: {-dd_bofa_pnl:+.1f} pts")

# What if we only kept ES Absorption?
abs_pnl = sum(float(t['outcome_pnl'] or 0) for t in abs_trades)
print(f"\n  ES Absorption only: {len(abs_trades)} trades, PnL={abs_pnl:+.1f}")

# ── 12. Running totals (all time) ──
print(f"\n{'─'*80}")
print(f"  ALL-TIME RUNNING TOTALS (for context)")
print(f"{'─'*80}")
all_time = c.execute("""
    SELECT setup_name,
           COUNT(*) as cnt,
           SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
           SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
           COALESCE(SUM(outcome_pnl), 0) as total_pnl
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    GROUP BY setup_name
    ORDER BY total_pnl DESC
""").fetchall()
grand_total = 0
for r in all_time:
    decided = r['wins'] + r['losses']
    wr = r['wins'] / decided * 100 if decided > 0 else 0
    grand_total += float(r['total_pnl'])
    print(f"  {r['setup_name']:20s}  {r['cnt']:3d} trades  {r['wins']}W/{r['losses']}L  WR={wr:5.1f}%  PnL={float(r['total_pnl']):+7.1f}")
print(f"  {'─'*60}")
print(f"  {'GRAND TOTAL':20s}  PnL={grand_total:+7.1f}")

c.close()
