"""Calculate capital requirements for credit spread trading."""
import sqlalchemy, json, os
from sqlalchemy import text
from datetime import datetime, timedelta
engine = sqlalchemy.create_engine(os.environ['DATABASE_URL'])

with engine.begin() as conn:
    setups = conn.execute(text("""
        SELECT id, setup_name, direction, spot, outcome_result, outcome_pnl,
               outcome_elapsed_min, ts,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date,
               vix, overvix, greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts >= '2026-03-01' AND ts < '2026-03-19'
          AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'AG Short')
        ORDER BY ts
    """)).mappings().all()

OPTION_SETUPS = {'Skew Charm', 'DD Exhaustion', 'AG Short'}
trades = []
for su in setups:
    setup_name = su['setup_name']
    is_long = su['direction'].lower() in ('long', 'bullish')
    vix = float(su.get('vix') or 0)
    overvix = float(su.get('overvix') or 0) if su.get('overvix') else None
    alignment = int(su.get('greek_alignment') or 0) if su.get('greek_alignment') is not None else None
    v9sc = False
    if is_long:
        if alignment is not None and alignment >= 2:
            if 'Skew Charm' in setup_name:
                v9sc = True
            elif vix <= 22:
                v9sc = True
            elif overvix is not None and overvix >= 2:
                v9sc = True
    else:
        if 'Skew Charm' in setup_name or 'AG Short' in setup_name:
            v9sc = True
        elif 'DD Exhaustion' in setup_name and alignment is not None and alignment != 0:
            v9sc = True
    if not v9sc:
        continue
    hold = float(su.get('outcome_elapsed_min') or 30)
    fired = su['ts']
    resolved = fired + timedelta(minutes=hold)
    setup_pts = float(su.get('outcome_pnl') or 0)
    result = su.get('outcome_result', '?')
    trade_date = str(su.get('trade_date', ''))
    trades.append({
        'id': su['id'], 'setup': setup_name, 'date': trade_date,
        'fired': fired, 'resolved': resolved, 'hold': hold,
        'result': result, 'setup_pts': setup_pts,
    })

dates = sorted(set(t['date'] for t in trades))
print("V9-SC trades: %d across %d days" % (len(trades), len(dates)))
print()

WIDTH = 2.0
CREDIT_RATE = 0.45
CREDIT = WIDTH * CREDIT_RATE
MAX_LOSS = WIDTH - CREDIT
MARGIN_PER = MAX_LOSS * 100  # $110

print("=== $2 ATM CREDIT SPREAD ===")
print("Credit: $%.2f | Max loss: $%.2f | Margin/contract: $%d" % (CREDIT, MAX_LOSS, MARGIN_PER))
print()

def spread_pnl(setup_pts):
    spy_move = setup_pts / 10.0
    if spy_move >= WIDTH:
        return CREDIT * 100
    elif spy_move >= 0:
        remaining = max(0, WIDTH - spy_move)
        sv = remaining * 0.20
        return (CREDIT - sv) * 100
    elif abs(spy_move) >= WIDTH:
        return -MAX_LOSS * 100
    else:
        intrinsic = abs(spy_move)
        sv = intrinsic + (WIDTH - intrinsic) * 0.15
        return (CREDIT - min(sv, WIDTH)) * 100

# Per-day analysis
print("%-12s %4s %6s %10s %10s" % ('Date', '#', 'MaxCon', 'Margin$', 'DayPnL$'))
print("-" * 48)
daily = []
for d in dates:
    dt = [t for t in trades if t['date'] == d]
    events = []
    for t in dt:
        events.append((t['fired'], +1))
        events.append((t['resolved'], -1))
    events.sort(key=lambda x: x[0])
    current = 0
    max_conc = 0
    for ts, delta in events:
        current += delta
        max_conc = max(max_conc, current)
    day_pnl = round(sum(spread_pnl(t['setup_pts']) for t in dt))
    margin = max_conc * MARGIN_PER
    daily.append({'date': d, 'n': len(dt), 'conc': max_conc, 'margin': margin, 'pnl': day_pnl})
    print("%-12s %4d %6d %10d %+10d" % (d, len(dt), max_conc, margin, day_pnl))

print("-" * 48)
max_conc = max(s['conc'] for s in daily)
max_margin = max(s['margin'] for s in daily)
worst = min(s['pnl'] for s in daily)
best = max(s['pnl'] for s in daily)
total = sum(s['pnl'] for s in daily)
avg = total / len(daily)
print("Peak concurrent: %d | Peak margin: $%d" % (max_conc, max_margin))
print("Worst day: $%+d | Best day: $%+d | Avg: $%+.0f/day" % (worst, best, avg))
print()

# Capital tiers
print("=" * 55)
print("CAPITAL REQUIREMENTS (1 contract/signal)")
print("=" * 55)
min_cap = max_margin
cons_cap = max_margin + abs(worst)
safe_cap = max_margin + abs(worst) * 2
print("  Margin per spread:   $%d" % MARGIN_PER)
print("  Peak concurrent:     %d positions" % max_conc)
print("  Peak margin needed:  $%d" % max_margin)
print("  Worst day loss:      $%d" % abs(worst))
print()
print("  Minimum:      $%d  (just peak margin)" % min_cap)
print("  Conservative: $%d  (margin + worst day)" % cons_cap)
print("  Safe:         $%d  (margin + 2x worst day)" % safe_cap)
print()

# Monthly projection
td = 21  # trading days/month
monthly = avg * td
comm = 0.60 * 4 * (len(trades) / len(dates)) * td
net = monthly - comm
print("=== MONTHLY PROJECTION ===")
for label, cap in [("Minimum $%d" % min_cap, min_cap),
                    ("Conservative $%d" % cons_cap, cons_cap),
                    ("Safe $%d" % safe_cap, safe_cap)]:
    roi = net / cap * 100 if cap else 0
    print("  %-25s $%+.0f/mo = %.0f%% ROI" % (label, net, roi))

print()

# Scale: 2 and 5 contracts
print("=== SCALING ===")
for qty in [1, 2, 5]:
    cap = cons_cap * qty
    mo = net * qty
    print("  %d contract(s): capital $%d, net $%+.0f/mo (%.0f%% ROI)" % (
        qty, cap, mo, mo / cap * 100 if cap else 0))

print()

# Single-position mode (cheapest)
print("=== SINGLE POSITION MODE (skip if busy) ===")
sm_total = 0
sm_trades = 0
for d in dates:
    dt = sorted([t for t in trades if t['date'] == d], key=lambda x: x['fired'])
    busy_until = None
    for t in dt:
        if busy_until and t['fired'] < busy_until:
            continue
        sm_total += spread_pnl(t['setup_pts'])
        sm_trades += 1
        busy_until = t['resolved']

sm_daily = sm_total / len(dates)
sm_worst = 0
for d in dates:
    dt = sorted([t for t in trades if t['date'] == d], key=lambda x: x['fired'])
    dpnl = 0
    busy_until = None
    for t in dt:
        if busy_until and t['fired'] < busy_until:
            continue
        dpnl += spread_pnl(t['setup_pts'])
        busy_until = t['resolved']
    sm_worst = min(sm_worst, dpnl)

sm_monthly = sm_daily * td
sm_comm = 0.60 * 4 * (sm_trades / len(dates)) * td
sm_net = sm_monthly - sm_comm
sm_cap = MARGIN_PER + abs(sm_worst)
print("  Trades: %d/%d (%.0f%% taken)" % (sm_trades, len(trades), sm_trades / len(trades) * 100))
print("  Avg/day: $%+.0f" % sm_daily)
print("  Capital: $%d (1 spread margin + worst day)" % sm_cap)
print("  Monthly: $%+.0f net (%.0f%% ROI)" % (sm_net, sm_net / sm_cap * 100 if sm_cap else 0))
