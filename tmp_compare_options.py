"""Compare single-leg vs credit spread using ALL setup_log data (not just options trades).
Simulates options P&L from setup outcomes across full March."""
import sqlalchemy, json, os, math
from sqlalchemy import text
from datetime import datetime

engine = sqlalchemy.create_engine(os.environ['DATABASE_URL'])

with engine.begin() as conn:
    # ALL setup outcomes in March with V9-SC relevant fields
    setups = conn.execute(text("""
        SELECT id, setup_name, direction, spot, outcome_result, outcome_pnl,
               outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
               ts, vix, overvix, greek_alignment,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts >= '2026-03-01'
          AND ts < '2026-03-19'
        ORDER BY id
    """)).mappings().all()

print("Total setup outcomes in March:", len(setups))

# Filter to setups that the options trader WOULD trade
# (SC, DD, AG — same setups that had options trades)
OPTION_SETUPS = {'Skew Charm', 'DD Exhaustion', 'AG Short'}

trades = []
for su in setups:
    setup_name = su.get('setup_name', '')
    if setup_name not in OPTION_SETUPS:
        continue

    setup_pts = float(su.get('outcome_pnl') or 0)
    result = su.get('outcome_result', '?')
    hold = float(su.get('outcome_elapsed_min') or 0)
    is_long = su.get('direction', '').lower() in ('long', 'bullish')
    trade_date = str(su.get('trade_date', ''))
    vix = float(su.get('vix') or 0)
    overvix = float(su.get('overvix') or 0) if su.get('overvix') else None
    alignment = int(su.get('greek_alignment') or 0) if su.get('greek_alignment') is not None else None

    # V9-SC filter
    v9sc_pass = False
    if is_long:
        if alignment is not None and alignment >= 2:
            if 'Skew Charm' in setup_name:
                v9sc_pass = True
            elif vix <= 22:
                v9sc_pass = True
            elif overvix is not None and overvix >= 2:
                v9sc_pass = True
    else:
        if 'Skew Charm' in setup_name or 'AG Short' in setup_name:
            v9sc_pass = True
        elif 'DD Exhaustion' in setup_name and alignment is not None and alignment != 0:
            v9sc_pass = True

    trades.append({
        'id': su['id'], 'setup': setup_name[:14], 'date': trade_date,
        'dir': 'L' if is_long else 'S', 'result': result,
        'setup_pts': setup_pts, 'hold': hold, 'is_long': is_long,
        'v9sc': v9sc_pass, 'vix': vix, 'alignment': alignment
    })

# Simulate P&L for each strategy
WIDTH = 2.0
CREDIT_RATE = 0.45

for t in trades:
    spy_move = t['setup_pts'] / 10.0

    # --- SINGLE-LEG (buy option) ---
    # At 0.50 delta on SPY: $5/SPX-pt, minus theta
    # Estimate theta based on hold time: ~$0.03/min for $2 ATM 0DTE SPY option
    delta_pnl = spy_move * 0.50 * 100  # delta capture in $
    # Theta: heavier later in day, avg ~$2/min for ATM 0DTE
    theta_per_min = 0.025  # $0.025/min per contract ($2.50/min per 100 shares... no)
    # More realistic: $2 option loses ~30-50% in 60 min mid-day
    # So ~$0.01-0.02/min per share, x100 = $1-2/min
    # Conservative: $1.5/min
    theta_cost = min(t['hold'], 90) * 1.5  # capped at 90 min
    t['single_pnl'] = round(delta_pnl - theta_cost)
    # Cap at max loss = premium paid (~$200 for $2 ATM SPY option)
    t['single_pnl'] = max(t['single_pnl'], -200)

    # --- CREDIT SPREAD ---
    credit = WIDTH * CREDIT_RATE
    max_loss_cr = WIDTH - credit
    if spy_move >= WIDTH:
        t['credit_pnl'] = round(credit * 100)
    elif spy_move >= 0:
        remaining = max(0, WIDTH - spy_move)
        spread_val = remaining * 0.20
        t['credit_pnl'] = round((credit - spread_val) * 100)
    elif abs(spy_move) >= WIDTH:
        t['credit_pnl'] = round(-max_loss_cr * 100)
    else:
        intrinsic = abs(spy_move)
        spread_val = intrinsic + (WIDTH - intrinsic) * 0.15
        t['credit_pnl'] = round((credit - min(spread_val, WIDTH)) * 100)

    # --- DEBIT SPREAD ---
    debit = WIDTH * (1 - CREDIT_RATE)
    max_profit_db = WIDTH - debit
    if spy_move >= WIDTH:
        t['debit_pnl'] = round(max_profit_db * 100)
    elif spy_move > 0:
        spread_val = min(spy_move, WIDTH)
        if spy_move < 0.5:
            spread_val = spy_move * 0.7
        t['debit_pnl'] = round((spread_val - debit) * 100)
    else:
        t['debit_pnl'] = round(-debit * 100)


def report(label, tlist):
    n = len(tlist)
    if n == 0:
        print("  %s: no trades" % label)
        return

    s_total = sum(t['single_pnl'] for t in tlist)
    cr_total = sum(t['credit_pnl'] for t in tlist)
    db_total = sum(t['debit_pnl'] for t in tlist)
    setup_total = sum(t['setup_pts'] for t in tlist)

    s_w = sum(1 for t in tlist if t['single_pnl'] > 0)
    cr_w = sum(1 for t in tlist if t['credit_pnl'] > 0)
    db_w = sum(1 for t in tlist if t['debit_pnl'] > 0)

    s_wins = [t['single_pnl'] for t in tlist if t['single_pnl'] > 0]
    s_loss = [t['single_pnl'] for t in tlist if t['single_pnl'] <= 0]
    cr_wins = [t['credit_pnl'] for t in tlist if t['credit_pnl'] > 0]
    cr_loss = [t['credit_pnl'] for t in tlist if t['credit_pnl'] <= 0]
    db_wins = [t['debit_pnl'] for t in tlist if t['debit_pnl'] > 0]
    db_loss = [t['debit_pnl'] for t in tlist if t['debit_pnl'] <= 0]

    pf_s = abs(sum(s_wins) / sum(s_loss)) if s_loss and sum(s_loss) else 0
    pf_cr = abs(sum(cr_wins) / sum(cr_loss)) if cr_loss and sum(cr_loss) else 0
    pf_db = abs(sum(db_wins) / sum(db_loss)) if db_loss and sum(db_loss) else 0

    cs = 0.60 * 2 * n
    cc = 0.60 * 4 * n
    ndays = len(set(t['date'] for t in tlist))

    print("%-30s %12s %12s %12s" % (label + " (%d, %dd)" % (n, ndays), 'Single-Leg', 'Credit Spr', 'Debit Spr'))
    print("-" * 66)
    print("%-30s %+12.1f" % ('Setup Points', setup_total))
    print("%-30s %+12d %+12d %+12d" % ('Total P&L ($)', s_total, cr_total, db_total))
    print("%-30s %+12.1f %+12.1f %+12.1f" % ('Per trade ($)', s_total / n, cr_total / n, db_total / n))
    print("%-30s %+12.1f %+12.1f %+12.1f" % ('Per day ($)', s_total / ndays, cr_total / ndays, db_total / ndays))
    print("%-30s %11.1f%% %11.1f%% %11.1f%%" % ('Win Rate', s_w / n * 100, cr_w / n * 100, db_w / n * 100))
    print("%-30s %12.2f %12.2f %12.2f" % ('Profit Factor', pf_s, pf_cr, pf_db))
    avg = lambda lst: sum(lst) / len(lst) if lst else 0
    print("%-30s %+12.0f %+12.0f %+12.0f" % ('Avg Winner ($)', avg(s_wins), avg(cr_wins), avg(db_wins)))
    print("%-30s %+12.0f %+12.0f %+12.0f" % ('Avg Loser ($)', avg(s_loss), avg(cr_loss), avg(db_loss)))
    print("%-30s %12.0f %12.0f %12.0f" % ('Commission ($)', cs, cc, cc))
    print("%-30s %+12.0f %+12.0f %+12.0f" % ('Net after comm ($)', s_total - cs, cr_total - cc, db_total - cc))
    print()


dates = sorted(set(t['date'] for t in trades))
print("Options-eligible setups (SC, DD, AG): %d trades across %d days (%s to %s)" % (
    len(trades), len(dates), dates[0], dates[-1]))
print()

# ALL (no filter)
print("=" * 66)
report("ALL (no filter)", trades)

# V9-SC
v9sc = [t for t in trades if t['v9sc']]
print("=" * 66)
report("V9-SC FILTERED", v9sc)

# Per-day (V9-SC)
print("=" * 66)
print("PER-DAY (V9-SC)")
print("%-12s %4s %6s %8s %10s %10s %10s" % ('Date', '#', 'WR%', 'SetPts', 'Single$', 'Credit$', 'Debit$'))
print("-" * 68)
for d in dates:
    dt = [t for t in v9sc if t['date'] == d]
    if not dt:
        continue
    sp = sum(t['setup_pts'] for t in dt)
    sl = sum(t['single_pnl'] for t in dt)
    cr = sum(t['credit_pnl'] for t in dt)
    db = sum(t['debit_pnl'] for t in dt)
    wr = sum(1 for t in dt if t['result'] == 'WIN') / len(dt) * 100
    print("%-12s %4d %5.0f%% %+8.1f %+10d %+10d %+10d" % (d, len(dt), wr, sp, sl, cr, db))
sp = sum(t['setup_pts'] for t in v9sc)
sl = sum(t['single_pnl'] for t in v9sc)
cr = sum(t['credit_pnl'] for t in v9sc)
db = sum(t['debit_pnl'] for t in v9sc)
print("-" * 68)
print("%-12s %4d %5s %+8.1f %+10d %+10d %+10d" % ('TOTAL', len(v9sc), '', sp, sl, cr, db))

# By direction (V9-SC)
print()
print("BY DIRECTION (V9-SC):")
for d, label in [('L', 'Longs'), ('S', 'Shorts')]:
    dt = [t for t in v9sc if t['dir'] == d]
    if not dt:
        continue
    st = sum(t['single_pnl'] for t in dt)
    cr = sum(t['credit_pnl'] for t in dt)
    db = sum(t['debit_pnl'] for t in dt)
    wr = sum(1 for t in dt if t['result'] == 'WIN') / len(dt) * 100
    print("  %-8s (%3d, %.0f%% WR): Single %+7d  Credit %+7d  Debit %+7d" % (
        label, len(dt), wr, st, cr, db))

# By setup (V9-SC)
print()
print("BY SETUP (V9-SC):")
setup_names = sorted(set(t['setup'] for t in v9sc))
for sn in setup_names:
    dt = [t for t in v9sc if t['setup'] == sn]
    if not dt:
        continue
    st = sum(t['single_pnl'] for t in dt)
    cr = sum(t['credit_pnl'] for t in dt)
    db = sum(t['debit_pnl'] for t in dt)
    wr = sum(1 for t in dt if t['result'] == 'WIN') / len(dt) * 100
    print("  %-14s (%3d, %.0f%% WR): Single %+7d  Credit %+7d  Debit %+7d" % (
        sn, len(dt), wr, st, cr, db))

# Winning days / losing days (V9-SC credit spread)
print()
win_days = sum(1 for d in dates if sum(t['credit_pnl'] for t in v9sc if t['date'] == d) > 0)
lose_days = sum(1 for d in dates if sum(t['credit_pnl'] for t in v9sc if t['date'] == d) <= 0)
print("CREDIT SPREAD: %d winning days, %d losing days out of %d" % (win_days, lose_days, len(dates)))

print()
print("NOTE: Single-leg P&L is SIMULATED (0.50 delta + theta model).")
print("      Credit/debit spread P&L is SIMULATED from setup outcome SPX points.")
print("      Credit spreads require MARGIN account. Debit spreads work on CASH.")
