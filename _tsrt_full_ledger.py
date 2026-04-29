"""TSRT full ledger: Mar 24 (go-live) -> Apr 22 (today).

Joins real_trade_orders to setup_log.
Per-day + per-era + per-account breakdown.
$5/pt * 1 MES (TSRT is 1 MES per account).
"""
import psycopg2
from collections import defaultdict
from datetime import date

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

GO_LIVE = date(2026, 3, 24)
TODAY = date(2026, 4, 22)
QTY_SIGN_FIX = date(2026, 4, 8)   # pre-fix era = Mar 24 - Apr 7 (qty sign + ghost bugs)
V13_DATE = date(2026, 4, 17)
DOLLAR_PER_PT = 5.0
COMMISSION_PER_TRADE = 3.0    # round-trip commission estimate

# ---- Pull all TSRT trades + portal outcome ----
cur.execute("""
SELECT r.setup_log_id, r.state, r.created_at,
       s.setup_name, s.direction, s.outcome_pnl, s.outcome_result,
       (s.ts AT TIME ZONE 'America/New_York')::date AS trade_date
FROM real_trade_orders r
LEFT JOIN setup_log s ON s.id = r.setup_log_id
WHERE (s.ts AT TIME ZONE 'America/New_York')::date >= %s
  AND (s.ts AT TIME ZONE 'America/New_York')::date <= %s
ORDER BY r.created_at
""", (GO_LIVE, TODAY))
rows = cur.fetchall()

# ---- Also pull setup_log signals to find bot-down days ----
cur.execute("""
SELECT (s.ts AT TIME ZONE 'America/New_York')::date AS trade_date, COUNT(*) AS n
FROM setup_log s
WHERE (s.ts AT TIME ZONE 'America/New_York')::date >= %s
  AND (s.ts AT TIME ZONE 'America/New_York')::date <= %s
  AND s.setup_name IN ('Skew Charm','AG Short','DD Exhaustion')
GROUP BY 1
""", (GO_LIVE, TODAY))
signals_by_date = {r[0]: r[1] for r in cur.fetchall()}

cur.close(); conn.close()


def real_pnl_pts(st, direction):
    fill = st.get('fill_price')
    reason = st.get('close_reason')
    exit_px = None
    if reason == 'stop_filled':
        exit_px = st.get('stop_fill_price')
    elif reason == 'target_filled':
        exit_px = st.get('target_fill_price') or st.get('target_price')
    elif reason == 'WIN':
        exit_px = st.get('current_stop')
    elif reason in ('eod_flatten', 'ghost_reconcile', 'stale_overnight', 'pre_market_cleanup'):
        # Approximation: current_stop level at time of resolution
        exit_px = st.get('current_stop')
    else:
        exit_px = st.get('current_stop')
    if fill is None or exit_px is None:
        return None
    try:
        fill = float(fill); exit_px = float(exit_px)
    except (TypeError, ValueError):
        return None
    if direction in ('short', 'bearish'):
        return fill - exit_px
    return exit_px - fill


trades = []
skipped = 0
for sid, state, created, setup, direction, outcome_pnl, outcome_result, trade_date in rows:
    if not state:
        skipped += 1; continue
    real_pts = real_pnl_pts(state, direction)
    portal_pts = float(outcome_pnl) if outcome_pnl is not None else 0.0
    trades.append({
        'id': sid, 'date': trade_date, 'setup': setup, 'dir': direction,
        'real_pts': real_pts, 'portal_pts': portal_pts,
        'reason': state.get('close_reason'),
        'account': state.get('account_id'),
        'fill': state.get('fill_price'),
        'status': state.get('status'),
    })


def era_of(d):
    if d < QTY_SIGN_FIX:
        return 'bug-era (pre Apr 8)'
    if d < V13_DATE:
        return 'post-fix pre-V13 (Apr 8-16)'
    return 'V13-live (Apr 17+)'


def safe_sum(vals):
    return sum(v for v in vals if v is not None)


# ---- Per-day breakdown ----
by_date = defaultdict(list)
for t in trades:
    by_date[t['date']].append(t)

# ---- Per-era totals ----
by_era = defaultdict(list)
for t in trades:
    by_era[era_of(t['date'])].append(t)

# ---- Per-account totals ----
by_account = defaultdict(list)
for t in trades:
    by_account[t['account']].append(t)


def summarize(lst):
    real_pts = safe_sum([t['real_pts'] for t in lst])
    portal_pts = sum(t['portal_pts'] for t in lst)
    real_usd = real_pts * DOLLAR_PER_PT
    portal_usd = portal_pts * DOLLAR_PER_PT
    cap = (real_usd / portal_usd * 100) if portal_usd != 0 else float('nan')
    fees = len(lst) * COMMISSION_PER_TRADE
    return dict(n=len(lst), real_pts=real_pts, portal_pts=portal_pts,
                real_usd=real_usd, portal_usd=portal_usd, cap=cap, fees=fees,
                net_usd=real_usd - fees)


print("=" * 110)
print("TSRT FULL LEDGER — Mar 24 (go-live) -> Apr 22 (today)")
print(f"Total TSRT trades: {len(trades)}   (skipped: {skipped})")
print("=" * 110)

# Per-era
print("\n### PER-ERA TOTALS ###")
print(f"{'Era':<30} {'N':>4} {'Portal pts':>11} {'Real pts':>10} {'Portal $':>10} {'Real $':>10} {'Fees $':>8} {'Net $':>10} {'Cap %':>7}")
for era in ['bug-era (pre Apr 8)', 'post-fix pre-V13 (Apr 8-16)', 'V13-live (Apr 17+)']:
    a = summarize(by_era[era])
    print(f"{era:<30} {a['n']:>4} {a['portal_pts']:>+11.2f} {a['real_pts']:>+10.2f} "
          f"{a['portal_usd']:>+10.1f} {a['real_usd']:>+10.1f} {a['fees']:>8.0f} "
          f"{a['net_usd']:>+10.1f} {a['cap']:>6.1f}%")
a = summarize(trades)
print(f"{'OVERALL':<30} {a['n']:>4} {a['portal_pts']:>+11.2f} {a['real_pts']:>+10.2f} "
      f"{a['portal_usd']:>+10.1f} {a['real_usd']:>+10.1f} {a['fees']:>8.0f} "
      f"{a['net_usd']:>+10.1f} {a['cap']:>6.1f}%")

# Per-account
print("\n### PER-ACCOUNT TOTALS ###")
print(f"{'Account':<12} {'Role':<20} {'N':>4} {'Portal pts':>11} {'Real pts':>10} {'Real $':>10} {'Fees $':>8} {'Net $':>10}")
ACCT_ROLE = {'210VYX65': 'longs', '210VYX91': 'shorts'}
for acct, lst in sorted(by_account.items()):
    a = summarize(lst)
    role = ACCT_ROLE.get(acct, '?')
    print(f"{acct:<12} {role:<20} {a['n']:>4} {a['portal_pts']:>+11.2f} {a['real_pts']:>+10.2f} "
          f"{a['real_usd']:>+10.1f} {a['fees']:>8.0f} {a['net_usd']:>+10.1f}")

# Timeline summary
print("\n### TIMELINE ###")
trading_days = sorted([d for d in signals_by_date.keys() if d.weekday() < 5])
days_with_trades = set(by_date.keys())
days_with_signals = set(signals_by_date.keys())
bot_down_days = sorted([d for d in days_with_signals if d not in days_with_trades and d.weekday() < 5])
active_days = sorted([d for d in days_with_trades if d.weekday() < 5])
print(f"Live trading window: {GO_LIVE} -> {TODAY}  ({len(trading_days)} trading days with any signal)")
print(f"Days with TSRT trades placed: {len(active_days)}")
print(f"Bot-down days (signals fired, no trades placed): {len(bot_down_days)}  -> {[str(d) for d in bot_down_days]}")

# Day-by-day last 10 trading days
print("\n### LAST 10 ACTIVE TRADING DAYS ###")
last10 = sorted(active_days)[-10:]
print(f"{'Date':<12} {'Era':<25} {'N':>3} {'Setups':<30} {'Portal pts':>11} {'Real pts':>10} {'Real $':>9} {'Cap %':>7} {'Flags':<30}")
for d in last10:
    lst = by_date[d]
    a = summarize(lst)
    setups_ct = defaultdict(int)
    for t in lst:
        key = f"{t['setup'][:4]} {t['dir'][:1]}"
        setups_ct[key] += 1
    setup_str = ' '.join(f"{k}={v}" for k, v in setups_ct.items())
    flags = []
    for t in lst:
        if t['reason'] == 'ghost_reconcile':
            flags.append(f"ghost(id={t['id']})")
        if t['real_pts'] is None:
            flags.append(f"null(id={t['id']})")
    flag_str = ','.join(flags)[:30]
    era = era_of(d)[:25]
    print(f"{str(d):<12} {era:<25} {a['n']:>3} {setup_str:<30} {a['portal_pts']:>+11.2f} "
          f"{a['real_pts']:>+10.2f} {a['real_usd']:>+9.1f} {a['cap']:>6.1f}% {flag_str:<30}")

# Full day-by-day too (compact)
print("\n### ALL DAYS (compact) ###")
print(f"{'Date':<12} {'Era':<25} {'N':>3} {'Portal $':>9} {'Real $':>9} {'Cap %':>7} {'Flags'}")
for d in sorted(active_days):
    lst = by_date[d]
    a = summarize(lst)
    flags = []
    for t in lst:
        if t['reason'] == 'ghost_reconcile':
            flags.append(f"ghost")
        if t['real_pts'] is None:
            flags.append(f"null")
        if t['reason'] in ('eod_flatten','stale_overnight','pre_market_cleanup'):
            flags.append(t['reason'])
    flag_set = sorted(set(flags))
    era = era_of(d)[:25]
    print(f"{str(d):<12} {era:<25} {a['n']:>3} {a['portal_usd']:>+9.1f} {a['real_usd']:>+9.1f} "
          f"{a['cap']:>6.1f}% {','.join(flag_set)}")

# Fees + balance reconciliation
print("\n### FEES + RECONCILIATION ###")
total = summarize(trades)
print(f"Gross real $ (trade P&L):  {total['real_usd']:>+10.1f}")
print(f"Commissions (${COMMISSION_PER_TRADE}/trade x {total['n']}):  {-total['fees']:>+10.1f}")
print(f"NET real $:                {total['net_usd']:>+10.1f}")
print()
print("Starting funds (per MEMORY.md):  $4,000 combined + $2,000 recent top-up = $6,000 total")
print(f"Expected current balance = $6,000 + NET = ${6000 + total['net_usd']:,.1f}")
print(f"User says 'near BE around $6,000'  --> gap vs books = ${(6000 + total['net_usd']) - 6000:+.1f}")
