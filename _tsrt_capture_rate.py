"""TSRT capture rate: real P&L vs portal outcome_pnl, split by V13 era.

Pre-V13: Mar 24 -> Apr 16
V13-live: Apr 17 -> Apr 22 (today)
Overall: all TSRT trades

Real $ = (exit - entry) * dir * $5/pt (1 MES contract assumed)
Portal $ = setup_log.outcome_pnl * $5/pt

Excludes: setup_log_id 1256 (Mar 26 TS outage), 1352 (Mar 30 S19 basis incident)
         reports both with/without.
Excludes: open trades (status != 'closed').
"""
import psycopg2
from collections import defaultdict
from datetime import date

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

V13_DATE = date(2026, 4, 17)
TODAY = date(2026, 4, 22)
PRE_START = date(2026, 3, 24)
DOLLAR_PER_PT = 5.0
OUTLIER_IDS = {1256, 1352}

cur.execute("""
SELECT r.setup_log_id, r.state, r.created_at,
       s.setup_name, s.direction, s.outcome_pnl, s.outcome_result,
       (s.ts AT TIME ZONE 'America/New_York')::date AS trade_date
FROM real_trade_orders r
LEFT JOIN setup_log s ON s.id = r.setup_log_id
ORDER BY r.created_at
""")
rows = cur.fetchall()
cur.close(); conn.close()

def real_pnl_pts(st, direction):
    """Compute real exit pts from state JSONB."""
    fill = st.get('fill_price')
    reason = st.get('close_reason')
    if fill is None:
        return None
    if reason == 'stop_filled':
        exit_px = st.get('stop_fill_price')
    elif reason == 'target_filled':
        exit_px = st.get('target_fill_price') or st.get('target_price')
    elif reason == 'WIN':
        # trail hit — current_stop is the locked profit stop level
        exit_px = st.get('current_stop')
    elif reason in ('eod_flatten', 'ghost_reconcile', 'stale_overnight', 'pre_market_cleanup'):
        # approximation — use current_stop
        exit_px = st.get('current_stop')
    else:
        exit_px = st.get('current_stop')
    if exit_px is None:
        return None
    try:
        fill = float(fill); exit_px = float(exit_px)
    except (TypeError, ValueError):
        return None
    if direction in ('short', 'bearish'):
        return fill - exit_px
    return exit_px - fill

# Assemble trade records
trades = []
skipped_open = 0
skipped_no_fill = 0
for sid, state, created, setup, direction, outcome_pnl, outcome_result, trade_date in rows:
    if not state: continue
    if state.get('status') != 'closed':
        skipped_open += 1
        continue
    real_pts = real_pnl_pts(state, direction)
    if real_pts is None:
        skipped_no_fill += 1
        continue
    portal_pts = float(outcome_pnl) if outcome_pnl is not None else 0.0
    trades.append({
        'id': sid, 'date': trade_date, 'setup': setup, 'dir': direction,
        'real_pts': real_pts, 'portal_pts': portal_pts,
        'reason': state.get('close_reason'),
    })

print(f"Closed TSRT trades: {len(trades)}")
print(f"Skipped open: {skipped_open}, skipped (no fill): {skipped_no_fill}")
print()

def era_of(d):
    if d is None: return 'unknown'
    if d < PRE_START: return 'pre-mar24'
    if d < V13_DATE: return 'pre-V13'
    if d <= TODAY: return 'V13-live'
    return 'future'

def aggregate(lst, label):
    n = len(lst)
    portal_pts = sum(t['portal_pts'] for t in lst)
    real_pts = sum(t['real_pts'] for t in lst)
    portal_usd = portal_pts * DOLLAR_PER_PT
    real_usd = real_pts * DOLLAR_PER_PT
    cap = (real_usd / portal_usd * 100) if portal_usd != 0 else float('nan')
    return {
        'label': label, 'n': n,
        'portal_pts': portal_pts, 'real_pts': real_pts,
        'portal_usd': portal_usd, 'real_usd': real_usd, 'capture_pct': cap,
    }

# group
by_era = defaultdict(list)
for t in trades:
    by_era[era_of(t['date'])].append(t)

print("=" * 100)
print("TSRT CAPTURE RATE — with all trades (incl outliers 1256, 1352)")
print("=" * 100)
print(f"{'Era':<15} {'N':>5} {'Portal pts':>12} {'Real pts':>12} {'Portal $':>12} {'Real $':>12} {'Capture %':>12}")
for era in ['pre-V13', 'V13-live']:
    lst = by_era.get(era, [])
    a = aggregate(lst, era)
    print(f"{a['label']:<15} {a['n']:>5} {a['portal_pts']:>+12.2f} {a['real_pts']:>+12.2f} "
          f"{a['portal_usd']:>+12.1f} {a['real_usd']:>+12.1f} {a['capture_pct']:>11.1f}%")
overall_all = aggregate(trades, 'OVERALL')
print(f"{overall_all['label']:<15} {overall_all['n']:>5} {overall_all['portal_pts']:>+12.2f} {overall_all['real_pts']:>+12.2f} "
      f"{overall_all['portal_usd']:>+12.1f} {overall_all['real_usd']:>+12.1f} {overall_all['capture_pct']:>11.1f}%")

# Now exclude outliers
trades_clean = [t for t in trades if t['id'] not in OUTLIER_IDS]
by_era_clean = defaultdict(list)
for t in trades_clean:
    by_era_clean[era_of(t['date'])].append(t)

print()
print("=" * 100)
print("TSRT CAPTURE RATE — EXCLUDING outliers 1256 (Mar 26 TS outage) + 1352 (Mar 30 S19)")
print("=" * 100)
print(f"{'Era':<15} {'N':>5} {'Portal pts':>12} {'Real pts':>12} {'Portal $':>12} {'Real $':>12} {'Capture %':>12}")
for era in ['pre-V13', 'V13-live']:
    lst = by_era_clean.get(era, [])
    a = aggregate(lst, era)
    print(f"{a['label']:<15} {a['n']:>5} {a['portal_pts']:>+12.2f} {a['real_pts']:>+12.2f} "
          f"{a['portal_usd']:>+12.1f} {a['real_usd']:>+12.1f} {a['capture_pct']:>11.1f}%")
overall_clean = aggregate(trades_clean, 'OVERALL')
print(f"{overall_clean['label']:<15} {overall_clean['n']:>5} {overall_clean['portal_pts']:>+12.2f} {overall_clean['real_pts']:>+12.2f} "
      f"{overall_clean['portal_usd']:>+12.1f} {overall_clean['real_usd']:>+12.1f} {overall_clean['capture_pct']:>11.1f}%")

# Per-direction drift
print()
print("=" * 100)
print("PER-DIRECTION AVG DRIFT (real_pts - portal_pts), excluding outliers")
print("=" * 100)
for era in ['pre-V13', 'V13-live', 'OVERALL']:
    src = trades_clean if era == 'OVERALL' else by_era_clean.get(era, [])
    print(f"-- {era} --")
    for d in ['long', 'short', 'bullish', 'bearish']:
        lst = [t for t in src if t['dir'] == d]
        if not lst: continue
        drifts = [t['real_pts'] - t['portal_pts'] for t in lst]
        avg = sum(drifts) / len(drifts)
        print(f"   {d:<10} n={len(lst):>3}  avg drift = {avg:+.2f} pts/trade  "
              f"portal_avg={sum(t['portal_pts'] for t in lst)/len(lst):+.2f}  "
              f"real_avg={sum(t['real_pts'] for t in lst)/len(lst):+.2f}")

# List V13-era trades so we can eyeball sample size
print()
print("=" * 100)
print("V13-live trades detail")
print("=" * 100)
for t in sorted(by_era_clean.get('V13-live', []), key=lambda x: (x['date'], x['id'])):
    print(f"  id={t['id']:>5}  {t['date']}  {t['setup']:<12} {t['dir']:<8} "
          f"portal={t['portal_pts']:>+6.2f}  real={t['real_pts']:>+6.2f}  reason={t['reason']}")

# Flag outliers detail
print()
print("Outlier trades (excluded in clean tables):")
for t in trades:
    if t['id'] in OUTLIER_IDS:
        print(f"  id={t['id']}  {t['date']}  {t['setup']} {t['dir']}  "
              f"portal={t['portal_pts']:+.2f}  real={t['real_pts']:+.2f}  reason={t['reason']}")
