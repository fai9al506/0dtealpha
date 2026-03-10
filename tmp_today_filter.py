import psycopg2, os, json
from datetime import timedelta
from decimal import Decimal

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

TODAY = '2026-03-02'

# Today's trades
cur.execute("""
SELECT id, setup_name, direction, grade, score, spot,
       outcome_result, outcome_pnl, outcome_first_event, outcome_max_profit, outcome_max_loss,
       ts, comments, paradigm, lis, target
FROM setup_log
WHERE ts::date = %s
ORDER BY id
""", (TODAY,))
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

print("=== FILTER ANALYSIS — March 2 (46 trades) ===\n")
print(f"{'ID':>4} | {'Time':>5} | {'Setup':20s} | {'Dir':5s} | {'Res':>7} | {'PnL':>7} | Filters Hit")
print("-" * 120)

# Counters for different filter combos
deployed_blocked = []
proposed_blocked = []
all_trades = []

for r in rows:
    d = dict(zip(cols, r))
    tid = d['id']
    setup = d['setup_name']
    direction = d['direction']
    pnl = float(d['outcome_pnl'] or 0)
    result = d['outcome_result'] or 'OPEN'
    paradigm = d['paradigm'] or ''
    ts = d['ts']

    ts_et = ts - timedelta(hours=5) if ts else None
    time_str = ts_et.strftime('%H:%M') if ts_et else '?'
    hour_et = ts_et.hour if ts_et else 0
    minute_et = ts_et.minute if ts_et else 0
    time_decimal = hour_et + minute_et / 60.0

    # Get vanna ALL
    cur.execute("""
        SELECT SUM(value::numeric)::float
        FROM volland_exposure_points
        WHERE greek = 'vanna' AND expiration_option = 'ALL'
              AND ts_utc = (
                  SELECT MAX(ts_utc) FROM volland_exposure_points
                  WHERE greek = 'vanna' AND expiration_option = 'ALL' AND ts_utc <= %s
              )
    """, (ts,))
    vanna_row = cur.fetchone()
    vanna_all = vanna_row[0] if vanna_row and vanna_row[0] else None

    # Get DD hedging and charm
    cur.execute("""
        SELECT payload->'statistics'->>'deltaDecayHedging',
               payload->'statistics'->>'aggregatedCharm'
        FROM volland_snapshots
        WHERE ts <= %s AND payload->'statistics' IS NOT NULL
              AND CASE WHEN payload->>'exposure_points_saved' ~ '^[0-9]+$'
                       THEN (payload->>'exposure_points_saved')::int > 0 ELSE false END
        ORDER BY ts DESC LIMIT 1
    """, (ts,))
    vol_row = cur.fetchone()
    dd_str = vol_row[0] if vol_row else None
    charm_str = vol_row[1] if vol_row else None

    # Parse DD value
    def parse_money(s):
        if not s: return None
        s = s.replace('$','').replace(',','')
        neg = '-' in s
        s = s.replace('-','').replace('+','')
        try:
            val = float(s)
            return -val if neg else val
        except: return None

    dd_val = parse_money(dd_str)
    charm_val = parse_money(charm_str)

    filters = []

    # === DEPLOYED FILTERS (on SIM auto-trader) ===
    deployed = False

    # D1: GEX Long vanna filter
    if setup == 'GEX Long' and vanna_all is not None and vanna_all <= 0:
        filters.append(f"[D] VANNA_NEG ({vanna_all/1e9:.1f}B)")
        deployed = True

    # D2: DD Exhaustion after 14:00
    if setup == 'DD Exhaustion' and time_decimal >= 14.0:
        filters.append(f"[D] DD_AFTER_14")
        deployed = True

    # D3: DD Exhaustion BOFA-PURE
    if setup == 'DD Exhaustion' and 'BOFA-PURE' in paradigm:
        filters.append(f"[D] DD_BOFA_PURE")
        deployed = True

    # === PROPOSED FILTERS (not yet deployed) ===

    # P1: GEX Long MESSY paradigm
    if setup == 'GEX Long' and 'MESSY' in paradigm:
        filters.append(f"[P] GEX_MESSY")

    # P2: GEX Long after 14:00
    if setup == 'GEX Long' and time_decimal >= 14.0:
        filters.append(f"[P] GEX_AFTER_14")

    # P3: DD shift threshold $500M
    if setup == 'DD Exhaustion' and dd_val is not None and abs(dd_val) < 500e6:
        filters.append(f"[P] DD_SHIFT_LOW")

    # P4: DD charm ceiling $200M
    if setup == 'DD Exhaustion' and charm_val is not None and abs(charm_val) > 200e6:
        filters.append(f"[P] DD_CHARM_HIGH")

    all_trades.append({'id': tid, 'setup': setup, 'result': result, 'pnl': pnl,
                       'deployed_blocked': deployed, 'filters': filters, 'time': time_str,
                       'paradigm': paradigm, 'direction': direction})

    if filters:
        print(f"#{tid:>4} | {time_str} | {setup:20s} | {direction:5s} | {result:>7} | {pnl:>+7.1f} | {', '.join(filters)}")

# === Impact Summary ===
print(f"\n{'='*80}")
print("FILTER IMPACT SUMMARY")
print(f"{'='*80}\n")

raw_pnl = sum(t['pnl'] for t in all_trades)
raw_wins = sum(1 for t in all_trades if t['result'] == 'WIN')
raw_losses = sum(1 for t in all_trades if t['result'] == 'LOSS')

# Deployed only
dep_blocked = [t for t in all_trades if t['deployed_blocked']]
dep_remaining = [t for t in all_trades if not t['deployed_blocked']]
dep_pnl = sum(t['pnl'] for t in dep_remaining)
dep_blocked_pnl = sum(t['pnl'] for t in dep_blocked)
dep_wins_blocked = sum(1 for t in dep_blocked if t['result'] == 'WIN')
dep_losses_blocked = sum(1 for t in dep_blocked if t['result'] == 'LOSS')
dep_remaining_wins = sum(1 for t in dep_remaining if t['result'] == 'WIN')
dep_remaining_losses = sum(1 for t in dep_remaining if t['result'] == 'LOSS')
dep_wl = dep_remaining_wins + dep_remaining_losses
dep_wr = round(100*dep_remaining_wins/dep_wl, 1) if dep_wl > 0 else 0

# All filters (deployed + proposed)
all_blocked = [t for t in all_trades if len(t['filters']) > 0]
all_remaining = [t for t in all_trades if len(t['filters']) == 0]
all_pnl = sum(t['pnl'] for t in all_remaining)
all_blocked_pnl = sum(t['pnl'] for t in all_blocked)
all_wins_blocked = sum(1 for t in all_blocked if t['result'] == 'WIN')
all_losses_blocked = sum(1 for t in all_blocked if t['result'] == 'LOSS')
all_remaining_wins = sum(1 for t in all_remaining if t['result'] == 'WIN')
all_remaining_losses = sum(1 for t in all_remaining if t['result'] == 'LOSS')
all_wl = all_remaining_wins + all_remaining_losses
all_wr = round(100*all_remaining_wins/all_wl, 1) if all_wl > 0 else 0

raw_wl = raw_wins + raw_losses
raw_wr = round(100*raw_wins/raw_wl, 1) if raw_wl > 0 else 0

print(f"{'Scenario':<35} | {'Trades':>6} | {'W/L':>7} | {'WR':>6} | {'PnL':>8} | {'Blocked':>8}")
print("-" * 90)
print(f"{'RAW (no filters)':<35} | {len(all_trades):>6} | {raw_wins}W/{raw_losses}L | {raw_wr:>5.1f}% | {raw_pnl:>+8.1f} | {'—':>8}")
print(f"{'DEPLOYED filters only':<35} | {len(dep_remaining):>6} | {dep_remaining_wins}W/{dep_remaining_losses}L | {dep_wr:>5.1f}% | {dep_pnl:>+8.1f} | {len(dep_blocked):>3} ({dep_blocked_pnl:>+.1f})")
print(f"{'ALL filters (deployed+proposed)':<35} | {len(all_remaining):>6} | {all_remaining_wins}W/{all_remaining_losses}L | {all_wr:>5.1f}% | {all_pnl:>+8.1f} | {len(all_blocked):>3} ({all_blocked_pnl:>+.1f})")

# Blocked trade details
print(f"\n=== DEPLOYED FILTER BLOCKS ({len(dep_blocked)}) ===\n")
for t in dep_blocked:
    print(f"#{t['id']:>4} | {t['time']} | {t['setup']:20s} | {t['result']:>7} | {t['pnl']:>+6.1f} | {[f for f in t['filters'] if f.startswith('[D]')]}")

print(f"\nBlocked: {dep_wins_blocked}W / {dep_losses_blocked}L — net blocked PnL: {dep_blocked_pnl:>+.1f}")

# Proposed filter blocks (additional)
proposed_only = [t for t in all_blocked if not t['deployed_blocked']]
if proposed_only:
    print(f"\n=== PROPOSED FILTER ADDITIONAL BLOCKS ({len(proposed_only)}) ===\n")
    for t in proposed_only:
        proposed_filters = [f for f in t['filters'] if f.startswith('[P]')]
        print(f"#{t['id']:>4} | {t['time']} | {t['setup']:20s} | {t['result']:>7} | {t['pnl']:>+6.1f} | {proposed_filters}")
    po_pnl = sum(t['pnl'] for t in proposed_only)
    po_wins = sum(1 for t in proposed_only if t['result'] == 'WIN')
    po_losses = sum(1 for t in proposed_only if t['result'] == 'LOSS')
    print(f"\nAdditional blocked: {po_wins}W / {po_losses}L — net: {po_pnl:>+.1f}")

# Per-setup performance after deployed filters
print(f"\n=== PER-SETUP AFTER DEPLOYED FILTERS ===\n")
by_setup_filtered = {}
for t in dep_remaining:
    s = t['setup']
    if s not in by_setup_filtered:
        by_setup_filtered[s] = {'trades': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'pnl': 0.0}
    by_setup_filtered[s]['trades'] += 1
    by_setup_filtered[s]['pnl'] += t['pnl']
    if t['result'] == 'WIN': by_setup_filtered[s]['wins'] += 1
    elif t['result'] == 'LOSS': by_setup_filtered[s]['losses'] += 1
    elif t['result'] == 'EXPIRED': by_setup_filtered[s]['expired'] += 1

for setup in sorted(by_setup_filtered.keys(), key=lambda x: by_setup_filtered[x]['pnl'], reverse=True):
    s = by_setup_filtered[setup]
    wl = s['wins'] + s['losses']
    wr = round(100*s['wins']/wl, 1) if wl > 0 else 0
    print(f"{setup:20s} | {s['trades']:2d} trades | {s['wins']}W/{s['losses']}L/{s['expired']}E | WR={wr:>5.1f}% | PnL={s['pnl']:>+7.1f}")

print(f"\n{'FILTERED TOTAL':20s} | {len(dep_remaining):2d} trades | PnL={dep_pnl:>+7.1f}")

# All-time with updated totals
print(f"\n=== ALL-TIME UPDATED ===\n")
cur.execute("""
SELECT setup_name,
       COUNT(*) as trades,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       COUNT(*) FILTER (WHERE outcome_result='EXPIRED') as expired,
       ROUND(100.0 * COUNT(*) FILTER (WHERE outcome_result='WIN') / NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')), 0), 1) as wr,
       ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1) as total_pnl
FROM setup_log
WHERE outcome_result IS NOT NULL
GROUP BY setup_name
ORDER BY total_pnl DESC
""")
for r in cur.fetchall():
    print(f"{r[0]:20s} | {r[1]:3d} trades | {r[2]}W/{r[3]}L/{r[4]}E | WR={r[5]}% | PnL={r[6]}")

cur.execute("""
SELECT COUNT(*),
       COUNT(*) FILTER (WHERE outcome_result='WIN'),
       COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       COUNT(*) FILTER (WHERE outcome_result='EXPIRED'),
       ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1),
       COUNT(DISTINCT ts::date)
FROM setup_log WHERE outcome_result IS NOT NULL
""")
r = cur.fetchone()
avg = round(float(r[4])/r[5], 1) if r[5] else 0
wl = r[1] + r[2]
wr = round(100*r[1]/wl, 1) if wl > 0 else 0
print(f"\nGRAND TOTAL: {r[0]} trades | {r[1]}W/{r[2]}L/{r[3]}E | WR={wr}% | PnL={r[4]} | {r[5]} days | avg={avg}/day")

# Monthly projection
print(f"\n=== MONTHLY INCOME PROJECTION ===\n")
days = int(r[5])
total_p = float(r[4])
avg_raw = total_p / days

# Simulate deployed filters on ALL history
cur.execute("""
SELECT id, setup_name, direction, paradigm, ts, outcome_result, outcome_pnl
FROM setup_log WHERE outcome_result IS NOT NULL ORDER BY id
""")
all_hist = cur.fetchall()

filtered_total = 0
filtered_trades = 0
filtered_wins = 0
filtered_losses = 0

for h in all_hist:
    hid, h_setup, h_dir, h_para, h_ts, h_res, h_pnl = h
    h_pnl = float(h_pnl or 0)
    h_para = h_para or ''
    ts_et = h_ts - timedelta(hours=5) if h_ts else None
    h_hour = ts_et.hour if ts_et else 0

    blocked = False

    # D1: GEX Long vanna — can't check retroactively without slow query per trade
    # D2: DD after 14:00
    if h_setup == 'DD Exhaustion' and h_hour >= 14:
        blocked = True
    # D3: DD BOFA-PURE
    if h_setup == 'DD Exhaustion' and 'BOFA-PURE' in h_para:
        blocked = True

    if not blocked:
        filtered_total += h_pnl
        filtered_trades += 1
        if h_res == 'WIN': filtered_wins += 1
        elif h_res == 'LOSS': filtered_losses += 1

filt_avg = filtered_total / days if days > 0 else 0
filt_wl = filtered_wins + filtered_losses
filt_wr = round(100*filtered_wins/filt_wl, 1) if filt_wl > 0 else 0

print(f"All-time raw:       {avg_raw:>+6.1f} pts/day × 21 trading days = {avg_raw*21:>+7.1f} pts/month")
print(f"Deployed DD filter: {filt_avg:>+6.1f} pts/day × 21 trading days = {filt_avg*21:>+7.1f} pts/month")
print(f"  (DD time+paradigm filter: {filtered_trades} trades over {days} days, WR={filt_wr}%)")
print()

# Income scenarios
print(f"{'Scenario':<40} | {'Pts/day':>8} | {'Pts/mo':>8} | {'2 ES':>10} | {'4 ES':>10} | {'10 MES':>10}")
print("-" * 100)
for label, ppd in [
    ("Raw (all signals)", avg_raw),
    ("Deployed DD filters", filt_avg),
    ("Conservative (70% of filtered)", filt_avg * 0.7),
    ("Pessimistic (50% of filtered)", filt_avg * 0.5),
]:
    ppm = ppd * 21
    es2 = ppm * 50 * 2  # $50/pt × 2 ES
    es4 = ppm * 50 * 4
    mes10 = ppm * 5 * 10  # $5/pt × 10 MES
    print(f"{label:<40} | {ppd:>+8.1f} | {ppm:>+8.1f} | ${es2:>+9,.0f} | ${es4:>+9,.0f} | ${mes10:>+9,.0f}")

conn.close()
