import psycopg2, os, json
from decimal import Decimal

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

TODAY = '2026-03-02'

# Today's trades
cur.execute("""
SELECT id, setup_name, direction, grade, score, spot,
       outcome_result, outcome_pnl, outcome_first_event, outcome_max_profit, outcome_max_loss,
       ts, comments, paradigm, lis, target, abs_es_price, abs_details::text
FROM setup_log
WHERE ts::date = %s
ORDER BY id
""", (TODAY,))
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
print(f"=== MARCH 2 TRADES ({len(rows)}) ===\n")

by_setup = {}
for r in rows:
    d = dict(zip(cols, r))
    time_et = str(d['ts'])[11:16] if d['ts'] else '?'
    pnl = float(d['outcome_pnl'] or 0)
    maxp = float(d['outcome_max_profit'] or 0)
    maxl = float(d['outcome_max_loss'] or 0)
    result = d['outcome_result'] or 'OPEN'
    setup = d['setup_name']

    if setup not in by_setup:
        by_setup[setup] = {'trades': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'pnl': 0.0, 'list': []}
    by_setup[setup]['trades'] += 1
    by_setup[setup]['pnl'] += pnl
    if result == 'WIN': by_setup[setup]['wins'] += 1
    elif result == 'LOSS': by_setup[setup]['losses'] += 1
    elif result == 'EXPIRED': by_setup[setup]['expired'] += 1
    by_setup[setup]['list'].append(d)

    hour = int(time_et.split(':')[0]) if ':' in time_et else 0
    time_utc = str(d['ts'])[11:16]
    # Convert UTC to ET (subtract 5 hours for EST)
    from datetime import datetime, timedelta
    ts_utc = d['ts']
    if ts_utc:
        ts_et = ts_utc - timedelta(hours=5)
        time_et_str = ts_et.strftime('%H:%M')
    else:
        time_et_str = '?'

    print(f"#{d['id']:>4} | {time_et_str} ET | {setup:20s} | {d['direction']:5s} | {d['grade']:8s} | spot={d['spot']:<10} | {result:>7} | pnl={pnl:>+7.1f} | maxP={maxp:>+6.1f} | maxL={maxl:>+6.1f} | {d['paradigm'] or ''}")

# Day summary by setup
total_pnl = sum(float(r[7] or 0) for r in rows)
total_wins = sum(1 for r in rows if r[6] == 'WIN')
total_losses = sum(1 for r in rows if r[6] == 'LOSS')
total_expired = sum(1 for r in rows if r[6] == 'EXPIRED')

print(f"\n=== MARCH 2 SUMMARY BY SETUP ===\n")
for setup in sorted(by_setup.keys(), key=lambda x: by_setup[x]['pnl'], reverse=True):
    s = by_setup[setup]
    wl = s['wins'] + s['losses']
    wr = round(100 * s['wins'] / wl, 1) if wl > 0 else 0
    print(f"{setup:20s} | {s['trades']:2d} trades | {s['wins']}W/{s['losses']}L/{s['expired']}E | WR={wr:>5.1f}% | PnL={s['pnl']:>+7.1f}")

print(f"\n{'TOTAL':20s} | {len(rows):2d} trades | {total_wins}W/{total_losses}L/{total_expired}E | PnL={total_pnl:>+7.1f}")

# Filter analysis - check each trade against known filters
print(f"\n=== FILTER ANALYSIS ===\n")
print("Checking each trade against deployed + proposed filters...\n")

for r in rows:
    d = dict(zip(cols, r))
    tid = d['id']
    setup = d['setup_name']
    direction = d['direction']
    pnl = float(d['outcome_pnl'] or 0)
    result = d['outcome_result'] or 'OPEN'
    paradigm = d['paradigm'] or ''
    ts = d['ts']

    # Get time ET
    from datetime import timedelta
    ts_et = ts - timedelta(hours=5) if ts else None
    hour_et = ts_et.hour if ts_et else 0
    minute_et = ts_et.minute if ts_et else 0
    time_decimal = hour_et + minute_et / 60.0

    filters_hit = []

    # Get vanna for this trade
    cur.execute("""
        SELECT SUM(points) FROM volland_exposure_points
        WHERE greek='vanna' AND expiration_option='ALL'
              AND snapshot_id = (
                  SELECT id FROM volland_snapshots
                  WHERE captured_at <= %s AND (payload->>'exposure_points_saved')::int > 0
                  ORDER BY captured_at DESC LIMIT 1
              )
    """, (ts,))
    vanna_row = cur.fetchone()
    vanna_all = float(vanna_row[0]) if vanna_row and vanna_row[0] else None

    # Get DD and charm
    cur.execute("""
        SELECT payload->'statistics'->>'deltaDecayHedging',
               payload->'statistics'->>'aggregatedCharm'
        FROM volland_snapshots
        WHERE captured_at <= %s AND payload->'statistics' IS NOT NULL
              AND (payload->>'exposure_points_saved')::int > 0
        ORDER BY captured_at DESC LIMIT 1
    """, (ts,))
    vol_row = cur.fetchone()
    dd_str = vol_row[0] if vol_row else None
    charm_str = vol_row[1] if vol_row else None

    # DEPLOYED FILTER 1: Single-position mode (can't check retroactively)

    # DEPLOYED FILTER 2: GEX Long vanna filter (blocks when vanna ALL <= 0)
    if setup == 'GEX Long' and vanna_all is not None and vanna_all <= 0:
        filters_hit.append(f"F_VANNA_BLOCK (vanna={vanna_all/1e9:.2f}B)")

    # DEPLOYED FILTER 3: DD after 14:00 ET
    if setup == 'DD Exhaustion' and time_decimal >= 14.0:
        filters_hit.append(f"F_DD_TIME (after 14:00)")

    # DEPLOYED FILTER 4: DD BOFA-PURE paradigm
    if setup == 'DD Exhaustion' and 'BOFA-PURE' in paradigm:
        filters_hit.append(f"F_DD_BOFA (paradigm={paradigm})")

    # PROPOSED FILTER 5: DD shift threshold $500M (from Analysis #5)
    # PROPOSED FILTER 6: DD charm ceiling $200M (from Analysis #5)
    # PROPOSED FILTER 7: DD concentration > 75% (from Analysis session 3)

    # PROPOSED FILTER: Block GEX-MESSY for GEX Long (from Analysis #4)
    if setup == 'GEX Long' and 'MESSY' in paradigm:
        filters_hit.append(f"F_GEX_MESSY (paradigm={paradigm})")

    # PROPOSED FILTER: Block all after 14:00 for GEX Long
    if setup == 'GEX Long' and time_decimal >= 14.0:
        filters_hit.append(f"F_GEX_TIME (after 14:00)")

    if filters_hit:
        would = "BLOCKED"
        print(f"#{tid:>4} | {setup:20s} | {result:>7} | pnl={pnl:>+6.1f} | {would} by: {', '.join(filters_hit)}")

# Calculate filter impact
print(f"\n=== FILTER IMPACT ON TODAY ===\n")
blocked_pnl = 0
blocked_count = 0
blocked_wins = 0
blocked_losses = 0

for r in rows:
    d = dict(zip(cols, r))
    setup = d['setup_name']
    direction = d['direction']
    pnl = float(d['outcome_pnl'] or 0)
    result = d['outcome_result'] or 'OPEN'
    paradigm = d['paradigm'] or ''
    ts = d['ts']
    ts_et = ts - timedelta(hours=5) if ts else None
    time_decimal = (ts_et.hour + ts_et.minute / 60.0) if ts_et else 0

    cur.execute("""
        SELECT SUM(points) FROM volland_exposure_points
        WHERE greek='vanna' AND expiration_option='ALL'
              AND snapshot_id = (
                  SELECT id FROM volland_snapshots
                  WHERE captured_at <= %s AND (payload->>'exposure_points_saved')::int > 0
                  ORDER BY captured_at DESC LIMIT 1
              )
    """, (ts,))
    vanna_row = cur.fetchone()
    vanna_all = float(vanna_row[0]) if vanna_row and vanna_row[0] else None

    blocked = False
    # Deployed filters
    if setup == 'GEX Long' and vanna_all is not None and vanna_all <= 0:
        blocked = True
    if setup == 'DD Exhaustion' and time_decimal >= 14.0:
        blocked = True
    if setup == 'DD Exhaustion' and 'BOFA-PURE' in paradigm:
        blocked = True

    if blocked:
        blocked_pnl += pnl
        blocked_count += 1
        if result == 'WIN': blocked_wins += 1
        elif result == 'LOSS': blocked_losses += 1

filtered_pnl = total_pnl - blocked_pnl
print(f"Raw day:      {len(rows)} trades | {total_pnl:>+7.1f} pts")
print(f"Blocked:      {blocked_count} trades | {blocked_pnl:>+7.1f} pts ({blocked_wins}W/{blocked_losses}L blocked)")
print(f"After filter: {len(rows)-blocked_count} trades | {filtered_pnl:>+7.1f} pts")

# All-time updated summary
print(f"\n=== ALL-TIME SUMMARY (updated through March 2) ===\n")
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
SELECT COUNT(*) as trades,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       COUNT(*) FILTER (WHERE outcome_result='EXPIRED') as expired,
       ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1) as total_pnl,
       COUNT(DISTINCT ts::date) as days
FROM setup_log WHERE outcome_result IS NOT NULL
""")
r = cur.fetchone()
avg = round(float(r[4])/r[5], 1) if r[5] else 0
print(f"\nGRAND TOTAL: {r[0]} trades | {r[1]}W/{r[2]}L/{r[3]}E | PnL={r[4]} | {r[5]} trading days | avg={avg}/day")

# New setup: Skew Charm detail
print(f"\n=== SKEW CHARM DETAIL (new setup) ===\n")
cur.execute("""
SELECT id, ts, direction, grade, score, spot, outcome_result, outcome_pnl,
       outcome_max_profit, outcome_max_loss, paradigm, comments
FROM setup_log
WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
ORDER BY id
""")
sc_rows = cur.fetchall()
for r in sc_rows:
    ts_et = r[1] - timedelta(hours=5) if r[1] else None
    time_str = ts_et.strftime('%m/%d %H:%M') if ts_et else '?'
    print(f"#{r[0]:>4} | {time_str} | {r[2]:5s} | {r[3]:6s} | {r[6]:>7} | pnl={float(r[7] or 0):>+6.1f} | maxP={float(r[8] or 0):>+6.1f} | {r[10] or ''}")

sc_pnl = sum(float(r[7] or 0) for r in sc_rows)
sc_wins = sum(1 for r in sc_rows if r[6] == 'WIN')
sc_losses = sum(1 for r in sc_rows if r[6] == 'LOSS')
sc_wl = sc_wins + sc_losses
sc_wr = round(100 * sc_wins / sc_wl, 1) if sc_wl > 0 else 0
print(f"\nSkew Charm: {len(sc_rows)} trades | {sc_wins}W/{sc_losses}L | WR={sc_wr}% | PnL={sc_pnl:+.1f}")

conn.close()
