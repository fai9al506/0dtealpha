import os, sys
from sqlalchemy import create_engine, text
from collections import defaultdict
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Check what extra data we have per signal
sample = c.execute(text("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'setup_log' ORDER BY ordinal_position
""")).fetchall()
sys.stdout.write('setup_log columns:\n')
for r in sample:
    sys.stdout.write('  %s\n' % r[0])

# Check what volland data we have at signal time
sys.stdout.write('\nVolland payload keys (sample):\n')
vp = c.execute(text("""
    SELECT payload->'statistics' as stats
    FROM volland_snapshots 
    WHERE payload->'statistics' IS NOT NULL
    ORDER BY ts DESC LIMIT 1
""")).fetchone()
if vp and vp[0]:
    import json
    stats = vp[0] if isinstance(vp[0], dict) else json.loads(vp[0])
    for k in sorted(stats.keys()):
        sys.stdout.write('  %s = %s\n' % (k, str(stats[k])[:60]))

# Check: do we store spot_vol_beta, charm, LIS in setup_log?
sys.stdout.write('\nSetup_log extra fields (sample from Mar 9):\n')
extras = c.execute(text("""
    SELECT setup_name, direction, greek_alignment, 
           paradigm, charm_value, spot_vol_beta,
           vanna_all, vanna_weekly, vanna_monthly,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE ts::date = '2026-03-09'
    ORDER BY ts
    LIMIT 10
""")).fetchall()
for r in extras:
    sys.stdout.write('  %-20s %-7s align=%s paradigm=%-15s charm=%s svb=%s vanna=%s result=%s pnl=%s\n' % (
        r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[9], r[10]))

# Check: can we get LIS at signal time?
sys.stdout.write('\nLIS data availability:\n')
lis = c.execute(text("""
    SELECT ts, payload->'statistics'->>'lis' as lis,
           payload->'statistics'->>'spot' as spot
    FROM volland_snapshots
    WHERE ts::date = '2026-03-09' AND payload->'statistics'->>'lis' IS NOT NULL
    ORDER BY ts LIMIT 5
""")).fetchall()
for r in lis:
    sys.stdout.write('  %s lis=%s spot=%s\n' % (r[0], r[1], r[2]))

# Check: do we have overvixing data?
sys.stdout.write('\nSpot-Vol-Beta / overvixing data:\n')
svb = c.execute(text("""
    SELECT ts, payload->'statistics'->>'spotVolBeta' as svb
    FROM volland_snapshots
    WHERE ts::date = '2026-03-09' AND payload->'statistics'->>'spotVolBeta' IS NOT NULL
    ORDER BY ts LIMIT 5
""")).fetchall()
for r in svb:
    sys.stdout.write('  %s svb=%s\n' % (r[0], r[1]))

# Now the key analysis: at align +2, does adding SVB/charm improve?
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('ALIGN +2 TRADES: DOES SVB/CHARM ADD VALUE?\n')
sys.stdout.write('='*70 + '\n')

a2_trades = c.execute(text("""
    SELECT ts::date, setup_name, direction, greek_alignment,
           charm_value, spot_vol_beta, paradigm,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE outcome_result IS NOT NULL AND greek_alignment >= 2
    ORDER BY ts
""")).fetchall()

# Split by SVB sign
for label, filter_fn in [
    ('SVB > 0 (overvixing/bullish)', lambda t: t[5] is not None and float(t[5]) > 0),
    ('SVB < 0 (undervixing/bearish)', lambda t: t[5] is not None and float(t[5]) < 0),
    ('SVB unknown', lambda t: t[5] is None),
    ('Charm > 0 (bullish)', lambda t: t[4] is not None and float(t[4]) > 0),
    ('Charm < 0 (bearish)', lambda t: t[4] is not None and float(t[4]) < 0),
    ('Charm unknown', lambda t: t[4] is None),
]:
    subset = [t for t in a2_trades if filter_fn(t)]
    if not subset:
        sys.stdout.write('\n  %s: 0 trades\n' % label)
        continue
    w = sum(1 for t in subset if t[7] and 'WIN' in t[7])
    l = sum(1 for t in subset if t[7] and 'LOSS' in t[7])
    pnl = sum(float(t[8] or 0) for t in subset)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('\n  %s:\n' % label)
    sys.stdout.write('    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(subset), w, l, wr, pnl))

# What about align +2 with SVB matching direction?
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('ALIGN +2 + DIRECTIONAL SVB MATCH\n')
sys.stdout.write('='*70 + '\n')

for label, filter_fn in [
    ('Long + SVB>0 (bullish alignment + overvixing)', 
     lambda t: t[2] in ('long','bullish') and t[5] is not None and float(t[5]) > 0),
    ('Short + SVB<0 (bearish alignment + undervixing)',
     lambda t: t[2] in ('short','bearish') and t[5] is not None and float(t[5]) < 0),
    ('Direction matches SVB',
     lambda t: (t[2] in ('long','bullish') and t[5] is not None and float(t[5]) > 0) or
               (t[2] in ('short','bearish') and t[5] is not None and float(t[5]) < 0)),
    ('Direction opposes SVB',
     lambda t: (t[2] in ('long','bullish') and t[5] is not None and float(t[5]) < 0) or
               (t[2] in ('short','bearish') and t[5] is not None and float(t[5]) > 0)),
]:
    subset = [t for t in a2_trades if filter_fn(t)]
    if not subset:
        sys.stdout.write('\n  %s: 0 trades\n' % label)
        continue
    w = sum(1 for t in subset if t[7] and 'WIN' in t[7])
    l = sum(1 for t in subset if t[7] and 'LOSS' in t[7])
    pnl = sum(float(t[8] or 0) for t in subset)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('\n  %s:\n' % label)
    sys.stdout.write('    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(subset), w, l, wr, pnl))

sys.stdout.flush()
c.close()
