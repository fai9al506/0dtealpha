"""Verify: what was the INITIAL stop for Skew Charm 09:48? Did drawdown breach it?"""
import os, sys, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# 1. Get full trade details from setup_log
sys.stdout.write('='*70 + '\n')
sys.stdout.write('SETUP_LOG: Skew Charm LONG 09:48 (ID 624)\n')
sys.stdout.write('='*70 + '\n')

trade = c.execute(text("""
    SELECT *
    FROM setup_log
    WHERE id = 624
""")).fetchone()
if trade:
    cols = c.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'setup_log' ORDER BY ordinal_position
    """)).fetchall()
    col_names = [r[0] for r in cols]
    for i, col in enumerate(col_names):
        val = trade[i]
        if val is not None and str(val) != '':
            sys.stdout.write('  %-30s = %s\n' % (col, val))

# 2. Check auto_trade_orders for the actual stop placed
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('AUTO_TRADE_ORDERS: What stop did the SIM place?\n')
sys.stdout.write('='*70 + '\n')

ato = c.execute(text("""
    SELECT setup_log_id, state, updated_at
    FROM auto_trade_orders
    WHERE setup_log_id = 624
""")).fetchone()
if ato:
    state = ato[1] if isinstance(ato[1], dict) else json.loads(ato[1])
    sys.stdout.write('  setup_log_id: %s\n' % ato[0])
    sys.stdout.write('  updated_at: %s\n' % ato[2])
    sys.stdout.write('  State:\n')
    for k, v in sorted(state.items()):
        sys.stdout.write('    %-25s = %s\n' % (k, v))
else:
    sys.stdout.write('  No auto_trade_orders record for ID 624\n')
    # Check if there are any records today
    all_ato = c.execute(text("""
        SELECT setup_log_id, state->'setup_name' as sn, state->'entry_price' as ep,
               state->'stop_price' as sp, state->'initial_stop' as isp, updated_at
        FROM auto_trade_orders
        ORDER BY updated_at DESC
        LIMIT 10
    """)).fetchall()
    sys.stdout.write('\n  Recent auto_trade_orders:\n')
    for r in all_ato:
        sys.stdout.write('    id=%s setup=%s entry=%s stop=%s init_stop=%s updated=%s\n' % (
            r[0], r[1], r[2], r[3], r[4], r[5]))

# 3. What are Skew Charm trail params?
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('SKEW CHARM STOP/TRAIL CONFIG\n')
sys.stdout.write('='*70 + '\n')
sys.stdout.write('  From MEMORY: initial_sl=20 (SIM), 12 (eval via max_stop_loss_pts)\n')
sys.stdout.write('  Trail: BE@10, activation=10, gap=8\n\n')

# Calculate
spot = 6779.53
sys.stdout.write('  SPX entry: %.2f\n' % spot)
sys.stdout.write('  Initial SL (20 pts): %.2f\n' % (spot - 20))
sys.stdout.write('  Max loss recorded: -19.79 pts -> price reached: %.2f\n' % (spot - 19.79))
sys.stdout.write('  Distance to stop: %.2f pts (BARELY survived)\n' % (20 - 19.79))

sys.stdout.write('\n  MES entry: 6785\n')
sys.stdout.write('  MES initial SL (20 pts): %.2f\n' % (6785 - 20))
sys.stdout.write('  MES would need to reach: 6765 to stop out\n')

# 4. Check open_trades tracking for this specific trade
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('OUTCOME TRACKING DETAILS\n')
sys.stdout.write('='*70 + '\n')

# All Skew Charm trades today with full outcome details
trades = c.execute(text("""
    SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t_et,
           direction, spot,
           outcome_result, outcome_pnl,
           outcome_stop_level, outcome_target_level,
           outcome_max_profit, outcome_max_loss,
           outcome_first_event, outcome_elapsed_min
    FROM setup_log
    WHERE ts::date = '2026-03-10' AND setup_name = 'Skew Charm'
    ORDER BY ts
""")).fetchall()

for t in trades:
    sys.stdout.write('\n  ID=%s  %s ET  %s  spot=%.2f\n' % (t[0], t[1], t[2], t[3]))
    sys.stdout.write('    Result: %s  PnL: %s  Elapsed: %s min\n' % (t[4], t[5], t[11]))
    sys.stdout.write('    Stop: %s  Target: %s\n' % (t[6], t[7]))
    sys.stdout.write('    Max profit: %s  Max loss: %s\n' % (t[8], t[9]))
    sys.stdout.write('    First event: %s\n' % t[10])
    if t[3] and t[6]:
        stop_dist = abs(float(t[3]) - float(t[6]))
        sys.stdout.write('    Initial stop distance: %.1f pts\n' % stop_dist)

sys.stdout.flush()
c.close()
