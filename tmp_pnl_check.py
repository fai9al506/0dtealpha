"""Check PnL by date from setup_log"""
import os, sys
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()
r = c.execute(text("""
    SELECT ts::date as d,
           COUNT(*) as trades,
           COALESCE(SUM(outcome_pnl),0) as total_pnl,
           SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
           SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses
    FROM setup_log
    WHERE outcome_result IS NOT NULL AND grade != 'LOG'
    GROUP BY ts::date
    ORDER BY d DESC
    LIMIT 20
""")).fetchall()
sys.stdout.write('Date          Trades  Wins  Losses  PnL\n')
sys.stdout.write('-'*55 + '\n')
total = 0
for row in r:
    pnl = float(row[2]) if row[2] else 0
    total += pnl
    sys.stdout.write('%s   %5d  %4d  %6d  %+8.1f\n' % (row[0], row[1], row[3], row[4], pnl))
sys.stdout.write('-'*55 + '\n')
sys.stdout.write('Grand total: %+.1f\n' % total)

# Also check today's trades individually
sys.stdout.write('\n\nToday trades:\n')
sys.stdout.write('-'*80 + '\n')
t = c.execute(text("""
    SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           setup_name, direction, grade, spot,
           outcome_result, outcome_pnl, outcome_stop_level, outcome_target_level,
           outcome_max_profit, outcome_max_loss
    FROM setup_log
    WHERE ts::date = CURRENT_DATE AND grade != 'LOG'
    ORDER BY ts
""")).fetchall()
for row in t:
    sys.stdout.write('  #%s %s %-18s %-5s [%s] spot=%.1f  %s %s  SL=%s TGT=%s  MP=%s ML=%s\n' % (
        row[0], row[1], row[2], row[3], row[4], row[5],
        row[6] or 'OPEN', '%+.1f' % row[7] if row[7] is not None else '?',
        row[8], row[9], row[10], row[11]))

sys.stdout.flush()
c.close()
