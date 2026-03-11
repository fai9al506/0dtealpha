"""Check PnL breakdown to find the drop"""
import os, sys
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

r2 = c.execute(text("""
    SELECT COALESCE(SUM(outcome_pnl),0), COUNT(*)
    FROM setup_log
    WHERE outcome_result IS NOT NULL AND grade != 'LOG'
      AND ts::date < '2026-03-09'
""")).fetchone()

r1 = c.execute(text("""
    SELECT COALESCE(SUM(outcome_pnl),0), COUNT(*)
    FROM setup_log
    WHERE outcome_result IS NOT NULL AND grade != 'LOG'
      AND ts::date < '2026-03-10'
""")).fetchone()

r0 = c.execute(text("""
    SELECT COALESCE(SUM(outcome_pnl),0), COUNT(*)
    FROM setup_log
    WHERE outcome_result IS NOT NULL AND grade != 'LOG'
""")).fetchone()

sys.stdout.write('Total thru Mar 8: %+.1f (%d trades)\n' % (float(r2[0]), r2[1]))
sys.stdout.write('Total thru Mar 9: %+.1f (%d trades)\n' % (float(r1[0]), r1[1]))
sys.stdout.write('Total incl today: %+.1f (%d trades)\n' % (float(r0[0]), r0[1]))
sys.stdout.write('\nMar 9 day PnL: %+.1f\n' % (float(r1[0]) - float(r2[0])))
sys.stdout.write('Mar 10 day PnL: %+.1f\n' % (float(r0[0]) - float(r1[0])))

# Check the portal query (limit 500)
sys.stdout.write('\n--- Portal view (last 500) ---\n')
rp = c.execute(text("""
    SELECT COALESCE(SUM(outcome_pnl),0), COUNT(*)
    FROM (
        SELECT outcome_pnl FROM setup_log
        WHERE outcome_result IS NOT NULL AND grade != 'LOG'
        ORDER BY ts DESC LIMIT 500
    ) sub
""")).fetchone()
sys.stdout.write('Portal total (last 500): %+.1f (%d trades)\n' % (float(rp[0]), rp[1]))

# Total trades ever
rt = c.execute(text("""
    SELECT COUNT(*) FROM setup_log WHERE grade != 'LOG'
""")).fetchone()
sys.stdout.write('Total signals (incl OPEN): %d\n' % rt[0])

sys.stdout.flush()
c.close()
