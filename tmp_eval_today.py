"""Check today's eval-eligible trades (matches eval trader real config)"""
import os, sys
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()
r = c.execute(text("""
    SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           setup_name, direction, grade, outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE ts::date = '2026-03-10' AND grade != 'LOG'
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'Paradigm Reversal', 'AG Short')
      AND ABS(COALESCE(greek_alignment, 0)) >= 3
    ORDER BY ts
""")).fetchall()
wins = losses = 0
total = 0
for row in r:
    res = row[5] or 'OPEN'
    pnl = float(row[6]) if row[6] is not None else 0
    if res == 'WIN': wins += 1
    elif res == 'LOSS': losses += 1
    total += pnl
    sys.stdout.write('%s  %-18s %-6s %-8s %-8s %+6.1f  align=%s\n' % (
        row[1], row[2], row[3], row[4], res, pnl, row[7]))
sys.stdout.write('\n')
wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
sys.stdout.write('Eval-eligible: %d trades | %dW/%dL | WR %.0f%% | Net %+.1f pts\n' % (
    len(r), wins, losses, wr, total))
sys.stdout.flush()
c.close()
