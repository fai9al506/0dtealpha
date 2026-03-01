import psycopg2, os, sys

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Simulate: what if GEX Long used hybrid trail like AG Short?
sys.stdout.write('=== GEX Long: what-if hybrid (BE@10, trail@15 gap=5) vs current rung trail ===\n')
cur.execute("""
SELECT id, ts AT TIME ZONE 'America/New_York',
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
FROM setup_log
WHERE setup_name = 'GEX Long'
  AND outcome_result IS NOT NULL
ORDER BY ts
""")
total_current = 0
total_hybrid = 0
for r in cur.fetchall():
    tid, ts, result, pnl, maxp, maxl = r
    current_pnl = float(pnl) if pnl else 0
    total_current += current_pnl

    maxp_f = float(maxp) if maxp else 0
    maxl_f = float(maxl) if maxl else 0

    if maxp_f >= 15:
        sim_pnl = maxp_f - 5
    elif maxp_f >= 10:
        if current_pnl < 0:
            sim_pnl = 0  # BE saved it
        else:
            sim_pnl = current_pnl
    else:
        sim_pnl = current_pnl  # never hit BE, same outcome

    total_hybrid += sim_pnl
    diff = sim_pnl - current_pnl
    marker = ' <<<' if abs(diff) > 1 else ''
    sys.stdout.write(f'  #{tid} {result:>7} pnl={current_pnl:+.1f} -> hybrid={sim_pnl:+.1f} (diff={diff:+.1f}) maxP={maxp_f:.1f} maxL={maxl_f:.1f}{marker}\n')

sys.stdout.write(f'\n  CURRENT TOTAL: {total_current:+.1f}\n')
sys.stdout.write(f'  HYBRID TOTAL:  {total_hybrid:+.1f}\n')
sys.stdout.write(f'  IMPROVEMENT:   {total_hybrid - total_current:+.1f}\n')

# Also simulate: what if just continuous trail (activation=12, gap=5) like DD?
sys.stdout.write('\n=== What-if continuous trail (activation=12, gap=5) ===\n')
cur.execute("""
SELECT id, ts AT TIME ZONE 'America/New_York',
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
FROM setup_log
WHERE setup_name = 'GEX Long'
  AND outcome_result IS NOT NULL
ORDER BY ts
""")
total_cont = 0
for r in cur.fetchall():
    tid, ts, result, pnl, maxp, maxl = r
    current_pnl = float(pnl) if pnl else 0
    maxp_f = float(maxp) if maxp else 0

    if maxp_f >= 12:
        sim_pnl = maxp_f - 5
    else:
        sim_pnl = current_pnl

    total_cont += sim_pnl
    diff = sim_pnl - current_pnl
    marker = ' <<<' if abs(diff) > 1 else ''
    sys.stdout.write(f'  #{tid} {result:>7} pnl={current_pnl:+.1f} -> cont={sim_pnl:+.1f} (diff={diff:+.1f}) maxP={maxp_f:.1f}{marker}\n')

sys.stdout.write(f'\n  CONTINUOUS TOTAL: {total_cont:+.1f}\n')
sys.stdout.write(f'  vs CURRENT:      {total_current:+.1f}\n')
sys.stdout.write(f'  IMPROVEMENT:     {total_cont - total_current:+.1f}\n')

# Also: what if same hybrid as AG (BE@10, trail@15, gap=5)?
# And what about hybrid with BE@8 (matching current stop)?
sys.stdout.write('\n=== What-if: BE@8 + continuous trail activation=12 gap=5 ===\n')
cur.execute("""
SELECT id, outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
FROM setup_log WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL ORDER BY ts
""")
total_be8 = 0
for r in cur.fetchall():
    tid, result, pnl, maxp, maxl = r
    current_pnl = float(pnl) if pnl else 0
    maxp_f = float(maxp) if maxp else 0
    maxl_f = float(maxl) if maxl else 0

    if maxp_f >= 12:
        sim_pnl = maxp_f - 5
    elif maxp_f >= 8:
        sim_pnl = 0 if current_pnl < 0 else current_pnl
    else:
        sim_pnl = current_pnl

    total_be8 += sim_pnl
    diff = sim_pnl - current_pnl
    marker = ' <<<' if abs(diff) > 1 else ''
    sys.stdout.write(f'  #{tid} {result:>7} pnl={current_pnl:+.1f} -> be8trail={sim_pnl:+.1f} (diff={diff:+.1f}) maxP={maxp_f:.1f}{marker}\n')

sys.stdout.write(f'\n  BE@8+TRAIL TOTAL: {total_be8:+.1f}\n')
sys.stdout.write(f'  vs CURRENT:       {total_current:+.1f}\n')
sys.stdout.write(f'  IMPROVEMENT:      {total_be8 - total_current:+.1f}\n')

sys.stdout.flush()
conn.close()
