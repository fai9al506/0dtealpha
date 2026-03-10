import psycopg2, os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("""
SELECT id, ts - interval '5 hours' as ts_et,
       direction, spot, outcome_result, outcome_pnl,
       outcome_max_profit, outcome_max_loss, outcome_first_event
FROM setup_log
WHERE setup_name = 'Paradigm Reversal' AND outcome_result IS NOT NULL
ORDER BY ts
""")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()
total = 0
wins = 0
win_max_profits = []
for r in rows:
    d = dict(zip(cols, r))
    pnl = float(d['outcome_pnl'] or 0)
    mp = float(d['outcome_max_profit'] or 0)
    ml = float(d['outcome_max_loss'] or 0)
    total += pnl
    left = mp - pnl if pnl > 0 else 0
    if d['outcome_result'] == 'WIN':
        wins += 1
        win_max_profits.append(mp)
    print("ID=%-4s %s %6s | %4s %+6.1f | maxP=%+6.1f maxL=%+6.1f | left=%.1f" % (
        d['id'], str(d['ts_et'])[:16], d['direction'],
        d['outcome_result'], pnl, mp, ml, left))
print("\nTotal: %+.1f pts, %d trades, %d wins" % (total, len(rows), wins))
if win_max_profits:
    print("Win avg max_profit: %.1f (min=%.1f, max=%.1f)" % (
        sum(win_max_profits)/len(win_max_profits), min(win_max_profits), max(win_max_profits)))
conn.close()
