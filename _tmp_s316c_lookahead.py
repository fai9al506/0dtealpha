# -*- coding: utf-8 -*-
"""S316c - LOOKAHEAD CHECK on setup_log.gex_state before anything is built on it.

The column was backfilled by a 16:30 job. If it was stamped with the state as it was
AT THE SIGNAL, the ACCELERATION finding is usable live. If it was stamped with the
END-OF-DAY state, the finding is lookahead and worthless - we would be filtering on
information that did not exist when the trade was placed.

Test: for each Skew Charm signal, pull the gex_state TABLE row that was current at the
signal's own timestamp, and compare it with the stamped column."""
import os
import psycopg2, psycopg2.extras


def p(*a):
    print(*[str(x).encode('ascii', 'replace').decode('ascii') for x in a])


c = psycopg2.connect(os.environ['DATABASE_URL']); c.autocommit = True
cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

p("=" * 100)
p("(1) does the stamped column match the LIVE state at the signal's own time?")
p("=" * 100)
cur.execute("""
WITH sc AS (
  SELECT id, ts, gex_state AS stamped
  FROM setup_log
  WHERE setup_name='Skew Charm' AND gex_state IS NOT NULL
    AND (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
)
SELECT sc.id, sc.ts AT TIME ZONE 'America/New_York' et, sc.stamped,
       (SELECT g.state FROM gex_state g
         WHERE g.et <= sc.ts AT TIME ZONE 'America/New_York'
         ORDER BY g.et DESC LIMIT 1) AS live_at_signal,
       (SELECT g.state FROM gex_state g
         WHERE (g.et)::date = (sc.ts AT TIME ZONE 'America/New_York')::date
         ORDER BY g.et DESC LIMIT 1) AS eod_state
FROM sc ORDER BY sc.ts""")
rows = cur.fetchall()
tot = len(rows)
m_live = sum(1 for r in rows if r['stamped'] == r['live_at_signal'])
m_eod = sum(1 for r in rows if r['stamped'] == r['eod_state'])
p("  signals with a stamped gex_state: %d" % tot)
if tot:
    p("  matches the state LIVE AT SIGNAL TIME : %5d  (%.1f%%)" % (m_live, m_live / tot * 100))
    p("  matches the END-OF-DAY state          : %5d  (%.1f%%)" % (m_eod, m_eod / tot * 100))
    p("")
    if m_live / tot > 0.9:
        p("  -> STAMPED AT SIGNAL TIME. Usable as a live filter.")
    elif m_eod / tot > 0.9:
        p("  -> STAMPED AT END OF DAY. LOOKAHEAD - the finding is NOT usable.")
    else:
        p("  -> AMBIGUOUS. Treat as unusable until resolved.")

p("")
p("=" * 100)
p("(2) sample rows, so the comparison is visible and not just a percentage")
p("=" * 100)
p("  %-8s%-18s%-18s%-18s%-18s" % ('id', 'signal time', 'stamped', 'live at signal', 'end of day'))
for r in rows[:12]:
    p("  %-8s%-18s%-18s%-18s%-18s" % (r['id'], r['et'].strftime('%Y-%m-%d %H:%M'),
                                      r['stamped'], r['live_at_signal'], r['eod_state']))

p("")
p("=" * 100)
p("(3) how often does the state CHANGE during a day? (if it never changes, the test above")
p("    cannot tell the two apart and the answer is inconclusive)")
p("=" * 100)
cur.execute("""SELECT (et)::date d, COUNT(DISTINCT state) n_states, COUNT(*) n_rows
               FROM gex_state WHERE (et)::date >= '2026-03-01'
               GROUP BY 1 ORDER BY 1 DESC LIMIT 10""")
for r in cur.fetchall():
    p("  %-12s distinct states %d   rows %d" % (r['d'], r['n_states'], r['n_rows']))
cur.execute("""SELECT AVG(n)::numeric(5,2) FROM (
                 SELECT COUNT(DISTINCT state) n FROM gex_state
                 WHERE (et)::date >= '2026-03-01' GROUP BY (et)::date) x""")
p("  average distinct states per day: %s" % cur.fetchone()['avg'])
