# -*- coding: utf-8 -*-
"""Why was 2026-08-19 a big loss, and would V22 have changed it?

V22 has two halves and BOTH are driven by the PREVIOUS session:
  short block   fires when prev open-to-close < -0.8%
  long size-up  fires when prev open-to-close < -0.5%
So the first thing to check is what 2026-08-18 actually did."""
import os
import psycopg2, psycopg2.extras


def p(*a):
    print(*[str(x).encode('ascii', 'replace').decode('ascii') for x in a])


c = psycopg2.connect(os.environ['DATABASE_URL']); c.autocommit = True
cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

p("=" * 96)
p("(1) WHAT THE PREVIOUS SESSION DID - this is what V22 reads")
p("=" * 96)
cur.execute("""
WITH d AS (
  SELECT (ts AT TIME ZONE 'America/New_York')::date dd,
         (array_agg(bar_open  ORDER BY ts ASC ))[1] o,
         (array_agg(bar_close ORDER BY ts DESC))[1] c
  FROM spx_ohlc_1m GROUP BY 1)
SELECT dd, o, c, (c-o)/o*100 oc, LAG(c) OVER (ORDER BY dd) pc,
       (c-LAG(c) OVER (ORDER BY dd))/LAG(c) OVER (ORDER BY dd)*100 cc
FROM d WHERE dd >= DATE '2026-08-13' ORDER BY dd""")
p("  %-12s%10s%10s%10s%12s" % ('session', 'open', 'close', 'open->close', 'close->close'))
for r in cur.fetchall():
    p("  %-12s%10.2f%10.2f%+10.2f%%%+12s" % (
        r['dd'], r['o'], r['c'], r['oc'],
        ('%+.2f%%' % r['cc']) if r['cc'] is not None else 'n/a'))

p("")
p("  V22 short block needs prev open->close < -0.80%")
p("  V22 long size-up needs prev open->close < -0.50%")

p("")
p("=" * 96)
p("(2) THE TRADES ON 2026-08-19")
p("=" * 96)
cur.execute("""
SELECT s.id, s.ts AT TIME ZONE 'America/New_York' et, s.setup_name, s.direction,
       s.grade, s.vix, s.basket_pct, s.outcome_pnl,
       o.state->>'quantity' q, o.state->>'fill_price' fill,
       o.state->>'close_fill_price' xit, o.state->>'close_reason' why
FROM setup_log s LEFT JOIN real_trade_orders o ON o.setup_log_id = s.id
WHERE (s.ts AT TIME ZONE 'America/New_York')::date = DATE '2026-08-19'
  AND o.setup_log_id IS NOT NULL
ORDER BY s.id""")
tot = 0.0
p("  %-6s%-7s%-13s%-6s%6s%8s%9s%9s%9s" % ('id', 'time', 'setup', 'dir', 'qty', 'basket', 'in', 'out', '$'))
for r in cur.fetchall():
    q = int(r['q'] or 1); f = float(r['fill']); x = float(r['xit'])
    pl = ((f - x) if r['direction'] == 'short' else (x - f)) * 5 * q
    tot += pl
    p("  %-6s%-7s%-13s%-6s%6d%+8.2f%9.2f%9.2f%+9.0f" % (
        r['id'], r['et'].strftime('%H:%M'), r['setup_name'][:12], r['direction'],
        q, float(r['basket_pct'] or 0), f, x, pl))
p("  %-48s TOTAL %+9.0f (before fees)" % ('', tot))

p("")
p("=" * 96)
p("(3) WAS THE SIZE THE PROBLEM?  what the same 3 trades cost at 1 MES")
p("=" * 96)
p("  at 2 MES (what happened) : %+.0f" % tot)
p("  at 1 MES (no basket 2x)  : %+.0f" % (tot / 2))
p("  the basket doubled them because tech was DOWN, which CONFIRMS a short")

p("")
p("=" * 96)
p("(4) THE WEEK - broker truth per day")
p("=" * 96)
cur.execute("""SELECT day, net, n_trades, n_wins FROM tsrt_daily_stmt
               WHERE day >= DATE '2026-08-10' ORDER BY day""")
wk = cur.fetchall()
run = 0.0
for r in wk:
    run += float(r['net'])
    p("  %-12s net %+9.2f   trades %2s  wins %2s   running %+9.2f" % (
        r['day'], float(r['net']), r['n_trades'], r['n_wins'], run))
if wk:
    p("  ---")
    p("  sum of the listed days: %+.2f" % run)

p("")
p("=" * 96)
p("(5) WOULD V22 HAVE CHANGED 2026-08-19?")
p("=" * 96)
cur.execute("""
WITH d AS (
  SELECT (ts AT TIME ZONE 'America/New_York')::date dd,
         (array_agg(bar_open  ORDER BY ts ASC ))[1] o,
         (array_agg(bar_close ORDER BY ts DESC))[1] c
  FROM spx_ohlc_1m GROUP BY 1)
SELECT (c-o)/o*100 oc FROM d WHERE dd = DATE '2026-08-18'""")
oc = float(cur.fetchone()['oc'])
p("  2026-08-18 open->close = %+.2f%%" % oc)
p("  short block needs < -0.80%%  ->  %s" % ("FIRES" if oc < -0.8 else "does NOT fire"))
p("  long size-up needs < -0.50%%  ->  %s" % ("FIRES" if oc < -0.5 else "does NOT fire"))
p("")
p("  CONCLUSION: V22 changes NOTHING on 2026-08-19." if oc >= -0.5 else "  V22 would have acted.")
