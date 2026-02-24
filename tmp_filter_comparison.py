"""Before vs After filter comparison -- run via: railway run --service 0dtealpha -- python tmp_filter_comparison.py"""
from sqlalchemy import create_engine, text
import os, sys

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("""
SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as t,
       s.setup_name, s.direction, s.grade, s.score, s.spot, s.paradigm,
       s.outcome_result, s.outcome_pnl, s.outcome_max_profit, s.outcome_max_loss,
       (SELECT payload->'statistics'->'spot_vol_beta'->>'correlation'
        FROM volland_snapshots v WHERE v.ts <= s.ts AND payload->'statistics' IS NOT NULL
        ORDER BY v.ts DESC LIMIT 1) as svb,
       EXTRACT(HOUR FROM s.ts AT TIME ZONE 'America/New_York') as hour,
       EXTRACT(MINUTE FROM s.ts AT TIME ZONE 'America/New_York') as minute,
       (s.ts AT TIME ZONE 'America/New_York')::date as trade_date
FROM setup_log s
WHERE s.outcome_result IS NOT NULL
ORDER BY s.id
""")).fetchall()

def cat(p):
    if 'MESSY' in p or 'EXTREME' in p or 'SIDIAL' in p: return 'MESSY/SIDIAL'
    if 'BOFA' in p or 'BofA' in p: return 'BOFA'
    if 'AG' in p: return 'AG'
    if 'GEX' in p: return 'GEX'
    return 'OTHER'

trades = []
for r in rows:
    svb_val = float(r[12]) if r[12] else None
    trades.append({
        'id': r[0], 'time': r[1], 'setup': r[2], 'dir': r[3], 'grade': r[4],
        'score': r[5], 'spot': r[6], 'paradigm': r[7] or '-', 'result': r[8],
        'pnl': r[9], 'maxP': r[10], 'maxL': r[11], 'svb': svb_val,
        'hour': int(r[13]), 'minute': int(r[14]), 'date': r[15]
    })

# ============================================================
# DEFINE FILTERS
# ============================================================
def passes_filter(t):
    """Return True if trade passes proposed filters."""
    s = t['setup']

    # FILTER 1: Disable GEX Long entirely (18% WR, -41.4 pts)
    if s == 'GEX Long':
        return False

    # FILTER 2: DD Exhaustion — <14:00 + no BOFA + SVB<1.0
    if s == 'DD Exhaustion':
        if t['hour'] >= 14:
            return False
        if cat(t['paradigm']) == 'BOFA':
            return False
        if t['svb'] is not None and t['svb'] >= 1.0:
            return False

    # AG Short, BofA Scalp, Paradigm Reversal, ES Absorption — no filter
    return True

def stats(bucket, label=""):
    if not bucket:
        return {'trades': 0, 'w': 0, 'l': 0, 'e': 0, 'wr': 0, 'pnl': 0,
                'avg_w': 0, 'avg_l': 0, 'pf': 0, 'max_dd': 0}
    w = sum(1 for t in bucket if t['result']=='WIN')
    l = sum(1 for t in bucket if t['result']=='LOSS')
    exp = sum(1 for t in bucket if t['result']=='EXPIRED')
    pnl = sum(t['pnl'] for t in bucket)
    gross_w = sum(t['pnl'] for t in bucket if t['result']=='WIN')
    gross_l = abs(sum(t['pnl'] for t in bucket if t['result']=='LOSS'))
    avg_w = gross_w/w if w else 0
    avg_l = -gross_l/l if l else 0
    wr = w/(w+l)*100 if (w+l) else 0
    pf = gross_w/gross_l if gross_l else float('inf')

    # Running drawdown
    running = 0
    peak = 0
    max_dd = 0
    for t in sorted(bucket, key=lambda x: x['time']):
        running += t['pnl']
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    return {'trades': len(bucket), 'w': w, 'l': l, 'e': exp, 'wr': wr,
            'pnl': pnl, 'avg_w': avg_w, 'avg_l': avg_l, 'pf': pf, 'max_dd': max_dd}

# ============================================================
# BEFORE vs AFTER — PER SETUP
# ============================================================
print("\\n" + "="*90)
print("BEFORE vs AFTER FILTER COMPARISON")
print("="*90)

setups = ['AG Short', 'BofA Scalp', 'DD Exhaustion', 'ES Absorption', 'GEX Long', 'Paradigm Reversal']

print("\\n%-20s | %-35s | %-35s" % ("", "BEFORE (no filter)", "AFTER (with filters)"))
print("-"*92)

total_before = []
total_after = []
total_blocked = []

for s in setups:
    before = [t for t in trades if t['setup'] == s]
    after = [t for t in before if passes_filter(t)]
    blocked = [t for t in before if not passes_filter(t)]

    total_before.extend(before)
    total_after.extend(after)
    total_blocked.extend(blocked)

    sb = stats(before)
    sa = stats(after)
    sbl = stats(blocked)

    # Setup name row
    b_str = "%2d trades  %2dW/%2dL/%dE  WR=%4.0f%%  %+7.1f" % (
        sb['trades'], sb['w'], sb['l'], sb['e'], sb['wr'], sb['pnl'])

    if len(after) == len(before):
        a_str = "  (unchanged)"
    elif len(after) == 0:
        a_str = "  DISABLED (%d trades blocked, %+.1f blocked)" % (len(blocked), sbl['pnl'])
    else:
        a_str = "%2d trades  %2dW/%2dL/%dE  WR=%4.0f%%  %+7.1f" % (
            sa['trades'], sa['w'], sa['l'], sa['e'], sa['wr'], sa['pnl'])

    print("%-20s | %-35s | %-35s" % (s, b_str, a_str))

# Totals
print("-"*92)
sb = stats(total_before)
sa = stats(total_after)
sbl = stats(total_blocked)

b_str = "%2d trades  %2dW/%2dL/%dE  WR=%4.0f%%  %+7.1f" % (
    sb['trades'], sb['w'], sb['l'], sb['e'], sb['wr'], sb['pnl'])
a_str = "%2d trades  %2dW/%2dL/%dE  WR=%4.0f%%  %+7.1f" % (
    sa['trades'], sa['w'], sa['l'], sa['e'], sa['wr'], sa['pnl'])
print("%-20s | %-35s | %-35s" % ("TOTAL", b_str, a_str))

# ============================================================
# SUMMARY METRICS
# ============================================================
print("\\n" + "="*90)
print("SUMMARY METRICS")
print("="*90)

print("\\n%-30s  %12s  %12s  %12s" % ("Metric", "BEFORE", "AFTER", "CHANGE"))
print("-"*70)

metrics = [
    ("Total trades", sb['trades'], sa['trades']),
    ("Wins", sb['w'], sa['w']),
    ("Losses", sb['l'], sa['l']),
    ("Expired", sb['e'], sa['e']),
    ("Win Rate (W/(W+L))", sb['wr'], sa['wr']),
    ("Total PnL (pts)", sb['pnl'], sa['pnl']),
    ("Avg Win (pts)", sb['avg_w'], sa['avg_w']),
    ("Avg Loss (pts)", sb['avg_l'], sa['avg_l']),
    ("Profit Factor", sb['pf'], sa['pf']),
    ("Max Drawdown (pts)", sb['max_dd'], sa['max_dd']),
]

for label, bv, av in metrics:
    if isinstance(bv, float):
        change = av - bv
        print("%-30s  %12.1f  %12.1f  %+12.1f" % (label, bv, av, change))
    else:
        change = av - bv
        print("%-30s  %12d  %12d  %+12d" % (label, bv, av, change))

# ============================================================
# BLOCKED TRADES ANALYSIS
# ============================================================
print("\\n" + "="*90)
print("BLOCKED TRADES BREAKDOWN (what we remove)")
print("="*90)

# By reason
gex_blocked = [t for t in total_blocked if t['setup'] == 'GEX Long']
dd_time = [t for t in total_blocked if t['setup'] == 'DD Exhaustion' and t['hour'] >= 14]
dd_bofa = [t for t in total_blocked if t['setup'] == 'DD Exhaustion' and t['hour'] < 14 and cat(t['paradigm']) == 'BOFA']
dd_svb = [t for t in total_blocked if t['setup'] == 'DD Exhaustion' and t['hour'] < 14 and cat(t['paradigm']) != 'BOFA' and t['svb'] is not None and t['svb'] >= 1.0]

print("\\n  Filter                           Blocked  Their PnL   Saved")
print("  " + "-"*65)

for label, bucket in [
    ("GEX Long disabled", gex_blocked),
    ("DD after 14:00", dd_time),
    ("DD in BOFA paradigm", dd_bofa),
    ("DD with SVB >= 1.0", dd_svb),
]:
    w = sum(1 for t in bucket if t['result']=='WIN')
    l = sum(1 for t in bucket if t['result']=='LOSS')
    pnl = sum(t['pnl'] for t in bucket)
    print("  %-35s  %2d       %+7.1f     %+7.1f" % (label, len(bucket), pnl, -pnl))

total_blocked_pnl = sum(t['pnl'] for t in total_blocked)
print("  " + "-"*65)
print("  %-35s  %2d       %+7.1f     %+7.1f" % (
    "TOTAL BLOCKED", len(total_blocked), total_blocked_pnl, -total_blocked_pnl))

# ============================================================
# TRADES PER DAY COMPARISON
# ============================================================
print("\\n" + "="*90)
print("TRADES PER DAY — BEFORE vs AFTER")
print("="*90)

all_dates = sorted(set(t['date'] for t in trades))
print("\\n  %-12s | %-30s | %-30s" % ("Date", "BEFORE", "AFTER"))
print("  " + "-"*75)

for d in all_dates:
    before_day = [t for t in total_before if t['date'] == d]
    after_day = [t for t in total_after if t['date'] == d]

    bw = sum(1 for t in before_day if t['result']=='WIN')
    bl = sum(1 for t in before_day if t['result']=='LOSS')
    bpnl = sum(t['pnl'] for t in before_day)

    aw = sum(1 for t in after_day if t['result']=='WIN')
    al = sum(1 for t in after_day if t['result']=='LOSS')
    apnl = sum(t['pnl'] for t in after_day)

    b_str = "%2d trades  %dW/%dL  %+7.1f" % (len(before_day), bw, bl, bpnl)
    a_str = "%2d trades  %dW/%dL  %+7.1f" % (len(after_day), aw, al, apnl)
    print("  %-12s | %-30s | %-30s" % (d, b_str, a_str))

# ============================================================
# EXECUTION GAP ANALYSIS
# ============================================================
print("\\n" + "="*90)
print("PORTAL vs EXECUTION GAP ANALYSIS")
print("="*90)

print("""
The gap between portal PnL (theoretical) and execution PnL arises from:

  1. SIGNAL LAG: API poll every 2s + order placement = 3-5s delay
     - On 113 trades, avg ~2 pts slippage = ~226 pts lost
     - On 72 trades (after filter), same rate = ~144 pts lost
     - SAVINGS: ~82 pts less slippage from fewer trades

  2. WHIPSAW / RAPID-FIRE: DD flips direction every 5-15 min
     - Execution enters, gets stopped, enters opposite, gets stopped again
     - Each whipsaw costs: stop loss + slippage + commission on BOTH legs
     - Portal counts each as separate trade; execution eats double slippage
""")

# Count rapid-fire signals before/after
dd_before = sorted([t for t in total_before if t['setup']=='DD Exhaustion'], key=lambda x: x['time'])
dd_after = sorted([t for t in total_after if t['setup']=='DD Exhaustion'], key=lambda x: x['time'])

def count_rapid(tlist, gap_min=15):
    cnt = 0
    flips = 0
    for i in range(1, len(tlist)):
        gap = (tlist[i]['time'] - tlist[i-1]['time']).total_seconds() / 60
        if gap < gap_min:
            cnt += 1
            if tlist[i]['dir'] != tlist[i-1]['dir']:
                flips += 1
    return cnt, flips

rapid_b, flips_b = count_rapid(dd_before)
rapid_a, flips_a = count_rapid(dd_after)

print("  DD rapid-fire signals (<15 min apart):")
print("    BEFORE: %d rapid signals, %d direction flips" % (rapid_b, flips_b))
print("    AFTER:  %d rapid signals, %d direction flips" % (rapid_a, flips_a))
print("    Each flip = ~2x stop + 2x slippage = ~28-30 pts execution cost")
print("    Saved flips: %d x ~28 pts = ~%d pts less whipsaw damage" % (
    flips_b - flips_a, (flips_b - flips_a) * 28))

print("""
  3. COMMISSIONS: $10 RT per 10 MES contracts
     - BEFORE: 113 trades x $10 = $1,130
     - AFTER:  72 trades x $10 = $720
     - SAVINGS: $410 (~8 MES pts)

  4. MISSED SIGNALS: not all portal signals get executed
     - Some fire during existing position (skip)
     - Some arrive stale (>120s old, skip)
     - Fewer total signals = higher % actually executed
""")

# ============================================================
# ESTIMATED EXECUTION PnL
# ============================================================
print("="*90)
print("ESTIMATED REAL-WORLD IMPACT")
print("="*90)

# Assumptions
slippage_per_trade = 1.5  # pts average entry slippage
commission_per_trade_pts = 0.2  # $10 / $50 per pt / 10 contracts = 0.2 pts

b_portal = sb['pnl']
b_execution_est = b_portal - (sb['trades'] * slippage_per_trade) - (sb['trades'] * commission_per_trade_pts)

a_portal = sa['pnl']
a_execution_est = a_portal - (sa['trades'] * slippage_per_trade) - (sa['trades'] * commission_per_trade_pts)

print("\\n  %-35s  %12s  %12s" % ("", "BEFORE", "AFTER"))
print("  " + "-"*60)
print("  %-35s  %+12.1f  %+12.1f" % ("Portal PnL (theoretical)", b_portal, a_portal))
print("  %-35s  %12.1f  %12.1f" % ("- Slippage (%.1f pts/trade)" % slippage_per_trade,
    sb['trades'] * slippage_per_trade, sa['trades'] * slippage_per_trade))
print("  %-35s  %12.1f  %12.1f" % ("- Commissions (%.1f pts/trade)" % commission_per_trade_pts,
    sb['trades'] * commission_per_trade_pts, sa['trades'] * commission_per_trade_pts))
print("  " + "-"*60)
print("  %-35s  %+12.1f  %+12.1f" % ("Est. Execution PnL", b_execution_est, a_execution_est))
print("  %-35s  %12.1f  %12.1f" % ("Portal-to-Execution gap",
    b_portal - b_execution_est, a_portal - a_execution_est))
print("  %-35s  %12.1f%%  %12.1f%%" % ("Execution capture rate",
    b_execution_est/b_portal*100 if b_portal else 0,
    a_execution_est/a_portal*100 if a_portal else 0))

print("\\n  KEY INSIGHT:")
print("  Portal PnL drops from %+.1f to %+.1f (%+.1f)" % (b_portal, a_portal, a_portal - b_portal))
print("  But execution PnL IMPROVES from %+.1f to %+.1f (%+.1f)" % (
    b_execution_est, a_execution_est, a_execution_est - b_execution_est))
print("  Because we eliminate %d trades that cost %.1f pts in slippage+commissions" % (
    sb['trades'] - sa['trades'], (sb['trades'] - sa['trades']) * (slippage_per_trade + commission_per_trade_pts)))
print("  while only giving up %+.1f pts of portal PnL (mostly losers)" % (a_portal - b_portal))

# ============================================================
# PER-TRADE QUALITY
# ============================================================
print("\\n" + "="*90)
print("PER-TRADE QUALITY METRICS")
print("="*90)

print("\\n  %-30s  %12s  %12s" % ("", "BEFORE", "AFTER"))
print("  " + "-"*55)
print("  %-30s  %12.1f  %12.1f" % ("Avg PnL per trade", b_portal/sb['trades'], a_portal/sa['trades']))
print("  %-30s  %12.1f  %12.1f" % ("Avg PnL per day",
    b_portal/len(all_dates), a_portal/len(all_dates)))
print("  %-30s  %12.1f  %12.1f" % ("Trades per day (avg)",
    sb['trades']/len(all_dates), sa['trades']/len(all_dates)))
print("  %-30s  %12.1f  %12.1f" % ("Losses per day (avg)",
    sb['l']/len(all_dates), sa['l']/len(all_dates)))
print("  %-30s  %12.1f  %12.1f" % ("Win amount / Loss amount",
    abs(sb['avg_w']/sb['avg_l']) if sb['avg_l'] else 0,
    abs(sa['avg_w']/sa['avg_l']) if sa['avg_l'] else 0))

# Losing streaks
def max_streak(tlist, result_type):
    streak = 0
    max_s = 0
    for t in sorted(tlist, key=lambda x: x['time']):
        if t['result'] == result_type:
            streak += 1
            if streak > max_s:
                max_s = streak
        else:
            streak = 0
    return max_s

print("  %-30s  %12d  %12d" % ("Max consecutive losses",
    max_streak(total_before, 'LOSS'), max_streak(total_after, 'LOSS')))
print("  %-30s  %12.1f  %12.1f" % ("Max drawdown (pts)", sb['max_dd'], sa['max_dd']))

sys.stdout.flush()
c.close()
