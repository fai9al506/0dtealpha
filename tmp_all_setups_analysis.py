"""Deep analysis for ALL setups -- run via: railway run --service 0dtealpha -- python tmp_all_setups_analysis.py"""
from sqlalchemy import create_engine, text
import os, sys

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Get all resolved trades with volland context
rows = c.execute(text("""
SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as t,
       s.setup_name, s.direction, s.grade, s.score, s.spot, s.paradigm,
       s.outcome_result, s.outcome_pnl, s.outcome_max_profit, s.outcome_max_loss,
       s.outcome_first_event, s.outcome_elapsed_min,
       (SELECT payload->'statistics'->>'delta_decay_hedging'
        FROM volland_snapshots v WHERE v.ts <= s.ts AND payload->'statistics' IS NOT NULL
        ORDER BY v.ts DESC LIMIT 1) as dd_hedging,
       (SELECT payload->'statistics'->>'aggregatedCharm'
        FROM volland_snapshots v WHERE v.ts <= s.ts AND payload->'statistics' IS NOT NULL
        ORDER BY v.ts DESC LIMIT 1) as charm,
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

def parse_dd(s):
    if not s: return None
    s = s.replace('$','').replace(',','')
    try: return float(s)
    except: return None

def cat(p):
    if 'MESSY' in p or 'EXTREME' in p or 'SIDIAL' in p: return 'MESSY/SIDIAL'
    if 'BOFA' in p or 'BofA' in p: return 'BOFA'
    if 'AG' in p: return 'AG'
    if 'GEX' in p: return 'GEX'
    return 'OTHER'

all_trades = []
for r in rows:
    dd_val = parse_dd(r[14])
    charm_val = float(r[15]) if r[15] else None
    svb_val = float(r[16]) if r[16] else None
    all_trades.append({
        'id': r[0], 'time': r[1], 'setup': r[2], 'dir': r[3], 'grade': r[4],
        'score': r[5], 'spot': r[6], 'paradigm': r[7] or '-', 'result': r[8],
        'pnl': r[9], 'maxP': r[10], 'maxL': r[11], 'first_event': r[12],
        'elapsed': r[13], 'dd': dd_val, 'charm': charm_val, 'svb': svb_val,
        'hour': int(r[17]), 'minute': int(r[18]), 'date': r[19]
    })

def show(label, bucket):
    if not bucket: return
    w = sum(1 for t in bucket if t['result']=='WIN')
    l = sum(1 for t in bucket if t['result']=='LOSS')
    exp = sum(1 for t in bucket if t['result']=='EXPIRED')
    pnl = sum(t['pnl'] for t in bucket)
    wr = w/(w+l)*100 if (w+l) else 0
    print("  %-24s %2d trades  %dW/%dL/%dE  WR=%5.1f%%  PnL=%+7.1f" % (label, len(bucket), w, l, exp, wr, pnl))

def sim(label, filtered):
    if not filtered:
        print("  %-25s  -- no trades --" % label)
        return
    w = sum(1 for t in filtered if t['result']=='WIN')
    l = sum(1 for t in filtered if t['result']=='LOSS')
    pnl = sum(t['pnl'] for t in filtered)
    wr = w/(w+l)*100 if (w+l) else 0
    print("  %-25s  %2d trades  %dW/%dL  WR=%.0f%%  PnL=%+.1f" % (label, len(filtered), w, l, wr, pnl))


# ============================================================
# GRAND SUMMARY
# ============================================================
print("\\n" + "="*70)
print("GRAND SUMMARY - ALL SETUPS")
print("="*70)

setups = sorted(set(t['setup'] for t in all_trades))
grand_pnl = 0
for s in setups:
    bucket = [t for t in all_trades if t['setup'] == s]
    w = sum(1 for t in bucket if t['result']=='WIN')
    l = sum(1 for t in bucket if t['result']=='LOSS')
    exp = sum(1 for t in bucket if t['result']=='EXPIRED')
    pnl = sum(t['pnl'] for t in bucket)
    wr = w/(w+l)*100 if (w+l) else 0
    avg_w = sum(t['pnl'] for t in bucket if t['result']=='WIN')/w if w else 0
    avg_l = sum(t['pnl'] for t in bucket if t['result']=='LOSS')/l if l else 0
    grand_pnl += pnl
    print("  %-20s %3d trades  %2dW/%2dL/%dE  WR=%5.1f%%  PnL=%+8.1f  AvgW=%+.1f AvgL=%+.1f" % (
        s, len(bucket), w, l, exp, wr, pnl, avg_w, avg_l))

print("  " + "-"*65)
print("  %-20s %3d trades                           PnL=%+8.1f" % ("TOTAL", len(all_trades), grand_pnl))


# ============================================================
# PER-SETUP DEEP DIVE
# ============================================================
for setup_name in setups:
    trades = [t for t in all_trades if t['setup'] == setup_name]
    resolved = trades  # already filtered to resolved in SQL
    wins = [t for t in resolved if t['result'] == 'WIN']
    losses = [t for t in resolved if t['result'] == 'LOSS']
    expired = [t for t in resolved if t['result'] == 'EXPIRED']

    if len(resolved) < 2:
        print("\\n" + "="*70)
        print("%s - %d trade (skipping deep dive)" % (setup_name, len(resolved)))
        print("="*70)
        continue

    print("\\n\\n" + "="*70)
    print("%s - %d resolved trades" % (setup_name, len(resolved)))
    print("="*70)
    wl = len(wins) + len(losses)
    print("W: %d | L: %d | E: %d | WR: %.0f%% (excl expired)" % (
        len(wins), len(losses), len(expired), len(wins)/wl*100 if wl else 0))
    print("Total PnL: %+.1f pts" % sum(t['pnl'] for t in resolved))
    if wins and losses:
        print("Avg Win: %+.1f | Avg Loss: %+.1f" % (
            sum(t['pnl'] for t in wins)/len(wins), sum(t['pnl'] for t in losses)/len(losses)))

    # BY HOUR
    print("\\n--- BY HOUR ---")
    for h in range(9,17):
        bucket = [t for t in resolved if t['hour'] == h]
        if bucket: show("%02d:00" % h, bucket)

    # BY PARADIGM
    print("\\n--- BY PARADIGM ---")
    for p in sorted(set(t['paradigm'] for t in resolved)):
        show(p, [t for t in resolved if t['paradigm'] == p])

    # BY PARADIGM CATEGORY
    print("\\n--- BY PARADIGM CATEGORY ---")
    for c_name in ['MESSY/SIDIAL', 'AG', 'GEX', 'BOFA', 'OTHER']:
        bucket = [t for t in resolved if cat(t['paradigm']) == c_name]
        if bucket: show(c_name, bucket)

    # BY DIRECTION
    print("\\n--- BY DIRECTION ---")
    for d in ['long','short']:
        bucket = [t for t in resolved if t['dir'] == d]
        if bucket: show(d, bucket)

    # BY GRADE
    print("\\n--- BY GRADE ---")
    for g in sorted(set(t['grade'] for t in resolved)):
        show(g, [t for t in resolved if t['grade'] == g])

    # BY |DD HEDGING| SIZE
    print("\\n--- BY |DD HEDGING| SIZE ---")
    for lo, hi, label in [(0,500e6,'<$500M'),(500e6,2e9,'$500M-2B'),(2e9,5e9,'$2B-5B'),(5e9,99e9,'$5B+')]:
        bucket = [t for t in resolved if t['dd'] is not None and lo <= abs(t['dd']) < hi]
        if bucket: show("|DD| " + label, bucket)

    # BY |CHARM| SIZE
    print("\\n--- BY |CHARM| SIZE ---")
    for lo, hi, label in [(0,20e6,'<$20M'),(20e6,60e6,'$20-60M'),(60e6,120e6,'$60-120M'),(120e6,999e9,'$120M+')]:
        bucket = [t for t in resolved if t['charm'] is not None and lo <= abs(t['charm']) < hi]
        if bucket: show("|Charm| " + label, bucket)

    # BY SVB
    print("\\n--- BY SPOT-VOL-BETA ---")
    for lo, hi, label in [(-2,-0.5,'SVB < -0.5'),(-.5,0,'SVB -0.5 to 0'),(0,0.5,'SVB 0 to +0.5'),(0.5,2,'SVB > +0.5')]:
        bucket = [t for t in resolved if t['svb'] is not None and lo <= t['svb'] < hi]
        if bucket: show(label, bucket)

    # DIRECTION x HOUR
    print("\\n--- DIRECTION x HOUR ---")
    for d in ['long','short']:
        for h in range(9,17):
            bucket = [t for t in resolved if t['dir']==d and t['hour']==h]
            if bucket: show("%s %02d:00" % (d, h), bucket)

    # PARADIGM x DIRECTION
    print("\\n--- PARADIGM x DIRECTION ---")
    for p in sorted(set(t['paradigm'] for t in resolved)):
        for d in ['long','short']:
            bucket = [t for t in resolved if t['paradigm']==p and t['dir']==d]
            if bucket: show("%s %s" % (p, d), bucket)

    # LOSS ANALYSIS
    print("\\n--- LOSS ANALYSIS ---")
    near_wins = [t for t in losses if t['maxP'] and t['maxP'] >= 7]
    no_move = [t for t in losses if t['maxP'] is not None and t['maxP'] < 2]
    print("  Losses with maxP >= 7 (saveable with BE stop): %d" % len(near_wins))
    for t in near_wins:
        print("    #%d %s %s %s maxP=%.1f -> LOSS %+.1f" % (
            t['id'], str(t['time'])[11:16], t['dir'], t['paradigm'], t['maxP'], t['pnl']))
    if near_wins:
        print("  Potential savings: +%.1f pts" % sum(abs(t['pnl']) for t in near_wins))
    print("  Losses with maxP < 2 (instant wrong dir): %d / %d" % (len(no_move), len(losses)))

    # DAILY BREAKDOWN
    print("\\n--- BY DATE ---")
    dates = sorted(set(t['date'] for t in resolved))
    for d in dates:
        bucket = [t for t in resolved if t['date'] == d]
        w = sum(1 for t in bucket if t['result']=='WIN')
        l = sum(1 for t in bucket if t['result']=='LOSS')
        exp = sum(1 for t in bucket if t['result']=='EXPIRED')
        pnl = sum(t['pnl'] for t in bucket)
        print("  %s  %2d trades  %dW/%dL/%dE  PnL=%+7.1f" % (d, len(bucket), w, l, exp, pnl))

    # FILTER SIMULATIONS
    if len(resolved) >= 5:
        print("\\n--- FILTER SIMULATIONS ---")
        sim("A) Time < 14:00", [t for t in resolved if t['hour'] < 14])
        sim("B) Block BOFA", [t for t in resolved if cat(t['paradigm']) != 'BOFA'])
        sim("C) MESSY+AG only", [t for t in resolved if cat(t['paradigm']) in ('MESSY/SIDIAL','AG')])
        sim("D) <14:00 + no BOFA", [t for t in resolved if t['hour'] < 14 and cat(t['paradigm']) != 'BOFA'])
        sim("E) SVB < 0.5", [t for t in resolved if t['svb'] is None or t['svb'] < 0.5])
        sim("F) SVB < -0.5", [t for t in resolved if t['svb'] is not None and t['svb'] < -0.5])
        sim("G) |Charm| < 120M", [t for t in resolved if t['charm'] is None or abs(t['charm']) < 120e6])
        sim("H) |DD| >= 1B", [t for t in resolved if t['dd'] is not None and abs(t['dd']) >= 1e9])

        # BE stop sim
        print("\\n  BE STOP SIMULATIONS:")
        for be_pt in [5, 7, 10]:
            sim_pnl = 0; sim_w = 0; sim_l = 0
            for t in resolved:
                if t['result'] == 'WIN':
                    sim_pnl += t['pnl']; sim_w += 1
                elif t['result'] == 'LOSS':
                    if t['maxP'] and t['maxP'] >= be_pt:
                        sim_pnl += 0; sim_w += 1
                    else:
                        sim_pnl += t['pnl']; sim_l += 1
                else:
                    sim_pnl += t['pnl']
            wr = sim_w/(sim_w+sim_l)*100 if (sim_w+sim_l) else 0
            actual = sum(t['pnl'] for t in resolved)
            print("  BE@+%dpts:  %dW/%dL  WR=%.0f%%  PnL=%+.1f  (vs actual %+.1f)" % (
                be_pt, sim_w, sim_l, wr, sim_pnl, actual))

sys.stdout.flush()
c.close()
