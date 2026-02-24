"""Deep DD Exhaustion analysis -- run via: railway run --service 0dtealpha -- python tmp_dd_analysis.py"""
from sqlalchemy import create_engine, text
import os, sys

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("""
SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as t,
       s.direction, s.grade, s.score, s.spot, s.paradigm,
       s.outcome_result, s.outcome_pnl, s.outcome_max_profit, s.outcome_max_loss,
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
       EXTRACT(MINUTE FROM s.ts AT TIME ZONE 'America/New_York') as minute
FROM setup_log s
WHERE s.setup_name = 'DD Exhaustion' AND s.outcome_result IS NOT NULL
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

trades = []
for r in rows:
    dd_val = parse_dd(r[11])
    charm_val = float(r[12]) if r[12] else None
    svb_val = float(r[13]) if r[13] else None
    trades.append({
        'id': r[0], 'time': r[1], 'dir': r[2], 'grade': r[3], 'score': r[4],
        'spot': r[5], 'paradigm': r[6] or '-', 'result': r[7], 'pnl': r[8],
        'maxP': r[9], 'maxL': r[10], 'dd': dd_val, 'charm': charm_val,
        'svb': svb_val, 'hour': int(r[14]), 'minute': int(r[15])
    })

resolved = [t for t in trades if t['result'] in ('WIN','LOSS','EXPIRED')]
wins = [t for t in resolved if t['result'] == 'WIN']
losses = [t for t in resolved if t['result'] == 'LOSS']
expired = [t for t in resolved if t['result'] == 'EXPIRED']

print("\\n" + "="*60)
print("DD EXHAUSTION DEEP ANALYSIS - %d resolved trades" % len(resolved))
print("="*60)
print("W: %d | L: %d | E: %d | WR: %.0f%% (excl expired)" % (
    len(wins), len(losses), len(expired), len(wins)/(len(wins)+len(losses))*100))
print("Total PnL: %+.1f pts" % sum(t['pnl'] for t in resolved))
if wins:
    print("Avg Win: %+.1f | Avg Loss: %+.1f" % (
        sum(t['pnl'] for t in wins)/len(wins), sum(t['pnl'] for t in losses)/len(losses)))

def show(label, bucket):
    w = sum(1 for t in bucket if t['result']=='WIN')
    l = sum(1 for t in bucket if t['result']=='LOSS')
    exp = sum(1 for t in bucket if t['result']=='EXPIRED')
    pnl = sum(t['pnl'] for t in bucket)
    wr = w/(w+l)*100 if (w+l) else 0
    print("  %-22s %2d trades  %dW/%dL/%dE  WR=%5.1f%%  PnL=%+7.1f" % (label, len(bucket), w, l, exp, wr, pnl))

print("\\n--- BY HOUR ---")
for h in range(10,17):
    bucket = [t for t in resolved if t['hour'] == h]
    if bucket: show("%02d:00" % h, bucket)

print("\\n--- BY PARADIGM ---")
for p in sorted(set(t['paradigm'] for t in resolved)):
    show(p, [t for t in resolved if t['paradigm'] == p])

print("\\n--- BY PARADIGM CATEGORY ---")
for c_name in ['MESSY/SIDIAL', 'AG', 'GEX', 'BOFA', 'OTHER']:
    bucket = [t for t in resolved if cat(t['paradigm']) == c_name]
    if bucket: show(c_name, bucket)

print("\\n--- BY DIRECTION ---")
for d in ['long','short']:
    show(d, [t for t in resolved if t['dir'] == d])

print("\\n--- BY GRADE ---")
for g in sorted(set(t['grade'] for t in resolved)):
    show(g, [t for t in resolved if t['grade'] == g])

print("\\n--- BY SCORE ---")
for lo, hi, label in [(0,50,'0-49 (low)'),(50,65,'50-64 (med)'),(65,80,'65-79 (high)'),(80,100,'80+ (A+)')]:
    bucket = [t for t in resolved if lo <= t['score'] < hi]
    if bucket: show(label, bucket)

print("\\n--- BY |DD HEDGING| SIZE ---")
for lo, hi, label in [(0,500e6,'<$500M'),(500e6,2e9,'$500M-2B'),(2e9,5e9,'$2B-5B'),(5e9,99e9,'$5B+')]:
    bucket = [t for t in resolved if t['dd'] is not None and lo <= abs(t['dd']) < hi]
    if bucket: show("|DD| " + label, bucket)

print("\\n--- BY |CHARM| SIZE ---")
for lo, hi, label in [(0,20e6,'<$20M'),(20e6,60e6,'$20-60M'),(60e6,120e6,'$60-120M'),(120e6,999e9,'$120M+')]:
    bucket = [t for t in resolved if t['charm'] is not None and lo <= abs(t['charm']) < hi]
    if bucket: show("|Charm| " + label, bucket)

print("\\n--- BY SPOT-VOL-BETA ---")
for lo, hi, label in [(-2,-0.5,'SVB < -0.5'),(-.5,0,'SVB -0.5 to 0'),(0,0.5,'SVB 0 to +0.5'),(0.5,2,'SVB > +0.5')]:
    bucket = [t for t in resolved if t['svb'] is not None and lo <= t['svb'] < hi]
    if bucket: show(label, bucket)

print("\\n--- DIRECTION x HOUR ---")
for d in ['long','short']:
    for h in range(10,17):
        bucket = [t for t in resolved if t['dir']==d and t['hour']==h]
        if bucket: show("%s %02d:00" % (d, h), bucket)

print("\\n--- PARADIGM x DIRECTION ---")
for p in sorted(set(t['paradigm'] for t in resolved)):
    for d in ['long','short']:
        bucket = [t for t in resolved if t['paradigm']==p and t['dir']==d]
        if bucket: show("%s %s" % (p, d), bucket)

print("\\n--- LOSS ANALYSIS ---")
near_wins = [t for t in losses if t['maxP'] and t['maxP'] >= 7]
no_move = [t for t in losses if t['maxP'] is not None and t['maxP'] < 2]
print("  Losses with maxP >= 7 (saveable with BE stop): %d" % len(near_wins))
for t in near_wins:
    print("    #%d %s %s %s maxP=%.1f -> LOSS %+.1f" % (
        t['id'], str(t['time'])[11:16], t['dir'], t['paradigm'], t['maxP'], t['pnl']))
print("  Potential savings: +%.1f pts" % sum(abs(t['pnl']) for t in near_wins))
print("\\n  Losses with maxP < 2 (instant wrong direction): %d" % len(no_move))
for t in no_move:
    print("    #%d %s %s %s maxP=%.1f -> LOSS %+.1f" % (
        t['id'], str(t['time'])[11:16], t['dir'], t['paradigm'], t['maxP'], t['pnl']))

print("\\n--- RAPID-FIRE SIGNALS (< 20 min apart) ---")
for i in range(1, len(trades)):
    gap = (trades[i]['time'] - trades[i-1]['time']).total_seconds() / 60
    if gap < 20:
        t = trades[i]
        tp = trades[i-1]
        flip = 'FLIP' if t['dir'] != tp['dir'] else 'SAME'
        print("  #%d->#%d  gap=%dmin  %s->%s (%s)  %s/%s  pnl: %+.1f/%+.1f" % (
            tp['id'], t['id'], gap, tp['dir'], t['dir'], flip,
            tp['result'], t['result'], tp['pnl'], t['pnl']))

print("\\n" + "="*60)
print("FILTER SIMULATIONS")
print("="*60)

def sim(label, filtered):
    w = sum(1 for t in filtered if t['result']=='WIN')
    l = sum(1 for t in filtered if t['result']=='LOSS')
    pnl = sum(t['pnl'] for t in filtered)
    wr = w/(w+l)*100 if (w+l) else 0
    print("  %-25s  %2d trades  %dW/%dL  WR=%.0f%%  PnL=%+.1f" % (label, len(filtered), w, l, wr, pnl))

sim("A) Time < 14:00", [t for t in resolved if t['hour'] < 14])
sim("B) Block BOFA", [t for t in resolved if cat(t['paradigm']) != 'BOFA'])
sim("C) MESSY+AG only", [t for t in resolved if cat(t['paradigm']) in ('MESSY/SIDIAL','AG')])
sim("D) <14:00 + no BOFA", [t for t in resolved if t['hour'] < 14 and cat(t['paradigm']) != 'BOFA'])
sim("E) D + score>=50", [t for t in resolved if t['hour'] < 14 and cat(t['paradigm']) != 'BOFA' and t['score'] >= 50])
sim("F) D + SVB < 1.0", [t for t in resolved if t['hour'] < 14 and cat(t['paradigm']) != 'BOFA' and (t['svb'] is None or t['svb'] < 1.0)])
sim("G) MESSY only <14:00", [t for t in resolved if t['hour'] < 14 and cat(t['paradigm']) == 'MESSY/SIDIAL'])
sim("H) <13:30 + no BOFA", [t for t in resolved if (t['hour'] < 13 or (t['hour']==13 and t['minute']<30)) and cat(t['paradigm']) != 'BOFA'])

print("\\n  BE STOP SIMULATIONS:")
for be_pt in [7, 8, 9, 10]:
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
    print("  BE@+%dpts:  %dW/%dL  WR=%.0f%%  PnL=%+.1f  (vs actual %+.1f)" % (be_pt, sim_w, sim_l, wr, sim_pnl, actual))

print("\\n  BEST COMBO: <14:00 + noBofa + BE@8:")
f = [t for t in resolved if t['hour'] < 14 and cat(t['paradigm']) != 'BOFA']
sim_pnl = 0; sim_w = 0; sim_l = 0
for t in f:
    if t['result'] == 'WIN':
        sim_pnl += t['pnl']; sim_w += 1
    elif t['result'] == 'LOSS':
        if t['maxP'] and t['maxP'] >= 8:
            sim_pnl += 0; sim_w += 1
        else:
            sim_pnl += t['pnl']; sim_l += 1
    else:
        sim_pnl += t['pnl']
wr = sim_w/(sim_w+sim_l)*100 if (sim_w+sim_l) else 0
print("     %d trades  %dW/%dL  WR=%.0f%%  PnL=%+.1f  (%d blocked)" % (
    len(f), sim_w, sim_l, wr, sim_pnl, len(resolved)-len(f)))

sys.stdout.flush()
c.close()
