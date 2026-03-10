import os
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, setup_name, direction,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               grade, score, spot, abs_es_price
        FROM setup_log
        WHERE ts::date = '2026-03-03'
          AND outcome_result IS NOT NULL
          AND outcome_pnl IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    paradigm_rows = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et, payload->>'paradigm' as paradigm
        FROM volland_snapshots
        WHERE ts::date = '2026-03-03'
          AND payload->>'paradigm' IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

paradigm_at = [(p['ts_et'], p['paradigm']) for p in paradigm_rows]

def get_paradigm(trade_ts):
    best = None
    for pts, par in paradigm_at:
        if pts <= trade_ts:
            best = par
    return best or 'UNKNOWN'

eval_setups = {'AG Short', 'DD Exhaustion', 'ES Absorption', 'Paradigm Reversal'}
trailing = {'DD Exhaustion', 'AG Short', 'ES Absorption', 'GEX Long', 'Skew Charm'}

print('='*85)
print('MARCH 3 - EVAL TRADER ANALYSIS (Real E2T Config)')
print('='*85)
hdr = f"{'#':>3} {'Time':>8} {'Setup':>20} {'Dir':>5} {'Res':>5} {'PnL':>6} {'MaxP':>5} {'Paradigm':>15} {'Status':>12}"
print(hdr)
print('-'*85)

eval_trades = []
blocked = []
skipped_setup = []

for r in rows:
    setup = r['setup_name']
    ts = r['ts_et']
    time_str = ts.strftime('%H:%M')
    pnl = float(r['outcome_pnl'] or 0)
    max_p = float(r['outcome_max_profit'] or 0)
    result = r['outcome_result']
    paradigm = get_paradigm(ts)

    # Split-target PnL
    t1_hit = max_p >= 10
    if setup in trailing and t1_hit:
        split_pnl = round((10.0 + pnl) / 2, 1)
    else:
        split_pnl = pnl

    if setup not in eval_setups:
        skipped_setup.append((r, split_pnl))
        continue

    # DD filters
    block_reason = ''
    if setup == 'DD Exhaustion':
        if ts.hour >= 14:
            block_reason = 'after 14:00'
        if 'BOFA' in paradigm.upper() and 'PURE' in paradigm.upper():
            block_reason = 'BOFA-PURE'

    status = block_reason if block_reason else 'TAKE'
    print(f'{r["id"]:3d} {time_str:>8} {setup:>20} {r["direction"]:>5} {result:>5} {split_pnl:>+6.1f} {max_p:>5.1f} {paradigm:>15} {status:>12}')

    if block_reason:
        blocked.append((r, split_pnl, block_reason))
    else:
        eval_trades.append((r, split_pnl))

print()
print(f'Disabled setups skipped: {len(skipped_setup)} (BofA, GEX Long, Skew Charm)')
print(f'DD blocked by filters: {len(blocked)}')
for bt, bp, br in blocked:
    print(f'  #{bt["id"]} DD {bt["direction"]} {bt["outcome_result"]} {bp:+.1f} (blocked: {br})')

# Simulate with and without cap
print()
print('='*85)
print('P&L SIMULATION (10 MES = $50/pt)')
print('='*85)

cum_pts = 0
cum_dollar = 0
capped_trade = None
losses = 0

for i, (r, sp) in enumerate(eval_trades):
    cum_pts += sp
    dollar = sp * 50
    cum_dollar += dollar
    if sp < 0:
        losses += 1

    cap_status = ''
    if cum_dollar >= 900 and capped_trade is None:
        capped_trade = i + 1
        cap_status = ' ** HIT $900 CAP **'

    print(f'  T{i+1}: {r["setup_name"]:>20} {r["direction"]:>5} {sp:>+6.1f} pts  ${dollar:>+6.0f}  cum=${cum_dollar:>+6.0f}  ({cum_pts:>+6.1f} pts){cap_status}')

print()
print(f'Total eval-eligible trades: {len(eval_trades)}')
wins = sum(1 for r,sp in eval_trades if sp > 0)
loss_ct = sum(1 for r,sp in eval_trades if sp < 0)
exp_ct = sum(1 for r,sp in eval_trades if sp == 0)
print(f'  Wins: {wins}')
print(f'  Losses: {loss_ct}')
print(f'  Expired: {exp_ct}')
wr = wins / max(wins + loss_ct, 1) * 100
print(f'  Win Rate: {wr:.0f}%')
print(f'  Total PnL: {cum_pts:+.1f} pts = ${cum_dollar:+.0f}')
if capped_trade:
    capped_pts = sum(sp for _,sp in eval_trades[:capped_trade])
    capped_dollar = capped_pts * 50
    after_pts = sum(sp for _,sp in eval_trades[capped_trade:])
    after_dollar = after_pts * 50
    print()
    print(f'  With $900 cap: ${capped_dollar:+.0f} ({capped_trade} trades, then stop)')
    print(f'  After cap trades: ${after_dollar:+.0f} ({len(eval_trades)-capped_trade} trades missed)')
    print(f'  Without cap: ${cum_dollar:+.0f}')

# Historical daily analysis
print()
print('='*85)
print('HISTORICAL DAILY P&L (Eval Trader rules applied to ALL days)')
print('='*85)

with engine.begin() as conn:
    all_rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, setup_name, direction,
               outcome_result, outcome_pnl, outcome_max_profit, grade
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND outcome_pnl IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    all_paradigm = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et, payload->>'paradigm' as paradigm
        FROM volland_snapshots
        WHERE payload->>'paradigm' IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

all_par = [(p['ts_et'], p['paradigm']) for p in all_paradigm]

def get_par_hist(trade_ts):
    best = None
    for pts, par in all_par:
        if pts <= trade_ts:
            best = par
        elif pts > trade_ts:
            break
    return best or 'UNKNOWN'

daily = defaultdict(lambda: {"trades": 0, "pts": 0.0, "wins": 0, "losses": 0, "blocked": 0})

for r in all_rows:
    setup = r['setup_name']
    if setup not in eval_setups:
        continue

    ts = r['ts_et']
    day = ts.strftime('%Y-%m-%d')
    pnl = float(r['outcome_pnl'] or 0)
    max_p = float(r['outcome_max_profit'] or 0)
    paradigm = get_par_hist(ts)

    # DD filters
    if setup == 'DD Exhaustion':
        if ts.hour >= 14:
            daily[day]["blocked"] += 1
            continue
        if 'BOFA' in paradigm.upper() and 'PURE' in paradigm.upper():
            daily[day]["blocked"] += 1
            continue

    # Split-target PnL
    t1_hit = max_p >= 10
    if setup in trailing and t1_hit:
        split_pnl = round((10.0 + pnl) / 2, 1)
    else:
        split_pnl = pnl

    daily[day]["trades"] += 1
    daily[day]["pts"] += split_pnl
    if split_pnl > 0:
        daily[day]["wins"] += 1
    elif split_pnl < 0:
        daily[day]["losses"] += 1

print(f"{'Date':>12} {'N':>4} {'Blk':>4} {'W':>3} {'L':>3} {'PnL pts':>8} {'$(10MES)':>10} {'Cap$900':>8}")
print('-'*60)

total_pts = 0
total_capped = 0
total_trades = 0
days_over_900 = 0
days_positive = 0
days_negative = 0

for d in sorted(daily.keys()):
    v = daily[d]
    dollar = v["pts"] * 50
    capped = min(dollar, 900) if dollar > 0 else dollar
    total_pts += v["pts"]
    total_capped += capped
    total_trades += v["trades"]
    if dollar > 900:
        days_over_900 += 1
    if v["pts"] > 0:
        days_positive += 1
    elif v["pts"] < 0:
        days_negative += 1
    cap_mark = " *" if dollar > 900 else ""
    print(f'{d:>12} {v["trades"]:4d} {v["blocked"]:4d} {v["wins"]:3d} {v["losses"]:3d} {v["pts"]:>+8.1f} ${dollar:>+9.0f} ${capped:>+7.0f}{cap_mark}')

n_days = len(daily)
print('-'*60)
print(f'{"TOTAL":>12} {total_trades:4d} {"":4s} {"":3s} {"":3s} {total_pts:>+8.1f} ${total_pts*50:>+9.0f} ${total_capped:>+7.0f}')
print(f'{"Per day":>12} {total_trades/n_days:4.1f} {"":4s} {"":3s} {"":3s} {total_pts/n_days:>+8.1f} ${total_pts/n_days*50:>+9.0f} ${total_capped/n_days:>+7.0f}')
print()
print(f'Trading days: {n_days}')
print(f'Positive days: {days_positive} ({days_positive/n_days*100:.0f}%)')
print(f'Negative days: {days_negative} ({days_negative/n_days*100:.0f}%)')
print(f'Days that hit $900 cap: {days_over_900} ({days_over_900/n_days*100:.0f}%)')
print(f'Uncapped total: ${total_pts*50:+.0f}')
print(f'Capped total:   ${total_capped:+.0f}')
print(f'Lost to cap:    ${total_pts*50 - total_capped:+.0f}')
