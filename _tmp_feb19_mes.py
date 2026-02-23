"""Simulate Feb 19 MES auto-trader PnL with split-target exits."""
import os
from sqlalchemy import create_engine, text

TICK = 0.25  # MES tick size
PTS_PER_TICK = 1.25  # $1.25 per tick per contract

engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, setup_name, direction, grade,
               spot, target, outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss,
               outcome_elapsed_min
        FROM setup_log
        WHERE ts AT TIME ZONE 'America/New_York' >= '2026-02-19'
          AND ts AT TIME ZONE 'America/New_York' < '2026-02-20'
          AND grade IN ('A', 'B', 'LOG')
        ORDER BY ts
    """)).mappings().all()

SINGLE_TARGET = {"BofA Scalp", "ES Absorption", "Paradigm Reversal"}
SPLIT_TARGET = {"GEX Long", "AG Short", "DD Exhaustion"}
FIRST_TARGET = 10.0  # pts

print("=" * 110)
print(f"{'ID':>5} {'Time':>5} {'Setup':<16} {'Dir':<6} {'Res':<8} {'SPX PnL':>8} | {'Flow':>6} {'T1 PnL':>8} {'T2 PnL':>8} {'MES Total':>10} {'MES $':>8}")
print("=" * 110)

total_mes_pts = 0.0
total_mes_dollars = 0.0

for r in rows:
    t = str(r['ts_et'])[11:16]
    name = r['setup_name']
    res = r['outcome_result'] or 'OPEN'
    spx_pnl = r['outcome_pnl'] if r['outcome_pnl'] is not None else 0
    max_profit = r['outcome_max_profit'] if r['outcome_max_profit'] is not None else 0
    spot = r['spot']
    target = r['target']
    stop_lvl = r['outcome_stop_level']
    direction = r['direction']
    is_long = direction.lower() in ('long', 'bullish')

    # Determine full target distance
    if target:
        full_target_dist = abs(target - spot)
    else:
        full_target_dist = None

    stop_dist = abs(spot - stop_lvl) if stop_lvl else 20  # default

    if name in SINGLE_TARGET:
        # Flow A: 10 MES, single limit at +10pts
        flow = "10@10"
        if res == 'WIN':
            # Target hit at +10
            t1_pnl = 10.0 * 10  # 10 contracts * 10 pts
            t2_pnl = 0
        elif res == 'LOSS':
            t1_pnl = -stop_dist * 10
            t2_pnl = 0
        elif res == 'EXPIRED':
            # Use actual SPX PnL as proxy
            t1_pnl = spx_pnl * 10
            t2_pnl = 0
        else:
            t1_pnl = 0
            t2_pnl = 0
        mes_pts = t1_pnl

    elif name in SPLIT_TARGET:
        flow = "split"
        # Did price reach +10? Check max_profit
        t1_hit = max_profit >= FIRST_TARGET or (res == 'WIN' and spx_pnl >= FIRST_TARGET)

        if name == "DD Exhaustion":
            # DD: T1=5@+10, T2=5 trail-only (no limit)
            if res == 'WIN':
                if t1_hit:
                    t1_pnl = FIRST_TARGET * 5  # T1: 5 contracts * +10
                    # T2: 5 contracts at the trail stop (= actual outcome PnL for the trail portion)
                    t2_pnl = spx_pnl * 5  # trail exit at actual PnL
                else:
                    # Won but didn't reach +10 (trail moved stop above entry)
                    t1_pnl = spx_pnl * 5
                    t2_pnl = spx_pnl * 5
            elif res == 'LOSS':
                if t1_hit:
                    # T1 filled at +10, then reversed to stop
                    t1_pnl = FIRST_TARGET * 5
                    t2_pnl = -stop_dist * 5  # T2 stopped out
                else:
                    # Never reached +10, all 10 stopped out
                    t1_pnl = -stop_dist * 5
                    t2_pnl = -stop_dist * 5
            elif res == 'EXPIRED':
                if t1_hit:
                    t1_pnl = FIRST_TARGET * 5
                    t2_pnl = spx_pnl * 5  # remaining at expiry PnL
                else:
                    t1_pnl = spx_pnl * 5
                    t2_pnl = spx_pnl * 5
            else:
                t1_pnl = 0
                t2_pnl = 0
        else:
            # GEX Long / AG Short: T1=5@+10, T2=5@full target
            t2_target = full_target_dist if full_target_dist else FIRST_TARGET
            t2_hit = max_profit >= t2_target or (res == 'WIN' and spx_pnl >= t2_target)

            if res == 'WIN':
                if t1_hit:
                    t1_pnl = FIRST_TARGET * 5
                    if t2_hit:
                        t2_pnl = t2_target * 5  # T2 limit filled
                    else:
                        # T2 trailed to the actual exit
                        t2_pnl = spx_pnl * 5
                else:
                    # Trail moved stop above entry, but never hit +10
                    t1_pnl = spx_pnl * 5
                    t2_pnl = spx_pnl * 5
            elif res == 'LOSS':
                if t1_hit:
                    t1_pnl = FIRST_TARGET * 5
                    t2_pnl = -stop_dist * 5
                else:
                    t1_pnl = -stop_dist * 5
                    t2_pnl = -stop_dist * 5
            elif res == 'EXPIRED':
                if t1_hit:
                    t1_pnl = FIRST_TARGET * 5
                    t2_pnl = spx_pnl * 5
                else:
                    t1_pnl = spx_pnl * 5
                    t2_pnl = spx_pnl * 5
            else:
                t1_pnl = 0
                t2_pnl = 0

        mes_pts = t1_pnl + t2_pnl
    else:
        flow = "?"
        t1_pnl = 0
        t2_pnl = 0
        mes_pts = 0

    # MES: $5 per point per contract (= $1.25 per tick, 4 ticks per point)
    mes_dollars = mes_pts * 5.0  # $5 per point (each point = 4 ticks * $1.25)

    total_mes_pts += mes_pts
    total_mes_dollars += mes_dollars

    print(f"#{r['id']:>4} {t} {name:<16} {direction[:5]:<6} {res:<8} {spx_pnl:>+7.1f}p | "
          f"{flow:>6} {t1_pnl:>+7.1f}p {t2_pnl:>+7.1f}p {mes_pts:>+9.1f}p ${mes_dollars:>+7.0f}")

print("=" * 110)
print(f"{'TOTAL':>55} | {'':>6} {'':>8} {'':>8} {total_mes_pts:>+9.1f}p ${total_mes_dollars:>+7.0f}")
print(f"\nNote: MES = $5 per point per contract. 10 contracts = $50 per point (all-in).")
print(f"      Split = 5 @ first target (+10pts) + 5 @ full target or trail")
print(f"      Single = 10 @ +10pts target")
