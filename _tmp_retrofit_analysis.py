"""Compute three things:
  1. Today's TSRT with all fixes (add back the 9 daily_loss_limit-blocked signals)
  2. May 1-20 actual vs retrofit-with-fixes P&L
  3. Honesty check on the [MES-sim] MES-sim badge for today's placed trades
"""
import os, psycopg2, json
from datetime import date

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

print("=" * 70)
print("Q1: TODAY — what TSRT would have made WITH S161 breaker fix")
print("=" * 70)

# Today's actual broker realized: +$388.50 (from broker BalanceDetail)
ACTUAL_TODAY = 388.50

# The 9 trades blocked by daily_loss_limit today
cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm, greek_alignment,
           outcome_pnl, outcome_result, mes_sim_outcome_pnl, mes_sim_outcome_result
    FROM setup_log
    WHERE ts::date = '2026-05-20'
      AND real_trade_skip_reason = 'daily_loss_limit'
    ORDER BY id
""")
blocked_today = cur.fetchall()
print(f"\n{len(blocked_today)} trades blocked by breaker bug today:")
total_chain_sim = 0.0
total_mes_sim = 0.0
n_with_mes = 0
for r in blocked_today:
    sid, name, dir_, grade, para, align, pnl, res, mes_pnl, mes_res = r
    pnl_f = float(pnl) if pnl is not None else 0.0
    mes_f = float(mes_pnl) if mes_pnl is not None else None
    total_chain_sim += pnl_f
    if mes_f is not None:
        total_mes_sim += mes_f
        n_with_mes += 1
    mes_str = f"{mes_f:+.1f}" if mes_f is not None else "  N/A"
    print(f"  lid={sid} {name:<14} {dir_:<8} g={grade} chain={pnl_f:+.1f}pt mes={mes_str}pt")

print(f"\n  chain-sim total of 9 blocked: {total_chain_sim:+.1f} pts = ${total_chain_sim*5:+.2f}")
if n_with_mes > 0:
    print(f"  MES-sim total (of {n_with_mes} with mes data): {total_mes_sim:+.1f} pts = ${total_mes_sim*5:+.2f}")

# Realistic capture rate (per feedback_capture_rate_anchor.md): 75-85% blend
print(f"\n  Applying realistic capture rate (75% blend on chain-sim):")
cap75 = total_chain_sim * 5 * 0.75
print(f"    Recovered $ from breaker fix: ${cap75:+.2f}")
print(f"\n  TSRT today WITH S161 fix from start: ${ACTUAL_TODAY + cap75:+.2f}")
print(f"  vs actual today: ${ACTUAL_TODAY:+.2f}")
print(f"  Improvement: ${cap75:+.2f}")

# === Q2: MAY 1-20 with all fixes ===
print()
print("=" * 70)
print("Q2: MAY 1-20 — actual vs retrofit-with-all-fixes")
print("=" * 70)

# Pull all real_trade_orders for May with their broker close fills
cur.execute("""
    SELECT rto.setup_log_id, rto.created_at, sl.setup_name, sl.direction,
           rto.state, sl.outcome_pnl
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE rto.created_at >= '2026-05-01'
      AND rto.created_at < '2026-05-21'
      AND rto.state->>'status' = 'closed'
    ORDER BY rto.created_at
""")
may_actual_total = 0.0  # sum of (close-fill)*direction*$5
may_unresolved = []
for sid, created, name, dir_, state, pnl in cur.fetchall():
    if isinstance(state, str): state = json.loads(state)
    fill = state.get("fill_price")
    close = state.get("close_fill_price")
    qty = state.get("quantity") or 1
    is_long = dir_ in ("long", "bullish")
    if fill is not None and close is not None:
        pts = (float(close) - float(fill)) * (1 if is_long else -1)
        may_actual_total += pts * 5.0 * qty
    else:
        may_unresolved.append(sid)
print(f"  Actual May 1-20 broker (resolved trades): ${may_actual_total:+.2f}")
print(f"  Unresolved (ghost) trades: {len(may_unresolved)}")

# Portal V16 sim for May 1-20 — sum of setup_log.outcome_pnl for V16-eligible trades
# Use a simpler proxy: all trades that WOULD have placed if filters were perfect
cur.execute("""
    SELECT COALESCE(SUM(outcome_pnl), 0) AS total_pts,
           COUNT(*) FILTER (WHERE outcome_pnl > 0) AS wins,
           COUNT(*) FILTER (WHERE outcome_pnl < 0) AS losses,
           COUNT(*) AS total
    FROM setup_log
    WHERE ts::date >= '2026-05-01'
      AND ts::date <= '2026-05-20'
      AND setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion')
      AND notified = true
      AND outcome_pnl IS NOT NULL
""")
total_pts, wins, losses, total = cur.fetchone()
total_pts = float(total_pts or 0)
print(f"\n  Portal V16 sim May 1-20 (all V14-whitelist notified trades):")
print(f"    {total} trades ({wins}W / {losses}L), {total_pts:+.1f} pts, ${total_pts*5:+.2f}")

# Realistic retrofit estimate (per PROJECT_BRAIN $1,000-1,400/mo at 1 MES, 60-65% capture)
# Per S111: forward floor with current fixes only = +$200-400/mo at 1 MES
# Per S161 today: +$300+/day on the breaker bug alone for green days
# Per session log: May 1-18 retrofit was +$731 vs actual -$858 = $1,589 swing
RETROFIT_MAY_PROJ = total_pts * 5 * 0.60  # 60% capture rate (PROJECT_BRAIN baseline)
print(f"\n  Retrofit estimate (60% capture rate on V16 sim): ${RETROFIT_MAY_PROJ:+.2f}")
print(f"  Retrofit estimate (75% capture rate):              ${total_pts*5*0.75:+.2f}")
print(f"  Actual broker May 1-20:                            ${may_actual_total:+.2f}")
print(f"  GAP (60% capture):                                 ${RETROFIT_MAY_PROJ - may_actual_total:+.2f}")
print(f"  GAP (75% capture):                                 ${total_pts*5*0.75 - may_actual_total:+.2f}")

# === Q3: [MES-sim] MES-sim honesty for today's PLACED trades ===
print()
print("=" * 70)
print("Q3: [MES-sim] MES-sim badge honesty — compare to actual broker today")
print("=" * 70)
cur.execute("""
    SELECT rto.setup_log_id, sl.setup_name, sl.direction,
           sl.outcome_pnl, sl.mes_sim_outcome_pnl, rto.state
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE rto.created_at::date = '2026-05-20'
      AND rto.state->>'status' = 'closed'
      AND sl.mes_sim_outcome_pnl IS NOT NULL
    ORDER BY rto.setup_log_id
""")
chain_errors = []
mes_errors = []
for sid, name, dir_, chain, mes, state in cur.fetchall():
    if isinstance(state, str): state = json.loads(state)
    fill = state.get("fill_price")
    close = state.get("close_fill_price")
    if fill is None or close is None:
        continue
    is_long = dir_ in ("long", "bullish")
    broker_pts = (float(close) - float(fill)) * (1 if is_long else -1)
    chain_f = float(chain) if chain is not None else None
    mes_f = float(mes) if mes is not None else None
    if chain_f is not None:
        chain_errors.append(abs(broker_pts - chain_f))
    if mes_f is not None:
        mes_errors.append(abs(broker_pts - mes_f))
    cstr = f"{chain_f:+.1f}" if chain_f is not None else "N/A"
    mstr = f"{mes_f:+.1f}" if mes_f is not None else "N/A"
    print(f"  lid={sid} {name:<14} broker={broker_pts:+6.2f}pt | chain={cstr} (err {abs(broker_pts - (chain_f or 0)):.1f}) | mes={mstr} (err {abs(broker_pts - (mes_f or 0)):.1f})")

if chain_errors and mes_errors:
    print(f"\n  Chain-sim mean error today: {sum(chain_errors)/len(chain_errors):.2f} pt")
    print(f"  MES-sim mean error today:   {sum(mes_errors)/len(mes_errors):.2f} pt")
    if sum(mes_errors)/len(mes_errors) < sum(chain_errors)/len(chain_errors):
        print(f"  --> MES-sim IS more honest today by {(sum(chain_errors)-sum(mes_errors))/len(chain_errors):.2f} pt")
    else:
        print(f"  --> Chain-sim is more honest today (MES-sim worse by {(sum(mes_errors)-sum(chain_errors))/len(chain_errors):.2f} pt)")

cur.close(); c.close()
