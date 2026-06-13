"""Why does portal show +53pts (~$265) but TSRT broker shows +$388.50?

Compare per-trade:
  - real_trade_orders.state.fill_price vs close_fill_price (broker P&L per trade)
  - setup_log.outcome_pnl (SPX-side label, what portal P&L bar uses)
  - Sum each side and identify the gap.
"""
import os, psycopg2, json

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
    SELECT rto.setup_log_id, sl.setup_name, sl.direction, sl.outcome_pnl,
           sl.outcome_result, rto.state
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE rto.created_at::date = '2026-05-20'
      AND rto.state->>'status' = 'closed'
    ORDER BY rto.setup_log_id
""")

print(f"{'lid':<5} {'setup':<14} {'dir':<8} {'fill':>8} {'close':>8} {'real_pts':>9} {'real_$':>8} {'sl_pts':>7} {'sl_$':>7}")
print("=" * 92)

real_pnl_dollars = 0.0   # broker truth (fill -> close, × $5/pt, signed by dir)
sl_pnl_dollars = 0.0     # portal/setup_log label
real_pnl_pts = 0.0
sl_pnl_pts = 0.0
unresolved_close = []

for sid, name, dir_, sl_pnl, sl_res, state in cur.fetchall():
    if isinstance(state, str):
        state = json.loads(state)
    fill = state.get("fill_price")
    close = state.get("close_fill_price")
    qty = state.get("quantity") or 1
    sl_pnl_f = float(sl_pnl) if sl_pnl is not None else 0.0
    sl_pnl_dollars_t = sl_pnl_f * 5.0 * qty
    sl_pnl_dollars += sl_pnl_dollars_t
    sl_pnl_pts += sl_pnl_f

    real_pts = None
    real_dollars = None
    if fill is not None and close is not None:
        # For long: P&L = close - fill (in MES pts) × $5 × qty
        # For short: P&L = fill - close
        is_long = dir_ in ("long", "bullish")
        real_pts = (float(close) - float(fill)) * (1 if is_long else -1)
        real_dollars = real_pts * 5.0 * qty
        real_pnl_pts += real_pts
        real_pnl_dollars += real_dollars
    else:
        unresolved_close.append(sid)

    fill_s = f"{fill:.2f}" if fill is not None else "  N/A "
    close_s = f"{close:.2f}" if close is not None else "  N/A "
    rp = f"{real_pts:+.2f}" if real_pts is not None else "  ghost"
    rd = f"{real_dollars:+.2f}" if real_dollars is not None else "       "
    print(f"{sid:<5} {name:<14} {dir_:<8} {fill_s:>8} {close_s:>8} {rp:>9} {rd:>8} {sl_pnl_f:+.2f} {sl_pnl_dollars_t:+.2f}")

print("=" * 92)
print(f"\nTOTALS:")
print(f"  Broker truth (fill-to-close):  {real_pnl_pts:+.1f} pts  =  ${real_pnl_dollars:+.2f}")
print(f"  Portal setup_log.outcome_pnl:  {sl_pnl_pts:+.1f} pts  =  ${sl_pnl_dollars:+.2f}")
print(f"  Gap (broker - portal):                       ${real_pnl_dollars - sl_pnl_dollars:+.2f}")
print(f"\n  4 ghost_reconcile trades have NULL close_fill_price (S159 backfill pending):")
print(f"    {unresolved_close}")
print(f"  These don't add to 'Broker truth' line above. Their absence is ~half the gap.")

# Show what portal V16 dropdown would total — same data but only V16-eligible trades?
# Portal V16's filter is in JS; without replicating, the user said +53pts is the portal V16 filter total.
print(f"\n  User reported portal +53 pts (~$265) — likely V16 filter view")
print(f"  TS broker BalanceDetail RealizedProfitLoss: ~+$388.50")
print(f"  Likely gaps:")
print(f"    1. Portal V16 filters DROP some trades that real broker actually placed (cap=2 era,")
print(f"       grade gates, etc) — these contribute to broker P&L but not V16 filtered sim")
print(f"    2. The 3-MES atomic margin test (~+$0.15 net) is in broker but not real_trade_orders")
print(f"    3. SPX-vs-MES divergence on trail exits — broker gets MES fills, portal sums SPX outcomes")

cur.close(); c.close()
