"""Check today's SIM auto-trader activity."""
import os, json
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
conn = engine.connect()

# Today's setup_log entries
rows = conn.execute(text("""
    SELECT id, setup_name, direction, grade, score, spot, outcome_result, outcome_pnl,
           outcome_max_profit, greek_alignment, spot_vol_beta, ts,
           outcome_first_event
    FROM setup_log
    WHERE ts::date = '2026-03-04'
    ORDER BY ts
""")).fetchall()

print(f"Today Mar 4: {len(rows)} signals")
print()
print(f"{'ID':<5} {'Setup':<18} {'Dir':<8} {'Gr':<4} {'Scr':<4} {'SPX':<8} {'Align':<6} {'SVB':<7} {'Result':<8} {'PnL':<8} {'1st Evt':<8} {'MaxP':<6} {'Time'}")
print("-" * 120)
total_pnl = 0
for r in rows:
    rid, nm, dr, gr, sc, sp, res, pnl, mp, al, svb, ts, fe = r
    pnl_val = pnl or 0
    total_pnl += pnl_val
    al_str = f"{al:+d}" if al is not None else "--"
    svb_str = f"{svb:+.2f}" if svb is not None else "--"
    mp_str = f"{mp:.1f}" if mp is not None else "--"
    t = str(ts)[11:16]
    print(f"{rid:<5} {nm:<18} {dr:<8} {gr:<4} {sc:<4} {sp:<8.1f} {al_str:<6} {svb_str:<7} {res or 'OPEN':<8} {pnl_val:>+7.1f} {fe or '--':<8} {mp_str:<6} {t}")
print("-" * 120)
print(f"Total PnL today (all signals): {total_pnl:+.1f} pts")

# Check auto_trade_orders for today
print()
print("=== AUTO-TRADE ORDERS (SIM) ===")
try:
    orders = conn.execute(text("""
        SELECT setup_log_id, state, created_at FROM auto_trade_orders
        ORDER BY created_at DESC
        LIMIT 30
    """)).fetchall()
    today_orders = []
    for o in orders:
        sid = o[0]
        st = json.loads(o[1]) if isinstance(o[1], str) else o[1]
        created = str(o[2])[:16]
        if '2026-03-04' in created or st.get('status') != 'closed':
            status = st.get('status', '?')
            setup = st.get('setup_name', '?')
            dirn = st.get('direction', '?')
            t1 = st.get('t1_status', '?')
            t2 = st.get('t2_status', '?')
            stop_st = st.get('stop_status', '?')
            rpnl = st.get('realized_pnl', '?')
            print(f"  log_id={sid} {setup:<18} {dirn:<6} status={status} t1={t1} t2={t2} stop={stop_st} pnl={rpnl} created={created}")
            today_orders.append((sid, st))
    if not today_orders:
        print("  No orders found for today")
except Exception as e:
    print(f"  Error reading orders: {e}")

# Check Railway logs hint - what was skipped by Greek filter
print()
print("=== GREEK FILTER ANALYSIS ===")
for r in rows:
    rid, nm, dr, gr, sc, sp, res, pnl, mp, al, svb, ts, fe = r
    al_str = f"{al:+d}" if al is not None else "?"
    would_skip = False
    reasons = []

    if al is not None:
        # F1: Charm alignment
        is_long = dr in ('long', 'bullish')
        # alignment < 0 for longs or > 0 for shorts suggests charm opposes
        # (this is approximate - we don't have raw charm value)

        # F2: GEX Long alignment gate
        if nm == 'GEX Long' and al < 1:
            would_skip = True
            reasons.append(f"F2: alignment {al:+d} < +1")

        # F3: AG Short total misalignment
        if nm == 'AG Short' and al == -3:
            would_skip = True
            reasons.append("F3: alignment -3")

        # F4: DD Exhaustion
        if nm == 'DD Exhaustion':
            if svb is not None and -0.5 <= svb < 0:
                would_skip = True
                reasons.append(f"F4: SVB weak-neg {svb:+.2f}")

    skip_str = "BLOCKED" if would_skip else "PASSED"
    reason_str = " | ".join(reasons) if reasons else ""
    t = str(ts)[11:16]
    print(f"  #{rid} {nm:<18} align={al_str} -> {skip_str} {reason_str}")

conn.close()
