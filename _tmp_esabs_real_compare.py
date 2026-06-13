"""ES Abs real_trade_orders vs portal vs MES-sim comparison post-C6."""
import os
import json
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DATABASE_URL"])

with eng.connect() as c:
    # Get all ES Abs real trades post-C6 with full state
    r = c.execute(text("""
        SELECT
            r.setup_log_id,
            r.state,
            r.created_at AT TIME ZONE 'America/New_York' AS et,
            sl.direction,
            sl.outcome_pnl AS portal_pnl,
            sl.outcome_result AS portal_result,
            sl.mes_sim_outcome_pnl,
            sl.mes_sim_outcome_result
        FROM real_trade_orders r
        JOIN setup_log sl ON sl.id = r.setup_log_id
        WHERE sl.setup_name='ES Absorption'
          AND sl.ts >= '2026-05-06'
        ORDER BY r.created_at
    """))

    rows = list(r)
    print(f"=== ES Abs real_trade_orders post-C6: {len(rows)} rows ===\n")

    real_pnl_pts_total = 0.0
    portal_total = 0.0
    mes_sim_total = 0.0
    n_real_closed = 0
    n_real_open = 0
    samples = []

    for row in rows:
        d = dict(row._mapping)
        st = d["state"] if isinstance(d["state"], dict) else json.loads(d["state"])
        status = st.get("status")
        direction = d["direction"]
        fill = st.get("fill_price")
        # Try multiple close-price fields
        close = (st.get("stop_fill_price")
                 or st.get("target_fill_price")
                 or st.get("close_fill_price")
                 or st.get("flatten_fill_price")
                 or st.get("exit_price"))
        close_reason = st.get("close_reason")

        if status == "closed" and fill is not None and close is not None:
            n_real_closed += 1
            sign = 1 if direction in ("long", "bullish") else -1
            real_pnl = sign * (float(close) - float(fill))
            real_pnl_pts_total += real_pnl
            portal_total += float(d["portal_pnl"] or 0)
            mes_sim_total += float(d["mes_sim_outcome_pnl"] or 0)
            if len(samples) < 25:
                samples.append({
                    "lid": d["setup_log_id"],
                    "et": d["et"].strftime("%m-%d %H:%M"),
                    "dir": direction[:1].upper(),
                    "fill": float(fill),
                    "close": float(close),
                    "real_pts": round(real_pnl, 2),
                    "portal_pts": float(d["portal_pnl"] or 0),
                    "mes_sim_pts": float(d["mes_sim_outcome_pnl"] or 0) if d["mes_sim_outcome_pnl"] else None,
                    "reason": close_reason,
                })
        elif status == "open":
            n_real_open += 1
        else:
            # closed but missing fill/close — print to see
            print(f"  unhandled lid={d['setup_log_id']} status={status} fill={fill} close={close} reason={close_reason}")

    print(f"\n=== Per-trade detail ({len(samples)} of {n_real_closed} closed real trades) ===")
    for s in samples:
        print(s)

    print(f"\n=== Totals (post-C6: 2026-05-06 to today) ===")
    print(f"  Real-broker closed trades   : {n_real_closed}")
    print(f"  Real-broker open / unresolved: {n_real_open}")
    print(f"  Real total P&L (pts)        : {real_pnl_pts_total:+.1f}")
    print(f"  Portal total P&L (pts)      : {portal_total:+.1f}")
    print(f"  MES-sim total P&L (pts)     : {mes_sim_total:+.1f}")
    print(f"  Gap (real - portal)         : {real_pnl_pts_total - portal_total:+.1f}")
    print(f"  Gap (real - mes_sim)        : {real_pnl_pts_total - mes_sim_total:+.1f}")
