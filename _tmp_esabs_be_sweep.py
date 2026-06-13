"""C6.1 BE-trigger sweep on post-C6 ES Abs trades.

For each ES Abs signal since May 6, replay the trade with BE-trigger candidates
{None, 3, 4, 5 (current), 6, 7, 8}. All other params held constant:
  SL=8, trail_act=8, trail_gap=3.

Walks ES range bars (vps_es_range_bars, 5pt) via mes_walk().
"""
import os
import sys
from datetime import timedelta
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.mes_sim_backfill import mes_walk, _fetch_bars_for_window, _first_es_open_after

eng = create_engine(os.environ["DATABASE_URL"])

BE_CANDIDATES = [None, 3, 4, 5, 6, 7, 8]

with eng.begin() as conn:
    rows = conn.execute(text("""
        SELECT sl.id, sl.ts, sl.direction, sl.spot, sl.abs_es_price,
               sl.outcome_pnl, sl.outcome_result,
               (rto.state->>'signal_es_price')::float AS sig_es,
               (rto.state->>'fill_price')::float AS fill_px
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='ES Absorption'
          AND sl.ts >= '2026-02-01' AND sl.ts < '2026-05-22'
          AND sl.outcome_result IS NOT NULL
          AND sl.grade IN ('A', 'A+')
          AND COALESCE(sl.paradigm, '') NOT IN ('AG-TARGET','AG-LIS')
        ORDER BY sl.ts ASC
    """)).fetchall()

print(f"Loaded {len(rows)} ES Abs post-C6 signals")

# Stats per BE candidate
stats = {be: {"n": 0, "wins": 0, "losses": 0, "expired": 0,
              "be_snaps": 0, "pnl": 0.0, "mfes": []} for be in BE_CANDIDATES}

# Per-trade rows for spot-checking
detail = []

for r in rows:
    lid, ts_signal, direction, spot, abs_es, portal_pnl, portal_res, sig_es, fill_px = r
    is_long = (direction or "").lower() in ("long", "bullish")

    # Resolve entry ES price
    entry_es = None
    if abs_es and abs_es > 0:
        entry_es = float(abs_es)
    elif sig_es and sig_es > 0:
        entry_es = float(sig_es)
    elif fill_px and fill_px > 0:
        entry_es = float(fill_px)
    else:
        entry_es = _first_es_open_after(eng, ts_signal, max_wait_minutes=10)
    if entry_es is None:
        continue

    bars = _fetch_bars_for_window(eng, ts_signal, max_minutes=120)
    if not bars:
        continue

    row_d = {"lid": lid, "et": ts_signal.strftime("%m-%d %H:%M"),
             "dir": "L" if is_long else "S",
             "entry_es": entry_es,
             "portal_pnl": float(portal_pnl) if portal_pnl is not None else None}

    for be in BE_CANDIDATES:
        result = mes_walk(
            bars,
            entry_es=entry_es,
            is_long=is_long,
            sl_pts=8.0,
            be_trigger=float(be) if be is not None else None,
            be_lock=0,
            trail_act=8.0,
            trail_gap=3.0,
            max_minutes=90,
        )
        pnl = round(float(result["pnl"]), 2)
        st = stats[be]
        st["n"] += 1
        st["pnl"] += pnl
        st["mfes"].append(float(result["mfe"]))
        if pnl > 0.001:
            st["wins"] += 1
        elif pnl < -0.001:
            st["losses"] += 1
        else:
            st["expired"] += 1
            if result["reason"] != "eod":
                # BE-snap: exited at BE (stop), not at end of window
                st["be_snaps"] += 1
        row_d[f"be{be}"] = pnl
    detail.append(row_d)

# Print summary table
print()
print("=== C6.1 BE-trigger sweep — post-C6 ES Abs (PURE filtered, A/A+) ===")
print(f"{'BE':<8} {'N':>4} {'W':>4} {'L':>4} {'EXP':>4} {'BEsnap':>7} {'PnL':>10} {'avg':>7} {'avg MFE':>9}")
for be in BE_CANDIDATES:
    s = stats[be]
    avg = s["pnl"] / s["n"] if s["n"] else 0
    mfe_avg = sum(s["mfes"]) / len(s["mfes"]) if s["mfes"] else 0
    label = f"BE={be}" if be is not None else "no BE"
    print(f"{label:<8} {s['n']:>4} {s['wins']:>4} {s['losses']:>4} {s['expired']:>4} "
          f"{s['be_snaps']:>7} {s['pnl']:>+10.1f} {avg:>+7.2f} {mfe_avg:>9.2f}")

# Sample per-trade rows for spot check
print("\n=== first 10 trades, per-BE P&L ===")
print(f"{'lid':>5} {'et':<12} {'dir':<4} {'entry_es':>8} {'portal':>7} " +
      " ".join(f"{'BE=' + str(b):>7}" for b in BE_CANDIDATES))
for d in detail[:10]:
    print(f"{d['lid']:>5} {d['et']:<12} {d['dir']:<4} {d['entry_es']:>8.2f} "
          f"{(d['portal_pnl'] if d['portal_pnl'] is not None else 0):>+7.1f} " +
          " ".join(f"{d[f'be{b}']:>+7.1f}" for b in BE_CANDIDATES))

print(f"\nTotal trades simulated: {len(detail)}")
