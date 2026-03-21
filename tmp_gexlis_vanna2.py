"""
GEX-LIS vanna: Get latest value for EACH expiration_option independently.
The scraper captures different exposure types at slightly different timestamps.
"""
import sqlalchemy as sa
import os

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    # First verify: how many distinct ts_utc per greek+option combo in one cycle?
    print("=== EXPOSURE TIMESTAMPS (today, last 5 per type) ===")
    for opt in ['TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL']:
        ts = c.execute(sa.text("""
            SELECT DISTINCT ts_utc FROM volland_exposure_points
            WHERE greek='vanna' AND expiration_option=:opt AND ts_utc::date='2026-03-20'
            ORDER BY ts_utc DESC LIMIT 3
        """), {"opt": opt}).fetchall()
        print(f"  vanna {opt:20s}: {[str(t[0])[11:19] for t in ts]}")

    # Get GEX-LIS trades
    gex_lis = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date,
               outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE setup_name IN ('Skew Charm', 'DD Exhaustion')
          AND direction IN ('short', 'bearish')
          AND paradigm = 'GEX-LIS'
          AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    # For each trade, get LATEST of each vanna type independently
    enriched = []
    for r in gex_lis:
        vals = {}
        for greek, opt, key in [
            ('vanna', 'TODAY', 'v0'),
            ('vanna', 'THIS_WEEK', 'vw'),
            ('vanna', 'THIRTY_NEXT_DAYS', 'vm'),
            ('vanna', 'ALL', 'va'),
            ('charm', 'TODAY', 'charm'),
        ]:
            row = c.execute(sa.text("""
                SELECT SUM(value) as total
                FROM volland_exposure_points
                WHERE greek = :g AND expiration_option = :opt
                  AND ts_utc = (
                    SELECT MAX(ts_utc) FROM volland_exposure_points
                    WHERE greek = :g AND expiration_option = :opt
                      AND ts_utc <= :ts
                  )
            """), {"g": greek, "opt": opt, "ts": r.ts}).fetchone()
            vals[key] = float(row.total) if row and row.total else None

        enriched.append({"r": r, **vals})

    # Display
    print(f"\n{'='*170}")
    print(f"GEX-LIS: VANNA (each type latest independently) — {len(enriched)} trades")
    print("="*170)
    def fmt(v):
        return f"{v/1e6:+.0f}M" if v is not None else "n/a"

    for t in enriched:
        r = t["r"]
        p = r.outcome_pnl or 0
        mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
        print(f"  #{r.id:4d} {r.trade_date} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f} mxP={mxp:>6s} | "
              f"v0={fmt(t['v0']):>8s} vW={fmt(t['vw']):>8s} vM={fmt(t['vm']):>8s} vAll={fmt(t['va']):>8s} | charm={fmt(t['charm']):>8s} | vix={r.vix:.1f}")

    # Winners vs Losers
    print(f"\n{'='*170}")
    print("WINNERS vs LOSERS: FULL VANNA COMPARISON")
    print("="*170)
    winners = [t for t in enriched if t["r"].outcome_result == "WIN"]
    losers = [t for t in enriched if t["r"].outcome_result == "LOSS"]

    for label, group in [("WINNERS", winners), ("LOSERS", losers)]:
        print(f"\n  {label} ({len(group)}):")
        for k, name in [("v0","Vanna 0DTE"), ("vw","Vanna Weekly"), ("vm","Vanna Monthly"),
                         ("va","Vanna ALL"), ("charm","Charm 0DTE")]:
            vals = [t[k] for t in group if t[k] is not None]
            if vals:
                avg = sum(vals)/len(vals)
                print(f"    {name:15s}: avg={avg/1e6:+8.0f}M  min={min(vals)/1e6:+8.0f}M  max={max(vals)/1e6:+8.0f}M  n={len(vals)}/{len(group)}")
            else:
                print(f"    {name:15s}: no data")

    # Filter tests
    def stats(subset, label):
        if not subset:
            print(f"  {label}: 0 trades")
            return
        tlist = [t["r"] for t in subset]
        w = sum(1 for t in tlist if t.outcome_result == 'WIN')
        lo = sum(1 for t in tlist if t.outcome_result == 'LOSS')
        ex = sum(1 for t in tlist if t.outcome_result == 'EXPIRED')
        pnl = round(sum(t.outcome_pnl or 0 for t in tlist), 1)
        wr = round(w/(w+lo)*100,1) if w+lo > 0 else 0
        print(f"  {label}: {len(tlist)}t, {w}W/{lo}L/{ex}E, WR={wr}%, PnL={pnl:+.1f}")

    print(f"\n{'='*170}")
    print("FILTER TESTS (GEX-LIS only)")
    print("="*170)
    stats(enriched, "Baseline (all GEX-LIS)")

    filters = {
        "v0 < 0 (bearish 0DTE)": lambda t: t["v0"] is not None and t["v0"] < 0,
        "v0 >= 0 (bullish 0DTE)": lambda t: t["v0"] is not None and t["v0"] >= 0,
        "vW < 0 (bearish weekly)": lambda t: t["vw"] is not None and t["vw"] < 0,
        "vW >= 0 (bullish weekly)": lambda t: t["vw"] is not None and t["vw"] >= 0,
        "vM < 0 (bearish monthly)": lambda t: t["vm"] is not None and t["vm"] < 0,
        "vM >= 0 (bullish monthly)": lambda t: t["vm"] is not None and t["vm"] >= 0,
        "vAll < 0": lambda t: t["va"] is not None and t["va"] < 0,
        "vAll >= 0": lambda t: t["va"] is not None and t["va"] >= 0,
        "charm < 0 (bearish)": lambda t: t["charm"] is not None and t["charm"] < 0,
        "charm >= 0 (bullish)": lambda t: t["charm"] is not None and t["charm"] >= 0,
        "charm < -100M": lambda t: t["charm"] is not None and t["charm"] < -100_000_000,
        "charm >= -100M (or n/a)": lambda t: t["charm"] is None or t["charm"] >= -100_000_000,
    }
    for fname, fn in filters.items():
        kept = [t for t in enriched if fn(t)]
        if kept:
            stats(kept, fname)

    # Today
    print(f"\n{'='*170}")
    print("TODAY GEX-LIS: ALL VANNA DATA")
    print("="*170)
    today = [t for t in enriched if t["r"].trade_date.month == 3 and t["r"].trade_date.day == 20]
    for t in today:
        r = t["r"]
        p = r.outcome_pnl or 0
        print(f"  #{r.id} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f}pts | v0={fmt(t['v0']):>8s} vW={fmt(t['vw']):>8s} vM={fmt(t['vm']):>8s} vAll={fmt(t['va']):>8s} charm={fmt(t['charm']):>8s}")
