"""
GEX-LIS shorts: Vanna analysis with CORRECT column names.
expiration_option values: TODAY, THIS_WEEK, THIRTY_NEXT_DAYS, ALL
"""
import sqlalchemy as sa
import os

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
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

    print(f"GEX-LIS SC/DD shorts: {len(gex_lis)} trades\n")

    enriched = []
    for r in gex_lis:
        # Get closest vanna snapshot — no date filter, just <= trade time
        sums = c.execute(sa.text("""
            SELECT
                SUM(CASE WHEN greek='vanna' AND expiration_option='TODAY' THEN value ELSE 0 END) as v_0dte,
                SUM(CASE WHEN greek='vanna' AND expiration_option='THIS_WEEK' THEN value ELSE 0 END) as v_weekly,
                SUM(CASE WHEN greek='vanna' AND expiration_option='THIRTY_NEXT_DAYS' THEN value ELSE 0 END) as v_monthly,
                SUM(CASE WHEN greek='vanna' AND expiration_option='ALL' THEN value ELSE 0 END) as v_all,
                SUM(CASE WHEN greek='charm' AND expiration_option='TODAY' THEN value ELSE 0 END) as charm_0dte
            FROM volland_exposure_points
            WHERE ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE ts_utc <= :ts AND greek = 'vanna'
            )
        """), {"ts": r.ts}).fetchone()

        v0 = float(sums.v_0dte) if sums and sums.v_0dte else None
        vw = float(sums.v_weekly) if sums and sums.v_weekly else None
        vm = float(sums.v_monthly) if sums and sums.v_monthly else None
        va = float(sums.v_all) if sums and sums.v_all else None
        ch = float(sums.charm_0dte) if sums and sums.charm_0dte else None

        enriched.append({"r": r, "v0": v0, "vw": vw, "vm": vm, "va": va, "charm": ch})

    # Display
    print("="*160)
    print("GEX-LIS: FULL VANNA + CHARM (aggregated per-strike sums)")
    print("="*160)
    for t in enriched:
        r = t["r"]
        p = r.outcome_pnl or 0
        mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
        def fmt(v):
            return f"{v/1e6:+.0f}M" if v is not None and v != 0 else "n/a"
        print(f"  #{r.id:4d} {r.trade_date} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f} mxP={mxp:>6s} | v0={fmt(t['v0']):>8s} vW={fmt(t['vw']):>8s} vM={fmt(t['vm']):>8s} vAll={fmt(t['va']):>8s} | charm={fmt(t['charm']):>8s} | vix={r.vix:.1f}")

    # Winners vs Losers
    print(f"\n{'='*160}")
    print("WINNERS vs LOSERS")
    print("="*160)
    winners = [t for t in enriched if t["r"].outcome_result == "WIN"]
    losers = [t for t in enriched if t["r"].outcome_result == "LOSS"]

    for label, group in [("WINNERS", winners), ("LOSERS", losers)]:
        print(f"\n  {label} ({len(group)}):")
        for k, name in [("v0", "Vanna 0DTE"), ("vw", "Vanna Weekly"), ("vm", "Vanna Monthly"),
                         ("va", "Vanna ALL"), ("charm", "Charm 0DTE")]:
            vals = [t[k] for t in group if t[k] is not None and t[k] != 0]
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

    print(f"\n{'='*160}")
    print("FILTER TESTS ON GEX-LIS SHORTS")
    print("="*160)

    stats(enriched, "Baseline (all GEX-LIS)")

    filters = {
        "v_0dte < 0": lambda t: t["v0"] is not None and t["v0"] < 0,
        "v_0dte >= 0": lambda t: t["v0"] is not None and t["v0"] >= 0,
        "v_weekly < 0": lambda t: t["vw"] is not None and t["vw"] < 0,
        "v_weekly >= 0": lambda t: t["vw"] is not None and t["vw"] >= 0,
        "v_monthly < 0": lambda t: t["vm"] is not None and t["vm"] < 0,
        "v_monthly >= 0": lambda t: t["vm"] is not None and t["vm"] >= 0,
        "v_all < 0": lambda t: t["va"] is not None and t["va"] < 0,
        "v_all >= 0": lambda t: t["va"] is not None and t["va"] >= 0,
        "charm < 0": lambda t: t["charm"] is not None and t["charm"] < 0,
        "charm >= 0": lambda t: t["charm"] is not None and t["charm"] >= 0,
    }
    for fname, fn in filters.items():
        kept = [t for t in enriched if fn(t)]
        if kept:
            stats(kept, fname)

    # Today
    print(f"\n{'='*160}")
    print("TODAY GEX-LIS: FULL DATA")
    print("="*160)
    today = [t for t in enriched if t["r"].trade_date.month == 3 and t["r"].trade_date.day == 20]
    for t in today:
        r = t["r"]
        p = r.outcome_pnl or 0
        def fmt(v):
            return f"{v/1e6:+.0f}M" if v is not None and v != 0 else "0"
        checks = {}
        for k, name in [("v0","v0"), ("vw","vW"), ("vm","vM"), ("va","vA"), ("charm","ch")]:
            v = t[k]
            if v is not None and v != 0:
                checks[name] = f"{v/1e6:+.0f}M"
            else:
                checks[name] = "0"
        print(f"  #{r.id} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f}pts | v0={checks['v0']:>8s} vW={checks['vW']:>8s} vM={checks['vM']:>8s} vAll={checks['vA']:>8s} charm={checks['ch']:>8s}")
