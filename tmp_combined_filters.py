"""
Combined filter tests for SC/DD shorts.
Top findings from study:
- VIX >= 25 shorts: 47.5% WR, -134.3 pts (STRONGEST)
- GEX-LIS shorts: 33.3% WR, -87.6 pts (CLEANEST)
- Drop > 40pts: 45.8% WR, -102.4 pts
- Range < 20%: 54.3% WR, -14.3 pts
"""
import sqlalchemy as sa
import os
from collections import defaultdict
from datetime import datetime

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, score, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, charm_limit_entry,
               outcome_max_profit, outcome_max_loss,
               lis, target, ts::date as trade_date
        FROM setup_log
        WHERE setup_name IN ('Skew Charm', 'DD Exhaustion')
          AND direction IN ('short', 'bearish')
          AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    # V9-SC filter
    def passes_v9sc(name, direction, align, vix_val, overvix_val):
        is_long = direction in ("long", "bullish")
        al = align or 0
        if is_long:
            if al < 2: return False
            if name == "Skew Charm": return True
            if vix_val is not None and vix_val > 22:
                ov = overvix_val if overvix_val is not None else -99
                if ov < 2: return False
            return True
        else:
            if name in ("Skew Charm", "AG Short"): return True
            if name == "DD Exhaustion" and al != 0: return True
            return False

    live = [r for r in rows if passes_v9sc(r.setup_name, r.direction, r.greek_alignment or 0, r.vix, r.overvix)]

    # Build session context (drop from high, range position)
    all_spots = c.execute(sa.text("""
        SELECT ts::date as d, ts, spot FROM setup_log
        WHERE outcome_result IS NOT NULL ORDER BY ts
    """)).fetchall()
    date_spots = defaultdict(list)
    for s in all_spots:
        date_spots[s.d].append((s.ts, float(s.spot)))

    trades = []
    for r in live:
        d = r.trade_date
        spots_before = [(ts, sp) for ts, sp in date_spots[d] if ts <= r.ts]
        if spots_before:
            sh = max(sp for _, sp in spots_before)
            sl_val = min(sp for _, sp in spots_before)
            drop = sh - float(r.spot)
            rng = (float(r.spot) - sl_val) / (sh - sl_val) * 100 if sh != sl_val else 50
        else:
            drop = 0
            rng = 50
        trades.append({"r": r, "drop": drop, "range": rng})

    def stats(subset, label=""):
        if not subset:
            print(f"  {label}: 0 trades")
            return {"n": 0, "pnl": 0, "wr": 0}
        tlist = [t["r"] if isinstance(t, dict) else t for t in subset]
        w = sum(1 for t in tlist if t.outcome_result == 'WIN')
        lo = sum(1 for t in tlist if t.outcome_result == 'LOSS')
        ex = sum(1 for t in tlist if t.outcome_result == 'EXPIRED')
        pnl = round(sum(t.outcome_pnl or 0 for t in tlist), 1)
        wr = round(w / (w + lo) * 100, 1) if (w + lo) > 0 else 0
        # Compute max drawdown
        running = 0
        peak = 0
        max_dd = 0
        for t in sorted(tlist, key=lambda x: x.ts):
            running += t.outcome_pnl or 0
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd
        print(f"  {label}: {len(tlist)}t, {w}W/{lo}L/{ex}E, WR={wr}%, PnL={pnl:+.1f}, MaxDD={max_dd:.1f}")
        return {"n": len(tlist), "pnl": pnl, "wr": wr, "max_dd": max_dd}

    print("="*70)
    print("BASELINE: V9-SC SC/DD shorts")
    print("="*70)
    stats(trades, "ALL")

    # ---- SINGLE FILTERS ----
    print("\n" + "="*70)
    print("SINGLE FILTERS")
    print("="*70)

    filters = {
        "VIX < 25": lambda t: t["r"].vix is None or t["r"].vix < 25,
        "VIX < 24": lambda t: t["r"].vix is None or t["r"].vix < 24,
        "Not GEX-LIS": lambda t: t["r"].paradigm != "GEX-LIS",
        "Not BOFA-PURE": lambda t: t["r"].paradigm != "BOFA-PURE",
        "Drop < 30": lambda t: t["drop"] < 30,
        "Drop < 40": lambda t: t["drop"] < 40,
        "Range >= 20%": lambda t: t["range"] >= 20,
        "Range >= 10%": lambda t: t["range"] >= 10,
    }

    for name, fn in filters.items():
        kept = [t for t in trades if fn(t)]
        blocked = [t for t in trades if not fn(t)]
        print(f"\n--- {name} ---")
        stats(kept, "KEPT")
        stats(blocked, "BLOCKED")

    # ---- COMBINED FILTERS ----
    print("\n" + "="*70)
    print("COMBINED FILTERS")
    print("="*70)

    combos = {
        "C1: VIX<25 + Not GEX-LIS": lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["r"].paradigm != "GEX-LIS",
        "C2: VIX<25 + Drop<30": lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["drop"] < 30,
        "C3: VIX<25 + Range>=20%": lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["range"] >= 20,
        "C4: Not GEX-LIS + Drop<30": lambda t: t["r"].paradigm != "GEX-LIS" and t["drop"] < 30,
        "C5: VIX<25 + Not GEX-LIS + Drop<30": lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["r"].paradigm != "GEX-LIS" and t["drop"] < 30,
        "C6: VIX<25 + Not BOFA-PURE": lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["r"].paradigm != "BOFA-PURE",
        "C7: VIX<25 + Not(GEX-LIS or BOFA-PURE)": lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["r"].paradigm not in ("GEX-LIS", "BOFA-PURE"),
        "C8: Not(GEX-LIS or BOFA-PURE)": lambda t: t["r"].paradigm not in ("GEX-LIS", "BOFA-PURE"),
    }

    for name, fn in combos.items():
        kept = [t for t in trades if fn(t)]
        blocked = [t for t in trades if not fn(t)]
        print(f"\n--- {name} ---")
        stats(kept, "KEPT")
        stats(blocked, "BLOCKED")

    # ---- SC vs DD SPLIT for best filter ----
    print("\n" + "="*70)
    print("BEST FILTERS: SC vs DD DETAIL")
    print("="*70)
    for fname, fn in [("VIX<25", lambda t: t["r"].vix is None or t["r"].vix < 25),
                       ("Not GEX-LIS", lambda t: t["r"].paradigm != "GEX-LIS"),
                       ("C1: VIX<25 + Not GEX-LIS", lambda t: (t["r"].vix is None or t["r"].vix < 25) and t["r"].paradigm != "GEX-LIS")]:
        print(f"\n--- {fname} ---")
        sc_k = [t for t in trades if fn(t) and t["r"].setup_name == "Skew Charm"]
        sc_b = [t for t in trades if not fn(t) and t["r"].setup_name == "Skew Charm"]
        dd_k = [t for t in trades if fn(t) and t["r"].setup_name == "DD Exhaustion"]
        dd_b = [t for t in trades if not fn(t) and t["r"].setup_name == "DD Exhaustion"]
        stats(sc_k, "SC kept")
        stats(sc_b, "SC blocked")
        stats(dd_k, "DD kept")
        stats(dd_b, "DD blocked")

    # ---- DAILY PnL COMPARISON ----
    print("\n" + "="*70)
    print("DAILY PnL: V9-SC vs V9-SC + VIX<25")
    print("="*70)
    by_date = defaultdict(lambda: {"base": 0, "filtered": 0})
    for t in trades:
        d = t["r"].trade_date
        p = t["r"].outcome_pnl or 0
        by_date[d]["base"] += p
        if t["r"].vix is None or t["r"].vix < 25:
            by_date[d]["filtered"] += p

    losing_days_base = 0
    losing_days_filt = 0
    for d in sorted(by_date.keys()):
        b = by_date[d]["base"]
        f = by_date[d]["filtered"]
        diff = f - b
        flag = " ***" if diff > 10 or diff < -10 else ""
        print(f"  {d}: base={b:+.1f} filtered={f:+.1f} diff={diff:+.1f}{flag}")
        if b < 0: losing_days_base += 1
        if f < 0: losing_days_filt += 1
    print(f"\n  Losing days: base={losing_days_base}, filtered={losing_days_filt}")

    # ---- TODAY SIMULATION ----
    print("\n" + "="*70)
    print("TODAY (2026-03-20): WHAT EACH FILTER WOULD DO")
    print("="*70)
    today = [t for t in trades if t["r"].trade_date == datetime(2026, 3, 20).date()]
    print(f"Today's V9-SC shorts: {len(today)}")
    for t in today:
        r = t["r"]
        checks = {
            "VIX<25": r.vix is None or r.vix < 25,
            "!GEX-LIS": r.paradigm != "GEX-LIS",
            "Drop<30": t["drop"] < 30,
            "Rng>=20": t["range"] >= 20,
        }
        flags = " ".join(f"{'PASS' if v else 'BLOCK':5s}" for v in checks.values())
        print(f"  #{r.id} {r.setup_name:15s} {r.outcome_result:8s} {r.outcome_pnl or 0:+.1f}pts vix={r.vix} par={r.paradigm} drop={t['drop']:.0f} rng={t['range']:.0f}% | {' | '.join(f'{k}={v}' for k,v in checks.items())}")
