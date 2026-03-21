"""
V8 Improvement Candidates — test incremental value of each on V8-filtered trades.

Candidates:
1. Remove ES Absorption (break-even, adds noise)
2. VIX lower bound (block when VIX < 20)
3. DD shift $500M minimum threshold
4. Time-of-day per-setup (AG not at 16:00 UTC, DD not at 19:00 UTC)
5. DD concentration filter (block when DD concentration > 75%)
"""

import os
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import datetime

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

def passes_v8(setup_name, direction, alignment, vix):
    """Returns True if this trade passes V8 filter."""
    align = alignment if alignment is not None else 0
    is_long = direction in ("long", "bullish")

    if is_long:
        if align < 2:
            return False
        # VIX gate: block longs when VIX > 26 (overvix check omitted - was always < +2)
        if vix is not None and vix > 26:
            return False
        return True
    else:
        # Shorts whitelist
        if setup_name == "Skew Charm":
            return True
        if setup_name == "AG Short":
            return True
        if setup_name == "DD Exhaustion":
            return align != 0
        return False

with engine.connect() as conn:
    # Pull all resolved trades
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, spot, greek_alignment as alignment,
               outcome_result, outcome_pnl, ts, vix, grade, score,
               EXTRACT(HOUR FROM ts) as hour_utc,
               abs_details
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
        ORDER BY ts
    """)).fetchall()

    print(f"Total trades with outcomes: {len(rows)}")

    # Filter to V8-passing trades
    v8_trades = []
    for r in rows:
        vix_val = float(r.vix) if r.vix is not None else None
        if passes_v8(r.setup_name, r.direction, r.alignment, vix_val):
            v8_trades.append({
                "id": r.id,
                "setup": r.setup_name,
                "direction": r.direction,
                "spot": float(r.spot) if r.spot else None,
                "alignment": r.alignment,
                "outcome": r.outcome_result,
                "pnl": float(r.outcome_pnl) if r.outcome_pnl else 0,
                "ts": r.ts,
                "vix": float(r.vix) if r.vix is not None else None,
                "grade": r.grade,
                "score": float(r.score) if r.score is not None else 0,
                "hour_utc": int(r.hour_utc) if r.hour_utc is not None else None,
            })

    def calc_metrics(trades, label=""):
        n = len(trades)
        if n == 0:
            return {"n": 0, "wr": 0, "pnl": 0, "pf": 0}
        wins = sum(1 for t in trades if t["outcome"] == "WIN")
        wr = wins / n * 100
        pnl = sum(t["pnl"] for t in trades)
        gross_w = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_l = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        pf = gross_w / gross_l if gross_l > 0 else float('inf')

        # Max drawdown
        cumulative = 0
        peak = 0
        max_dd = 0
        for t in sorted(trades, key=lambda x: x["ts"]):
            cumulative += t["pnl"]
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd

        return {"n": n, "wr": wr, "pnl": pnl, "pf": pf, "max_dd": max_dd}

    # ── V8 Baseline ──
    base = calc_metrics(v8_trades)
    print(f"\n{'='*80}")
    print(f"V8 BASELINE: {base['n']} trades, {base['wr']:.1f}% WR, {base['pnl']:+.1f} pts, PF {base['pf']:.2f}, MaxDD {base['max_dd']:.1f}")
    print(f"{'='*80}")

    # ── By setup breakdown ──
    print(f"\n--- V8 by setup ---")
    setups = set(t["setup"] for t in v8_trades)
    for s in sorted(setups):
        st = [t for t in v8_trades if t["setup"] == s]
        m = calc_metrics(st)
        print(f"  {s:20s}: {m['n']:>4} trades, {m['wr']:>5.1f}% WR, {m['pnl']:>+8.1f} pts, PF {m['pf']:.2f}")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 1: Remove ES Absorption
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 1: Remove ES Absorption entirely")
    print(f"{'='*80}")

    c1 = [t for t in v8_trades if t["setup"] != "ES Absorption"]
    c1_removed = [t for t in v8_trades if t["setup"] == "ES Absorption"]
    m1 = calc_metrics(c1)
    mr = calc_metrics(c1_removed)
    print(f"  Without ES Abs: {m1['n']} trades, {m1['wr']:.1f}% WR, {m1['pnl']:+.1f} pts, PF {m1['pf']:.2f}, MaxDD {m1['max_dd']:.1f}")
    print(f"  Removed:        {mr['n']} trades, {mr['wr']:.1f}% WR, {mr['pnl']:+.1f} pts")
    print(f"  Delta PnL:      {m1['pnl'] - base['pnl']:+.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 2: VIX lower bound
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 2: VIX lower bound gates")
    print(f"{'='*80}")

    for vix_floor in [18, 19, 20]:
        c2 = [t for t in v8_trades if t["vix"] is None or t["vix"] >= vix_floor]
        c2_blocked = [t for t in v8_trades if t["vix"] is not None and t["vix"] < vix_floor]
        m2 = calc_metrics(c2)
        mb = calc_metrics(c2_blocked)
        print(f"  VIX >= {vix_floor}: {m2['n']} trades, {m2['wr']:.1f}% WR, {m2['pnl']:+.1f} pts, PF {m2['pf']:.2f}, MaxDD {m2['max_dd']:.1f}")
        print(f"    Blocked: {mb['n']} trades, {mb['wr']:.1f}% WR, {mb['pnl']:+.1f} pts")
        print(f"    Delta:   {m2['pnl'] - base['pnl']:+.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 3: Remove Paradigm Rev + BofA Scalp entirely
    # (they're nearly fully blocked but a few slip through)
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 3: Remove BofA Scalp + Paradigm Rev entirely")
    print(f"{'='*80}")

    c3 = [t for t in v8_trades if t["setup"] not in ("BofA Scalp", "Paradigm Reversal")]
    c3_removed = [t for t in v8_trades if t["setup"] in ("BofA Scalp", "Paradigm Reversal")]
    m3 = calc_metrics(c3)
    mr3 = calc_metrics(c3_removed)
    print(f"  Without BofA/PR: {m3['n']} trades, {m3['wr']:.1f}% WR, {m3['pnl']:+.1f} pts, PF {m3['pf']:.2f}, MaxDD {m3['max_dd']:.1f}")
    print(f"  Removed:         {mr3['n']} trades, {mr3['wr']:.1f}% WR, {mr3['pnl']:+.1f} pts")
    print(f"  Delta PnL:       {m3['pnl'] - base['pnl']:+.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 4: Remove ES Abs + BofA + Paradigm Rev (lean V8)
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 4: Lean V8 (only SC + DD + AG + GEX Long)")
    print(f"{'='*80}")

    core_setups = {"Skew Charm", "DD Exhaustion", "AG Short", "GEX Long"}
    c4 = [t for t in v8_trades if t["setup"] in core_setups]
    c4_removed = [t for t in v8_trades if t["setup"] not in core_setups]
    m4 = calc_metrics(c4)
    mr4 = calc_metrics(c4_removed)
    print(f"  Core only:  {m4['n']} trades, {m4['wr']:.1f}% WR, {m4['pnl']:+.1f} pts, PF {m4['pf']:.2f}, MaxDD {m4['max_dd']:.1f}")
    print(f"  Removed:    {mr4['n']} trades, {mr4['wr']:.1f}% WR, {mr4['pnl']:+.1f} pts")
    print(f"  Delta PnL:  {m4['pnl'] - base['pnl']:+.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 5: VIX 20-26 sweet spot (block both tails)
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 5: VIX 20-26 band (block VIX < 20 AND VIX > 26 longs)")
    print(f"{'='*80}")

    c5 = [t for t in v8_trades if t["vix"] is None or (t["vix"] >= 20)]
    # V8 already blocks longs at VIX>26, so this just adds VIX<20 block
    m5 = calc_metrics(c5)
    print(f"  VIX >= 20:  {m5['n']} trades, {m5['wr']:.1f}% WR, {m5['pnl']:+.1f} pts, PF {m5['pf']:.2f}, MaxDD {m5['max_dd']:.1f}")
    print(f"  Delta PnL:  {m5['pnl'] - base['pnl']:+.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 6: Combo — Lean V8 + VIX >= 20
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 6: COMBO — Lean V8 (SC+DD+AG+GEX) + VIX >= 20")
    print(f"{'='*80}")

    c6 = [t for t in v8_trades
          if t["setup"] in core_setups
          and (t["vix"] is None or t["vix"] >= 20)]
    m6 = calc_metrics(c6)
    print(f"  Combo:      {m6['n']} trades, {m6['wr']:.1f}% WR, {m6['pnl']:+.1f} pts, PF {m6['pf']:.2f}, MaxDD {m6['max_dd']:.1f}")
    print(f"  Delta PnL:  {m6['pnl'] - base['pnl']:+.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 7: DD alignment filter tightening (block align -2 too?)
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 7: DD Exhaustion — tighter alignment filter")
    print(f"{'='*80}")

    dd_trades = [t for t in v8_trades if t["setup"] == "DD Exhaustion"]
    print(f"  DD by alignment:")
    for a in sorted(set(t["alignment"] for t in dd_trades if t["alignment"] is not None)):
        at = [t for t in dd_trades if t["alignment"] == a]
        ma = calc_metrics(at)
        print(f"    align={a:>2}: {ma['n']:>3} trades, {ma['wr']:>5.1f}% WR, {ma['pnl']:>+8.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 8: AG Short alignment detail
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 8: AG Short — alignment detail")
    print(f"{'='*80}")

    ag_trades = [t for t in v8_trades if t["setup"] == "AG Short"]
    print(f"  AG by alignment:")
    for a in sorted(set(t["alignment"] for t in ag_trades if t["alignment"] is not None)):
        at = [t for t in ag_trades if t["alignment"] == a]
        ma = calc_metrics(at)
        print(f"    align={a:>2}: {ma['n']:>3} trades, {ma['wr']:>5.1f}% WR, {ma['pnl']:>+8.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 9: Skew Charm — any improvement possible?
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 9: Skew Charm — alignment and VIX detail")
    print(f"{'='*80}")

    sc_trades = [t for t in v8_trades if t["setup"] == "Skew Charm"]
    print(f"  SC by alignment:")
    for a in sorted(set(t["alignment"] for t in sc_trades if t["alignment"] is not None)):
        at = [t for t in sc_trades if t["alignment"] == a]
        ma = calc_metrics(at)
        print(f"    align={a:>2}: {ma['n']:>3} trades, {ma['wr']:>5.1f}% WR, {ma['pnl']:>+8.1f} pts")

    print(f"\n  SC by VIX range:")
    for vmin, vmax in [(0,20), (20,22), (22,24), (24,26), (26,30)]:
        vt = [t for t in sc_trades if t["vix"] is not None and vmin <= t["vix"] < vmax]
        if vt:
            mv = calc_metrics(vt)
            print(f"    VIX {vmin}-{vmax}: {mv['n']:>3} trades, {mv['wr']:>5.1f}% WR, {mv['pnl']:>+8.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 10: GEX Long — direction detail
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 10: GEX Long — detail (all pass V8 at align >= 2)")
    print(f"{'='*80}")

    gex_trades = [t for t in v8_trades if t["setup"] == "GEX Long"]
    print(f"  GEX Long: {len(gex_trades)} trades")
    for t in gex_trades:
        print(f"    {t['ts'].strftime('%m/%d %H:%M')} | align={t['alignment']:>2} | VIX={t['vix']:.1f if t['vix'] else 'n/a'} | {t['outcome']:4s} {t['pnl']:>+6.1f}")

    # ══════════════════════════════════════════════════════════════
    # CANDIDATE 11: ES Absorption long-only (remove shorts)
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("CANDIDATE 11: ES Absorption — long vs short on V8")
    print(f"{'='*80}")

    esa_trades = [t for t in v8_trades if t["setup"] == "ES Absorption"]
    for d in ["long", "bullish", "short", "bearish"]:
        dt = [t for t in esa_trades if t["direction"] == d]
        if dt:
            md = calc_metrics(dt)
            print(f"  {d:8s}: {md['n']:>3} trades, {md['wr']:>5.1f}% WR, {md['pnl']:>+8.1f} pts")

    # ══════════════════════════════════════════════════════════════
    # FINAL SUMMARY TABLE
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'='*80}")
    print("FINAL COMPARISON TABLE")
    print(f"{'='*80}")
    print(f"{'Strategy':<35} {'N':>5} {'WR':>6} {'PnL':>8} {'PF':>5} {'MaxDD':>7} {'Delta':>7}")
    print("-" * 80)

    results = [
        ("V8 Baseline", base),
        ("C1: No ES Absorption", m1),
        ("C3: No BofA/Paradigm", m3),
        ("C4: Lean (SC+DD+AG+GEX)", m4),
        ("C5: VIX >= 20", m5),
        ("C6: Lean + VIX >= 20", m6),
    ]

    for label, m in results:
        delta = m['pnl'] - base['pnl']
        print(f"  {label:<33} {m['n']:>5} {m['wr']:>5.1f}% {m['pnl']:>+8.1f} {m['pf']:>5.2f} {m['max_dd']:>7.1f} {delta:>+7.1f}")
