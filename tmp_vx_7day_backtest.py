"""7-Day Backtest: VX flow vs setup outcomes (Mar 18-25, 2026)
297 setups x 7 days of VX tick data from Sierra Chart."""
import struct, json
from datetime import datetime, timedelta
from collections import defaultdict

SCID_FILE = r"C:\SierraChart\Data\VXM26_FUT_CFE.scid"
SETUPS_FILE = "tmp_setups_full.json"
SC_EPOCH = datetime(1899, 12, 30)
MICROS_PER_DAY = 86_400_000_000


def load_vx_ticks():
    """Load ALL VX ticks, grouped by date."""
    with open(SCID_FILE, "rb") as f:
        f.seek(0, 2)
        n = (f.tell() - 56) // 40
        f.seek(56)
        data = f.read()

    by_date = defaultdict(list)
    for i in range(n):
        dt_raw, o, h, l, c, nt, vol, bv, av = struct.unpack_from("<qffffIIII", data, i * 40)
        if dt_raw <= 0:
            continue
        dt = SC_EPOCH + timedelta(days=dt_raw // MICROS_PER_DAY, microseconds=dt_raw % MICROS_PER_DAY)
        # Skip non-tick records
        if abs(o) > 0.001 and o > -1e30:
            continue
        dt_et = dt - timedelta(hours=4)  # UTC -> ET
        # Market hours only
        if dt_et.hour < 9 or (dt_et.hour == 9 and dt_et.minute < 30) or dt_et.hour >= 16:
            continue
        date_key = dt_et.strftime("%Y-%m-%d")
        by_date[date_key].append({
            "dt_et": dt_et,
            "price": c,
            "volume": vol,
            "delta": int(av) - int(bv),
            "buy": av,
            "sell": bv,
        })
    return by_date


def vx_flow_at(ticks, time_et, window_min=10):
    """Get VX flow in window around time. Returns (net_delta, cvd_from_open, vx_price, vx_chg_30m)."""
    t_start = time_et - timedelta(minutes=window_min)
    t_end = time_et + timedelta(minutes=2)

    window = [t for t in ticks if t_start <= t["dt_et"] <= t_end]
    net = sum(t["delta"] for t in window) if window else 0

    # CVD from market open
    before = [t for t in ticks if t["dt_et"] <= time_et]
    cvd = sum(t["delta"] for t in before)

    # VX price
    price = window[-1]["price"] if window else 0

    # VX 30-min change (is VX rising or falling?)
    t_30_ago = time_et - timedelta(minutes=30)
    past = [t for t in ticks if t_30_ago <= t["dt_et"] <= t_30_ago + timedelta(minutes=5)]
    price_30_ago = past[-1]["price"] if past else price
    vx_chg = price - price_30_ago

    return net, cvd, price, vx_chg


def classify_vx_regime(ticks, time_et):
    """Classify VX regime at this moment.
    Returns: 'SELL_REGIME', 'BUY_REGIME', or 'MIXED'
    Based on 30-min rolling delta + CVD direction."""
    t_start = time_et - timedelta(minutes=30)
    window = [t for t in ticks if t_start <= t["dt_et"] <= time_et]
    if not window:
        return "UNKNOWN", 0

    net = sum(t["delta"] for t in window)
    buy = sum(t["buy"] for t in window)
    sell = sum(t["sell"] for t in window)
    total = buy + sell
    sell_pct = (sell / total * 100) if total > 0 else 50

    if sell_pct >= 60:
        return "SELL_REGIME", net  # vol sellers dominating = bullish SPX
    elif sell_pct <= 40:
        return "BUY_REGIME", net   # vol buyers dominating = bearish SPX
    else:
        return "MIXED", net


def main():
    # Load VX ticks
    vx_by_date = load_vx_ticks()
    print("VX tick data loaded:")
    for d in sorted(vx_by_date.keys()):
        ticks = vx_by_date[d]
        buy = sum(t["buy"] for t in ticks)
        sell = sum(t["sell"] for t in ticks)
        net = sum(t["delta"] for t in ticks)
        total = buy + sell
        sell_pct = sell / total * 100 if total > 0 else 0
        print(f"  {d}: {len(ticks):,} ticks, vol={total:,}, sell={sell_pct:.0f}%, net_delta={net:+,}")

    # Load setups
    with open(SETUPS_FILE) as f:
        setups = json.load(f)
    print(f"\nSetups loaded: {len(setups)}")

    # Cross-reference
    results = []
    skipped = 0
    for s in setups:
        ts = datetime.fromisoformat(s["ts"])
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
        ts_et = ts - timedelta(hours=4)  # UTC -> ET
        date_key = ts_et.strftime("%Y-%m-%d")

        if date_key not in vx_by_date:
            skipped += 1
            continue

        # Skip setups outside market hours (some fire at 16:xx)
        if ts_et.hour < 9 or ts_et.hour >= 16:
            skipped += 1
            continue

        ticks = vx_by_date[date_key]
        net_delta, cvd, vx_price, vx_chg = vx_flow_at(ticks, ts_et)
        regime, regime_delta = classify_vx_regime(ticks, ts_et)

        direction = s["direction"]
        is_long = direction in ("long", "bullish")
        is_short = direction in ("short", "bearish")

        # Alignment logic
        if is_long:
            if regime == "SELL_REGIME":
                vx_align = "ALIGNED"    # vol sellers + long = both bullish
            elif regime == "BUY_REGIME":
                vx_align = "AGAINST"    # vol buyers + long = conflicting
            else:
                vx_align = "NEUTRAL"
        else:
            if regime == "BUY_REGIME":
                vx_align = "ALIGNED"    # vol buyers + short = both bearish
            elif regime == "SELL_REGIME":
                vx_align = "AGAINST"    # vol sellers + short = conflicting
            else:
                vx_align = "NEUTRAL"

        # CVD direction alignment
        cvd_bullish = cvd < -100  # heavy selling from open = bullish SPX
        cvd_bearish = cvd > 100
        if is_long and cvd_bullish:
            cvd_align = "ALIGNED"
        elif is_short and cvd_bearish:
            cvd_align = "ALIGNED"
        elif is_long and cvd_bearish:
            cvd_align = "AGAINST"
        elif is_short and cvd_bullish:
            cvd_align = "AGAINST"
        else:
            cvd_align = "NEUTRAL"

        results.append({
            **s,
            "ts_et": ts_et,
            "vx_regime": regime,
            "vx_net_10m": net_delta,
            "vx_cvd": cvd,
            "vx_price": vx_price,
            "vx_chg_30m": vx_chg,
            "vx_align": vx_align,
            "cvd_align": cvd_align,
        })

    print(f"Matched: {len(results)} setups, Skipped: {skipped} (no VX data / after hours)")

    # ============================================================
    # ANALYSIS 1: VX Regime alignment vs outcome
    # ============================================================
    print("\n" + "=" * 90)
    print("ANALYSIS 1: VX 30-MIN REGIME vs SETUP OUTCOME (297 setups, 7 days)")
    print("=" * 90)

    for bucket in ["ALIGNED", "AGAINST", "NEUTRAL"]:
        group = [r for r in results if r["vx_align"] == bucket]
        if not group:
            continue
        wins = sum(1 for r in group if r["outcome"] == "WIN")
        total = len(group)
        pnl = sum(r["pnl"] for r in group)
        wr = wins / total * 100
        avg_pnl = pnl / total
        print(f"\n  {bucket:8s}: {total:3d} trades | {wins}W/{total-wins}L | "
              f"WR={wr:.1f}% | PnL={pnl:+.0f} pts | avg={avg_pnl:+.1f}/trade")

    # ============================================================
    # ANALYSIS 2: Per-setup breakdown
    # ============================================================
    print("\n" + "=" * 90)
    print("ANALYSIS 2: PER-SETUP VX ALIGNMENT")
    print("=" * 90)

    setup_names = sorted(set(r["setup_name"] for r in results))
    for sn in setup_names:
        group = [r for r in results if r["setup_name"] == sn]
        if len(group) < 3:
            continue
        print(f"\n  {sn} ({len(group)} trades):")
        for bucket in ["ALIGNED", "AGAINST", "NEUTRAL"]:
            sub = [r for r in group if r["vx_align"] == bucket]
            if not sub:
                continue
            wins = sum(1 for r in sub if r["outcome"] == "WIN")
            pnl = sum(r["pnl"] for r in sub)
            wr = wins / len(sub) * 100
            print(f"    {bucket:8s}: {len(sub):2d}t | {wins}W/{len(sub)-wins}L | "
                  f"WR={wr:.0f}% | PnL={pnl:+.0f}")

    # ============================================================
    # ANALYSIS 3: Per-day summary
    # ============================================================
    print("\n" + "=" * 90)
    print("ANALYSIS 3: PER-DAY VX REGIME + SETUP PERFORMANCE")
    print("=" * 90)

    for date_key in sorted(vx_by_date.keys()):
        ticks = vx_by_date[date_key]
        day_setups = [r for r in results if r["ts_et"].strftime("%Y-%m-%d") == date_key]
        if not day_setups:
            continue

        buy = sum(t["buy"] for t in ticks)
        sell = sum(t["sell"] for t in ticks)
        net = sum(t["delta"] for t in ticks)
        total_vol = buy + sell
        sell_pct = sell / total_vol * 100 if total_vol > 0 else 50
        regime = "SELL-DAY" if sell_pct >= 55 else "BUY-DAY" if sell_pct <= 45 else "MIXED-DAY"

        wins = sum(1 for s in day_setups if s["outcome"] == "WIN")
        pnl = sum(s["pnl"] for s in day_setups)
        long_pnl = sum(s["pnl"] for s in day_setups if s["direction"] in ("long", "bullish"))
        short_pnl = sum(s["pnl"] for s in day_setups if s["direction"] in ("short", "bearish"))

        print(f"\n  {date_key} | {regime:9s} | sell={sell_pct:.0f}% | net_delta={net:+,}")
        print(f"    Setups: {len(day_setups)} | {wins}W/{len(day_setups)-wins}L | "
              f"PnL={pnl:+.0f} | Longs={long_pnl:+.0f} | Shorts={short_pnl:+.0f}")

    # ============================================================
    # ANALYSIS 4: FILTER SIMULATION
    # ============================================================
    print("\n" + "=" * 90)
    print("ANALYSIS 4: FILTER SIMULATIONS")
    print("=" * 90)

    # Baseline
    total = len(results)
    wins = sum(1 for r in results if r["outcome"] == "WIN")
    pnl = sum(r["pnl"] for r in results)
    print(f"\n  BASELINE (no VX filter):")
    print(f"    {total} trades | {wins}W | WR={wins/total*100:.1f}% | PnL={pnl:+.0f} pts")

    # Filter A: Block AGAINST trades
    filtered_a = [r for r in results if r["vx_align"] != "AGAINST"]
    wins_a = sum(1 for r in filtered_a if r["outcome"] == "WIN")
    pnl_a = sum(r["pnl"] for r in filtered_a)
    blocked_a = total - len(filtered_a)
    blocked_pnl_a = pnl - pnl_a
    print(f"\n  FILTER A: Block when VX regime AGAINST setup direction")
    print(f"    Blocked: {blocked_a} trades ({blocked_pnl_a:+.0f} pts removed)")
    print(f"    Remaining: {len(filtered_a)} | {wins_a}W | "
          f"WR={wins_a/len(filtered_a)*100:.1f}% | PnL={pnl_a:+.0f} pts")
    print(f"    Delta: {pnl_a-pnl:+.0f} pts, WR {wins/total*100:.1f}% -> {wins_a/len(filtered_a)*100:.1f}%")

    # Filter B: Only take ALIGNED trades
    filtered_b = [r for r in results if r["vx_align"] == "ALIGNED"]
    if filtered_b:
        wins_b = sum(1 for r in filtered_b if r["outcome"] == "WIN")
        pnl_b = sum(r["pnl"] for r in filtered_b)
        print(f"\n  FILTER B: Only take ALIGNED trades (VX confirms direction)")
        print(f"    Trades: {len(filtered_b)} | {wins_b}W | "
              f"WR={wins_b/len(filtered_b)*100:.1f}% | PnL={pnl_b:+.0f} pts")
        print(f"    Per-trade avg: {pnl_b/len(filtered_b):+.1f} pts")

    # Filter C: Block shorts on SELL_REGIME days (vol sellers = bullish = don't short)
    filtered_c = [r for r in results
                  if not (r["direction"] in ("short", "bearish") and r["vx_regime"] == "SELL_REGIME")]
    wins_c = sum(1 for r in filtered_c if r["outcome"] == "WIN")
    pnl_c = sum(r["pnl"] for r in filtered_c)
    blocked_c = total - len(filtered_c)
    print(f"\n  FILTER C: Block SHORTS when VX regime is SELL (vol sellers = bullish)")
    print(f"    Blocked: {blocked_c} shorts")
    print(f"    Remaining: {len(filtered_c)} | {wins_c}W | "
          f"WR={wins_c/len(filtered_c)*100:.1f}% | PnL={pnl_c:+.0f} pts")
    print(f"    Delta: {pnl_c-pnl:+.0f} pts")

    # Filter D: Block longs on BUY_REGIME (vol buyers = bearish = don't go long)
    filtered_d = [r for r in results
                  if not (r["direction"] in ("long", "bullish") and r["vx_regime"] == "BUY_REGIME")]
    wins_d = sum(1 for r in filtered_d if r["outcome"] == "WIN")
    pnl_d = sum(r["pnl"] for r in filtered_d)
    blocked_d = total - len(filtered_d)
    print(f"\n  FILTER D: Block LONGS when VX regime is BUY (vol buyers = bearish)")
    print(f"    Blocked: {blocked_d} longs")
    if filtered_d:
        print(f"    Remaining: {len(filtered_d)} | {wins_d}W | "
              f"WR={wins_d/len(filtered_d)*100:.1f}% | PnL={pnl_d:+.0f} pts")
        print(f"    Delta: {pnl_d-pnl:+.0f} pts")

    # Filter E: VX as alignment boost (+1/-1 to existing alignment)
    print(f"\n  FILTER E: VX as alignment boost (+1 aligned, -1 against)")
    for min_align in [+2, +3, +4]:
        boosted = []
        for r in results:
            align = r["alignment"]
            if r["vx_align"] == "ALIGNED":
                align += 1
            elif r["vx_align"] == "AGAINST":
                align -= 1
            if align >= min_align:
                boosted.append(r)
        if boosted:
            w = sum(1 for r in boosted if r["outcome"] == "WIN")
            p = sum(r["pnl"] for r in boosted)
            print(f"    align>={min_align:+d}: {len(boosted)}t | {w}W | "
                  f"WR={w/len(boosted)*100:.1f}% | PnL={p:+.0f} | avg={p/len(boosted):+.1f}")

    # ============================================================
    # FINAL VERDICT
    # ============================================================
    print("\n" + "=" * 90)
    print("FINAL VERDICT")
    print("=" * 90)

    best_filter = max([
        ("A: Block AGAINST", pnl_a, len(filtered_a), wins_a),
        ("B: Only ALIGNED", pnl_b if filtered_b else -9999, len(filtered_b) if filtered_b else 0, wins_b if filtered_b else 0),
        ("C: Block shorts in SELL", pnl_c, len(filtered_c), wins_c),
        ("D: Block longs in BUY", pnl_d, len(filtered_d), wins_d),
    ], key=lambda x: x[1])

    print(f"\n  Baseline PnL: {pnl:+.0f} pts ({total} trades, {wins/total*100:.1f}% WR)")
    print(f"  Best filter:  {best_filter[0]} = {best_filter[1]:+.0f} pts "
          f"({best_filter[2]} trades, {best_filter[3]/best_filter[2]*100:.1f}% WR)")
    print(f"  Improvement:  {best_filter[1]-pnl:+.0f} pts")

    improvement = best_filter[1] - pnl
    if improvement > 50:
        print(f"\n  >> STRONG SIGNAL: VX filter adds {improvement:+.0f} pts. WORTH buying CFE.")
    elif improvement > 20:
        print(f"\n  >> MODERATE SIGNAL: VX filter adds {improvement:+.0f} pts. Consider buying CFE.")
    elif improvement > 0:
        print(f"\n  >> WEAK SIGNAL: VX filter adds only {improvement:+.0f} pts. Marginal value.")
    else:
        print(f"\n  >> NO SIGNAL: VX filter doesn't help ({improvement:+.0f} pts). Don't buy CFE.")


if __name__ == "__main__":
    main()
