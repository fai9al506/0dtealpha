import os, sys, traceback
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])

for date in ['2026-03-05', '2026-03-06']:
    c = e.connect()
    print(f"\n{'='*60}", flush=True)
    print(f"  {date}", flush=True)
    print(f"{'='*60}", flush=True)

    # ES range bar prices
    r = c.execute(text("""
        SELECT min(bar_low), max(bar_high), avg(bar_close)::int, count(*)
        FROM es_range_bars WHERE trade_date=:d AND source='rithmic'
    """), {"d": date}).fetchone()
    print(f"ES bars: {r[3]} bars, low={r[0]} high={r[1]} avg={r[2]}", flush=True)

    # Dominant vanna levels
    for tf_name in ['THIS_WEEK', 'THIRTY_NEXT_DAYS']:
        r2 = c.execute(text("""
            WITH latest AS (
                SELECT MAX(ts_utc) AS ts FROM volland_exposure_points
                WHERE greek='vanna' AND expiration_option=:tf AND ts_utc::date=:d
            )
            SELECT strike, value::float FROM volland_exposure_points
            WHERE greek='vanna' AND expiration_option=:tf
              AND ts_utc = (SELECT ts FROM latest)
        """), {"tf": tf_name, "d": date}).fetchall()
        total = sum(abs(x[1]) for x in r2) if r2 else 0
        if total == 0:
            print(f"  {tf_name}: no data", flush=True)
            continue
        dom = [(x[0], abs(x[1])/total*100, x[1]) for x in r2 if abs(x[1])/total*100 >= 10]
        dom.sort(key=lambda x: x[1], reverse=True)
        print(f"  {tf_name} ({len(r2)} strikes, dominant >=10%):", flush=True)
        for s, pct, val in dom[:6]:
            sign = "+" if val > 0 else "-"
            print(f"    {s:.0f} {sign} {pct:.1f}%", flush=True)

    # Now simulate VP - get bars and run swing detection
    bars = c.execute(text("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, cumulative_delta AS cvd,
               ts_start, ts_end, status
        FROM es_range_bars
        WHERE trade_date = :d AND source = 'rithmic' AND status = 'closed'
        ORDER BY bar_idx ASC
    """), {"d": date}).mappings().all()
    bars = [dict(r) for r in bars]

    # Get vanna levels (using _get_dominant_vanna_levels logic)
    vl_rows = c.execute(text("""
        WITH latest AS (
            SELECT expiration_option, MAX(ts_utc) AS ts
            FROM volland_exposure_points
            WHERE greek = 'vanna'
              AND expiration_option IN ('THIS_WEEK', 'THIRTY_NEXT_DAYS')
              AND ts_utc::date = :d
            GROUP BY expiration_option
        )
        SELECT vep.strike, vep.value::float AS value, vep.expiration_option AS timeframe
        FROM volland_exposure_points vep
        JOIN latest l ON vep.expiration_option = l.expiration_option AND vep.ts_utc = l.ts
        WHERE vep.greek = 'vanna'
    """), {"d": date}).mappings().all()

    by_tf = {}
    for rv in vl_rows:
        tf = rv["timeframe"]
        if tf not in by_tf:
            by_tf[tf] = []
        by_tf[tf].append({"strike": float(rv["strike"]), "value": float(rv["value"])})

    levels = []
    strike_tfs = {}
    for tf, points in by_tf.items():
        total = sum(abs(p["value"]) for p in points)
        if total == 0: continue
        for p in points:
            pct = abs(p["value"]) / total * 100.0
            if pct >= 12:
                levels.append({"strike": p["strike"], "value": p["value"],
                               "timeframe": tf, "pct": round(pct, 1), "confluence": False})
                s_key = int(p["strike"])
                if s_key not in strike_tfs: strike_tfs[s_key] = set()
                strike_tfs[s_key].add(tf)
    for lv in levels:
        if len(strike_tfs.get(int(lv["strike"]), set())) > 1:
            lv["confluence"] = True

    print(f"\nDominant vanna levels for VP: {len(levels)}", flush=True)
    for lv in levels:
        sign = "+" if lv["value"] > 0 else "-"
        conf = " [CONF]" if lv["confluence"] else ""
        print(f"  {lv['strike']:.0f} {sign} ({lv['pct']}%) [{lv['timeframe']}]{conf}", flush=True)

    if not bars or not levels:
        print("Missing data, skip simulation", flush=True)
        c.close()
        continue

    # Run swing + divergence detection
    sys.path.insert(0, '.')
    from app.setup_detector import _vp_find_swings, _vp_detect_divergences

    # Simulate bar-by-bar, looking for recent divergences near vanna levels
    from datetime import timedelta
    import pytz
    NY = pytz.timezone("US/Eastern")

    signals = []
    cooldown = {"long": None, "short": None}
    cooldown_min = 15

    for i in range(10, len(bars)):
        bar = bars[i]
        ts = bar.get("ts_end")
        if ts is None:
            continue
        if hasattr(ts, 'astimezone'):
            ts_et = ts.astimezone(NY)
        else:
            continue

        if ts_et.hour < 10 or (ts_et.hour == 15 and ts_et.minute > 30) or ts_et.hour >= 16:
            continue

        sub_bars = bars[:i+1]
        spot = float(bar["bar_close"])

        swings = _vp_find_swings(sub_bars, pivot_n=2)
        if len(swings) < 2:
            continue
        divs = _vp_detect_divergences(sub_bars, swings)
        if not divs:
            continue

        last_idx = len(sub_bars) - 1
        recent = [d for d in divs if d["bar_idx"] >= last_idx - 40]
        if not recent:
            continue

        for div in recent:
            div_price = div["price"]
            div_dir = div["direction"]

            for vl in levels:
                strike = vl["strike"]
                vanna_val = vl["value"]
                dist = abs(div_price - strike)
                if dist > 15:
                    continue
                if vanna_val > 0 and div_dir != "long":
                    continue
                if vanna_val < 0 and div_dir != "short":
                    continue

                cd_key = div_dir
                if cooldown[cd_key] and ts_et < cooldown[cd_key]:
                    continue

                cooldown[cd_key] = ts_et + timedelta(minutes=cooldown_min)
                signals.append({
                    "time": ts_et.strftime("%H:%M"),
                    "bar_idx": i,
                    "direction": div_dir.upper(),
                    "spot": spot,
                    "vanna_strike": strike,
                    "vanna_pct": vl["pct"],
                    "pattern": div.get("pattern", "?"),
                    "confluence": vl.get("confluence", False),
                    "dist": dist,
                })
                break
            else:
                continue
            break

    print(f"\nSignals that WOULD have fired: {len(signals)}", flush=True)

    if not signals:
        c.close()
        continue

    for s in signals:
        conf = " [CONF]" if s["confluence"] else ""
        print(f"  {s['time']} {s['direction']:5s} @ {s['spot']:.1f}  "
              f"vanna={s['vanna_strike']:.0f} ({s['vanna_pct']}%){conf}  "
              f"pat={s['pattern']}  dist={s['dist']:.1f}pt", flush=True)

    # Outcome simulation
    print(f"\nOutcomes (T=+10, SL=-8):", flush=True)
    total_pts = 0
    wins = 0
    losses = 0
    for sig in signals:
        entry = sig["spot"]
        is_long = sig["direction"] == "LONG"
        target = entry + 10 if is_long else entry - 10
        stop = entry - 8 if is_long else entry + 8

        result = "EXPIRED"
        pts = 0
        for j in range(sig["bar_idx"] + 1, len(bars)):
            fb = bars[j]
            hi = float(fb["bar_high"])
            lo = float(fb["bar_low"])
            if is_long:
                if lo <= stop:
                    result = "LOSS"; pts = -8; break
                if hi >= target:
                    result = "WIN"; pts = 10; break
            else:
                if hi >= stop:
                    result = "LOSS"; pts = -8; break
                if lo <= target:
                    result = "WIN"; pts = 10; break

        if result == "EXPIRED":
            exit_p = float(bars[-1]["bar_close"])
            pts = (exit_p - entry) if is_long else (entry - exit_p)

        if result == "WIN": wins += 1
        elif result == "LOSS": losses += 1
        total_pts += pts
        print(f"  {sig['time']} {sig['direction']:5s} @ {entry:.1f} -> {result} ({pts:+.1f})", flush=True)

    print(f"\n  TOTAL: {wins}W/{losses}L, {total_pts:+.1f} pts", flush=True)
    c.close()

print("\nDone.", flush=True)
