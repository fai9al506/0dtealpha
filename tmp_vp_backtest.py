"""Check if Vanna Pivot Bounce would have fired on Mar 5 and Mar 6 if the source bug was fixed."""
import sys, os
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])

for date in ['2026-03-05', '2026-03-06']:
    print(f"\n{'='*60}", flush=True)
    print(f"  DATE: {date}", flush=True)
    print(f"{'='*60}", flush=True)

    with e.connect() as c:
        # Get range bars (rithmic source)
        bars = c.execute(text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_delta, cumulative_delta AS cvd,
                   bar_buy_volume, bar_sell_volume,
                   ts_start, ts_end, status
            FROM es_range_bars
            WHERE trade_date = :td AND source = 'rithmic'
            ORDER BY bar_idx ASC
        """), {"td": date}).mappings().all()
        bars = [dict(r) for r in bars]
        print(f"Range bars: {len(bars)}", flush=True)

        if not bars:
            print("No bars, skipping", flush=True)
            continue

        # Get vanna levels from the closest volland snapshot
        vanna_rows = c.execute(text("""
            WITH latest AS (
                SELECT expiration_option, MAX(ts_utc) AS ts
                FROM volland_exposure_points
                WHERE greek = 'vanna'
                  AND expiration_option IN ('THIS_WEEK', 'THIRTY_NEXT_DAYS')
                  AND ts_utc::date = :td
                GROUP BY expiration_option
            )
            SELECT vep.strike, vep.value::float AS value, vep.expiration_option AS timeframe
            FROM volland_exposure_points vep
            JOIN latest l ON vep.expiration_option = l.expiration_option AND vep.ts_utc = l.ts
            WHERE vep.greek = 'vanna'
        """), {"td": date}).mappings().all()

        print(f"Vanna exposure points: {len(vanna_rows)}", flush=True)

        if not vanna_rows:
            print("No vanna data, skipping", flush=True)
            continue

        # Compute dominant levels (same logic as _get_dominant_vanna_levels)
        by_tf = {}
        for r in vanna_rows:
            tf = r["timeframe"]
            if tf not in by_tf:
                by_tf[tf] = []
            by_tf[tf].append({"strike": float(r["strike"]), "value": float(r["value"])})

        levels = []
        strike_tfs = {}
        min_pct = 12.0

        for tf, points in by_tf.items():
            total = sum(abs(p["value"]) for p in points)
            if total == 0:
                continue
            for p in points:
                pct = abs(p["value"]) / total * 100.0
                if pct >= min_pct:
                    levels.append({
                        "strike": p["strike"],
                        "value": p["value"],
                        "timeframe": tf,
                        "pct": round(pct, 1),
                        "confluence": False,
                    })
                    s_key = int(p["strike"])
                    if s_key not in strike_tfs:
                        strike_tfs[s_key] = set()
                    strike_tfs[s_key].add(tf)

        for lv in levels:
            if len(strike_tfs.get(int(lv["strike"]), set())) > 1:
                lv["confluence"] = True

        print(f"Dominant vanna levels (>={min_pct}%):", flush=True)
        for lv in sorted(levels, key=lambda x: x["pct"], reverse=True):
            sign = "+" if lv["value"] > 0 else "-"
            conf = " [CONFLUENCE]" if lv["confluence"] else ""
            print(f"  {lv['strike']:.0f} {sign} ({lv['pct']}%) [{lv['timeframe']}]{conf}", flush=True)

        if not levels:
            print("No dominant levels found", flush=True)
            continue

        # Get spot prices during the day from chain_snapshots
        spots = c.execute(text("""
            SELECT spot_price, created_at AT TIME ZONE 'US/Eastern' as ts_et
            FROM chain_snapshots
            WHERE created_at::date = :td
            ORDER BY created_at ASC
        """), {"td": date}).fetchall()

        if spots:
            print(f"SPX range: {min(s[0] for s in spots):.1f} - {max(s[0] for s in spots):.1f}", flush=True)

        # Now run the actual evaluate_vanna_pivot_bounce at each bar
        # We need to import from setup_detector
        sys.path.insert(0, os.path.dirname(os.path.abspath('.')))
        from app.setup_detector import evaluate_vanna_pivot_bounce, _vp_find_swings, _vp_detect_divergences

        # Default settings
        settings = {
            "vanna_pivot_enabled": True,
            "vp_market_start": "10:00",
            "vp_market_end": "15:30",
            "vp_proximity_pts": 15,
            "vp_target_pts": 10,
            "vp_stop_pts": 8,
            "vp_dominant_pct": 12,
        }

        # Simulate: for each completed bar, run evaluate with bars up to that point
        # Use spot = bar close price (approximate)
        signals = []
        cooldown = {"long": None, "short": None}
        cooldown_min = 15

        from datetime import datetime, timedelta
        import pytz
        NY = pytz.timezone("US/Eastern")

        for i in range(10, len(bars)):
            bar = bars[i]
            if bar["status"] != "closed":
                continue

            # Approximate spot from bar close
            spot = float(bar["bar_close"])

            # Get timestamp for time check
            ts = bar.get("ts_end")
            if ts is None:
                continue

            if hasattr(ts, 'astimezone'):
                ts_et = ts.astimezone(NY)
            else:
                continue

            # Time window
            if ts_et.hour < 10 or (ts_et.hour == 15 and ts_et.minute > 30) or ts_et.hour >= 16:
                continue

            sub_bars = bars[:i+1]

            # We can't call evaluate_vanna_pivot_bounce directly because it checks datetime.now()
            # Instead, let's do the swing/divergence detection manually
            swings = _vp_find_swings(sub_bars, pivot_n=2)
            if len(swings) < 2:
                continue
            divs = _vp_detect_divergences(sub_bars, swings)
            if not divs:
                continue

            last_bar_idx = len(sub_bars) - 1
            recent_divs = [d for d in divs if d["bar_idx"] >= last_bar_idx - 40]
            if not recent_divs:
                continue

            # Match to vanna levels
            for div in recent_divs:
                div_price = div["price"]
                div_dir = div["direction"]

                for vl in levels:
                    strike = vl["strike"]
                    vanna_val = vl["value"]

                    dist = abs(div_price - strike)
                    if dist > 15:
                        continue

                    # Direction agreement
                    if vanna_val > 0 and div_dir != "long":
                        continue
                    if vanna_val < 0 and div_dir != "short":
                        continue

                    # Check cooldown
                    cd_key = div_dir
                    if cooldown[cd_key] and ts_et < cooldown[cd_key]:
                        continue

                    # This would be a signal!
                    cooldown[cd_key] = ts_et + timedelta(minutes=cooldown_min)
                    signals.append({
                        "time": ts_et.strftime("%H:%M"),
                        "bar_idx": i,
                        "direction": div_dir.upper(),
                        "spot": spot,
                        "vanna_strike": strike,
                        "vanna_pct": vl["pct"],
                        "div_pattern": div.get("pattern", "?"),
                        "confluence": vl.get("confluence", False),
                        "dist": dist,
                    })
                    break  # one signal per divergence

        print(f"\nSignals that WOULD have fired: {len(signals)}", flush=True)
        for s in signals:
            conf = " [CONF]" if s["confluence"] else ""
            print(f"  {s['time']} {s['direction']:5s} @ {s['spot']:.1f}  "
                  f"vanna={s['vanna_strike']:.0f} ({s['vanna_pct']}%){conf}  "
                  f"pattern={s['div_pattern']}  dist={s['dist']:.1f}pt", flush=True)

        # Check outcomes (10pt target, 8pt stop)
        if signals:
            print(f"\nOutcome simulation (T=+10, SL=-8):", flush=True)
            total_pts = 0
            wins = 0
            losses = 0
            for sig in signals:
                entry = sig["spot"]
                is_long = sig["direction"] == "LONG"
                target = entry + 10 if is_long else entry - 10
                stop = entry - 8 if is_long else entry + 8

                # Walk forward from signal bar
                result = "EXPIRED"
                exit_price = None
                for j in range(sig["bar_idx"] + 1, len(bars)):
                    fb = bars[j]
                    hi = float(fb["bar_high"])
                    lo = float(fb["bar_low"])

                    if is_long:
                        if lo <= stop:
                            result = "LOSS"
                            exit_price = stop
                            break
                        if hi >= target:
                            result = "WIN"
                            exit_price = target
                            break
                    else:
                        if hi >= stop:
                            result = "LOSS"
                            exit_price = stop
                            break
                        if lo <= target:
                            result = "WIN"
                            exit_price = target
                            break

                if result == "EXPIRED":
                    # Use last bar close
                    exit_price = float(bars[-1]["bar_close"])
                    pts = (exit_price - entry) if is_long else (entry - exit_price)
                else:
                    pts = 10 if result == "WIN" else -8

                if result == "WIN":
                    wins += 1
                elif result == "LOSS":
                    losses += 1
                total_pts += pts

                print(f"  {sig['time']} {sig['direction']:5s} @ {entry:.1f} -> {result} ({pts:+.1f} pts)", flush=True)

            print(f"\n  TOTAL: {wins}W/{losses}L, {total_pts:+.1f} pts", flush=True)

print("\nDone.", flush=True)
