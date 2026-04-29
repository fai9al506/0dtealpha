"""VX as LEADING indicator: Do VX flow CHANGES predict SPX direction?
Apollo uses VX to ANTICIPATE the next move, not as a static filter.

Key signals to detect:
1. Vol seller exhaustion: sellers were dominant, then delta spikes positive = SPX about to drop
2. Vol buyer exhaustion: buyers were dominant, then delta spikes negative = SPX about to rally
3. Delta divergence: VX delta changes direction BEFORE price changes
4. Large print clusters: institutional size hitting bid/ask = commitment signal
"""
import struct, json
from datetime import datetime, timedelta
from collections import defaultdict

SCID_FILE = r"C:\SierraChart\Data\VXM26_FUT_CFE.scid"
SETUPS_FILE = "tmp_setups_full.json"
SC_EPOCH = datetime(1899, 12, 30)
MICROS_PER_DAY = 86_400_000_000


def load_vx_ticks():
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
        if abs(o) > 0.001 and o > -1e30:
            continue
        dt_et = dt - timedelta(hours=4)
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


def build_5min_bars(ticks):
    """Aggregate into 5-min bars with delta, CVD, price."""
    bars = {}
    for t in ticks:
        # 5-min bucket
        m = t["dt_et"].minute
        bucket_m = (m // 5) * 5
        key = t["dt_et"].replace(minute=bucket_m, second=0, microsecond=0)
        key_str = key.strftime("%H:%M")

        if key_str not in bars:
            bars[key_str] = {
                "time": key, "open": t["price"], "high": t["price"],
                "low": t["price"], "close": t["price"],
                "volume": 0, "buy": 0, "sell": 0, "delta": 0,
                "trades": 0, "large_buy": 0, "large_sell": 0,
            }
        b = bars[key_str]
        b["high"] = max(b["high"], t["price"])
        b["low"] = min(b["low"], t["price"])
        b["close"] = t["price"]
        b["volume"] += t["volume"]
        b["buy"] += t["buy"]
        b["sell"] += t["sell"]
        b["delta"] += t["delta"]
        b["trades"] += 1
        if t["volume"] >= 15:
            if t["buy"] > 0:
                b["large_buy"] += t["buy"]
            else:
                b["large_sell"] += t["sell"]
    return bars


def detect_flow_shifts(bars_sorted):
    """Detect moments where VX flow shifts direction.
    A shift = 3-bar rolling delta changes sign significantly."""
    shifts = []
    keys = [k for k, _ in bars_sorted]
    vals = [v for _, v in bars_sorted]

    for i in range(3, len(vals)):
        # Previous 3-bar delta
        prev_delta = sum(vals[j]["delta"] for j in range(i-3, i))
        # Current bar delta
        curr_delta = vals[i]["delta"]

        # Shift: previous was selling, now buying (or vice versa)
        if prev_delta < -30 and curr_delta > 15:
            shifts.append({
                "time": vals[i]["time"],
                "type": "SELL_TO_BUY",  # vol sellers -> vol buyers = bearish SPX turn
                "prev_delta": prev_delta,
                "curr_delta": curr_delta,
                "vx_price": vals[i]["close"],
                "vx_chg": vals[i]["close"] - vals[i]["open"],
            })
        elif prev_delta > 30 and curr_delta < -15:
            shifts.append({
                "time": vals[i]["time"],
                "type": "BUY_TO_SELL",  # vol buyers -> vol sellers = bullish SPX turn
                "prev_delta": prev_delta,
                "curr_delta": curr_delta,
                "vx_price": vals[i]["close"],
                "vx_chg": vals[i]["close"] - vals[i]["open"],
            })

    return shifts


def detect_delta_spikes(bars_sorted):
    """Detect 5-min bars with abnormal delta (>2x average).
    These are moments of sudden vol buying/selling."""
    vals = [v for _, v in bars_sorted]
    if len(vals) < 5:
        return []

    avg_abs_delta = sum(abs(v["delta"]) for v in vals) / len(vals)
    threshold = max(avg_abs_delta * 2, 30)

    spikes = []
    for _, v in bars_sorted:
        if abs(v["delta"]) >= threshold:
            spikes.append({
                "time": v["time"],
                "delta": v["delta"],
                "type": "VOL_BUYERS" if v["delta"] > 0 else "VOL_SELLERS",
                "vx_price": v["close"],
                "volume": v["volume"],
                "spx_implication": "BEARISH" if v["delta"] > 0 else "BULLISH",
            })
    return spikes


def main():
    vx_by_date = load_vx_ticks()
    with open(SETUPS_FILE) as f:
        setups = json.load(f)

    print("=" * 95)
    print("VX AS LEADING INDICATOR: FLOW SHIFTS + DELTA SPIKES vs SPX MOVEMENT")
    print("=" * 95)

    # Load SPX spot from setups (approximate price at each time)
    # We'll use setup spot prices as price reference points

    all_spikes = []
    all_shifts = []

    for date_key in sorted(vx_by_date.keys()):
        ticks = vx_by_date[date_key]
        bars = build_5min_bars(ticks)
        bars_sorted = sorted(bars.items(), key=lambda x: x[0])

        # Build CVD per bar
        cvd = 0
        for k, v in bars_sorted:
            cvd += v["delta"]
            v["cvd"] = cvd

        shifts = detect_flow_shifts(bars_sorted)
        spikes = detect_delta_spikes(bars_sorted)

        # Get SPX prices from setups for this day
        day_setups = [s for s in setups
                      if datetime.fromisoformat(s["ts"]).replace(tzinfo=None).strftime("%Y-%m-%d") == date_key
                      or (datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4)).strftime("%Y-%m-%d") == date_key]

        print(f"\n{'='*95}")
        print(f"  {date_key} | {len(ticks):,} ticks | {len(bars_sorted)} 5-min bars | "
              f"{len(shifts)} flow shifts | {len(spikes)} delta spikes")
        print(f"{'='*95}")

        # Show flow shifts
        if shifts:
            print(f"\n  FLOW SHIFTS (direction changes):")
            for s in shifts:
                t = s["time"].strftime("%H:%M")
                # Find nearest setup within +/- 15 min to see if a setup fired near this shift
                nearby_setups = []
                for su in day_setups:
                    su_ts = datetime.fromisoformat(su["ts"]).replace(tzinfo=None) - timedelta(hours=4)
                    diff = abs((su_ts - s["time"]).total_seconds()) / 60
                    if diff <= 15:
                        nearby_setups.append((diff, su))
                nearby_str = ""
                if nearby_setups:
                    nearest = min(nearby_setups, key=lambda x: x[0])
                    su = nearest[1]
                    nearby_str = (f" << {su['setup_name']} {su['direction']} "
                                  f"{su['outcome']}({su['pnl']:+.0f}) @ {nearest[0]:.0f}min away")

                print(f"    {t} | {s['type']:12s} | prev_d={s['prev_delta']:+4d} -> curr_d={s['curr_delta']:+4d} | "
                      f"VX={s['vx_price']:.2f}{nearby_str}")

        # Show delta spikes
        if spikes:
            print(f"\n  DELTA SPIKES (abnormal vol flow):")
            for sp in spikes:
                t = sp["time"].strftime("%H:%M")
                # Find setups that fired AFTER this spike (within 5-20 min)
                following_setups = []
                for su in day_setups:
                    su_ts = datetime.fromisoformat(su["ts"]).replace(tzinfo=None) - timedelta(hours=4)
                    diff = (su_ts - sp["time"]).total_seconds() / 60
                    if 0 < diff <= 20:
                        following_setups.append((diff, su))

                follow_str = ""
                if following_setups:
                    for diff, su in sorted(following_setups, key=lambda x: x[0])[:2]:
                        w = "W" if su["outcome"] == "WIN" else "L"
                        follow_str += (f" >> {su['setup_name'][:10]} {su['direction'][:1].upper()} "
                                       f"{w}({su['pnl']:+.0f}) +{diff:.0f}m")

                print(f"    {t} | {sp['type']:11s} | delta={sp['delta']:+4d} | vol={sp['volume']:4d} | "
                      f"SPX->{sp['spx_implication']:7s}{follow_str}")

        for k, v in bars_sorted:
            all_spikes.append(v)

    # ============================================================
    # PREDICTIVE ANALYSIS: What happens to setups AFTER VX events?
    # ============================================================
    print(f"\n{'='*95}")
    print("PREDICTIVE ANALYSIS: Setup outcomes AFTER VX delta spikes")
    print(f"{'='*95}")

    # For each delta spike, look at setups that fire in the next 5-20 minutes
    spike_then_setup = {"predicted_right": 0, "predicted_wrong": 0,
                        "pnl_right": 0, "pnl_wrong": 0, "details": []}

    for date_key in sorted(vx_by_date.keys()):
        ticks = vx_by_date[date_key]
        bars = build_5min_bars(ticks)
        bars_sorted = sorted(bars.items(), key=lambda x: x[0])
        spikes = detect_delta_spikes(bars_sorted)

        day_setups = [s for s in setups
                      if (datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4)).strftime("%Y-%m-%d") == date_key]

        for sp_k, sp_v in bars_sorted:
            sp_time = sp_v["time"]
            if abs(sp_v["delta"]) < max(30, sum(abs(v["delta"]) for _, v in bars_sorted) / len(bars_sorted) * 1.5):
                continue

            is_vol_buyers = sp_v["delta"] > 0   # VX buying = bearish SPX
            is_vol_sellers = sp_v["delta"] < 0   # VX selling = bullish SPX

            for su in day_setups:
                su_ts = datetime.fromisoformat(su["ts"]).replace(tzinfo=None) - timedelta(hours=4)
                diff_min = (su_ts - sp_time).total_seconds() / 60
                if diff_min < 2 or diff_min > 20:
                    continue

                is_win = su["outcome"] == "WIN"
                su_is_long = su["direction"] in ("long", "bullish")
                su_is_short = su["direction"] in ("short", "bearish")

                # Did VX predict the setup's outcome?
                # Vol sellers (bullish SPX) + long setup = VX predicts WIN
                # Vol buyers (bearish SPX) + short setup = VX predicts WIN
                vx_predicts_win = (is_vol_sellers and su_is_long) or (is_vol_buyers and su_is_short)
                vx_predicts_loss = (is_vol_buyers and su_is_long) or (is_vol_sellers and su_is_short)

                if vx_predicts_win and is_win:
                    spike_then_setup["predicted_right"] += 1
                    spike_then_setup["pnl_right"] += su["pnl"]
                elif vx_predicts_loss and not is_win:
                    spike_then_setup["predicted_right"] += 1
                    spike_then_setup["pnl_right"] += abs(su["pnl"])  # saved this loss
                elif vx_predicts_win and not is_win:
                    spike_then_setup["predicted_wrong"] += 1
                    spike_then_setup["pnl_wrong"] += su["pnl"]
                elif vx_predicts_loss and is_win:
                    spike_then_setup["predicted_wrong"] += 1
                    spike_then_setup["pnl_wrong"] -= su["pnl"]  # missed this win

    total_pred = spike_then_setup["predicted_right"] + spike_then_setup["predicted_wrong"]
    if total_pred > 0:
        accuracy = spike_then_setup["predicted_right"] / total_pred * 100
        print(f"\n  VX spike -> setup outcome prediction:")
        print(f"    Total predictions: {total_pred}")
        print(f"    Correct: {spike_then_setup['predicted_right']} ({accuracy:.1f}%)")
        print(f"    Wrong: {spike_then_setup['predicted_wrong']} ({100-accuracy:.1f}%)")
    else:
        print(f"\n  No VX spike -> setup predictions found")

    # ============================================================
    # VX PRICE DIRECTION vs SPX (using setup spots as price proxy)
    # ============================================================
    print(f"\n{'='*95}")
    print("VX PRICE DIRECTION vs NEXT-30-MIN SPX MOVEMENT")
    print(f"{'='*95}")
    print("\n  For each 5-min bar: does VX price change predict SPX direction?")

    correct = 0
    wrong = 0
    for date_key in sorted(vx_by_date.keys()):
        ticks = vx_by_date[date_key]
        bars = build_5min_bars(ticks)
        bars_sorted = sorted(bars.items(), key=lambda x: x[0])

        day_setups = sorted(
            [s for s in setups
             if (datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4)).strftime("%Y-%m-%d") == date_key],
            key=lambda x: x["ts"]
        )

        for i in range(len(bars_sorted) - 1):
            k1, v1 = bars_sorted[i]
            k2, v2 = bars_sorted[min(i + 6, len(bars_sorted) - 1)]  # 30 min later

            vx_up = v2["close"] > v1["close"]  # VX went up = bearish SPX
            vx_down = v2["close"] < v1["close"]  # VX went down = bullish SPX

            # Find SPX spot change in this window
            t1 = v1["time"]
            t2 = v2["time"]
            spots_before = [s["spot"] for s in day_setups
                            if abs((datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4) - t1).total_seconds()) < 300]
            spots_after = [s["spot"] for s in day_setups
                           if abs((datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4) - t2).total_seconds()) < 300]

            if not spots_before or not spots_after:
                continue

            spx_before = spots_before[0]
            spx_after = spots_after[0]
            spx_up = spx_after > spx_before

            # VX down -> SPX up (inverse), VX up -> SPX down (inverse)
            if (vx_down and spx_up) or (vx_up and not spx_up):
                correct += 1
            elif vx_up != vx_down:  # skip flat VX
                wrong += 1

    total_dir = correct + wrong
    if total_dir > 0:
        print(f"    VX-SPX inverse correlation: {correct}/{total_dir} = {correct/total_dir*100:.1f}%")
        if correct / total_dir > 0.55:
            print(f"    >> VX price direction IS inversely predictive of SPX")
        elif correct / total_dir < 0.45:
            print(f"    >> VX price direction is NOT predictive (or same-direction!)")
        else:
            print(f"    >> VX price direction is COIN FLIP for SPX prediction")

    # ============================================================
    # VX DELTA MOMENTUM: 5-min delta acceleration
    # ============================================================
    print(f"\n{'='*95}")
    print("VX DELTA MOMENTUM: Does delta ACCELERATION predict next moves?")
    print(f"{'='*95}")

    # Look at delta change rate: if delta was -50, then -20, then +10 = sellers weakening
    accel_signals = {"sellers_weakening_then_spx_up": 0, "sellers_weakening_then_spx_down": 0,
                     "buyers_weakening_then_spx_down": 0, "buyers_weakening_then_spx_up": 0}

    for date_key in sorted(vx_by_date.keys()):
        ticks = vx_by_date[date_key]
        bars = build_5min_bars(ticks)
        bars_sorted = sorted(bars.items(), key=lambda x: x[0])

        day_setups = sorted(
            [s for s in setups
             if (datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4)).strftime("%Y-%m-%d") == date_key],
            key=lambda x: x["ts"]
        )

        for i in range(2, len(bars_sorted) - 3):
            d0 = bars_sorted[i - 2][1]["delta"]
            d1 = bars_sorted[i - 1][1]["delta"]
            d2 = bars_sorted[i][1]["delta"]

            # Sellers weakening: d0 very negative, d1 less negative, d2 near zero or positive
            sellers_weakening = d0 < -20 and d1 > d0 and d2 > d1 and d2 > -5
            # Buyers weakening: d0 very positive, d1 less positive, d2 near zero or negative
            buyers_weakening = d0 > 20 and d1 < d0 and d2 < d1 and d2 < 5

            if not sellers_weakening and not buyers_weakening:
                continue

            # What did SPX do in next 15 min?
            future_time = bars_sorted[i][1]["time"] + timedelta(minutes=15)
            curr_time = bars_sorted[i][1]["time"]

            spots_now = [s["spot"] for s in day_setups
                         if abs((datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4) - curr_time).total_seconds()) < 300]
            spots_future = [s["spot"] for s in day_setups
                            if abs((datetime.fromisoformat(s["ts"]).replace(tzinfo=None) - timedelta(hours=4) - future_time).total_seconds()) < 300]

            if not spots_now or not spots_future:
                continue

            spx_went_up = spots_future[0] > spots_now[0]

            if sellers_weakening:
                # Vol sellers weakening = VX about to rise = SPX about to DROP
                if not spx_went_up:
                    accel_signals["sellers_weakening_then_spx_down"] += 1
                else:
                    accel_signals["sellers_weakening_then_spx_up"] += 1
            elif buyers_weakening:
                # Vol buyers weakening = VX about to drop = SPX about to RISE
                if spx_went_up:
                    accel_signals["buyers_weakening_then_spx_down"] += 1  # mislabeled, should be up
                else:
                    accel_signals["buyers_weakening_then_spx_up"] += 1

    print(f"\n  Vol SELLERS weakening (VX about to rise -> SPX should drop):")
    sw_total = accel_signals["sellers_weakening_then_spx_down"] + accel_signals["sellers_weakening_then_spx_up"]
    if sw_total > 0:
        sw_correct = accel_signals["sellers_weakening_then_spx_down"]
        print(f"    SPX dropped: {sw_correct}/{sw_total} = {sw_correct/sw_total*100:.1f}%")
    else:
        print(f"    No signals detected")

    bw_total = accel_signals["buyers_weakening_then_spx_up"] + accel_signals["buyers_weakening_then_spx_down"]
    print(f"\n  Vol BUYERS weakening (VX about to drop -> SPX should rise):")
    if bw_total > 0:
        bw_correct = accel_signals["buyers_weakening_then_spx_down"]
        print(f"    SPX rose: {bw_correct}/{bw_total} = {bw_correct/bw_total*100:.1f}%")
    else:
        print(f"    No signals detected")


if __name__ == "__main__":
    main()
