"""
Two studies:
A) DD improvements on V9-SC filter — concrete PnL impact
B) EOD butterfly target using AGGREGATE DD hedging value
"""

from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import datetime, timedelta
import bisect, json

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)


# ========================================================================
#  SHARED HELPERS
# ========================================================================

def fetch_setups():
    sql = text("""
        SELECT id, ts, setup_name, direction, grade, score, spot, lis, target,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment, vix, paradigm, overvix
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
          AND spot IS NOT NULL AND ts::date >= '2026-02-11'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        return conn.execute(sql).fetchall()


def fetch_dd_charm_snaps():
    print("Fetching DD + charm per-strike...")
    sql = text("""
        SELECT greek, ts_utc, strike::numeric AS strike,
               value::numeric AS val, current_price::numeric AS cp
        FROM volland_exposure_points
        WHERE (greek = 'deltaDecay' OR
               (greek = 'charm' AND (expiration_option IS NULL OR expiration_option = 'TODAY')))
          AND ts_utc::date >= '2026-02-11' AND value != 0
        ORDER BY greek, ts_utc, strike
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    raw = defaultdict(lambda: defaultdict(list))
    for r in rows:
        raw[r.greek][r.ts_utc].append((float(r.strike), float(r.val)))
    snaps = {}
    for greek, ts_dict in raw.items():
        sorted_ts = sorted(ts_dict.keys())
        snaps[greek] = {'timestamps': sorted_ts, 'data': ts_dict}
        print(f"  {greek}: {len(sorted_ts)} snapshots")
    return snaps


def nearest(snaps, greek, ts, max_s=300):
    if greek not in snaps: return None
    timestamps = snaps[greek]['timestamps']
    if not timestamps: return None
    idx = bisect.bisect_left(timestamps, ts)
    cands = []
    if idx > 0: cands.append(timestamps[idx-1])
    if idx < len(timestamps): cands.append(timestamps[idx])
    best = min(cands, key=lambda t: abs((t - ts).total_seconds()))
    if abs((best - ts).total_seconds()) > max_s: return None
    return snaps[greek]['data'][best]


def dd_near_spot(strike_vals, spot, radius=5):
    if not strike_vals: return 0
    return sum(v for s, v in strike_vals if abs(s - spot) <= radius)


def dd_stacked_check(dd_strikes, charm_strikes, spot, is_long, tol=5):
    if not dd_strikes or not charm_strikes: return False
    dd_near = [(s, v) for s, v in dd_strikes if abs(s - spot) <= 30]
    ch_near = [(s, v) for s, v in charm_strikes if abs(s - spot) <= 30]
    if is_long:
        dd_s = max([(s,v) for s,v in dd_near if s <= spot and v < 0], key=lambda x: abs(x[1]), default=None)
        ch_s = max([(s,v) for s,v in ch_near if s <= spot], key=lambda x: abs(x[1]), default=None)
    else:
        dd_s = max([(s,v) for s,v in dd_near if s > spot and v > 0], key=lambda x: x[1], default=None)
        ch_s = max([(s,v) for s,v in ch_near if s > spot], key=lambda x: abs(x[1]), default=None)
    if dd_s and ch_s and abs(dd_s[0] - ch_s[0]) <= tol:
        return True
    return False


def v9sc(setup, direction, alignment, vix, overvix, grade):
    if grade == 'LOG': return False
    is_long = direction in ('long', 'bullish')
    if not is_long:
        if setup == 'Skew Charm': return True
        if setup == 'AG Short': return True
        if setup == 'DD Exhaustion': return alignment is not None and alignment != 0
        return False
    if alignment is None or alignment < 2: return False
    if setup == 'Skew Charm': return True
    if vix is not None and vix <= 22: return True
    if overvix is not None and overvix >= 2: return True
    return False


def maxdd(pnls):
    peak = cum = 0
    md = 0
    for p in pnls:
        cum += p
        if cum > peak: peak = cum
        if peak - cum > md: md = peak - cum
    return md


def wr(trades):
    if not trades: return "N/A (0)"
    w = sum(1 for t in trades if t['result'] == 'WIN')
    p = sum(t['pnl'] for t in trades)
    return f"{w}W/{len(trades)-w}L ({w/len(trades)*100:.0f}%), {p:+.1f}pts ({p/len(trades):+.1f}/t)"


def stats_line(label, trades, n_days):
    if not trades:
        print(f"  {label}: no trades")
        return
    w = sum(1 for t in trades if t['result'] == 'WIN')
    l = len(trades) - w
    p = sum(t['pnl'] for t in trades)
    md = maxdd([t['pnl'] for t in trades])
    gw = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gl = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    pf = gw / gl if gl > 0 else 999
    print(f"  {label}: {w}W/{l}L ({w/len(trades)*100:.0f}% WR), {p:+.1f} pts, "
          f"{p/n_days:+.1f}/day, {p/len(trades):+.1f}/trade, MaxDD {md:.1f}, PF {pf:.2f}")


# ========================================================================
#  PART A: DD IMPROVEMENTS ON V9-SC
# ========================================================================

def part_a():
    print("="*80)
    print("PART A: DD IMPROVEMENT ON V9-SC FILTER")
    print("="*80)

    setups = fetch_setups()
    snaps = fetch_dd_charm_snaps()

    enriched = []
    for s in setups:
        ts, spot = s.ts, float(s.spot)
        direction = s.direction
        is_long = direction in ('long', 'bullish')

        dd_raw = nearest(snaps, 'deltaDecay', ts)
        ch_raw = nearest(snaps, 'charm', ts)

        dd_ns = dd_near_spot(dd_raw, spot) if dd_raw else 0
        dd_bullish = dd_ns < 0  # negative DD near spot = dealers buy = bullish
        stacked = dd_stacked_check(dd_raw, ch_raw, spot, is_long) if dd_raw and ch_raw else False

        passes_v9 = v9sc(s.setup_name, direction, s.greek_alignment,
                         float(s.vix) if s.vix else None,
                         float(s.overvix) if s.overvix else None, s.grade)

        enriched.append({
            'id': s.id, 'ts': s.ts, 'setup': s.setup_name,
            'direction': direction, 'spot': spot,
            'result': s.outcome_result,
            'pnl': float(s.outcome_pnl) if s.outcome_pnl else 0,
            'alignment': s.greek_alignment,
            'vix': float(s.vix) if s.vix else None,
            'v9': passes_v9,
            'dd_ns': dd_ns,
            'dd_bullish': dd_bullish,
            'stacked': stacked,
            'is_long': is_long,
            'grade': s.grade,
        })

    dates = sorted(set(str(e['ts'])[:10] for e in enriched))
    n_days = len(dates)
    v9 = [e for e in enriched if e['v9']]

    print(f"\nDate range: {dates[0]} to {dates[-1]} ({n_days} days)")
    print(f"V9-SC trades: {len(v9)}\n")

    # --- Baseline ---
    stats_line("V9-SC baseline", v9, n_days)

    # --- Variant 1: V9-SC + DD bias for longs ---
    # Block longs where DD near-spot is bearish (positive)
    v1 = [e for e in enriched if e['v9'] and not (e['is_long'] and e['dd_ns'] > 0)]
    blocked_v1 = [e for e in v9 if e['is_long'] and e['dd_ns'] > 0]
    print(f"\n--- V1: V9-SC + block longs when DD bearish near spot ---")
    stats_line("V1 result", v1, n_days)
    stats_line("V1 blocked", blocked_v1, n_days)
    print(f"  Delta vs V9-SC: {sum(t['pnl'] for t in v1) - sum(t['pnl'] for t in v9):+.1f} pts, "
          f"{len(v1) - len(v9):+d} trades")

    # --- Variant 2: V9-SC + DD stacking for ES Absorption ---
    # Block ES Absorption when NOT stacked
    v2 = [e for e in enriched if e['v9'] and not (e['setup'] == 'ES Absorption' and not e['stacked'])]
    blocked_v2 = [e for e in v9 if e['setup'] == 'ES Absorption' and not e['stacked']]
    print(f"\n--- V2: V9-SC + ES Absorption stacked-only ---")
    stats_line("V2 result", v2, n_days)
    stats_line("V2 blocked", blocked_v2, n_days)
    print(f"  Delta vs V9-SC: {sum(t['pnl'] for t in v2) - sum(t['pnl'] for t in v9):+.1f} pts, "
          f"{len(v2) - len(v9):+d} trades")

    # --- Variant 3: Both combined ---
    v3 = [e for e in enriched if e['v9']
          and not (e['is_long'] and e['dd_ns'] > 0)
          and not (e['setup'] == 'ES Absorption' and not e['stacked'])]
    blocked_v3 = [e for e in v9 if (e['is_long'] and e['dd_ns'] > 0) or
                  (e['setup'] == 'ES Absorption' and not e['stacked'])]
    print(f"\n--- V3: V9-SC + DD long bias + ES Abs stacking (both) ---")
    stats_line("V3 result", v3, n_days)
    stats_line("V3 blocked", blocked_v3, n_days)
    print(f"  Delta vs V9-SC: {sum(t['pnl'] for t in v3) - sum(t['pnl'] for t in v9):+.1f} pts, "
          f"{len(v3) - len(v9):+d} trades")

    # --- Variant 4: DD as alignment boost (not gate) ---
    # Add +1 alignment for longs when DD bullish, -1 when bearish
    # Then re-apply V9-SC alignment gate (>= 2)
    print(f"\n--- V4: DD as 4th alignment component (not a gate, alignment boost) ---")
    v4 = []
    v4_changed = 0
    for e in enriched:
        if not e['v9']:
            # Could a DD boost make this trade pass V9-SC?
            if e['is_long'] and e['grade'] != 'LOG':
                adj_align = (e['alignment'] or 0)
                if e['dd_bullish']:
                    adj_align += 1
                else:
                    adj_align -= 1
                if adj_align >= 2:
                    # Check VIX gate too
                    if (e['setup'] == 'Skew Charm' or
                        (e['vix'] and e['vix'] <= 22)):
                        v4.append(e)
                        v4_changed += 1
                        continue
            continue  # still blocked
        else:
            v4.append(e)

    print(f"  V4 trades: {len(v4)} (V9-SC {len(v9)} + {v4_changed} new from DD boost)")
    stats_line("V4 result", v4, n_days)
    new_trades = [e for e in v4 if not e['v9']]
    if new_trades:
        stats_line("V4 new trades (promoted by DD)", new_trades, n_days)

    # --- Per-day comparison: V9-SC vs best variant ---
    print(f"\n--- Daily comparison: V9-SC vs V1 (DD long bias filter) ---")
    print(f"  {'Date':>12} {'V9 PnL':>8} {'V1 PnL':>8} {'Delta':>8}")
    v9c = v1c = 0
    for d in dates:
        v9d = sum(e['pnl'] for e in v9 if str(e['ts'])[:10] == d)
        v1d = sum(e['pnl'] for e in v1 if str(e['ts'])[:10] == d)
        v9c += v9d; v1c += v1d
        delta = v1d - v9d
        mark = " ***" if abs(delta) > 15 else ""
        print(f"  {d:>12} {v9d:>+8.1f} {v1d:>+8.1f} {delta:>+8.1f}{mark}")
    print(f"  Cumulative: V9-SC {v9c:+.1f} | V1 {v1c:+.1f} | Delta {v1c-v9c:+.1f}")

    # --- Show blocked long trades (DD bearish) ---
    print(f"\n--- V1 blocked longs (DD bearish near spot): {len(blocked_v1)} trades ---")
    by_date = defaultdict(list)
    for e in blocked_v1:
        by_date[str(e['ts'])[:10]].append(e)
    for d in sorted(by_date.keys()):
        trades = by_date[d]
        w = sum(1 for t in trades if t['result'] == 'WIN')
        p = sum(t['pnl'] for t in trades)
        print(f"  {d}: {len(trades)} trades, {w}W/{len(trades)-w}L, {p:+.1f} pts "
              f"[{', '.join(t['setup'][:8] for t in trades)}]")


# ========================================================================
#  PART B: EOD BUTTERFLY TARGET (AGGREGATE DD)
# ========================================================================

def part_b():
    print(f"\n\n{'='*80}")
    print("PART B: EOD BUTTERFLY TARGET (Aggregate DD Hedging)")
    print("="*80)
    print("\nWizard: 'where DD is neutral = perfect close for dealers'")
    print("Method: Track aggregate DD value through the day, find when it trends to zero\n")

    # Fetch aggregate DD from volland_snapshots
    sql = text("""
        SELECT ts, payload
        FROM volland_snapshots
        WHERE ts::date >= '2026-02-11'
          AND payload IS NOT NULL
        ORDER BY ts
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    print(f"Volland snapshots: {len(rows)}")

    # Parse DD hedging value and spot from each snapshot
    daily = defaultdict(list)
    for r in rows:
        payload = r.payload if isinstance(r.payload, dict) else json.loads(r.payload)
        stats = payload.get('statistics', {})
        dd_str = stats.get('delta_decay_hedging') or stats.get('deltadecayHedging')
        spot = payload.get('spot')
        lis = stats.get('lis')

        if not dd_str or not spot:
            continue

        # Parse "$7,298,110,681" or "-$500,000,000" to numeric
        dd_clean = dd_str.replace('$', '').replace(',', '').strip()
        try:
            dd_val = float(dd_clean)
        except ValueError:
            continue

        # Parse spot
        if isinstance(spot, str):
            try:
                spot = float(spot.replace(',', ''))
            except ValueError:
                continue

        d = str(r.ts.date())
        # Estimate ET hour (UTC-4 for EDT after Mar 9, UTC-5 before)
        utc_hour = r.ts.hour
        utc_min = r.ts.minute
        if r.ts.month > 3 or (r.ts.month == 3 and r.ts.day >= 9):
            et_hour = utc_hour - 4
        else:
            et_hour = utc_hour - 5
        et_min = utc_min
        et_decimal = et_hour + et_min / 60.0

        daily[d].append({
            'ts': r.ts, 'dd': dd_val, 'spot': float(spot),
            'lis': lis, 'et_hour': et_decimal
        })

    print(f"Days with DD data: {len(daily)}")

    # Fetch actual closes
    sql2 = text("""
        SELECT DISTINCT ON (ts::date)
            ts::date as d, spot
        FROM chain_snapshots
        WHERE ts::date >= '2026-02-11' AND spot IS NOT NULL
        ORDER BY ts::date, ts DESC
    """)
    with engine.connect() as conn:
        close_rows = conn.execute(sql2).fetchall()
    actual_closes = {str(r.d): float(r.spot) for r in close_rows if r.spot}
    print(f"Days with close data: {len(actual_closes)}")

    # For each day, analyze DD trajectory and predict close
    print(f"\n{'='*80}")
    print("B1. AGGREGATE DD TRAJECTORY THROUGH THE DAY")
    print(f"{'='*80}\n")

    print("  DD > 0 = bearish (dealers must sell). DD < 0 = bullish (dealers must buy).")
    print("  DD trending toward 0 = dealers unwinding = price settling.\n")

    all_eod_data = []

    for d in sorted(daily.keys()):
        snaps = daily[d]
        actual = actual_closes.get(d)
        if not actual or len(snaps) < 5:
            continue

        # Sample DD at key times
        open_snaps = [s for s in snaps if 9.5 <= s['et_hour'] <= 10.0]
        mid_snaps = [s for s in snaps if 12.0 <= s['et_hour'] <= 12.5]
        pm_snaps = [s for s in snaps if 14.0 <= s['et_hour'] <= 14.5]
        late_snaps = [s for s in snaps if 15.0 <= s['et_hour'] <= 15.5]
        close_snaps = [s for s in snaps if 15.5 <= s['et_hour'] <= 16.0]

        def avg_snap(sl):
            if not sl: return None, None
            return sum(s['dd'] for s in sl)/len(sl), sum(s['spot'] for s in sl)/len(sl)

        dd_open, spot_open = avg_snap(open_snaps)
        dd_mid, spot_mid = avg_snap(mid_snaps)
        dd_pm, spot_pm = avg_snap(pm_snaps)
        dd_late, spot_late = avg_snap(late_snaps)
        dd_close, spot_close = avg_snap(close_snaps)

        all_eod_data.append({
            'd': d, 'actual': actual,
            'dd_open': dd_open, 'dd_mid': dd_mid,
            'dd_pm': dd_pm, 'dd_late': dd_late, 'dd_close': dd_close,
            'spot_open': spot_open, 'spot_mid': spot_mid,
            'spot_pm': spot_pm, 'spot_late': spot_late, 'spot_close': spot_close,
        })

    # Print DD trajectory table
    print(f"  {'Date':>12} {'DD@Open':>10} {'DD@12':>10} {'DD@14':>10} {'DD@15':>10} {'DD@Close':>10} {'Close':>8} {'Move':>6}")
    print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*6}")

    for e in all_eod_data:
        def fmt_dd(v):
            if v is None: return "---"
            return f"{v/1e9:+.1f}B"
        move = ""
        if e['spot_open'] and e['actual']:
            move = f"{e['actual'] - e['spot_open']:+.0f}"
        print(f"  {e['d']:>12} {fmt_dd(e['dd_open']):>10} {fmt_dd(e['dd_mid']):>10} "
              f"{fmt_dd(e['dd_pm']):>10} {fmt_dd(e['dd_late']):>10} {fmt_dd(e['dd_close']):>10} "
              f"{e['actual']:>8.1f} {move:>6}")

    # ── B2: DD direction vs actual move ──
    print(f"\n{'='*80}")
    print("B2. DD DIRECTION vs AFTERNOON MOVE")
    print(f"{'='*80}\n")

    print("  If DD@14:00 is positive (bearish), does price go down? And vice versa.\n")
    correct_pm = 0
    total_pm = 0
    details_pm = []
    for e in all_eod_data:
        if e['dd_pm'] is None or e['spot_pm'] is None:
            continue
        dd_bearish = e['dd_pm'] > 0
        price_fell = e['actual'] < e['spot_pm']
        correct = dd_bearish == price_fell
        total_pm += 1
        if correct: correct_pm += 1
        details_pm.append({
            'd': e['d'], 'dd_pm': e['dd_pm'], 'spot_pm': e['spot_pm'],
            'actual': e['actual'], 'move': e['actual'] - e['spot_pm'],
            'correct': correct
        })

    print(f"  DD@14:00 predicts direction: {correct_pm}/{total_pm} ({correct_pm/total_pm*100:.0f}%)\n")
    print(f"  {'Date':>12} {'DD@14':>10} {'Signal':>8} {'SPX@14':>8} {'Close':>8} {'PM Move':>8} {'Correct':>8}")
    for dp in details_pm:
        sig = "BEAR" if dp['dd_pm'] > 0 else "BULL"
        ok = "YES" if dp['correct'] else "NO"
        print(f"  {dp['d']:>12} {dp['dd_pm']/1e9:>+10.1f}B {sig:>8} {dp['spot_pm']:>8.1f} "
              f"{dp['actual']:>8.1f} {dp['move']:>+8.1f} {ok:>8}")

    # ── B3: DD Magnitude vs Move Size ──
    print(f"\n{'='*80}")
    print("B3. DD MAGNITUDE vs AFTERNOON MOVE SIZE")
    print(f"{'='*80}\n")

    print("  Does larger |DD| at 14:00 predict larger afternoon moves?\n")
    for lo, hi, label in [(0, 2e9, "<2B"), (2e9, 5e9, "2-5B"),
                          (5e9, 10e9, "5-10B"), (10e9, 1e15, ">10B")]:
        bucket = [d for d in details_pm if lo <= abs(d['dd_pm']) < hi]
        if bucket:
            avg_move = sum(abs(d['move']) for d in bucket) / len(bucket)
            correct = sum(1 for d in bucket if d['correct'])
            print(f"  |DD| {label}: {len(bucket)} days, avg |move| {avg_move:.1f} pts, "
                  f"direction correct {correct}/{len(bucket)} ({correct/len(bucket)*100:.0f}%)")

    # ── B4: DD Trend (shrinking toward zero = settling) ──
    print(f"\n{'='*80}")
    print("B4. DD TREND: Does DD Shrinking Toward Zero = Price Settling?")
    print(f"{'='*80}\n")

    print("  When |DD| decreases from 14:00 to 15:00, does price range narrow?\n")
    shrinking = []
    growing = []
    for e in all_eod_data:
        if e['dd_pm'] is None or e['dd_late'] is None or e['spot_pm'] is None:
            continue
        dd_change = abs(e['dd_late']) - abs(e['dd_pm'])
        pm_range = abs(e['actual'] - e['spot_pm'])
        entry = {'d': e['d'], 'dd_change': dd_change, 'pm_range': pm_range, 'actual': e['actual']}
        if dd_change < 0:
            shrinking.append(entry)
        else:
            growing.append(entry)

    if shrinking:
        avg_range_s = sum(e['pm_range'] for e in shrinking) / len(shrinking)
        print(f"  DD shrinking (14->15): {len(shrinking)} days, avg PM range {avg_range_s:.1f} pts")
    if growing:
        avg_range_g = sum(e['pm_range'] for e in growing) / len(growing)
        print(f"  DD growing (14->15):   {len(growing)} days, avg PM range {avg_range_g:.1f} pts")

    # ── B5: Butterfly strike targeting ──
    print(f"\n{'='*80}")
    print("B5. BUTTERFLY STRIKE TARGET")
    print(f"{'='*80}\n")

    print("  Concept: When DD is approaching zero, price is settling. Center butterfly at current spot.")
    print("  When DD is large, price will move. Use DD direction for wing placement.\n")
    print("  Ideal butterfly entry: 14:00-14:30 when DD is shrinking toward zero.")
    print("  Target strike = spot at that time (DD neutral = price stays put).\n")

    # Simulate: at 14:00, if |DD| < threshold, enter butterfly centered at spot
    # Profit = how close price closed to the center strike
    print(f"  {'Date':>12} {'Spot@14':>8} {'|DD|@14':>10} {'Close':>8} {'|Error|':>8} "
          f"{'$5w BFly':>10} {'$10w BFly':>10}")
    print(f"  {'-'*12} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*10} {'-'*10}")

    bfly_5_total = 0
    bfly_10_total = 0
    bfly_days = 0

    for dp in details_pm:
        err = abs(dp['actual'] - dp['spot_pm'])
        # $5-wide butterfly: max profit if close = center, zero if close >= center +/- 5
        bfly_5_pnl = max(0, 5 - err) * 100  # per contract, SPX options $100 multiplier
        bfly_5_cost = 250  # typical $5-wide SPX butterfly costs ~$2.50
        bfly_5_net = bfly_5_pnl - bfly_5_cost

        # $10-wide butterfly
        bfly_10_pnl = max(0, 10 - err) * 100
        bfly_10_cost = 450  # typical $10-wide costs ~$4.50
        bfly_10_net = bfly_10_pnl - bfly_10_cost

        bfly_5_total += bfly_5_net
        bfly_10_total += bfly_10_net
        bfly_days += 1

        print(f"  {dp['d']:>12} {dp['spot_pm']:>8.1f} {abs(dp['dd_pm'])/1e9:>10.1f}B "
              f"{dp['actual']:>8.1f} {err:>8.1f} {bfly_5_net:>+10.0f} {bfly_10_net:>+10.0f}")

    if bfly_days:
        print(f"\n  Total P&L ($5-wide butterflies): ${bfly_5_total:+,.0f} over {bfly_days} days (${bfly_5_total/bfly_days:+,.0f}/day)")
        print(f"  Total P&L ($10-wide butterflies): ${bfly_10_total:+,.0f} over {bfly_days} days (${bfly_10_total/bfly_days:+,.0f}/day)")

    # Better approach: only enter butterfly when DD is SMALL (settling)
    print(f"\n  --- Selective entry: only when |DD@14| < 5B (settling) ---")
    settling = [dp for dp in details_pm if abs(dp['dd_pm']) < 5e9]
    active = [dp for dp in details_pm if abs(dp['dd_pm']) >= 5e9]

    if settling:
        err_s = sum(abs(d['actual'] - d['spot_pm']) for d in settling) / len(settling)
        bfly_s = sum(max(0, 10 - abs(d['actual']-d['spot_pm']))*100 - 450 for d in settling)
        print(f"  Settling ({len(settling)} days): avg error {err_s:.1f} pts, $10-wide BFly ${bfly_s:+,.0f}")
    if active:
        err_a = sum(abs(d['actual'] - d['spot_pm']) for d in active) / len(active)
        bfly_a = sum(max(0, 10 - abs(d['actual']-d['spot_pm']))*100 - 450 for d in active)
        print(f"  Active  ({len(active)} days): avg error {err_a:.1f} pts, $10-wide BFly ${bfly_a:+,.0f}")

    # ── B6: DD direction for directional butterfly ──
    print(f"\n{'='*80}")
    print("B6. DIRECTIONAL BUTTERFLY (Use DD Direction for Wing Bias)")
    print(f"{'='*80}\n")

    print("  Instead of centering at spot, shift butterfly toward DD's predicted direction.")
    print("  DD bullish (negative) -> center 5 pts above spot")
    print("  DD bearish (positive) -> center 5 pts below spot\n")

    dir_bfly_total = 0
    for dp in details_pm:
        if dp['dd_pm'] < 0:  # bullish
            center = dp['spot_pm'] + 5
        else:  # bearish
            center = dp['spot_pm'] - 5
        err = abs(dp['actual'] - center)
        bfly_pnl = max(0, 10 - err) * 100 - 450
        dir_bfly_total += bfly_pnl

    neutral_bfly_total = sum(max(0, 10 - abs(d['actual']-d['spot_pm']))*100 - 450 for d in details_pm)
    print(f"  Neutral butterfly (center = spot):         ${neutral_bfly_total:+,.0f} total")
    print(f"  Directional butterfly (shift 5 pts by DD): ${dir_bfly_total:+,.0f} total")
    print(f"  Improvement: ${dir_bfly_total - neutral_bfly_total:+,.0f}")

    # ── B7: Summary ──
    print(f"\n{'='*80}")
    print("PART B SUMMARY")
    print(f"{'='*80}\n")

    if details_pm:
        print(f"  Days analyzed: {len(details_pm)}")
        print(f"  DD@14 direction accuracy: {correct_pm}/{total_pm} ({correct_pm/total_pm*100:.0f}%)")
        avg_err = sum(abs(d['move']) for d in details_pm) / len(details_pm)
        print(f"  Avg afternoon |move|: {avg_err:.1f} pts")
        if settling:
            print(f"  Settling days ({len(settling)}): avg error {err_s:.1f} pts from spot@14")


# ========================================================================
#  MAIN
# ========================================================================

if __name__ == '__main__':
    part_a()
    part_b()
