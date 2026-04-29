"""Backtest: Would VX flow data have improved our setup outcomes today?
Cross-references each setup signal with VX seller/buyer flow at that moment."""
import struct
from datetime import datetime, timedelta
from collections import defaultdict

SCID_FILE = r"C:\SierraChart\Data\VXM26_FUT_CFE.scid"
SC_EPOCH = datetime(1899, 12, 30)
MICROS_PER_DAY = 86_400_000_000


def sc_dt(val):
    if val <= 0: return None
    return SC_EPOCH + timedelta(days=val // MICROS_PER_DAY, microseconds=val % MICROS_PER_DAY)


def load_vx_ticks():
    """Load all VX ticks for 2026-03-25 market hours."""
    with open(SCID_FILE, "rb") as f:
        f.seek(0, 2)
        n = (f.tell() - 56) // 40
        f.seek(56)
        data = f.read()

    ticks = []
    for i in range(n):
        dt_raw, o, h, l, c, nt, vol, bv, av = struct.unpack_from("<qffffIIII", data, i * 40)
        dt = sc_dt(dt_raw)
        if not dt or dt.date() != datetime(2026, 3, 25).date():
            continue
        if dt.hour < 13 or (dt.hour == 13 and dt.minute < 30) or dt.hour >= 20:
            continue
        if abs(o) > 0.001 and o > -1e30:
            continue  # skip bar records
        dt_et = dt - timedelta(hours=4)  # UTC -> ET
        ticks.append({
            "dt_et": dt_et,
            "price": c,
            "volume": vol,
            "delta": int(av) - int(bv),
            "buy": av,
            "sell": bv,
        })
    return ticks


def vx_flow_at(ticks, time_et, window_min=10):
    """Get VX flow in a window around a given ET time.
    Returns (net_delta, buy_vol, sell_vol, vx_price, cvd_from_open)."""
    t_start = time_et - timedelta(minutes=window_min)
    t_end = time_et + timedelta(minutes=2)  # slight forward look

    window = [t for t in ticks if t_start <= t["dt_et"] <= t_end]
    if not window:
        return 0, 0, 0, 0, 0

    net = sum(t["delta"] for t in window)
    buy = sum(t["buy"] for t in window)
    sell = sum(t["sell"] for t in window)
    price = window[-1]["price"]

    # CVD from market open to this point
    before = [t for t in ticks if t["dt_et"] <= time_et]
    cvd = sum(t["delta"] for t in before)

    return net, buy, sell, price, cvd


def main():
    ticks = load_vx_ticks()
    print(f"Loaded {len(ticks)} VX ticks\n")

    # Today's setups (from DB query output, timestamps in UTC -> convert to ET)
    # Format: (ts_utc_str, setup_name, direction, grade, outcome, pnl, align)
    setups = [
        ("13:50:21", "Skew Charm",      "long",    "A+", "LOSS",    -14, +3),
        ("14:00:51", "DD Exhaustion",    "long",    "B",  "WIN",     +15, +3),
        ("14:12:21", "AG Short",         "short",   "A",  "WIN",     +18, -3),
        ("14:16:46", "ES Absorption",    "short",   "C",  "LOSS",     -8, -3),
        ("14:16:51", "VIX Compression",  "long",    "B",  "LOSS",    -20, +3),
        ("14:19:26", "SB2 Absorption",   "short",   "A",  "LOSS",     -8, -2),
        ("14:20:51", "Skew Charm",       "long",    "A+", "WIN",      +6, +3),
        ("14:32:51", "DD Exhaustion",    "long",    "B",  "LOSS",    -12, +3),
        ("14:57:46", "Skew Charm",       "long",    "A+", "LOSS",    -14, +3),
        ("14:59:22", "ES Absorption",    "long",    "B",  "LOSS",     -8, +3),
        ("15:03:15", "AG Short",         "short",   "A",  "WIN",     +24, -3),
        ("15:06:46", "DD Exhaustion",    "long",    "B",  "LOSS",    -12, +3),
        ("15:16:22", "ES Absorption",    "long",    "B",  "LOSS",     -8, +3),
        ("15:24:45", "AG Short",         "short",   "B",  "WIN",      +5, -3),
        ("15:28:27", "ES Absorption",    "short",   "A",  "LOSS",     -8, +3),  # align seems wrong but from data
        ("15:30:15", "AG Short",         "short",   "A+", "LOSS",    -20, -3),
        ("15:37:45", "Skew Charm",       "long",    "A",  "WIN",     +10, +3),
        ("15:39:15", "DD Exhaustion",    "long",    "B",  "WIN",     +14, +3),
        ("15:41:35", "ES Absorption",    "long",    "B",  "WIN",     +10, +2),
        ("15:44:45", "AG Short",         "short",   "A+", "LOSS",    -20, -3),
        ("16:01:13", "ES Absorption",    "long",    "A",  "WIN",     +10, +3),
        ("16:08:45", "Skew Charm",       "long",    "A",  "WIN",     +10, +3),
        ("16:09:15", "DD Exhaustion",    "long",    "A",  "WIN",     +15, +3),
        ("16:20:13", "ES Absorption",    "short",   "A",  "LOSS",     -8, -3),
        ("16:33:55", "ES Absorption",    "short",   "A",  "WIN",     +10, -3),
        ("16:43:45", "DD Exhaustion",    "long",    "A",  "LOSS",     -1, +3),
        ("16:46:45", "Skew Charm",       "long",    "B",  "WIN",      +7, +3),
        ("17:02:45", "AG Short",         "short",   "A+", "LOSS",    -20, -3),
        ("17:17:15", "Skew Charm",       "long",    "B",  "WIN",     +11, +3),
        ("17:18:15", "DD Exhaustion",    "long",    "A",  "LOSS",     -1, +3),
        ("17:19:36", "ES Absorption",    "long",    "A",  "WIN",     +10, +3),
        ("17:30:15", "SB Absorption",    "short",   "A",  "LOSS",     -8, -2),
        ("17:43:26", "ES Absorption",    "short",   "A",  "WIN",     +10, -3),
        ("17:44:15", "AG Short",         "short",   "A+", "WIN",     +12, -3),
        ("17:49:15", "Skew Charm",       "long",    "B",  "LOSS",    -14, +3),
        ("17:50:15", "DD Exhaustion",    "long",    "A",  "LOSS",    -12, +3),
        ("17:58:45", "AG Short",         "short",   "A+", "WIN",     +13, -3),
        ("18:05:45", "AG Short",         "short",   "A+", "WIN",     +13, -3),
        ("18:16:45", "AG Short",         "short",   "A+", "WIN",     +14, -3),
        ("18:17:57", "ES Absorption",    "short",   "A",  "WIN",     +10, -3),
        ("18:24:45", "DD Exhaustion",    "long",    "A",  "LOSS",    -12, +3),
        ("18:27:15", "AG Short",         "short",   "A+", "WIN",     +12, -3),
        ("18:28:45", "Skew Charm",       "long",    "B",  "LOSS",    -14, +3),
        ("18:36:44", "AG Short",         "short",   "A+", "WIN",     +11, -1),
        ("18:39:15", "AG Short",         "short",   "A+", "WIN",     +11, -1),
        ("18:50:32", "ES Absorption",    "long",    "A",  "LOSS",     -8, +1),
        ("18:54:46", "DD Exhaustion",    "long",    "C",  "LOSS",    -12, +3),
        ("18:59:01", "Skew Charm",       "long",    "C",  "EXPIRED", -1, +3),
        ("19:13:44", "ES Absorption",    "short",   "B",  "LOSS",     -8, -3),
        ("19:27:15", "DD Exhaustion",    "long",    "B",  "EXPIRED", -1, +3),
        ("19:31:15", "Skew Charm",       "long",    "C",  "EXPIRED", +1, +3),
    ]

    print("=" * 100)
    print("SETUP vs VX FLOW CROSS-REFERENCE")
    print("=" * 100)
    print(f"{'Time ET':>8} | {'Setup':>16} | {'Dir':>5} | {'Grade':>4} | {'Outcome':>7} | "
          f"{'PnL':>5} | {'VX 10m':>7} | {'VX CVD':>7} | {'VX Flow':>12} | Match?")
    print("-" * 100)

    # Track stats
    stats = {
        "aligned": {"wins": 0, "losses": 0, "pnl": 0, "trades": 0},
        "against": {"wins": 0, "losses": 0, "pnl": 0, "trades": 0},
        "neutral": {"wins": 0, "losses": 0, "pnl": 0, "trades": 0},
    }
    setup_stats = defaultdict(lambda: {"aligned_w": 0, "aligned_l": 0, "against_w": 0, "against_l": 0,
                                        "neutral_w": 0, "neutral_l": 0,
                                        "aligned_pnl": 0, "against_pnl": 0, "neutral_pnl": 0})

    for ts_str, name, direction, grade, outcome, pnl, align in setups:
        # Convert UTC to ET
        h, m, s = map(int, ts_str.split(":"))
        et_h = h - 4  # UTC to ET
        time_et = datetime(2026, 3, 25, et_h, m, s)

        # Skip if outside VX data range (after 16:00 ET)
        if et_h >= 16:
            # No VX data after 16:00
            continue

        net_delta, buy, sell, vx_price, cvd = vx_flow_at(ticks, time_et, window_min=10)

        # Determine VX flow direction
        if net_delta < -30:
            vx_flow = "SELLERS"  # vol sellers = bullish SPX
        elif net_delta > 30:
            vx_flow = "BUYERS"  # vol buyers = bearish SPX
        else:
            vx_flow = "NEUTRAL"

        # Does VX flow align with the setup direction?
        if direction in ("long",):
            if vx_flow == "SELLERS":
                match = "ALIGNED"  # vol sellers + long = both bullish
            elif vx_flow == "BUYERS":
                match = "AGAINST"  # vol buyers + long = conflicting
            else:
                match = "NEUTRAL"
        else:  # short
            if vx_flow == "BUYERS":
                match = "ALIGNED"  # vol buyers + short = both bearish
            elif vx_flow == "SELLERS":
                match = "AGAINST"  # vol sellers + short = conflicting
            else:
                match = "NEUTRAL"

        is_win = outcome == "WIN"
        bucket = match.lower()
        stats[bucket]["trades"] += 1
        stats[bucket]["pnl"] += pnl
        if is_win:
            stats[bucket]["wins"] += 1
        else:
            stats[bucket]["losses"] += 1

        sn = name
        if is_win:
            setup_stats[sn][f"{bucket}_w"] += 1
        else:
            setup_stats[sn][f"{bucket}_l"] += 1
        setup_stats[sn][f"{bucket}_pnl"] += pnl

        et_str = f"{et_h:02d}:{m:02d}"
        win_tag = "W" if is_win else "L"
        print(f"{et_str:>8} | {name:>16} | {direction:>5} | {grade:>4} | {outcome:>7} | "
              f"{pnl:>+5.0f} | {net_delta:>+7d} | {cvd:>+7d} | {vx_flow:>12} | {match} {win_tag}")

    # SUMMARY
    print("\n" + "=" * 100)
    print("SUMMARY: VX FLOW ALIGNMENT vs OUTCOME")
    print("=" * 100)

    for bucket in ["aligned", "against", "neutral"]:
        s = stats[bucket]
        if s["trades"] == 0:
            continue
        wr = s["wins"] / s["trades"] * 100
        print(f"\n  {bucket.upper()} (setup agrees with VX flow):")
        print(f"    Trades: {s['trades']}  |  Wins: {s['wins']}  Losses: {s['losses']}  |  "
              f"WR: {wr:.0f}%  |  PnL: {s['pnl']:+.0f} pts")

    # Per-setup breakdown
    print("\n" + "=" * 100)
    print("PER-SETUP BREAKDOWN")
    print("=" * 100)

    for sn in sorted(setup_stats.keys()):
        ss = setup_stats[sn]
        al_t = ss["aligned_w"] + ss["aligned_l"]
        ag_t = ss["against_w"] + ss["against_l"]
        ne_t = ss["neutral_w"] + ss["neutral_l"]

        print(f"\n  {sn}:")
        if al_t:
            al_wr = ss["aligned_w"] / al_t * 100
            print(f"    ALIGNED:  {al_t} trades, {ss['aligned_w']}W/{ss['aligned_l']}L, "
                  f"WR={al_wr:.0f}%, PnL={ss['aligned_pnl']:+.0f}")
        if ag_t:
            ag_wr = ss["against_w"] / ag_t * 100
            print(f"    AGAINST:  {ag_t} trades, {ss['against_w']}W/{ss['against_l']}L, "
                  f"WR={ag_wr:.0f}%, PnL={ss['against_pnl']:+.0f}")
        if ne_t:
            ne_wr = ss["neutral_w"] / ne_t * 100
            print(f"    NEUTRAL:  {ne_t} trades, {ss['neutral_w']}W/{ss['neutral_l']}L, "
                  f"WR={ne_wr:.0f}%, PnL={ss['neutral_pnl']:+.0f}")

    # FILTER SIMULATION
    print("\n" + "=" * 100)
    print("FILTER SIMULATION: Block trades when VX flow AGAINST setup direction")
    print("=" * 100)

    total_all = sum(s["trades"] for s in stats.values())
    pnl_all = sum(s["pnl"] for s in stats.values())
    wins_all = sum(s["wins"] for s in stats.values())

    blocked = stats["against"]["trades"]
    blocked_pnl = stats["against"]["pnl"]
    remaining = total_all - blocked
    remaining_pnl = pnl_all - blocked_pnl
    remaining_wins = wins_all - stats["against"]["wins"]

    print(f"\n  WITHOUT VX filter:")
    print(f"    {total_all} trades, {wins_all}W, WR={wins_all/total_all*100:.0f}%, PnL={pnl_all:+.0f} pts")
    print(f"\n  WITH VX filter (block AGAINST):")
    print(f"    Blocked: {blocked} trades ({blocked_pnl:+.0f} pts removed)")
    print(f"    Remaining: {remaining} trades, {remaining_wins}W, "
          f"WR={remaining_wins/remaining*100:.0f}%, PnL={remaining_pnl:+.0f} pts")
    print(f"\n  IMPROVEMENT: {remaining_pnl - pnl_all:+.0f} pts, "
          f"WR {wins_all/total_all*100:.0f}% -> {remaining_wins/remaining*100:.0f}%")


if __name__ == "__main__":
    main()
