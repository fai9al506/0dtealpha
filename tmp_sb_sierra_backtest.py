"""
SB Absorption Backtest from Sierra Chart tick data (.scid files)
Builds 5-pt and 10-pt range bars from raw ticks, runs SB detection on both.
"""

import struct
import os
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import pytz

NY = pytz.timezone("US/Eastern")
OLE_BASE = datetime(1899, 12, 30)

# ── SCID Reader ──────────────────────────────────────────────────────────────

def read_scid(path, start_date=None, end_date=None):
    """Read Sierra Chart .scid file, yield (dt, price, volume, buy_vol, sell_vol, delta) per tick.

    SCID format: 56-byte header + 40-byte records.
    Timestamp: SCDateTimeMS (int64 microseconds since Dec 30, 1899).
    Prices: stored as price × 100 for ES futures.
    """
    USEC_PER_DAY = 86_400_000_000

    with open(path, "rb") as f:
        header = f.read(56)
        if header[:4] != b"SCID":
            raise ValueError(f"Not a SCID file: {path}")
        record_size = struct.unpack_from("<I", header, 8)[0]
        if record_size < 40:
            raise ValueError(f"Unexpected record size: {record_size}")

        file_size = os.path.getsize(path)
        total_records = (file_size - 56) // record_size
        print(f"    {os.path.basename(path)}: {total_records:,} records...", end="", flush=True)
        rec_count = 0

        # Read in bulk for speed (1000 records at a time)
        chunk_size = 1000 * record_size
        buf = b""
        pos = 0
        while True:
            if pos >= len(buf):
                buf = f.read(chunk_size)
                pos = 0
                if not buf:
                    break
            if pos + record_size > len(buf):
                break
            rec = buf[pos:pos + record_size]
            pos += record_size
            rec_count += 1

            # SCDateTimeMS as int64 (first 8 bytes)
            ts_usec = struct.unpack_from("<q", rec, 0)[0]
            if ts_usec == 0:
                continue

            # OHLC (floats, price × 100 for ES), NumTrades, TotalVol, BidVol, AskVol
            o, h, l, c = struct.unpack_from("<4f", rec, 8)
            num_trades, total_vol, bid_vol, ask_vol = struct.unpack_from("<4I", rec, 24)

            if total_vol == 0:
                continue

            # Convert timestamp: microseconds since Dec 30, 1899 → datetime
            days = ts_usec / USEC_PER_DAY
            dt = OLE_BASE + timedelta(days=days)
            dt_et = NY.localize(dt)

            if start_date and dt_et.date() < start_date:
                continue
            if end_date and dt_et.date() > end_date:
                continue

            # ES prices stored as × 100
            price = c / 100.0

            # bid_vol = volume traded at bid (seller aggressor)
            # ask_vol = volume traded at ask (buyer aggressor)
            buy_vol = ask_vol
            sell_vol = bid_vol
            delta = buy_vol - sell_vol

            yield dt_et, price, total_vol, buy_vol, sell_vol, delta

        print(f" done ({rec_count:,} read)", flush=True)


# ── Range Bar Builder ────────────────────────────────────────────────────────

class RangeBarBuilder:
    def __init__(self, range_pts):
        self.range_pts = range_pts
        self.bars = []
        self._forming = None
        self._cvd = 0
        self._bar_idx = 0

    def process_tick(self, dt, price, volume, buy_vol, sell_vol, delta):
        """Process one tick. Returns completed bar dict or None."""
        self._cvd += delta

        if self._forming is None:
            self._forming = {
                "open": price, "high": price, "low": price, "close": price,
                "volume": 0, "buy": 0, "sell": 0, "delta": 0,
                "ts_start": dt, "ts_end": dt,
                "cvd_open": self._cvd, "cvd_high": self._cvd, "cvd_low": self._cvd,
            }

        bar = self._forming
        bar["close"] = price
        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["volume"] += volume
        bar["buy"] += buy_vol
        bar["sell"] += sell_vol
        bar["delta"] += delta
        bar["ts_end"] = dt
        bar["cvd_high"] = max(bar["cvd_high"], self._cvd)
        bar["cvd_low"] = min(bar["cvd_low"], self._cvd)

        if bar["high"] - bar["low"] >= self.range_pts - 0.001:
            completed = {
                "idx": self._bar_idx,
                "open": bar["open"], "high": bar["high"],
                "low": bar["low"], "close": bar["close"],
                "volume": bar["volume"], "delta": bar["delta"],
                "buy_volume": bar["buy"], "sell_volume": bar["sell"],
                "cvd": self._cvd,
                "cvd_open": bar["cvd_open"], "cvd_high": bar["cvd_high"],
                "cvd_low": bar["cvd_low"], "cvd_close": self._cvd,
                "ts_start": bar["ts_start"], "ts_end": bar["ts_end"],
                "status": "closed",
            }
            self.bars.append(completed)
            self._bar_idx += 1
            self._forming = {
                "open": price, "high": price, "low": price, "close": price,
                "volume": 0, "buy": 0, "sell": 0, "delta": 0,
                "ts_start": dt, "ts_end": dt,
                "cvd_open": self._cvd, "cvd_high": self._cvd, "cvd_low": self._cvd,
            }
            return completed
        return None

    def reset_session(self):
        """Reset for new trading day."""
        self.bars = []
        self._forming = None
        self._cvd = 0
        self._bar_idx = 0


# ── SB Absorption Detection ─────────────────────────────────────────────────

def detect_sb_absorption(bars, vol_mult=2.0, delta_mult=2.0, cvd_lookback=8, vol_window=20):
    """Detect single-bar absorption on the latest closed bar.
    Returns signal dict or None."""
    min_bars = vol_window + cvd_lookback
    closed = [b for b in bars if b.get("status") == "closed"]
    if len(closed) < min_bars:
        return None

    trigger = closed[-1]

    # Volume gate
    recent_vols = [b["volume"] for b in closed[-(vol_window + 1):-1]]
    vol_avg = sum(recent_vols) / len(recent_vols) if recent_vols else 0
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < vol_mult:
        return None

    # Delta gate
    recent_deltas = [abs(b.get("delta", 0)) for b in closed[-(vol_window + 1):-1]]
    delta_avg = sum(recent_deltas) / len(recent_deltas) if recent_deltas else 0
    if delta_avg <= 0:
        return None
    delta_ratio = abs(trigger.get("delta", 0)) / delta_avg
    if delta_ratio < delta_mult:
        return None

    # Absorption check: close vs delta direction
    bar_delta = trigger.get("delta", 0)
    is_red = trigger["close"] < trigger["open"]
    is_green = trigger["close"] > trigger["open"]

    direction = None
    if is_red and bar_delta > 0:
        direction = "bearish"
    elif is_green and bar_delta < 0:
        direction = "bullish"
    if direction is None:
        return None

    # CVD trend alignment
    cvd_start = closed[-(cvd_lookback + 1)]["cvd"]
    cvd_end = trigger["cvd"]
    cvd_trend = cvd_end - cvd_start

    if direction == "bearish" and cvd_trend <= 0:
        return None
    if direction == "bullish" and cvd_trend >= 0:
        return None

    # Score
    vol_score = min(25, int((vol_ratio - vol_mult) / vol_mult * 25))
    delta_score = min(25, int((delta_ratio - delta_mult) / delta_mult * 25))
    cvd_score = min(20, int(abs(cvd_trend) / 500 * 20))
    total_score = vol_score + delta_score + cvd_score

    if total_score >= 70:
        grade = "A+"
    elif total_score >= 50:
        grade = "A"
    elif total_score >= 30:
        grade = "B"
    else:
        grade = "C"

    return {
        "direction": direction,
        "grade": grade,
        "score": total_score,
        "bar_idx": trigger["idx"],
        "entry": trigger["close"],
        "vol_ratio": round(vol_ratio, 1),
        "delta_ratio": round(delta_ratio, 1),
        "bar_delta": bar_delta,
        "cvd_trend": cvd_trend,
        "ts": trigger["ts_end"],
    }


# ── Outcome Simulation ──────────────────────────────────────────────────────

def simulate_outcome(bars, signal, sl=8, t1_target=10, trail_activation=20, trail_gap=10):
    """Simulate split-target outcome: T1 fixed +10, T2 trail (BE@10, act@20, gap@10).
    PnL = average of T1 and T2."""
    is_long = signal["direction"] == "bullish"
    entry = signal["entry"]
    bar_idx = signal["bar_idx"]

    hit_t1 = False
    trail_stop = entry - sl if is_long else entry + sl
    trail_peak = 0.0
    trail_exit_pnl = None
    max_profit = 0.0
    max_loss = 0.0

    for bar in bars:
        if bar["idx"] <= bar_idx:
            continue
        bh, bl = bar["high"], bar["low"]

        ph = (bh - entry) if is_long else (entry - bl)
        pl = (bl - entry) if is_long else (entry - bh)

        if ph > max_profit:
            max_profit = ph
        if pl < max_loss:
            max_loss = pl

        # T1
        if not hit_t1:
            if (is_long and bh >= entry + t1_target) or (not is_long and bl <= entry - t1_target):
                hit_t1 = True

        # Trail
        if trail_exit_pnl is None:
            if ph > trail_peak:
                trail_peak = ph
            if trail_peak >= trail_activation:
                trail_lock = max(trail_peak - trail_gap, 0)
                if is_long:
                    ns = entry + trail_lock
                    if ns > trail_stop:
                        trail_stop = ns
                else:
                    ns = entry - trail_lock
                    if ns < trail_stop:
                        trail_stop = ns
            elif trail_peak >= t1_target:
                # BE after T1 target reached
                if is_long and entry > trail_stop:
                    trail_stop = entry
                elif not is_long and entry < trail_stop:
                    trail_stop = entry

            if (is_long and bl <= trail_stop) or (not is_long and bh >= trail_stop):
                trail_exit_pnl = round((trail_stop - entry) if is_long else (entry - trail_stop), 2)

    if trail_exit_pnl is None:
        trail_exit_pnl = round(trail_peak, 1)  # EOD unrealized

    if hit_t1:
        pnl = round((t1_target + trail_exit_pnl) / 2, 1)
    elif trail_exit_pnl < 0:
        pnl = round(trail_exit_pnl, 1)
    else:
        pnl = round(trail_exit_pnl, 1)

    result = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "EXPIRED")
    return {
        "result": result, "pnl": pnl, "t1_hit": hit_t1,
        "t2_pnl": trail_exit_pnl, "mfe": max_profit, "mae": max_loss,
        "trail_peak": trail_peak,
    }


# ── Main Backtest ────────────────────────────────────────────────────────────

def run_backtest():
    from datetime import date

    # Sierra data files — process both contracts
    scid_files = [
        ("C:/SierraChart/Data/ESH6.CME.scid", "ESH6"),
        ("C:/SierraChart/Data/ESM6.CME.scid", "ESM6"),
    ]

    start = date(2026, 1, 2)
    end = date(2026, 3, 20)

    print(f"Loading tick data from Sierra Chart ({start} to {end})...")
    print(f"Streaming per-day (not loading all into memory)...")

    # PASS 1: Stream through files, group RTH ticks by date, process one day at a time
    sessions = defaultdict(list)  # date -> list of ticks
    tick_count = 0
    for path, sym in scid_files:
        if not os.path.exists(path):
            print(f"  SKIP {path} (not found)")
            continue
        file_ticks = 0
        for tick in read_scid(path, start, end):
            dt_et = tick[0]
            t = dt_et.time()
            if dtime(9, 30) <= t <= dtime(16, 0):
                sessions[dt_et.date()].append(tick)
                file_ticks += 1
        tick_count += file_ticks
        print(f"  {sym}: {file_ticks:,} RTH ticks", flush=True)

    if not sessions:
        print("No tick data found!")
        return

    trading_days = sorted(sessions.keys())
    print(f"Total: {tick_count:,} RTH ticks across {len(trading_days)} days ({trading_days[0]} to {trading_days[-1]})")

    # Run backtest for both 5-pt and 10-pt
    for range_pts, label in [(5.0, "SB-5pt"), (10.0, "SB10-10pt")]:
        print(f"\n{'='*100}")
        print(f"  {label} BACKTEST ({range_pts}pt range bars)")
        print(f"{'='*100}")

        all_signals = []
        daily_stats = {}
        cooldown_bearish = -100
        cooldown_bullish = -100
        cooldown_dist = 10 if range_pts == 5 else 5

        for day in trading_days:
            ticks = sessions[day]
            builder = RangeBarBuilder(range_pts)

            day_signals = []
            for dt_et, price, vol, buy, sell, delta in ticks:
                completed = builder.process_tick(dt_et, price, vol, buy, sell, delta)
                if completed is None:
                    continue

                # Time filter: 10:00-15:55
                bar_time = completed["ts_end"]
                if isinstance(bar_time, datetime):
                    bt = bar_time.time()
                else:
                    bt = bar_time
                if not (dtime(10, 0) <= bt <= dtime(15, 55)):
                    continue

                # Run SB detection on all bars so far
                signal = detect_sb_absorption(builder.bars)
                if signal is None:
                    continue

                # Cooldown
                bidx = signal["bar_idx"]
                if signal["direction"] == "bearish":
                    if bidx - cooldown_bearish < cooldown_dist:
                        continue
                    cooldown_bearish = bidx
                else:
                    if bidx - cooldown_bullish < cooldown_dist:
                        continue
                    cooldown_bullish = bidx

                signal["date"] = day
                day_signals.append(signal)

            # Simulate outcomes AFTER all bars for the day are built
            for signal in day_signals:
                outcome = simulate_outcome(builder.bars, signal)
                signal["outcome"] = outcome

            # Reset cooldowns per day
            cooldown_bearish = -100
            cooldown_bullish = -100

            all_signals.extend(day_signals)
            if day_signals:
                day_pnl = sum(s["outcome"]["pnl"] for s in day_signals)
                day_wins = sum(1 for s in day_signals if s["outcome"]["result"] == "WIN")
                day_losses = sum(1 for s in day_signals if s["outcome"]["result"] == "LOSS")
                daily_stats[day] = {"signals": len(day_signals), "wins": day_wins,
                                    "losses": day_losses, "pnl": day_pnl}

            total_bars = len(builder.bars)

        # Summary
        if not all_signals:
            print("  No signals generated!")
            continue

        wins = sum(1 for s in all_signals if s["outcome"]["result"] == "WIN")
        losses = sum(1 for s in all_signals if s["outcome"]["result"] == "LOSS")
        expired = sum(1 for s in all_signals if s["outcome"]["result"] == "EXPIRED")
        total_pnl = sum(s["outcome"]["pnl"] for s in all_signals)
        avg_pnl = total_pnl / len(all_signals) if all_signals else 0
        avg_mfe = sum(s["outcome"]["mfe"] for s in all_signals) / len(all_signals)
        wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

        # Max drawdown
        running = 0
        peak = 0
        max_dd = 0
        for s in all_signals:
            running += s["outcome"]["pnl"]
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd

        # Max consecutive losses
        max_consec_loss = 0
        consec = 0
        for s in all_signals:
            if s["outcome"]["result"] == "LOSS":
                consec += 1
                max_consec_loss = max(max_consec_loss, consec)
            else:
                consec = 0

        # Profit factor
        gross_win = sum(s["outcome"]["pnl"] for s in all_signals if s["outcome"]["pnl"] > 0)
        gross_loss = abs(sum(s["outcome"]["pnl"] for s in all_signals if s["outcome"]["pnl"] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

        print(f"\n  SUMMARY")
        print(f"  Signals: {len(all_signals)} | {wins}W / {losses}L / {expired}E")
        print(f"  Win Rate: {wr:.1f}%")
        print(f"  Total PnL: {total_pnl:+.1f} pts")
        print(f"  Avg PnL: {avg_pnl:+.2f} | Avg MFE: {avg_mfe:.1f}")
        print(f"  PF: {pf:.2f} | MaxDD: {max_dd:.1f} | MaxConsecLoss: {max_consec_loss}")
        print(f"  PnL/Day: {total_pnl / len(trading_days):+.1f}")

        # Direction breakdown
        longs = [s for s in all_signals if s["direction"] == "bullish"]
        shorts = [s for s in all_signals if s["direction"] == "bearish"]
        for dir_name, sigs in [("LONG", longs), ("SHORT", shorts)]:
            if not sigs:
                continue
            w = sum(1 for s in sigs if s["outcome"]["result"] == "WIN")
            l = sum(1 for s in sigs if s["outcome"]["result"] == "LOSS")
            p = sum(s["outcome"]["pnl"] for s in sigs)
            wr2 = w / (w + l) * 100 if (w + l) > 0 else 0
            print(f"  {dir_name}: {len(sigs)} signals, {w}W/{l}L, WR={wr2:.1f}%, PnL={p:+.1f}")

        # Grade breakdown
        print(f"\n  GRADE BREAKDOWN")
        for grade in ["A+", "A", "B", "C"]:
            gs = [s for s in all_signals if s["grade"] == grade]
            if not gs:
                continue
            gw = sum(1 for s in gs if s["outcome"]["result"] == "WIN")
            gl = sum(1 for s in gs if s["outcome"]["result"] == "LOSS")
            gp = sum(s["outcome"]["pnl"] for s in gs)
            gwr = gw / (gw + gl) * 100 if (gw + gl) > 0 else 0
            print(f"    {grade}: {len(gs)} signals, {gw}W/{gl}L, WR={gwr:.1f}%, PnL={gp:+.1f}")

        # Monthly breakdown
        print(f"\n  MONTHLY BREAKDOWN")
        months = defaultdict(list)
        for s in all_signals:
            m = s["date"].strftime("%Y-%m")
            months[m].append(s)
        for m in sorted(months.keys()):
            ms = months[m]
            mw = sum(1 for s in ms if s["outcome"]["result"] == "WIN")
            ml = sum(1 for s in ms if s["outcome"]["result"] == "LOSS")
            mp = sum(s["outcome"]["pnl"] for s in ms)
            mwr = mw / (mw + ml) * 100 if (mw + ml) > 0 else 0
            print(f"    {m}: {len(ms)} signals, {mw}W/{ml}L, WR={mwr:.1f}%, PnL={mp:+.1f}")

        # Per-date detail
        print(f"\n  PER-DATE DETAIL (signal days only)")
        print(f"  {'Date':<12} {'Sigs':>5} {'W':>3} {'L':>3} {'PnL':>8}  Details")
        print(f"  {'-'*90}")
        for day in trading_days:
            ds = [s for s in all_signals if s["date"] == day]
            if not ds:
                continue
            dw = sum(1 for s in ds if s["outcome"]["result"] == "WIN")
            dl = sum(1 for s in ds if s["outcome"]["result"] == "LOSS")
            dp = sum(s["outcome"]["pnl"] for s in ds)
            details = ", ".join(
                f"{s['ts'].strftime('%H:%M') if isinstance(s['ts'], datetime) else str(s['ts'])[:5]} "
                f"{'S' if s['direction']=='bearish' else 'L'}"
                f"{s['outcome']['pnl']:+.1f}"
                for s in ds
            )
            print(f"  {day}  {len(ds):>5} {dw:>3} {dl:>3} {dp:+8.1f}  {details}")

        # Equity curve summary
        print(f"\n  EQUITY CURVE (cumulative)")
        running = 0
        for i, s in enumerate(all_signals):
            running += s["outcome"]["pnl"]
            if (i + 1) % 10 == 0 or i == len(all_signals) - 1:
                print(f"    Trade #{i+1}: cumPnL={running:+.1f}")


if __name__ == "__main__":
    run_backtest()
