"""
apollo_iv_bridge.py — Python IV Bridge for Sierra Chart Apollo Momentum Study
==============================================================================

Reads chain_snapshots from the Railway PostgreSQL database every 30 seconds,
computes fixed-strike IV changes over a 15-minute window, and writes signals
to a file that Sierra Chart's Apollo Momentum study reads.

Signal Logic:
  LONG:  spot rose >= 5pts in 15min AND put IV at nearby strikes dropped >= 0.05
  SHORT: spot fell >= 5pts in 15min AND put IV at nearby strikes rose >= 0.05

The "nearby strikes" are ATM, ATM-5, ATM-10 (put side). We track IV changes
at FIXED strikes to avoid strike-selection drift as spot moves.

Output Files:
  1. apollo_signal.txt   — Single-line signal file (Sierra Chart reads this)
     Format: direction,iv_change,spot_price
     Example: 1,0.07,5890.50  (long signal, IV dropped 0.07, spot at 5890.50)
              -1,0.09,5885.25  (short signal, IV rose 0.09)
              0,0.02,5888.00   (no signal)

  2. apollo_iv_log.csv   — Full history log (for analysis/debugging)
     Format: timestamp,spot,spot_15m_ago,spot_change,atm_strike,
             iv_atm,iv_atm5,iv_atm10,iv_atm_15m,iv_atm5_15m,iv_atm10_15m,
             iv_change_avg,direction,signal

Usage:
  python apollo_iv_bridge.py [--db-url DATABASE_URL] [--output-dir C:\\SierraChart\\Data]
                             [--interval 30] [--lookback 15] [--spot-threshold 5.0]
                             [--iv-threshold 0.05]

Environment:
  DATABASE_URL — PostgreSQL connection string (same as Railway app)

Requirements:
  pip install psycopg2-binary sqlalchemy pandas
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import deque

try:
    import pandas as pd
    from sqlalchemy import create_engine, text
except ImportError:
    print("ERROR: pip install pandas sqlalchemy psycopg2-binary")
    sys.exit(1)


# ─── Configuration ───────────────────────────────────────────────────────────

DEFAULT_DB_URL = os.environ.get("DATABASE_URL", "")
DEFAULT_OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Documents", "SierraChart", "Data")
DEFAULT_INTERVAL = 30       # seconds between polls
DEFAULT_LOOKBACK = 15       # minutes for IV change window
DEFAULT_SPOT_THRESHOLD = 5.0   # points
DEFAULT_IV_THRESHOLD = 0.05    # absolute IV change

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("apollo_iv_bridge")


# ─── IV Snapshot Cache ───────────────────────────────────────────────────────

class IVSnapshot:
    """A single point-in-time IV observation at fixed strikes."""
    def __init__(self, ts: datetime, spot: float, strike_ivs: dict):
        self.ts = ts
        self.spot = spot
        self.strike_ivs = strike_ivs  # {strike: put_iv, ...}

    def __repr__(self):
        return f"IVSnapshot(ts={self.ts:%H:%M:%S}, spot={self.spot:.1f}, strikes={len(self.strike_ivs)})"


class ApolloIVBridge:
    """Main bridge class. Polls DB, computes IV signals, writes to file."""

    def __init__(self, db_url: str, output_dir: str,
                 interval: int = DEFAULT_INTERVAL,
                 lookback_min: int = DEFAULT_LOOKBACK,
                 spot_threshold: float = DEFAULT_SPOT_THRESHOLD,
                 iv_threshold: float = DEFAULT_IV_THRESHOLD):
        self.engine = create_engine(db_url)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.interval = interval
        self.lookback_min = lookback_min
        self.spot_threshold = spot_threshold
        self.iv_threshold = iv_threshold

        # Rolling window of IV snapshots (keep 20 min worth)
        self.iv_history: deque[IVSnapshot] = deque(maxlen=50)

        # Fixed reference strikes (set once we see first data)
        self._ref_strikes: list[float] | None = None
        self._ref_spot: float | None = None
        self._ref_set_time: datetime | None = None

        # Signal file paths
        self.signal_file = self.output_dir / "apollo_signal.txt"
        self.log_file = self.output_dir / "apollo_iv_log.csv"

        # Initialize log file with header if it doesn't exist
        if not self.log_file.exists():
            with open(self.log_file, "w") as f:
                f.write("timestamp,spot,spot_15m_ago,spot_change,"
                        "atm_strike,iv_atm,iv_atm5,iv_atm10,"
                        "iv_atm_15m,iv_atm5_15m,iv_atm10_15m,"
                        "iv_change_avg,direction,signal\n")

        self._running = True
        log.info(f"Apollo IV Bridge initialized")
        log.info(f"  DB: {db_url[:40]}...")
        log.info(f"  Output: {self.output_dir}")
        log.info(f"  Interval: {interval}s, Lookback: {lookback_min}min")
        log.info(f"  Spot threshold: {spot_threshold}pts, IV threshold: {iv_threshold}")

    # ─── DB Query ────────────────────────────────────────────────────────────

    def fetch_latest_chain(self) -> tuple[datetime, float, list[dict]] | None:
        """Fetch the most recent chain_snapshot from the database.

        Returns (timestamp, spot, rows_list) or None if no data.
        The rows are the JSONB array of option chain rows.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT ts, spot, columns, rows
                    FROM chain_snapshots
                    ORDER BY ts DESC
                    LIMIT 1
                """)).mappings().first()

                if not result:
                    return None

                ts = result["ts"]
                spot = float(result["spot"])
                columns = json.loads(result["columns"]) if isinstance(result["columns"], str) else result["columns"]
                raw_rows = json.loads(result["rows"]) if isinstance(result["rows"], str) else result["rows"]

                # Convert array-of-arrays to list-of-dicts using column names
                # columns format: ["Volume","Open Int","IV","Gamma","Delta","BID","BID QTY",
                #                   "ASK","ASK QTY","LAST","Strike","LAST","ASK","ASK QTY",
                #                   "BID","BID QTY","Delta","Gamma","IV","Open Int","Volume"]
                #
                # This is side-by-side format. We need to extract put IVs.
                # Calls are columns 0-9, Strike is 10, Puts are columns 11-20.
                # Call IV = columns[2], Put IV = columns[18]

                rows = []
                for arr in raw_rows:
                    if len(arr) < 21:
                        continue
                    row = {}
                    for i, col in enumerate(columns):
                        row[f"{col}_{i}"] = arr[i]
                    # Extract the fields we need
                    strike = arr[10]  # Strike column
                    put_iv = arr[18]  # Put IV (right side)
                    call_iv = arr[2]  # Call IV (left side)
                    if strike is not None:
                        rows.append({
                            "strike": float(strike) if strike else 0,
                            "put_iv": float(put_iv) if put_iv else 0,
                            "call_iv": float(call_iv) if call_iv else 0,
                        })

                return (ts, spot, rows)

        except Exception as e:
            log.error(f"DB query failed: {e}")
            return None

    def fetch_chain_at_time(self, target_time: datetime) -> tuple[datetime, float, list[dict]] | None:
        """Fetch the chain_snapshot closest to a target time (for lookback comparison).

        Finds the snapshot closest to target_time within +/- 3 minutes.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT ts, spot, columns, rows
                    FROM chain_snapshots
                    WHERE ts BETWEEN :t_start AND :t_end
                    ORDER BY ABS(EXTRACT(EPOCH FROM ts - :target))
                    LIMIT 1
                """), {
                    "t_start": target_time - timedelta(minutes=3),
                    "t_end": target_time + timedelta(minutes=3),
                    "target": target_time,
                }).mappings().first()

                if not result:
                    return None

                ts = result["ts"]
                spot = float(result["spot"])
                columns = json.loads(result["columns"]) if isinstance(result["columns"], str) else result["columns"]
                raw_rows = json.loads(result["rows"]) if isinstance(result["rows"], str) else result["rows"]

                rows = []
                for arr in raw_rows:
                    if len(arr) < 21:
                        continue
                    strike = arr[10]
                    put_iv = arr[18]
                    call_iv = arr[2]
                    if strike is not None:
                        rows.append({
                            "strike": float(strike) if strike else 0,
                            "put_iv": float(put_iv) if put_iv else 0,
                            "call_iv": float(call_iv) if call_iv else 0,
                        })

                return (ts, spot, rows)

        except Exception as e:
            log.error(f"DB lookback query failed: {e}")
            return None

    # ─── IV Extraction ───────────────────────────────────────────────────────

    def extract_put_ivs(self, rows: list[dict], target_strikes: list[float]) -> dict:
        """Extract put IVs at specific strikes from chain data.

        Returns {strike: iv, ...}. Missing strikes get iv=0.
        """
        # Build strike -> put_iv map
        strike_map = {}
        for r in rows:
            s = r["strike"]
            if s > 0 and r["put_iv"] > 0:
                strike_map[s] = r["put_iv"]

        result = {}
        for ts in target_strikes:
            result[ts] = strike_map.get(ts, 0.0)

        return result

    def get_reference_strikes(self, spot: float) -> list[float]:
        """Determine the 3 reference put strikes: ATM, ATM-5, ATM-10.

        Strikes are rounded to nearest 5 (SPX options are 5-pt wide).
        These stay FIXED for 15 minutes to avoid drift.
        """
        atm = round(spot / 5) * 5
        return [atm, atm - 5, atm - 10]

    def update_reference_strikes(self, spot: float, now: datetime):
        """Update reference strikes if they are stale (> lookback window) or not set."""
        if self._ref_strikes is None or self._ref_set_time is None:
            self._ref_strikes = self.get_reference_strikes(spot)
            self._ref_spot = spot
            self._ref_set_time = now
            log.info(f"Reference strikes set: {self._ref_strikes} (spot={spot:.1f})")
            return

        # Re-anchor strikes if spot has moved > 15 pts from reference
        # This prevents tracking strikes that are now deep OTM/ITM
        if abs(spot - self._ref_spot) > 15:
            self._ref_strikes = self.get_reference_strikes(spot)
            self._ref_spot = spot
            self._ref_set_time = now
            log.info(f"Reference strikes re-anchored: {self._ref_strikes} (spot moved to {spot:.1f})")

    # ─── Signal Computation ──────────────────────────────────────────────────

    def compute_signal(self) -> dict:
        """Main signal computation. Fetches current + lookback data, compares IV.

        Returns:
            {
                "direction": 1 (long) / -1 (short) / 0 (none),
                "iv_change": float (average IV change across reference strikes),
                "spot_price": float,
                "spot_change": float,
                "details": str (human-readable description),
            }
        """
        # 1. Fetch latest chain
        latest = self.fetch_latest_chain()
        if not latest:
            return {"direction": 0, "iv_change": 0, "spot_price": 0,
                    "spot_change": 0, "details": "No chain data"}

        now_ts, spot, rows = latest

        # 2. Update reference strikes
        self.update_reference_strikes(spot, now_ts)

        # 3. Fetch lookback chain (15 min ago)
        lookback_target = now_ts - timedelta(minutes=self.lookback_min)
        lookback = self.fetch_chain_at_time(lookback_target)

        if not lookback:
            return {"direction": 0, "iv_change": 0, "spot_price": spot,
                    "spot_change": 0, "details": f"No lookback data at {lookback_target}"}

        lb_ts, lb_spot, lb_rows = lookback

        # 4. Extract IVs at reference strikes (current and lookback)
        current_ivs = self.extract_put_ivs(rows, self._ref_strikes)
        lookback_ivs = self.extract_put_ivs(lb_rows, self._ref_strikes)

        # 5. Compute IV changes
        iv_changes = {}
        valid_changes = []
        for strike in self._ref_strikes:
            curr = current_ivs.get(strike, 0)
            prev = lookback_ivs.get(strike, 0)
            if curr > 0 and prev > 0:
                change = curr - prev
                iv_changes[strike] = change
                valid_changes.append(change)

        if not valid_changes:
            return {"direction": 0, "iv_change": 0, "spot_price": spot,
                    "spot_change": 0, "details": "No valid IV data at reference strikes"}

        avg_iv_change = sum(valid_changes) / len(valid_changes)
        spot_change = spot - lb_spot

        # 6. Determine signal
        direction = 0
        signal_desc = ""

        if spot_change >= self.spot_threshold and avg_iv_change <= -self.iv_threshold:
            # LONG: spot UP + put IV DOWN = vol sellers aligned with uptrend
            direction = 1
            signal_desc = (f"LONG: spot +{spot_change:.1f}pts, "
                          f"put IV avg {avg_iv_change:+.4f} "
                          f"(sellers confirming uptrend)")
        elif spot_change <= -self.spot_threshold and avg_iv_change >= self.iv_threshold:
            # SHORT: spot DOWN + put IV UP = vol buyers aligned with downtrend
            direction = -1
            signal_desc = (f"SHORT: spot {spot_change:.1f}pts, "
                          f"put IV avg {avg_iv_change:+.4f} "
                          f"(buyers confirming downtrend)")
        else:
            signal_desc = (f"NO SIGNAL: spot {spot_change:+.1f}pts "
                          f"(need {self.spot_threshold:+.1f}), "
                          f"IV {avg_iv_change:+.4f} "
                          f"(need {self.iv_threshold:.4f})")

        # 7. Log details
        log.info(f"Spot: {spot:.1f} ({spot_change:+.1f}pts from {lb_spot:.1f})")
        for strike in self._ref_strikes:
            curr_iv = current_ivs.get(strike, 0)
            prev_iv = lookback_ivs.get(strike, 0)
            chg = iv_changes.get(strike, 0)
            label = "ATM" if strike == self._ref_strikes[0] else f"ATM-{int(self._ref_strikes[0] - strike)}"
            log.info(f"  {label} ({strike}): IV {prev_iv:.4f} -> {curr_iv:.4f} ({chg:+.4f})")

        if direction != 0:
            log.info(f">>> SIGNAL: {signal_desc}")
        else:
            log.debug(signal_desc)

        return {
            "direction": direction,
            "iv_change": avg_iv_change,
            "spot_price": spot,
            "spot_change": spot_change,
            "details": signal_desc,
            "iv_changes": iv_changes,
            "current_ivs": current_ivs,
            "lookback_ivs": lookback_ivs,
            "lookback_spot": lb_spot,
            "lookback_ts": lb_ts.isoformat() if hasattr(lb_ts, 'isoformat') else str(lb_ts),
            "now_ts": now_ts.isoformat() if hasattr(now_ts, 'isoformat') else str(now_ts),
        }

    # ─── File Output ─────────────────────────────────────────────────────────

    def write_signal_file(self, signal: dict):
        """Write the signal to the file Sierra Chart reads.

        Format: direction,iv_change,spot_price
        Single line, overwritten each cycle.
        """
        line = f"{signal['direction']},{abs(signal['iv_change']):.4f},{signal['spot_price']:.2f}"
        with open(self.signal_file, "w") as f:
            f.write(line)

    def append_log(self, signal: dict):
        """Append a row to the detailed CSV log."""
        ts = signal.get("now_ts", datetime.now().isoformat())
        spot = signal["spot_price"]
        lb_spot = signal.get("lookback_spot", 0)
        spot_change = signal["spot_change"]

        ref = self._ref_strikes or [0, 0, 0]
        current = signal.get("current_ivs", {})
        lookback = signal.get("lookback_ivs", {})

        iv_atm = current.get(ref[0], 0) if ref[0] else 0
        iv_atm5 = current.get(ref[1], 0) if len(ref) > 1 and ref[1] else 0
        iv_atm10 = current.get(ref[2], 0) if len(ref) > 2 and ref[2] else 0
        iv_atm_lb = lookback.get(ref[0], 0) if ref[0] else 0
        iv_atm5_lb = lookback.get(ref[1], 0) if len(ref) > 1 and ref[1] else 0
        iv_atm10_lb = lookback.get(ref[2], 0) if len(ref) > 2 and ref[2] else 0

        direction = signal["direction"]
        sig = "LONG" if direction == 1 else "SHORT" if direction == -1 else "NONE"

        row = (f"{ts},{spot:.2f},{lb_spot:.2f},{spot_change:.2f},"
               f"{ref[0]:.0f},{iv_atm:.4f},{iv_atm5:.4f},{iv_atm10:.4f},"
               f"{iv_atm_lb:.4f},{iv_atm5_lb:.4f},{iv_atm10_lb:.4f},"
               f"{signal['iv_change']:.4f},{direction},{sig}\n")

        with open(self.log_file, "a") as f:
            f.write(row)

    # ─── Main Loop ───────────────────────────────────────────────────────────

    def run(self):
        """Main polling loop. Runs until Ctrl+C."""
        log.info("Apollo IV Bridge started. Press Ctrl+C to stop.")
        log.info(f"Signal file: {self.signal_file}")
        log.info(f"Log file: {self.log_file}")

        while self._running:
            try:
                signal_result = self.compute_signal()
                self.write_signal_file(signal_result)
                self.append_log(signal_result)

                if signal_result["direction"] != 0:
                    log.info(f"Signal written: {signal_result['details']}")

            except Exception as e:
                log.error(f"Cycle error: {e}", exc_info=True)
                # Write a "no signal" on error so Sierra doesn't show stale data
                self.write_signal_file({"direction": 0, "iv_change": 0, "spot_price": 0})

            time.sleep(self.interval)

    def stop(self):
        """Graceful shutdown."""
        self._running = False
        log.info("Apollo IV Bridge stopping...")


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Apollo IV Bridge — reads chain IV from DB, writes signals for Sierra Chart"
    )
    parser.add_argument("--db-url", default=DEFAULT_DB_URL,
                        help="PostgreSQL connection string (or set DATABASE_URL env var)")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help=f"Directory for output files (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help=f"Poll interval in seconds (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK,
                        help=f"IV lookback window in minutes (default: {DEFAULT_LOOKBACK})")
    parser.add_argument("--spot-threshold", type=float, default=DEFAULT_SPOT_THRESHOLD,
                        help=f"Spot move threshold in pts (default: {DEFAULT_SPOT_THRESHOLD})")
    parser.add_argument("--iv-threshold", type=float, default=DEFAULT_IV_THRESHOLD,
                        help=f"IV change threshold (default: {DEFAULT_IV_THRESHOLD})")
    parser.add_argument("--once", action="store_true",
                        help="Run one cycle and exit (for testing)")

    args = parser.parse_args()

    if not args.db_url:
        print("ERROR: No database URL. Set DATABASE_URL env var or pass --db-url")
        sys.exit(1)

    bridge = ApolloIVBridge(
        db_url=args.db_url,
        output_dir=args.output_dir,
        interval=args.interval,
        lookback_min=args.lookback,
        spot_threshold=args.spot_threshold,
        iv_threshold=args.iv_threshold,
    )

    # Handle Ctrl+C gracefully
    def _sigint(sig, frame):
        bridge.stop()
    signal.signal(signal.SIGINT, _sigint)

    if args.once:
        result = bridge.compute_signal()
        bridge.write_signal_file(result)
        bridge.append_log(result)
        print(f"\nResult: {json.dumps(result, indent=2, default=str)}")
        print(f"\nSignal file: {bridge.signal_file}")
    else:
        bridge.run()


if __name__ == "__main__":
    main()
