"""
eval_trader_watchdog.py — Monitors eval_trader and alerts via Telegram if it stops.
Runs as a scheduled task every 5 minutes. If eval_trader.py is not running
during market hours, sends a Telegram alert and optionally restarts it.

False-positive hardening (2026-04-21):
- Each PowerShell check retries once on empty/timeout (handles transient hangs)
- Alerts only fire after 2 consecutive failed checks (state in watchdog_state.json)
- eval_trader.lock PID fallback if primary process enumeration fails
"""

import os, json, subprocess, sys, time, requests
from datetime import datetime, time as dtime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("US/Eastern")

# Telegram config
BOT_TOKEN = "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
CHAT_ID = "-1003886332593"

# Auto-restart: launches eval_trader hidden via VBS if dead
AUTO_RESTART = True
VBS_PATH = r"C:\Users\Administrator\0dtealpha\run_eval_trader.vbs"

# Persistent state to require 2 consecutive failed checks before alerting
STATE_FILE = r"C:\Users\Administrator\0dtealpha\eval_trader_watchdog_state.json"

# Paths used by fallback checks
LOCK_FILE = r"C:\Users\Administrator\0dtealpha\eval_trader.lock"
E2T_FILE = r"C:\Users\Administrator\Documents\NinjaTrader 8\outgoing\E2T.txt"


def is_market_hours():
    now = datetime.now(ET)
    # Mon-Fri, 9:00-16:15 ET (buffer before open, after close)
    if now.weekday() >= 5:
        return False
    return dtime(9, 0) <= now.time() <= dtime(16, 15)


def _run_ps(cmd, timeout=10, retries=1):
    """Run PowerShell with one retry on empty/timeout. Returns stdout string (may be empty)."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["powershell", "-Command", cmd],
                capture_output=True, text=True, timeout=timeout
            )
            out = (result.stdout or "").strip()
            if out:
                return out
        except Exception:
            pass
        if attempt < retries:
            time.sleep(3)
    return ""


def _pid_alive(pid):
    """Pure-Python Windows PID alive check via ctypes (no subprocess)."""
    try:
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not h:
            return False
        exit_code = ctypes.c_ulong(0)
        ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(exit_code))
        ctypes.windll.kernel32.CloseHandle(h)
        return exit_code.value == STILL_ACTIVE
    except Exception:
        return False


def is_eval_trader_running():
    """Check if any python process is running eval_trader.py.
    Primary: PowerShell CommandLine scan. Fallback: eval_trader.lock PID alive check."""
    out = _run_ps(
        "Get-Process python* -ErrorAction SilentlyContinue | "
        "ForEach-Object { (Get-CimInstance Win32_Process -Filter \"ProcessId=$($_.Id)\").CommandLine }"
    )
    if "eval_trader.py" in out:
        return True
    # Fallback: lockfile holds the PID; check if it's alive
    try:
        if os.path.exists(LOCK_FILE):
            pid = open(LOCK_FILE).read().strip()
            if pid and _pid_alive(pid):
                return True
    except Exception:
        pass
    return False


def is_nt8_running():
    """Check if NinjaTrader process is running with a visible window (not headless).
    Primary: MainWindowHandle check. Fallback: process ID + E2T.txt CONNECTED."""
    handle_out = _run_ps(
        "Get-Process NinjaTrader -ErrorAction SilentlyContinue | "
        "Select-Object -ExpandProperty MainWindowHandle"
    )
    if handle_out:
        handle = handle_out.splitlines()[0].strip()
        if handle and handle != "0":
            return True, "ok"
        # Process exists but no window — fall through to connection-based verification

    # Fallback: if process is running AND E2T.txt says CONNECTED, treat as up
    pid_out = _run_ps(
        "Get-Process NinjaTrader -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id",
        timeout=5, retries=1
    )
    if pid_out:
        try:
            if os.path.exists(E2T_FILE):
                status = open(E2T_FILE).read().strip()
                if status == "CONNECTED":
                    return True, "ok (via E2T.txt)"
                return False, f"E2T status: {status}"
        except Exception:
            pass
        return False, "running headless (no GUI)"
    return False, "not running"


def is_nt8_connected():
    """Check if NT8 is connected to Rithmic/E2T account.
    Checks E2T.txt connection file first, then position reporter files as backup."""
    import glob
    outgoing = r"C:\Users\Administrator\Documents\NinjaTrader 8\outgoing"

    # Check 1: E2T.txt connection status file
    e2t_file = os.path.join(outgoing, "E2T.txt")
    if os.path.exists(e2t_file):
        try:
            status = open(e2t_file).read().strip()
            if status == "CONNECTED":
                return True, "ok"
            else:
                return False, f"E2T status: {status}"
        except Exception:
            pass

    # Check 2: position reporter files (fallback)
    pos_files = glob.glob(os.path.join(outgoing, "*_position.txt"))
    if not pos_files:
        return False, "no connection file found"
    newest = max(pos_files, key=os.path.getmtime)
    age_min = (time.time() - os.path.getmtime(newest)) / 60
    if age_min > 5:
        return False, f"position file stale ({age_min:.0f}min old)"
    return True, "ok"


def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram failed: {e}")


def restart_eval_trader():
    try:
        subprocess.Popen(["wscript.exe", VBS_PATH], shell=False)
        return True
    except Exception as e:
        print(f"Restart failed: {e}")
        return False


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            return json.load(open(STATE_FILE))
        except Exception:
            return {}
    return {}


def save_state(state):
    try:
        json.dump(state, open(STATE_FILE, "w"))
    except Exception as e:
        print(f"State save failed: {e}")


if __name__ == "__main__":
    if not is_market_hours():
        print(f"Outside market hours, skipping check.")
        sys.exit(0)

    now_str = datetime.now(ET).strftime("%H:%M ET")
    alerts = []

    # Load previous check results (empty dict on first run)
    state = load_state()
    prev = state.get("last", {})

    # ── Check 1: eval_trader process ──
    eval_ok = is_eval_trader_running()
    if eval_ok:
        print("eval_trader is running. OK.")
    else:
        # Only alert/restart if the PREVIOUS check also failed (2-consecutive rule)
        prev_eval_ok = prev.get("eval_ok", True)  # benefit of doubt on first run
        if prev_eval_ok:
            print(f"eval_trader check failed once at {now_str} — will confirm next cycle")
        else:
            print("eval_trader DOWN confirmed (2 consecutive failures)")
            if AUTO_RESTART:
                restarted = restart_eval_trader()
                if restarted:
                    time.sleep(5)
                    if is_eval_trader_running():
                        alerts.append(f"⚠️ <b>Eval Trader CRASHED</b> — auto-restarted at {now_str}")
                        print("Restarted successfully.")
                    else:
                        alerts.append(f"🚨 <b>Eval Trader CRASHED</b> — restart FAILED at {now_str}\nManual intervention needed!")
                        print("Restart failed!")
                else:
                    alerts.append(f"🚨 <b>Eval Trader CRASHED</b> — restart FAILED at {now_str}\nManual intervention needed!")
            else:
                alerts.append(f"🚨 <b>Eval Trader DOWN</b> at {now_str}\nRestart manually on VPS!")

    # ── Check 2: NinjaTrader process ──
    nt8_ok, nt8_reason = is_nt8_running()
    if nt8_ok:
        print(f"NinjaTrader is running. OK ({nt8_reason}).")
    else:
        prev_nt8_ok = prev.get("nt8_ok", True)  # benefit of doubt on first run
        if prev_nt8_ok:
            print(f"NinjaTrader check failed once at {now_str} ({nt8_reason}) — will confirm next cycle")
        else:
            alerts.append(f"🚨 <b>NinjaTrader DOWN</b> at {now_str}\nReason: {nt8_reason}\nOrders will NOT execute!")
            print(f"NinjaTrader DOWN confirmed (2 consecutive failures): {nt8_reason}")

    # ── Check 3: NT8 Rithmic connection (only if NT8 process is up) ──
    conn_ok = True
    conn_reason = "ok"
    if nt8_ok:
        conn_ok, conn_reason = is_nt8_connected()
        if conn_ok:
            print("NT8 Rithmic connection: OK.")
        else:
            prev_conn_ok = prev.get("conn_ok", True)
            if prev_conn_ok:
                print(f"NT8 Rithmic check failed once at {now_str} ({conn_reason}) — will confirm next cycle")
            else:
                alerts.append(f"⚠️ <b>NT8 Rithmic may be disconnected</b> at {now_str}\nReason: {conn_reason}\nCheck NT8 connection status!")
                print(f"NT8 Rithmic DOWN confirmed (2 consecutive failures): {conn_reason}")

    # Persist this cycle's results for next run's 2-consecutive comparison
    save_state({
        "last": {
            "eval_ok": eval_ok,
            "nt8_ok": nt8_ok,
            "conn_ok": conn_ok,
        },
        "ts": now_str,
    })

    # Send all alerts
    for alert in alerts:
        send_telegram(alert)
