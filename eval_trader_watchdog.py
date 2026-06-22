"""
eval_trader_watchdog.py — Monitors eval_trader + bridge and alerts via Telegram if they stop.
Runs as a scheduled task every 5 minutes. If a monitored process is not running
during market hours, sends a Telegram alert and optionally restarts it.

False-positive hardening (2026-04-21):
- Each PowerShell check retries once on empty/timeout (handles transient hangs)
- Alerts only fire after 2 consecutive failed checks (state in watchdog_state.json)
- eval_trader.lock PID fallback if primary process enumeration fails

LONG/SHORT split + bridge monitoring (2026-06-22):
- The old is_eval_trader_running() grepped for "eval_trader.py" across ALL python
  procs, so a dead LONG eval went unnoticed while SHORT was alive (both run
  eval_trader.py). Now LONG and SHORT are checked independently by config name.
- Bridge (vps_data_bridge.py) is now monitored and AUTO-RESTARTED when dead
  (data-only process — no S185 state-corruption risk, so restart is unconditional).
- Eval auto-restart stays env-gated (WATCHDOG_AUTO_RESTART_EVAL, default false)
  per the S185 decision — eval deaths still alert, restart manually.
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
# S185 MITIGATION 2026-05-26: default disabled until state-corruption root cause found.
# Re-enable by setting WATCHDOG_AUTO_RESTART_EVAL=true env var or flipping default to True.
# Dead-process Telegram alerts still fire regardless of this flag.
AUTO_RESTART = os.getenv("WATCHDOG_AUTO_RESTART_EVAL", "false").lower() == "true"
VBS_LONG  = r"C:\Users\Administrator\0dtealpha\run_eval_trader.vbs"
VBS_SHORT = r"C:\Users\Administrator\0dtealpha\run_eval_trader_short.vbs"
VBS_BRIDGE = r"C:\Users\Administrator\0dtealpha\run_bridge.vbs"

# Bridge auto-restart is ALWAYS on — vps_data_bridge.py is a data-only process
# (no order placement, no S185 state risk), so self-recovery is safe.
BRIDGE_AUTO_RESTART = os.getenv("WATCHDOG_AUTO_RESTART_BRIDGE", "true").lower() == "true"

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


def get_python_cmdlines():
    """Return all running python process command lines, one per line (lower-cased)."""
    out = _run_ps(
        "Get-Process python* -ErrorAction SilentlyContinue | "
        "ForEach-Object { (Get-CimInstance Win32_Process -Filter \"ProcessId=$($_.Id)\").CommandLine }"
    )
    return [ln.strip().lower() for ln in out.splitlines() if ln.strip()]


def is_long_eval_running(cmdlines):
    """LONG eval = eval_trader.py with NO config OR the default eval_trader_config.json
    (NOT _short, NOT _sim). Fallback: eval_trader.lock PID alive (LONG holds this lock)."""
    for ln in cmdlines:
        if "eval_trader.py" not in ln:
            continue
        if "_short" in ln or "_sim" in ln:
            continue
        # Either no --config (defaults to long) or explicit long config
        return True
    # Fallback: lockfile holds the LONG eval PID
    try:
        if os.path.exists(LOCK_FILE):
            pid = open(LOCK_FILE).read().strip()
            if pid and _pid_alive(pid):
                return True
    except Exception:
        pass
    return False


def is_short_eval_running(cmdlines):
    """SHORT eval = eval_trader.py launched with the _short config."""
    return any("eval_trader.py" in ln and "_short" in ln for ln in cmdlines)


def is_bridge_running(cmdlines):
    """Bridge = vps_data_bridge.py (data-only process)."""
    return any("vps_data_bridge.py" in ln for ln in cmdlines)


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


def restart_via_vbs(vbs_path):
    try:
        subprocess.Popen(["wscript.exe", vbs_path], shell=False)
        return True
    except Exception as e:
        print(f"Restart failed ({vbs_path}): {e}")
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

    cmdlines = get_python_cmdlines()

    # ── Check 1a: LONG eval process ──
    long_ok = is_long_eval_running(cmdlines)
    if long_ok:
        print("LONG eval is running. OK.")
    else:
        prev_long_ok = prev.get("long_ok", True)  # benefit of doubt on first run
        if prev_long_ok:
            print(f"LONG eval check failed once at {now_str} — will confirm next cycle")
        else:
            print("LONG eval DOWN confirmed (2 consecutive failures)")
            if AUTO_RESTART:
                restart_via_vbs(VBS_LONG)
                time.sleep(5)
                if is_long_eval_running(get_python_cmdlines()):
                    alerts.append(f"⚠️ <b>LONG Eval CRASHED</b> — auto-restarted at {now_str}")
                else:
                    alerts.append(f"🚨 <b>LONG Eval CRASHED</b> — restart FAILED at {now_str}\nManual intervention needed!")
            else:
                alerts.append(f"🚨 <b>LONG Eval DOWN</b> at {now_str}\nRestart manually on VPS (run_eval_trader.vbs)!")

    # ── Check 1b: SHORT eval process ──
    short_ok = is_short_eval_running(cmdlines)
    if short_ok:
        print("SHORT eval is running. OK.")
    else:
        prev_short_ok = prev.get("short_ok", True)
        if prev_short_ok:
            print(f"SHORT eval check failed once at {now_str} — will confirm next cycle")
        else:
            print("SHORT eval DOWN confirmed (2 consecutive failures)")
            if AUTO_RESTART:
                restart_via_vbs(VBS_SHORT)
                time.sleep(5)
                if is_short_eval_running(get_python_cmdlines()):
                    alerts.append(f"⚠️ <b>SHORT Eval CRASHED</b> — auto-restarted at {now_str}")
                else:
                    alerts.append(f"🚨 <b>SHORT Eval CRASHED</b> — restart FAILED at {now_str}\nManual intervention needed!")
            else:
                alerts.append(f"🚨 <b>SHORT Eval DOWN</b> at {now_str}\nRestart manually on VPS (run_eval_trader_short.vbs)!")

    # ── Check 1c: VPS data bridge (data-only — auto-restart unconditionally) ──
    bridge_ok = is_bridge_running(cmdlines)
    if bridge_ok:
        print("Bridge is running. OK.")
    else:
        prev_bridge_ok = prev.get("bridge_ok", True)
        if prev_bridge_ok:
            print(f"Bridge check failed once at {now_str} — will confirm next cycle")
        else:
            print("Bridge DOWN confirmed (2 consecutive failures)")
            if BRIDGE_AUTO_RESTART:
                restart_via_vbs(VBS_BRIDGE)
                time.sleep(5)
                if is_bridge_running(get_python_cmdlines()):
                    alerts.append(f"⚠️ <b>VPS Bridge CRASHED</b> — auto-restarted at {now_str}")
                else:
                    alerts.append(f"🚨 <b>VPS Bridge CRASHED</b> — restart FAILED at {now_str}\nManual intervention needed!")
            else:
                alerts.append(f"🚨 <b>VPS Bridge DOWN</b> at {now_str}\nRestart manually on VPS (run_bridge.vbs)!")

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
            "long_ok": long_ok,
            "short_ok": short_ok,
            "bridge_ok": bridge_ok,
            "nt8_ok": nt8_ok,
            "conn_ok": conn_ok,
        },
        "ts": now_str,
    })

    # Send all alerts
    for alert in alerts:
        send_telegram(alert)
