"""Emergency local Volland runner — bypasses headless detection.

Why this exists: 2026-05-21 vol.land started blocking headless Chromium from
rendering workspace widgets. Railway Volland service logs in successfully but
captures 0 exposures. This script runs the SAME volland_worker_v2 locally with
headless=False (visible Chrome window) to bypass the detection.

Usage:
    python _tmp_run_volland_local.py

The browser window WILL be visible — that's intentional. Do NOT close it.
Minimize it. Sign-in is automated. Writes go to the SAME Railway DB as the
production worker.

This script auto-restarts the inner worker on any crash (network hiccups,
worker exceptions, etc.) so a single blip can't take Volland offline for >2
min. Stop with Ctrl+C to kill the outer loop too.
"""
import os
import sys
import time
import traceback

# Force UTF-8 stdout so Telegram emoji escaping in worker doesn't blow up on cp1252
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Env vars (pulled from Railway Volland service at 2026-05-21 ~9:55 ET)
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-1003835706217")
os.environ.setdefault("VOLLAND_EMAIL", "faisal.a.d@msn.com")
os.environ.setdefault("VOLLAND_PASSWORD", "Fad2024506!")
os.environ.setdefault("VOLLAND_URL", "https://vol.land/app/workspace/6787a95cfe7b13a115716f54")
os.environ.setdefault("VOLLAND_WORKSPACE_URL", "https://vol.land/app/workspace/69c2d38cce2143e384a8cfa1")

# Monkey-patch Playwright launch to force headless=False BEFORE worker imports it.
from playwright.sync_api._generated import BrowserType
_orig_launch = BrowserType.launch

def _launch_visible(self, *args, **kwargs):
    kwargs["headless"] = False
    extra_args = kwargs.get("args", []) or []
    if "--disable-blink-features=AutomationControlled" not in extra_args:
        extra_args.append("--disable-blink-features=AutomationControlled")
    kwargs["args"] = extra_args
    print("[local-runner] Launching VISIBLE Chrome (headless=False, anti-detect args)", flush=True)
    return _orig_launch(self, *args, **kwargs)

BrowserType.launch = _launch_visible

# Auto-restart loop: any crash (network, worker exception, watchdog exit) →
# wait briefly + restart. Vol.land 3-device limit is fine since the dead
# session vacates as soon as Chrome process exits.
RESTART_DELAY_S = 15
MAX_RESTARTS_PER_HOUR = 30  # safety cap — if crashing this often something's structurally wrong

print("[local-runner] Starting volland_worker_v2 with auto-restart wrapper", flush=True)
print("[local-runner] DB:", os.environ["DATABASE_URL"][:40] + "...", flush=True)
print("[local-runner] Workspace:", os.environ["VOLLAND_WORKSPACE_URL"], flush=True)
print("[local-runner] Stop with Ctrl+C", flush=True)

import volland_worker_v2

restart_times: list[float] = []
attempt = 0
while True:
    attempt += 1
    now = time.time()
    # Trim restart-time list to last hour
    restart_times = [t for t in restart_times if now - t < 3600]
    if len(restart_times) >= MAX_RESTARTS_PER_HOUR:
        print(f"[local-runner] FATAL: {MAX_RESTARTS_PER_HOUR}+ restarts in last hour. Stopping. Investigate.", flush=True)
        sys.exit(1)
    restart_times.append(now)

    print(f"\n[local-runner] === attempt #{attempt} (worker boot) ===", flush=True)
    try:
        volland_worker_v2.run()
        # If run() returns cleanly, treat as unexpected exit and restart anyway
        print("[local-runner] Worker run() returned normally — restarting", flush=True)
    except KeyboardInterrupt:
        print("[local-runner] Ctrl+C received, exiting cleanly", flush=True)
        sys.exit(0)
    except SystemExit as e:
        # Watchdog forcibly exits via sys.exit / os._exit — restart
        print(f"[local-runner] Worker SystemExit({e.code}) — restarting in {RESTART_DELAY_S}s", flush=True)
    except Exception as e:
        print(f"[local-runner] Worker crashed: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        print(f"[local-runner] Restarting in {RESTART_DELAY_S}s", flush=True)

    time.sleep(RESTART_DELAY_S)
