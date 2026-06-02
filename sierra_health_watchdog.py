"""Sierra ES/DOM feed health watchdog (2026-06-01).

Born from the 2026-06-01 incident: the Sierra ES feed was silently DELAYED ~10 min
for ~1 week+ (CME real-time entitlement lapsed when Services Balance hit $0) and
NOBODY noticed until a fast move exposed it. This makes that loud.

Runs every ~15 min during market hours (scheduled task). Checks the Railway DB and
Telegrams on:
  1. ES feed DELAYED  — latest bar's (received_at - ts_end) > 180s  (delayed-data signature)
  2. ES bars STALE    — no new ES bar posted in > 10 min during RTH  (bridge/feed down)
  3. ES DOM STALE      — vps_es_dom_snapshots not fresh in > 10 min   (depth study down)
  4. VX DOM STALE      — vps_vx_dom_snapshots not fresh in > 10 min   (depth study down)
Dedup via state file (alert once on transition; recovery message when good again).
Telegram creds read from eval_trader_config.json. Zero impact on bridge/Railway/eval.
"""
import json, sys, requests, psycopg2
from pathlib import Path
from datetime import datetime, timezone, time as dtime
from zoneinfo import ZoneInfo

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
ET = ZoneInfo("America/New_York")
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_DIR / "sierra_watchdog_state.json"
CFG_FILE = SCRIPT_DIR / "eval_trader_config.json"

DELAY_ALERT_S = 180      # ES feed delayed if (received_at - ts_end) exceeds this
STALE_ALERT_S = 600      # ES bars / DOM stale if newest row older than this (during RTH)
BARS_STUCK_S  = 1800     # bars stale this long WHILE DOM is live = range-bar builder stuck (not a quiet market)

def now_et():
    return datetime.now(ET)

def is_market_hours():
    t = now_et()
    if t.weekday() >= 5:
        return False
    return dtime(9, 30) <= t.time() <= dtime(16, 0)

def send_telegram(msg):
    try:
        cfg = json.loads(CFG_FILE.read_text())
        bot, chat = cfg.get("telegram_bot_token"), cfg.get("telegram_chat_id")
        if not bot or not chat:
            print("[watchdog] no telegram creds"); return
        requests.post(f"https://api.telegram.org/bot{bot}/sendMessage",
                      json={"chat_id": chat, "text": msg}, timeout=10)
        print(f"[watchdog] telegram sent: {msg.splitlines()[0]}")
    except Exception as e:
        print(f"[watchdog] telegram failed: {e}")

def load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}

def save_state(s):
    STATE_FILE.write_text(json.dumps(s, indent=2))

def main():
    if not is_market_hours():
        print(f"[watchdog] {now_et():%H:%M ET} — outside market hours, skip")
        return
    issues = {}   # key -> message
    now = datetime.now(timezone.utc)
    try:
        c = psycopg2.connect(DB, connect_timeout=20); cur = c.cursor()
        # DOM freshness FIRST — vps_es_dom_snapshots is posted ~1/sec straight from the
        # live ES depth feed, so it's a per-tick liveness signal that keeps flowing even
        # when no 5-pt range bar completes. We use ES DOM age to tell "feed/bridge down"
        # apart from "quiet market" below.
        dom_age = {}
        for tbl, key, label in [("vps_es_dom_snapshots", "es_dom", "ES DOM"),
                                 ("vps_vx_dom_snapshots", "vx_dom", "VX DOM")]:
            cur.execute(f"SELECT received_at FROM {tbl} ORDER BY id DESC LIMIT 1")
            d = cur.fetchone()
            age = (now - d[0]).total_seconds() if d and d[0] else 99999
            dom_age[key] = age
            if age > STALE_ALERT_S:
                issues[key] = f"⚠️ Sierra {label} STALE — newest snapshot {age/60:.0f} min ago (depth study/subscription down?)"
        es_dom_live = dom_age.get("es_dom", 99999) <= STALE_ALERT_S
        # ES range bars — delay + freshness (gated on ES DOM tick-flow to kill quiet-market noise)
        cur.execute("SELECT bar_idx, ts_end, received_at FROM vps_es_range_bars ORDER BY id DESC LIMIT 1")
        r = cur.fetchone()
        if r:
            bar_idx, ts_end, recv = r
            age_s = (now - recv).total_seconds() if recv else 99999
            if age_s > STALE_ALERT_S:
                if not es_dom_live:
                    # No new bar AND no ticks/depth either → genuine feed/bridge outage.
                    issues["es_stale"] = f"⚠️ Sierra ES bars STALE — newest bar #{bar_idx} posted {age_s/60:.0f} min ago, ES DOM also stale {dom_age['es_dom']/60:.0f} min (bridge/feed down?)"
                elif age_s > BARS_STUCK_S:
                    # Ticks/depth still flowing but no bar for 30+ min → range-bar builder likely stuck.
                    issues["es_stuck"] = f"⚠️ Sierra ES range-bar builder may be STUCK — no new bar for {age_s/60:.0f} min (#{bar_idx}) but ES DOM is live ({dom_age['es_dom']:.0f}s fresh). Quiet market unlikely this long — check bridge."
                else:
                    # Bars stale but ES DOM live within 30 min → just a quiet/rangebound market, no alert.
                    print(f"[watchdog] ES bars {age_s/60:.0f} min stale but ES DOM live ({dom_age['es_dom']:.0f}s) — quiet market, suppressing")
            elif recv and ts_end:
                delay_s = (recv - ts_end).total_seconds()
                if delay_s > DELAY_ALERT_S:
                    issues["es_delay"] = f"🔴 Sierra ES feed DELAYED ~{delay_s/60:.0f} min (bar #{bar_idx} market-time vs post-time). Likely CME real-time entitlement lapsed — check account.sierrachart.com Services Balance + CME verification."
        else:
            issues["es_stale"] = "⚠️ Sierra ES bars: vps_es_range_bars empty"
        c.close()
    except Exception as e:
        send_telegram(f"⚠️ Sierra watchdog DB error: {e}")
        return

    state = load_state()
    prev = set(state.get("active_issues", []))
    cur_keys = set(issues.keys())
    # New issues → alert
    for k in cur_keys - prev:
        send_telegram(issues[k])
    # Recovered issues → recovery message
    for k in prev - cur_keys:
        send_telegram(f"✅ Sierra {k} recovered — feed healthy again ({now_et():%H:%M ET}).")
    state["active_issues"] = sorted(cur_keys)
    state["last_run"] = now_et().isoformat()
    save_state(state)
    print(f"[watchdog] {now_et():%H:%M ET} — {'OK (all healthy)' if not issues else 'ISSUES: '+', '.join(cur_keys)}")

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
