"""Sierra ES/DOM feed health watchdog (2026-06-01, hardened 2026-08-08).

Born from the 2026-06-01 incident: the Sierra ES feed was silently DELAYED ~10 min
for ~1 week+ (CME real-time entitlement lapsed when Services Balance hit $0) and
NOBODY noticed until a fast move exposed it. This makes that loud.

Then it happened AGAIN, 2026-07-02 -> 2026-08-07 (CME "Verified Trading Accounts"
expired 2026-07-01), and this script stayed silent for five weeks. Three separate
defects let that happen; all three are fixed here:

  1. LATCHED ALERTS. State recorded a set of active issues and only alerted on the
     transition into that set. Once `es_delay` was in the set it never spoke again,
     so a permanent fault produced at most one message, five weeks earlier, in a busy
     channel. Persistent issues now RE-ALERT every REALERT_HOURS.
  2. SINGLE-SAMPLE DELAY. Delay was read off the one newest bar, so a single odd row
     could mask or fake the condition. Now the MEDIAN of the last 20 bars.
  3. FAKE LIVENESS PROOF. "ES DOM is fresh" was used to prove the feed was alive and
     to suppress bar-staleness alerts. It proves nothing: the Sierra DOM study emits
     ~1 snapshot/sec regardless of market data — measured 3,597 ES DOM rows in one
     hour on a Saturday with the market shut. Liveness now comes from VX TICKS, which
     only exist when trades actually print (0 in that same Saturday hour).

Runs every ~15 min during market hours (scheduled task). Checks the Railway DB and
Telegrams on:
  1. ES feed DELAYED  — median(received_at - ts_end) over last 20 bars > 180s
  2. ES bars STALE    — no new ES bar in > 10 min during RTH, with ticks also stopped
  3. ES DOM STALE      — vps_es_dom_snapshots not fresh in > 10 min   (depth study down)
  4. VX DOM STALE      — vps_vx_dom_snapshots not fresh in > 10 min   (depth study down)
Telegram creds read from eval_trader_config.json. Zero impact on bridge/Railway/eval.
"""
import json, sys, requests, psycopg2
from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
ET = ZoneInfo("America/New_York")
SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_DIR / "sierra_watchdog_state.json"
CFG_FILE = SCRIPT_DIR / "eval_trader_config.json"

DELAY_ALERT_S = 180      # median (received_at - ts_end) over recent bars; healthy ~4s
DELAY_SAMPLE = 20        # bars to take the median over
STALE_ALERT_S = 600      # ES bars / DOM stale if newest row older than this (during RTH)
BARS_STUCK_S = 1800      # bars stale this long WHILE ticks flow = builder stuck, not quiet
TICK_LIVE_S = 300        # VX ticks newer than this = market data genuinely flowing
REALERT_HOURS = 3        # re-nag on a still-broken condition (~2 per session)


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
        s = json.loads(STATE_FILE.read_text())
    except Exception:
        s = {}
    # Migrate the pre-2026-08-08 format (a bare list of active issue keys). Anything
    # already latched is given last_alert=epoch so it re-alerts on the very next run —
    # a condition that outlived the old format is exactly what we owe the user a
    # message about.
    if "issues" not in s:
        legacy = s.get("active_issues", []) or []
        s["issues"] = {k: {"first_seen": None, "last_alert": None} for k in legacy}
        s.pop("active_issues", None)
    return s


def save_state(s):
    STATE_FILE.write_text(json.dumps(s, indent=2))


def _age(now, ts):
    return (now - ts).total_seconds() if ts else 99999


def collect_issues(now):
    """Returns (issues dict key->message, debug string). Raises on DB failure."""
    issues = {}
    c = psycopg2.connect(DB, connect_timeout=20)
    c.autocommit = True
    cur = c.cursor()
    try:
        # ── liveness: VX ticks only exist when trades print ───────────────────
        cur.execute("SELECT received_at FROM vps_vix_ticks ORDER BY id DESC LIMIT 1")
        r = cur.fetchone()
        tick_age = _age(now, r[0] if r else None)
        ticks_live = tick_age <= TICK_LIVE_S

        # ── DOM studies: their own health, NOT proof the feed is alive ────────
        dom_age = {}
        for tbl, key, label in [("vps_es_dom_snapshots", "es_dom", "ES DOM"),
                                ("vps_vx_dom_snapshots", "vx_dom", "VX DOM")]:
            cur.execute(f"SELECT received_at FROM {tbl} ORDER BY id DESC LIMIT 1")
            d = cur.fetchone()
            age = _age(now, d[0] if d else None)
            dom_age[key] = age
            if age > STALE_ALERT_S:
                issues[key] = (f"⚠️ Sierra {label} STALE — newest snapshot {age/60:.0f} min "
                               f"ago (depth study/subscription down?)")

        # ── ES range bars: delay + staleness, measured separately ────────────
        cur.execute("SELECT bar_idx, ts_end, received_at FROM vps_es_range_bars "
                    "WHERE range_pts = 5 ORDER BY id DESC LIMIT %s", (DELAY_SAMPLE,))
        rows = cur.fetchall()
        if not rows:
            issues["es_stale"] = "⚠️ Sierra ES bars: vps_es_range_bars empty"
            return issues, "no bars"

        bar_idx, ts_end, recv = rows[0]
        age_s = _age(now, recv)

        if age_s > STALE_ALERT_S:
            if not ticks_live:
                issues["es_stale"] = (
                    f"⚠️ Sierra ES bars STALE — newest bar #{bar_idx} posted "
                    f"{age_s/60:.0f} min ago and VX ticks stopped {tick_age/60:.0f} min "
                    f"ago (bridge/feed down?)")
            elif age_s > BARS_STUCK_S:
                issues["es_stuck"] = (
                    f"⚠️ Sierra ES range-bar builder may be STUCK — no new bar for "
                    f"{age_s/60:.0f} min (#{bar_idx}) but ticks are live "
                    f"({tick_age:.0f}s fresh). Quiet market unlikely this long — "
                    f"check the bridge.")
            else:
                print(f"[watchdog] ES bars {age_s/60:.0f} min stale but ticks live "
                      f"({tick_age:.0f}s) — quiet market, suppressing")

        # Delay is a per-bar latency, so it is immune to the quiet-market problem
        # above and is never suppressed by any liveness cross-check.
        delays = sorted((rc - te).total_seconds() for _, te, rc in rows if te and rc)
        med = delays[len(delays) // 2] if delays else None
        if med is not None and med > DELAY_ALERT_S:
            issues["es_delay"] = (
                f"🔴 Sierra ES feed DELAYED ~{med/60:.1f} min — bars reaching the DB "
                f"{med:.0f}s behind market time (median of last {len(delays)}; healthy "
                f"is ~4s).\n"
                f"ES / SB / SB2 / Delta Absorption run on these bars and go silent or "
                f"fire on stale prices while this lasts.\n"
                f"Check account.sierrachart.com → Services Balance + Verified Trading "
                f"Accounts. CME delayed data is exactly 10 min (~614s).")
        return issues, (f"bar#{bar_idx} age={age_s:.0f}s median_delay="
                        f"{med:.0f}s ticks={tick_age:.0f}s dom_es={dom_age['es_dom']:.0f}s")
    finally:
        c.close()


def main():
    if not is_market_hours():
        print(f"[watchdog] {now_et():%H:%M ET} — outside market hours, skip")
        return
    now = datetime.now(timezone.utc)
    try:
        issues, debug = collect_issues(now)
    except Exception as e:
        send_telegram(f"⚠️ Sierra watchdog DB error: {e}")
        return

    state = load_state()
    tracked = state.get("issues", {})
    realert_after = timedelta(hours=REALERT_HOURS)

    for key, msg in issues.items():
        rec = tracked.get(key)
        if rec is None:
            send_telegram(msg)
            tracked[key] = {"first_seen": now.isoformat(), "last_alert": now.isoformat()}
            continue
        # Still broken. Re-nag on a schedule so a permanent fault can never decay into
        # silence the way the 2026-07-02 delay did.
        last = rec.get("last_alert")
        try:
            last_dt = datetime.fromisoformat(last) if last else None
        except Exception:
            last_dt = None
        if last_dt is None or (now - last_dt) >= realert_after:
            since = rec.get("first_seen")
            dur = ""
            try:
                if since:
                    hrs = (now - datetime.fromisoformat(since)).total_seconds() / 3600
                    dur = f"\n(unresolved for {hrs:.0f}h — this is a repeat notice)"
            except Exception:
                pass
            send_telegram(msg + dur)
            rec["last_alert"] = now.isoformat()
            rec.setdefault("first_seen", now.isoformat())

    for key in [k for k in tracked if k not in issues]:
        send_telegram(f"✅ Sierra {key} recovered — feed healthy again ({now_et():%H:%M ET}).")
        tracked.pop(key, None)

    state["issues"] = tracked
    state["last_run"] = now_et().isoformat()
    state["last_debug"] = debug
    save_state(state)
    print(f"[watchdog] {now_et():%H:%M ET} — "
          f"{'OK (all healthy)' if not issues else 'ISSUES: ' + ', '.join(issues)} | {debug}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
