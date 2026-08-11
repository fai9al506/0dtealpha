"""TSRT live monitor — one-shot health sweep, safe to run repeatedly.

Run it with Railway's env injected so it has both DB and TradeStation credentials:

    railway run -s 0dtealpha python tsrt_live_monitor.py

Read-only. It never places, cancels or modifies an order. It prints a compact
report and exits with a verdict so a caller can act:

    exit 0 = OK        exit 1 = WARN (investigate)      exit 2 = HALT (S239 rule fired)

The S239 stop rules it encodes (agreed 2026-08-08, Tasks.md):
  HALT on  - any bot-vs-broker position mismatch or orphan working order
           - the ES range-bar feed running late again (the S236 failure)
           - a day worse than -$500
           - drawdown past -$1,500 from the arming equity
  DO NOT halt on a red streak, a -$1,000 drawdown, or a losing month.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests

# Alert text carries emoji; a Windows cp1252 console raises UnicodeEncodeError on the
# first one and kills the sweep AFTER the summary line — i.e. the alerts go unread,
# which is the exact failure S243 exists to prevent. Never let encoding hide an alert.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"
ACCTS = ["210VYX65", "210VYX91"]

# --- arming baseline, 2026-08-10 09:35 ET (see memory project_tsrt_restart_2026_08_10) ---
ARM_DATE = "2026-08-10"
ARM_EQUITY = 5588.70          # 210VYX65 $2,667.93 + 210VYX91 $2,920.77
DD_HALT = -1500.0             # S239: drawdown past this = halt
DAY_HALT = -500.0             # S239: a single day worse than this = halt
BREAKER = -300.0              # automatic: blocks NEW entries only, does not flatten

# --- freshness budgets (seconds) ---
FEED_LAG_WARN = 60            # median received_at - ts_end on 5pt ES bars
FEED_LAG_HALT = 300           # this is the S236 signature (it sat at 614 s)
STALE = {                     # table -> (warn, halt) age of newest row
    "chain_snapshots":    (240, 900),
    "volland_snapshots":  (600, 1800),
    "semi_basket":        (600, 1800),   # basket gate reads this
    "vps_vix_ticks":      (300, 1800),
}

findings: list[tuple[str, str, str]] = []   # (level, area, message)


def note(level: str, area: str, msg: str) -> None:
    findings.append((level, area, msg))


def now_et() -> datetime:
    return datetime.now(ET)


def market_open(t: datetime) -> bool:
    return t.weekday() < 5 and "09:30" <= t.strftime("%H:%M") <= "16:00"


# ---------------------------------------------------------------- database
def check_db(cur, t: datetime) -> dict:
    out: dict = {}

    # ES range-bar feed lag — the thing that was broken for five weeks
    cur.execute("""
        SELECT round(percentile_cont(0.5) WITHIN GROUP (
                     ORDER BY EXTRACT(EPOCH FROM (received_at - ts_end)))::numeric, 1),
               count(*)
        FROM vps_es_range_bars
        WHERE range_pts = 5 AND received_at > now() - interval '90 minutes'
    """)
    lag, nbars = cur.fetchone()
    out["es_feed"] = {"median_lag_s": float(lag) if lag is not None else None, "bars_90m": nbars}
    if lag is None or nbars == 0:
        if market_open(t) and t.strftime("%H:%M") > "10:00":
            note("WARN", "es_feed", "no 5pt ES bars in the last 90 min during RTH")
    elif float(lag) >= FEED_LAG_HALT:
        note("HALT", "es_feed",
             f"ES bars {float(lag):.0f}s late (S236 signature) — ES Absorption is trading stale bars")
    elif float(lag) >= FEED_LAG_WARN:
        note("WARN", "es_feed", f"ES bar lag {float(lag):.0f}s (budget {FEED_LAG_WARN}s)")

    # pipeline freshness
    ts_col = {"semi_basket": "et", "vps_vix_ticks": "received_at"}
    out["freshness"] = {}
    for table, (warn_s, halt_s) in STALE.items():
        col = ts_col.get(table, "ts")
        try:
            if table == "semi_basket":
                cur.execute(f"SELECT EXTRACT(EPOCH FROM (now() - (max({col}) AT TIME ZONE 'America/New_York'))) FROM {table}")
            else:
                cur.execute(f"SELECT EXTRACT(EPOCH FROM (now() - max({col}))) FROM {table}")
            age = cur.fetchone()[0]
        except Exception as e:                                   # noqa: BLE001
            note("WARN", "freshness", f"{table} query failed: {e}")
            continue
        age = float(age) if age is not None else None
        out["freshness"][table] = age
        if age is None:
            note("WARN", "freshness", f"{table} is empty")
        elif market_open(t) and age > halt_s:
            note("WARN", "freshness", f"{table} stale {age/60:.0f} min")
        elif market_open(t) and age > warn_s:
            note("WARN", "freshness", f"{table} aging {age/60:.0f} min")

    # today's signals and what TSRT did with them
    day = t.strftime("%Y-%m-%d")
    cur.execute("SELECT count(*) FROM setup_log WHERE (ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date = %s", (day,))
    out["signals_today"] = cur.fetchone()[0]
    cur.execute("""
        SELECT real_trade_skip_reason, count(*)
        FROM setup_log
        WHERE (ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date = %s
          AND real_trade_skip_reason IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """, (day,))
    out["skips_today"] = {r[0]: r[1] for r in cur.fetchall()}
    if out["skips_today"].get("REAL_TRADE_DISABLED"):
        note("WARN", "tsrt", "trades are being skipped as REAL_TRADE_DISABLED — the arm did not take")

    cur.execute("""
        SELECT setup_log_id, state->>'status', state->>'setup_name', state->>'direction',
               state->>'quantity', state->>'fill_price', state->>'close_reason', updated_at
        FROM real_trade_orders
        WHERE created_at > now() - interval '20 hours'
        ORDER BY created_at
    """)
    rows = cur.fetchall()
    out["orders_today"] = [
        {"lid": r[0], "status": r[1], "setup": r[2], "dir": r[3], "qty": r[4],
         "fill": r[5], "close_reason": r[6], "updated": str(r[7])}
        for r in rows
    ]
    out["bot_open"] = [o for o in out["orders_today"] if o["status"] not in ("closed",)]
    return out


# ------------------------------------------------------------------ broker
def ts_token() -> str | None:
    cid, secret, refresh = (os.getenv("TS_CLIENT_ID"), os.getenv("TS_CLIENT_SECRET"),
                            os.getenv("TS_REFRESH_TOKEN"))
    if not (cid and secret and refresh):
        note("WARN", "broker", "TS credentials absent — run under `railway run` for broker checks")
        return None
    try:
        r = requests.post("https://signin.tradestation.com/oauth/token", timeout=20, data={
            "grant_type": "refresh_token", "client_id": cid,
            "client_secret": secret, "refresh_token": refresh})
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "broker", f"token refresh failed: {e}")
        return None


# TradeStation reports order state as a StatusDescription string. Classify explicitly rather
# than by exclusion: an unrecognised status is reported as a WARN and NOT counted as working,
# because guessing "still live" turns any new status string into a false orphan HALT.
# "UROut" is TS's description for the OUT status — a cancelled/replaced order. It is dead, and
# omitting it caused a false HALT at 11:08 ET on 2026-08-10.
LIVE_STATUS = {"Received", "Queued", "Sent", "Open", "Accepted", "Ack", "Acknowledged",
               "Pending", "Partially Filled", "Condition Met", "Suspended"}
DEAD_STATUS = {"Filled", "Canceled", "Cancelled", "Rejected", "Expired", "Replaced", "Broken",
               "Too Late to Cancel", "Out", "UROut", "Done", "Disconnected", "Queued Pending Ack"}


def _is_working(order: dict) -> bool:
    s = (order.get("StatusDescription") or "").strip()
    if s in LIVE_STATUS:
        return True
    if s in DEAD_STATUS:
        return False
    note("WARN", "orders", f"unrecognised TradeStation order status {s!r} on "
                           f"order {order.get('OrderID')} — treated as not working, check it")
    return False

SOD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tsrt_monitor_sod.json")


def _sod_equity(t: datetime, current_eq: float) -> float:
    """Start-of-day combined equity, persisted so day P&L survives going flat.

    Futures accounts are flat overnight and nothing is deposited intraday, so equity minus
    this snapshot is the true realised + unrealised day P&L. On the first run of a new day
    the current equity IS the start of day; on 2026-08-10 it is seeded with the arming
    equity so the first live session measures from the documented anchor.
    """
    day = t.strftime("%Y-%m-%d")
    try:
        with open(SOD_FILE) as f:
            data = json.load(f)
    except Exception:                                            # noqa: BLE001
        data = {}
    if data.get("date") == day:
        return float(data["sod_equity"])
    sod = ARM_EQUITY if day == ARM_DATE else current_eq
    if t.strftime("%H:%M") > "09:35" and day != ARM_DATE:
        note("WARN", "pnl",
             f"start-of-day equity first captured at {t:%H:%M} ET — day P&L misses anything "
             "closed before that")
    try:
        with open(SOD_FILE, "w") as f:
            json.dump({"date": day, "sod_equity": sod, "captured_et": t.strftime("%H:%M:%S")}, f)
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "pnl", f"could not persist start-of-day equity: {e}")
    return sod


def check_broker(token: str, db: dict, t: datetime) -> dict:
    H = {"Authorization": f"Bearer {token}"}
    ids = ",".join(ACCTS)
    out: dict = {}

    try:
        bal = requests.get(f"{TS_BASE}/brokerage/accounts/{ids}/balances", headers=H, timeout=20).json()
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "broker", f"balances call failed: {e}")
        return out
    eq, day_pl = 0.0, 0.0
    out["accounts"] = {}
    for b in bal.get("Balances", []) or []:
        e = float(b.get("Equity") or 0)
        p = float(b.get("TodaysProfitLoss") or 0)
        eq += e
        day_pl += p
        out["accounts"][b.get("AccountID")] = {"equity": e, "today_pl": p}
    out["equity"] = round(eq, 2)
    out["dd_from_arm"] = round(eq - ARM_EQUITY, 2)

    # TradeStation's TodaysProfitLoss only reflects OPEN positions — it snaps back to 0 the
    # moment the account goes flat, so it silently loses every realised trade of the day.
    # Keying the -$500 stop rule on it would mean the rule could never fire. Day P&L is
    # therefore measured as equity minus a persisted start-of-day equity snapshot.
    sod = _sod_equity(t, eq)
    out["open_pl"] = round(day_pl, 2)          # keep the broker's own figure for reference
    out["sod_equity"] = round(sod, 2)
    day_pl = round(eq - sod, 2)
    out["day_pl"] = day_pl

    if day_pl <= DAY_HALT:
        note("HALT", "pnl", f"day P&L ${day_pl:,.0f} is worse than the ${DAY_HALT:,.0f} stop rule")
    elif day_pl <= BREAKER:
        note("WARN", "pnl", f"day P&L ${day_pl:,.0f} — breaker territory, new entries blocked at ${BREAKER:,.0f} realized")
    if out["dd_from_arm"] <= DD_HALT:
        note("HALT", "pnl", f"drawdown ${out['dd_from_arm']:,.0f} from arming equity breaches ${DD_HALT:,.0f}")

    try:
        pos = requests.get(f"{TS_BASE}/brokerage/accounts/{ids}/positions", headers=H, timeout=20).json()
        orders = requests.get(f"{TS_BASE}/brokerage/accounts/{ids}/orders", headers=H, timeout=20).json()
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "broker", f"positions/orders call failed: {e}")
        return out

    plist = pos.get("Positions", []) or []
    out["positions"] = [{"acct": p.get("AccountID"), "sym": p.get("Symbol"),
                         "qty": p.get("Quantity"), "avg": p.get("AveragePrice"),
                         "open_pl": p.get("UnrealizedProfitLoss")} for p in plist]
    working = [o for o in (orders.get("Orders", []) or []) if _is_working(o)]
    out["working_orders"] = [{"acct": o.get("AccountID"), "id": o.get("OrderID"),
                              "status": o.get("StatusDescription"), "type": o.get("OrderType"),
                              "sym": (o.get("Legs") or [{}])[0].get("Symbol"),
                              "qty": (o.get("Legs") or [{}])[0].get("QuantityOrdered")}
                             for o in working]

    # --- mismatch / orphan detection: the S239 defect triggers ---
    broker_qty = sum(abs(int(float(p.get("Quantity") or 0))) for p in plist)
    bot_qty = sum(abs(int(float(o.get("qty") or 0))) for o in db.get("bot_open", []))
    out["broker_qty"], out["bot_qty"] = broker_qty, bot_qty
    if broker_qty != bot_qty:
        note("HALT", "mismatch",
             f"broker holds {broker_qty} MES but the bot tracks {bot_qty} — position mismatch")
    if broker_qty == 0 and working:
        note("HALT", "orphan",
             f"{len(working)} working order(s) resting with NO open position — orphan risk")

    # E2T-style end-of-day cleanliness: nothing may rest past 15:50 ET
    if t.strftime("%H:%M") >= "15:50" and (plist or working):
        note("WARN", "eod",
             f"{len(plist)} position(s) / {len(working)} order(s) still live after 15:50 ET")
    return out


# ------------------------------------------------------------------ telegram
ALERT_SEEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "tsrt_monitor_alerts_seen.json")

# Alert text that means a defect, not information. Matched case-insensitively.
ALERT_HALT = ("ghost position", "qty mismatch", "orphan", "flatten failed",
              "no exit price", "could not recover fill", "circuit breaker",
              "stop rejected", "wrong-side", "poll disabled", "sanity fail")
ALERT_WARN = ("fifo reconcile warn", "skip", "blocked", "failed", "error",
              "disabled", "stale")


def check_alerts(cur, t: datetime) -> dict:
    """Read what the bot ACTUALLY sent to Telegram since the last sweep.

    S243: the bot cannot read back its own channel posts, so main.py records every
    send into `telegram_alerts`. Before this existed, "monitoring Telegram" meant
    pinging getMe — proving the bot was reachable, not seeing what it said. On
    2026-08-10 a GHOST POSITION alert and a FIFO WARN both fired and neither was
    surfaced until the user pasted them in by hand.
    """
    out: dict = {}
    try:
        with open(ALERT_SEEN_FILE) as f:
            last_id = int(json.load(f).get("last_id", 0))
    except Exception:
        last_id = 0
    try:
        cur.execute("""SELECT id, ts, channel, message FROM telegram_alerts
                       WHERE id > %s AND ts > now() - interval '24 hours'
                       ORDER BY id""", (last_id,))
        rows = cur.fetchall()
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "alerts", f"telegram_alerts unreadable ({e}) — is the app deployed?")
        return out

    out["new"] = len(rows)
    out["messages"] = []
    for aid, ats, chan, msg in rows:
        low = msg.lower()
        first = msg.strip().splitlines()[0][:120] if msg.strip() else ""
        out["messages"].append({"id": aid, "channel": chan,
                                "et": ats.astimezone(ET).strftime("%H:%M:%S"),
                                "head": first})
        if any(k in low for k in ALERT_HALT):
            note("HALT", "alerts", f"[{ats.astimezone(ET):%H:%M}] {first}")
        elif any(k in low for k in ALERT_WARN):
            note("WARN", "alerts", f"[{ats.astimezone(ET):%H:%M}] {first}")
    if rows:
        try:
            with open(ALERT_SEEN_FILE, "w") as f:
                json.dump({"last_id": rows[-1][0]}, f)
        except Exception as e:                                   # noqa: BLE001
            note("WARN", "alerts", f"could not persist last seen alert id: {e}")
    return out


def check_missing_exit_price(cur, t: datetime) -> list:
    """Closed trades today carrying no exit price — invisible P&L holes.

    Lid 5853 on 2026-08-10 closed 'outcome_resolved_win' with no price at all and
    dropped +$52.50 out of every per-trade total. Nothing flagged it; the day only
    looked wrong because the bot's tracked figure disagreed with the broker's.
    """
    try:
        cur.execute("""
            SELECT setup_log_id FROM real_trade_orders
            WHERE created_at > now() - interval '20 hours'
              AND state->>'status' = 'closed'
              AND COALESCE(state->>'stop_fill_price', state->>'close_fill_price',
                           state->>'target_fill_price') IS NULL
            ORDER BY setup_log_id""")
        bad = [r[0] for r in cur.fetchall()]
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "exitprice", f"query failed: {e}")
        return []
    if bad:
        note("WARN", "exitprice",
             f"{len(bad)} closed trade(s) with NO exit price: {bad} — P&L missing from "
             f"per-trade totals (broker day-$ unaffected). Pull the app log NOW.")
    return bad


def check_telegram() -> dict:
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    out: dict = {}
    if not tok:
        note("WARN", "telegram", "TELEGRAM_BOT_TOKEN not in env — cannot verify the alert path")
        return out
    try:
        r = requests.get(f"https://api.telegram.org/bot{tok}/getMe", timeout=15)
        j = r.json()
        out["bot_ok"] = bool(j.get("ok"))
        out["bot"] = (j.get("result") or {}).get("username")
        if not j.get("ok"):
            note("HALT", "telegram", f"Telegram bot unreachable: {j}")
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "telegram", f"Telegram getMe failed: {e} (may be a local ISP block)")
    out["chats"] = {"alerts": os.getenv("TELEGRAM_CHAT_ID"),
                    "setups": os.getenv("TELEGRAM_CHAT_ID_SETUPS")}
    if not out["chats"]["setups"]:
        note("WARN", "telegram", "TELEGRAM_CHAT_ID_SETUPS unset — trade alerts have nowhere to go")
    return out


# ------------------------------------------------------------------- app
def check_app(t: datetime) -> dict:
    try:
        r = requests.get("https://0dtealpha.com/api/health", timeout=25)
        j = r.json()
        # The endpoint reports "closed" outside 09:30-16:00 ET by design — that is not a fault.
        if j.get("status") != "healthy" and not (j.get("status") == "closed" and not market_open(t)):
            note("WARN", "app", f"health endpoint says {j}")
        return j
    except Exception as e:                                       # noqa: BLE001
        note("WARN", "app", f"health endpoint unreachable: {e}")
        return {}


def main() -> int:
    t = now_et()
    report: dict = {"at_et": t.strftime("%Y-%m-%d %H:%M:%S %a"), "market_open": market_open(t)}

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True                 # never hold a long transaction against prod
    try:
        cur = conn.cursor()
        report["db"] = check_db(cur, t)
        report["alerts"] = check_alerts(cur, t)
        report["missing_exit_price"] = check_missing_exit_price(cur, t)
    finally:
        conn.close()

    report["app"] = check_app(t)
    report["telegram"] = check_telegram()
    tok = ts_token()
    if tok:
        report["broker"] = check_broker(tok, report["db"], t)

    halts = [f for f in findings if f[0] == "HALT"]
    warns = [f for f in findings if f[0] == "WARN"]
    report["verdict"] = "HALT" if halts else ("WARN" if warns else "OK")
    report["findings"] = [{"level": a, "area": b, "msg": c} for a, b, c in findings]

    print(json.dumps(report, indent=1, default=str))

    # One-line tail so a caller that only reads the end of the output still gets every
    # number it needs — the JSON above is long enough to be truncated by `tail`.
    b = report.get("broker", {})
    d = report["db"]
    lag = d["es_feed"]["median_lag_s"]
    print("SUMMARY | {} | {} | day P&L ${} | equity ${} (vs arm {:+.2f}) | open {} | working {} | "
          "ES lag {} | signals {} | orders {} | new alerts {} | {}".format(
              report["verdict"], report["at_et"],
              b.get("day_pl", "?"), b.get("equity", "?"), b.get("dd_from_arm", 0.0),
              len(b.get("positions", [])), len(b.get("working_orders", [])),
              f"{lag}s" if lag is not None else "n/a",
              d["signals_today"], len(d["orders_today"]),
              report.get("alerts", {}).get("new", "?"),
              "; ".join(f"{a}:{b_}:{c}" for a, b_, c in findings) or "no findings"))
    for m in report.get("alerts", {}).get("messages", []):
        print(f"   ALERT {m['et']} [{m['channel']}] {m['head']}")
    return 2 if halts else (1 if warns else 0)


if __name__ == "__main__":
    sys.exit(main())
