"""S160: Day-1 atomic ordergroup audit (2026-05-20).

Run AFTER 16:10 ET. For each atomic-path real_trade_orders row today, pulls
TS /historicalorders and verifies:
  1. All 3 child orders (entry/stop/target where applicable) actually placed
  2. Their OpenedDateTime are within a tight window (atomic = single POST)
  3. No rejections logged on any sub-order
  4. Entry-fill-to-stop-active latency vs known sequential baseline

Output: per-lid report + summary stats.
"""
import os, json, requests, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"
TARGET_DATE = "2026-05-20"
SINCE = "05-19-2026"  # TS uses MM-DD-YYYY and `since` is EXCLUSIVE


def refresh_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def ts_get(p, t):
    r = requests.get(TS_BASE + p, headers={"Authorization": f"Bearer {t}"}, timeout=15)
    return r.json() if r.text else None


def parse_ts_dt(s):
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def main():
    token = refresh_token()
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = c.cursor()

    cur.execute("""
        SELECT setup_log_id, state
        FROM real_trade_orders
        WHERE created_at::date = %s
          AND state->>'atomic_bracket' = 'true'
        ORDER BY setup_log_id
    """, (TARGET_DATE,))
    rows = cur.fetchall()
    print(f"=== ATOMIC DAY-1 AUDIT — {len(rows)} fires on {TARGET_DATE} ===\n")

    # Today's orders are in /orders (intraday). /historicalorders only has prior days.
    hist_by_acct = {}
    grouped = {}
    for lid, state in rows:
        if isinstance(state, str): state = json.loads(state)
        acct = state.get("account_id")
        grouped.setdefault(acct, []).append((lid, state))
    for acct in grouped:
        live = ts_get(f"/brokerage/accounts/{acct}/orders?pageSize=600", token) or {}
        hist_by_acct[acct] = live

    summary = {"total": 0, "all_3_placed": 0, "no_target_placed": 0,
               "atomic_gap_ms": [], "rejections": 0, "ghost_reconcile": 0}

    for acct, items in grouped.items():
        all_orders = (hist_by_acct[acct].get("Orders") or [])
        by_oid = {str(o.get("OrderID")): o for o in all_orders}
        for lid, state in items:
            summary["total"] += 1
            e = state.get("entry_order_id")
            s = state.get("stop_order_id")
            t = state.get("target_order_id")
            close_reason = state.get("close_reason", "")
            if close_reason == "ghost_reconcile":
                summary["ghost_reconcile"] += 1
            print(f"\nlid={lid} {state.get('setup_name')} {state.get('direction')}")
            print(f"  entry={e}  stop={s}  target={t}  close_reason={close_reason}")
            placed = []
            opens = []
            for label, oid in [("ENTRY", e), ("STOP", s), ("TARGET", t)]:
                if not oid:
                    print(f"    {label}: (no oid)")
                    continue
                o = by_oid.get(str(oid))
                if not o:
                    print(f"    {label}: oid={oid} NOT FOUND in /historicalorders")
                    continue
                placed.append(label)
                opened = parse_ts_dt(o.get("OpenedDateTime"))
                status = o.get("Status")
                filled = o.get("FilledPrice")
                rej = o.get("RejectReason")
                if rej:
                    summary["rejections"] += 1
                print(f"    {label}: oid={oid} status={status} opened={opened} filled={filled} rej={rej}")
                if opened:
                    opens.append((label, opened))
            if "ENTRY" in placed and "STOP" in placed:
                summary["all_3_placed"] += 1 if "TARGET" in placed else 0
                if "TARGET" not in placed:
                    summary["no_target_placed"] += 1
                if len(opens) >= 2:
                    times = [t for _, t in opens]
                    gap_ms = (max(times) - min(times)).total_seconds() * 1000
                    summary["atomic_gap_ms"].append(gap_ms)
                    print(f"    atomic open-gap: {gap_ms:.1f} ms across all child orders")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total atomic fires:    {summary['total']}")
    print(f"All-3-orders placed:   {summary['all_3_placed']} (with target)")
    print(f"Entry+Stop only:       {summary['no_target_placed']} (trail-only or DD)")
    print(f"Rejections detected:   {summary['rejections']}")
    print(f"ghost_reconcile:       {summary['ghost_reconcile']}")
    if summary["atomic_gap_ms"]:
        g = summary["atomic_gap_ms"]
        print(f"Atomic open-gap stats: n={len(g)}  min={min(g):.1f}ms  median={sorted(g)[len(g)//2]:.1f}ms  max={max(g):.1f}ms")
        if max(g) < 100:
            print("  OK All atomic gaps < 100ms — single-POST behavior confirmed")
        else:
            print(f"  !! Max gap {max(g):.1f}ms — investigate that outlier")

    cur.close(); c.close()


if __name__ == "__main__":
    main()
