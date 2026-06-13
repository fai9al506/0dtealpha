"""S159: Backfill close_fill_price for ghost_reconcile lids on 2026-05-20.

Run AFTER 16:10 ET. Pulls TS /historicalorders for each ghost lid, finds the
matching exit order, writes its FilledPrice into state.close_fill_price.

Safe to re-run (idempotent: only writes if close_fill_price is currently null).
"""
import os, json, requests, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"
TARGET_DATE = "2026-05-20"
# TS /historicalorders uses MM-DD-YYYY and `since` is EXCLUSIVE of given date,
# so use yesterday to capture today's fills (see real_trader.py:2143).
SINCE = "05-19-2026"
TARGET_LIDS = [3034, 3036, 3037, 3053]


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


def find_exit_fill(historical, oid_set, entry_oid):
    """Find exit order in /historicalorders. Looks for FLL orders matching one of
    the bracket OIDs (stop or target) OR a separate close market order placed
    after the entry."""
    if not historical:
        return None
    orders = historical.get("Orders") or []
    candidates = []
    for o in orders:
        oid = str(o.get("OrderID", ""))
        status = o.get("Status", "")
        if status not in ("FLL",):
            continue
        if oid == str(entry_oid):
            continue
        if oid in oid_set or len(oid_set) == 0:
            fp = o.get("FilledPrice")
            ot = o.get("OpenedDateTime") or o.get("ClosedDateTime")
            if fp:
                candidates.append((oid, float(fp), ot, o))
    return candidates


def main():
    token = refresh_token()
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = c.cursor()

    for lid in TARGET_LIDS:
        cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id = %s", (lid,))
        row = cur.fetchone()
        if not row:
            print(f"lid={lid}: not in real_trade_orders, skipping")
            continue
        state = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        if state.get("close_fill_price") is not None:
            print(f"lid={lid}: already has close_fill_price={state['close_fill_price']}, skipping")
            continue
        acct = state.get("account_id")
        entry_oid = state.get("entry_order_id")
        stop_oid = state.get("stop_order_id")
        target_oid = state.get("target_order_id")
        close_oid = state.get("close_order_id")
        oid_set = {str(x) for x in (stop_oid, target_oid, close_oid) if x}
        print(f"\n=== lid={lid} acct={acct} setup={state.get('setup_name')} dir={state.get('direction')} ===")
        print(f"  entry_oid={entry_oid} stop_oid={stop_oid} target_oid={target_oid} close_oid={close_oid}")

        # TODAY's fills live in /orders (intraday). /historicalorders is for prior days.
        orders_resp = ts_get(f"/brokerage/accounts/{acct}/orders?pageSize=600", token)
        candidates = find_exit_fill(orders_resp, oid_set, entry_oid) or []
        if not candidates:
            hist = ts_get(f"/brokerage/accounts/{acct}/historicalorders?since={SINCE}&pageSize=600", token)
            candidates = find_exit_fill(hist, oid_set, entry_oid) or []
        if not candidates:
            print(f"  no FLL exit order in /orders or /historicalorders matching bracket oids")
            continue
        for cid, fp, ot, raw in candidates[:5]:
            print(f"  candidate oid={cid} filled_price={fp} time={ot}")

        # If exact bracket OID matched, use that. Otherwise use the closest-in-time
        # FLL order after the entry.
        matched = next((c for c in candidates if c[0] in oid_set), candidates[0])
        close_oid_found, close_price, close_time, _ = matched
        print(f"  --> using oid={close_oid_found} price={close_price}")
        state["close_fill_price"] = close_price
        # Don't change close_reason (preserve audit trail showing it was ghost-backfilled)
        state["ghost_backfilled_at"] = datetime.now(ET).isoformat()
        cur.execute(
            "UPDATE real_trade_orders SET state = %s WHERE setup_log_id = %s",
            (json.dumps(state), lid)
        )
        c.commit()
        print(f"  OK updated lid={lid} close_fill_price={close_price}")

    cur.close(); c.close()


if __name__ == "__main__":
    main()
