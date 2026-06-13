"""S159 v2: Better ghost backfill — search by acct+symbol+date filtering on FILL price.

The bot's state.close_order_id can be None when close_trade fires market-close but
state doesn't get persisted before the bot loses track (ghost path). So we can't
just match by OID — need to find the close fill by:
  1. account + symbol (MESM26)
  2. date (today)
  3. status = FLL
  4. direction: opposite of entry (entry=Buy → exit=Sell, entry=Sell → exit=Buy)
  5. time AFTER entry fill time + reasonable window (1-60 min typical)

For each ghost lid, find the FIRST exit-direction FLL after entry that's closest
in time. That's almost certainly the close.
"""
import os, json, requests, psycopg2, socket
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import urllib3.util.connection as urllib3_cn

# Force IPv4 — IPv6 to TradeStation routes via broken NAT64 from this network
def _allowed_gai_family():
    return socket.AF_INET
urllib3_cn.allowed_gai_family = _allowed_gai_family

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"
TARGET_DATE = "2026-05-22"
TARGET_LIDS = [3200]


def refresh_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    }, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def ts_get(p, t):
    r = requests.get(TS_BASE + p, headers={"Authorization": f"Bearer {t}"}, timeout=60)
    return r.json() if r.text else None


def parse_dt(s):
    if not s: return None
    s = s.replace("Z", "+00:00")
    try: return datetime.fromisoformat(s)
    except Exception: return None


def get_all_orders(acct, token):
    """Pull both /orders (today's intraday) and /historicalorders (prior days).
    Combined view."""
    all_orders = []
    live = ts_get(f"/brokerage/accounts/{acct}/orders?pageSize=600", token) or {}
    for o in live.get("Orders") or []:
        all_orders.append(o)
    # Also pull historical for completeness
    hist = ts_get(f"/brokerage/accounts/{acct}/historicalorders?since=05-21-2026&pageSize=600", token) or {}
    seen_oids = {str(o.get("OrderID")) for o in all_orders}
    for o in hist.get("Orders") or []:
        if str(o.get("OrderID")) not in seen_oids:
            all_orders.append(o)
    return all_orders


def main():
    token = refresh_token()
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = c.cursor()

    # Pull orders once per account
    accts_seen = set()
    cur.execute("""SELECT setup_log_id, state FROM real_trade_orders
                   WHERE setup_log_id = ANY(%s)""", (TARGET_LIDS,))
    rows = cur.fetchall()
    orders_by_acct = {}
    for sid, state in rows:
        if isinstance(state, str): state = json.loads(state)
        acct = state.get("account_id")
        if acct and acct not in orders_by_acct:
            orders_by_acct[acct] = get_all_orders(acct, token)
            print(f"  fetched {len(orders_by_acct[acct])} orders for acct {acct}")

    for sid, state in rows:
        if isinstance(state, str): state = json.loads(state)
        acct = state.get("account_id")
        direction = state.get("direction")
        is_long = direction in ("long", "bullish")
        exit_side_label = "Sell" if is_long else "Buy"
        entry_oid = str(state.get("entry_order_id"))
        fill_price = state.get("fill_price")

        if state.get("close_fill_price") is not None:
            print(f"\nlid={sid}: already has close_fill_price={state['close_fill_price']}, skip")
            continue

        all_o = orders_by_acct.get(acct, [])
        # Find entry order to get its fill time
        entry_order = next((o for o in all_o if str(o.get("OrderID")) == entry_oid), None)
        entry_time = None
        if entry_order:
            entry_time = parse_dt(entry_order.get("OpenedDateTime")) or parse_dt(entry_order.get("ClosedDateTime"))
        print(f"\n=== lid={sid} acct={acct} {state.get('setup_name')} {direction} ===")
        print(f"  entry oid={entry_oid} fill={fill_price} time={entry_time}")

        # Find ALL exit-direction filled orders on this acct after the entry
        candidates = []
        for o in all_o:
            if str(o.get("OrderID")) == entry_oid:
                continue
            if o.get("Status") != "FLL":
                continue
            # Match symbol
            sym_match = False
            legs = o.get("Legs") or [{}]
            for leg in legs:
                if "MES" in str(leg.get("Symbol", "")):
                    sym_match = True
                    break
            if not sym_match and "MES" not in str(o.get("Symbol", "")):
                continue
            # Match direction (exit_side: opposite of entry)
            buy_sell = None
            if legs and legs[0].get("BuyOrSell"):
                buy_sell = legs[0]["BuyOrSell"]
            elif o.get("LegFillAction"):
                buy_sell = o["LegFillAction"]
            if buy_sell and exit_side_label not in str(buy_sell):
                continue
            # Time filter: must be after entry
            ot = parse_dt(o.get("OpenedDateTime")) or parse_dt(o.get("ClosedDateTime"))
            if entry_time and ot and ot < entry_time:
                continue
            fp = o.get("FilledPrice")
            if fp is None and legs:
                fp = legs[0].get("ExecutionPrice")
            if fp is None:
                continue
            candidates.append((str(o.get("OrderID")), float(fp), ot, o))

        # Sort by time-after-entry (ascending)
        if entry_time:
            candidates.sort(key=lambda x: (x[2] - entry_time).total_seconds() if x[2] else 999999)
        print(f"  found {len(candidates)} candidate exit fills")
        for cid, fp, ot, _ in candidates[:5]:
            dt_after = (ot - entry_time).total_seconds() if entry_time and ot else None
            print(f"    oid={cid} price={fp} time={ot}  +{dt_after}s after entry")

        if not candidates:
            print(f"  no exit fill found — manual TS lookup needed")
            continue

        # Use the closest-in-time
        close_oid, close_price, close_time, _ = candidates[0]
        print(f"  --> using close oid={close_oid} price={close_price}")
        state["close_fill_price"] = close_price
        state["ghost_backfilled_at"] = datetime.now(ET).isoformat()
        state["ghost_backfilled_oid"] = close_oid
        cur.execute("UPDATE real_trade_orders SET state = %s WHERE setup_log_id = %s",
                    (json.dumps(state), sid))
        c.commit()
        print(f"  OK backfilled lid={sid} close={close_price}")

    cur.close(); c.close()


if __name__ == "__main__":
    main()
