"""
ONE-OFF REAL-MONEY MARGIN TEST — Atomic NORMAL ordergroup.

Tests whether atomic entry+stop+target in ONE POST gets TradeStation to apply
day-rate margin ($265/MES) vs the overnight rate ($2,499/MES) we've been
suspected of getting via the sequential 3-POST path.

DIRECTION: SHORT (DD hedging -$5.1B + ES last bar delta -266 → likely drift down
           → manual close at small win/break-even).
ACCOUNT:   210VYX91 (SHORTS — whitelisted).
SIZE:      3 MES.
SYMBOL:    MESM26 (front-month per real_trader auto-rollover).
STOP:      ES + 3.0 pt (~$45 worst-case loss if stop hits).
TARGET:    ES - 10.0 pt (filler — won't hit, will manually close).
SAFETY:    Auto-flatten if unrealized loss exceeds $60.

SEQUENCE:
  1. Refresh TS token
  2. Read BalanceDetail BEFORE
  3. POST atomic ordergroup (entry + stop + target) — ONE call
  4. Wait for entry fill (60s timeout)
  5. Read BalanceDetail immediately after fill
  6. Wait 5s, read BalanceDetail again (settled)
  7. Watch unrealized P&L for ~10s with safety bail
  8. Cancel stop + target order IDs
  9. Market close 3 MES BUY
  10. Wait for flat
  11. Read BalanceDetail AFTER
  12. Print VERDICT (which margin rate applied?)

USAGE: python _tmp_atomic_margin_test.py
"""
import os
import time
import json
import requests
from datetime import datetime

# === CONFIG ===
ACCOUNT = "210VYX91"
SYMBOL = "MESM26"
QTY = 3
STOP_OFFSET = 3.0
TARGET_OFFSET = 10.0
MAX_LOSS_DOLLARS = 60.0
PROFIT_TAKE_DOLLARS = 5.0      # close as soon as we're up at least $5 (~0.33 pt on 3 MES)
MAX_HOLD_SECONDS = 300         # hard timeout — close after 5 min regardless

TS_BASE = "https://api.tradestation.com/v3"


def refresh_token() -> str:
    r = requests.post(
        "https://signin.tradestation.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ["TS_CLIENT_ID"],
            "client_secret": os.environ["TS_CLIENT_SECRET"],
            "refresh_token": os.environ["TS_REFRESH_TOKEN"],
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def ts_api(method: str, path: str, token: str, json_body=None):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = TS_BASE + path
    if method == "GET":
        r = requests.get(url, headers=headers, timeout=10)
    elif method == "POST":
        r = requests.post(url, headers=headers, json=json_body, timeout=15)
    elif method == "DELETE":
        r = requests.delete(url, headers=headers, timeout=10)
    else:
        raise ValueError(method)
    try:
        return r.json() if r.text else None
    except Exception:
        return {"_raw": r.text, "_status": r.status_code}


def get_balance(token: str) -> dict:
    data = ts_api("GET", f"/brokerage/accounts/{ACCOUNT}/balances", token)
    if not data:
        return {}
    bals = data.get("Balances", [])
    if isinstance(bals, list) and bals:
        b = bals[0]
    elif isinstance(bals, dict):
        b = bals
    else:
        return {}
    detail = b.get("BalanceDetail", {}) or {}
    return {
        "BP": b.get("BuyingPower"),
        "Cash": b.get("CashBalance"),
        "InitialMargin": detail.get("InitialMargin"),
        "DayTradeMargin": detail.get("DayTradeMargin"),
        "MaintenanceMargin": detail.get("MaintenanceMargin"),
        "RequiredMargin": detail.get("RequiredMargin"),
        "RealizedPnL": detail.get("RealizedProfitLoss"),
    }


def get_position(token: str) -> dict | None:
    data = ts_api("GET", f"/brokerage/accounts/{ACCOUNT}/positions", token)
    if not data:
        return None
    positions = data.get("Positions", []) or []
    for p in positions:
        if p.get("Symbol") == SYMBOL:
            return p
    return None


def get_es_price(token: str) -> float | None:
    data = ts_api("GET", f"/marketdata/quotes/{SYMBOL}", token)
    if not data:
        return None
    quotes = data.get("Quotes", [])
    if not quotes:
        return None
    q = quotes[0]
    for k in ("Last", "Bid", "Ask"):
        v = q.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    return None


def print_balance(label: str, b: dict) -> None:
    print(f"\n[{label}]")
    for k in ("BP", "Cash", "InitialMargin", "DayTradeMargin",
             "MaintenanceMargin", "RequiredMargin", "RealizedPnL"):
        print(f"  {k:<18} {b.get(k)}")


def flatten_emergency(token: str, stop_oid: str | None, target_oid: str | None):
    print("\n!! EMERGENCY FLATTEN !!")
    if stop_oid:
        try:
            ts_api("DELETE", f"/orderexecution/orders/{stop_oid}", token)
        except Exception as e:
            print(f"  stop cancel error: {e}")
    if target_oid:
        try:
            ts_api("DELETE", f"/orderexecution/orders/{target_oid}", token)
        except Exception as e:
            print(f"  target cancel error: {e}")
    close = {
        "AccountID": ACCOUNT, "Symbol": SYMBOL, "Quantity": str(QTY),
        "OrderType": "Market", "TradeAction": "Buy",
        "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
    }
    try:
        r = ts_api("POST", "/orderexecution/orders", token, close)
        print(f"  emergency close resp: {r}")
    except Exception as e:
        print(f"  emergency close error: {e}")


def main():
    started = datetime.utcnow().isoformat()
    print("=" * 72)
    print(f"ATOMIC MARGIN TEST | {started}Z")
    print(f"  account={ACCOUNT}  symbol={SYMBOL}  qty={QTY}  direction=SHORT")
    print("=" * 72)

    token = refresh_token()

    pre = get_balance(token)
    print_balance("PRE-TRADE", pre)

    es_price = get_es_price(token)
    if es_price is None:
        print("FAIL: cannot get ES price")
        return
    stop_price = round(es_price + STOP_OFFSET, 2)
    target_price = round(es_price - TARGET_OFFSET, 2)
    print(f"\nES current: {es_price}")
    print(f"  Entry: SELL {QTY} {SYMBOL} market")
    print(f"  Stop:  BUY  {QTY} stop @ {stop_price}")
    print(f"  Target:BUY  {QTY} limit @ {target_price} (filler — manual close)")

    entry = {
        "AccountID": ACCOUNT, "Symbol": SYMBOL, "Quantity": str(QTY),
        "OrderType": "Market", "TradeAction": "Sell",
        "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
    }
    stop = {
        "AccountID": ACCOUNT, "Symbol": SYMBOL, "Quantity": str(QTY),
        "OrderType": "StopMarket", "StopPrice": str(stop_price),
        "TradeAction": "Buy",
        "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
    }
    target = {
        "AccountID": ACCOUNT, "Symbol": SYMBOL, "Quantity": str(QTY),
        "OrderType": "Limit", "LimitPrice": str(target_price),
        "TradeAction": "Buy",
        "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
    }
    payload = {"Type": "NORMAL", "Orders": [entry, stop, target]}

    print("\n>>> POSTING ATOMIC ORDERGROUP <<<")
    resp = ts_api("POST", "/orderexecution/ordergroups", token, payload)
    print(f"Response:\n{json.dumps(resp, indent=2, default=str)}")

    orders = (resp or {}).get("Orders", [])
    if not orders or any(o.get("Error") == "FAILED" for o in orders[:1]):
        print("\n!! ENTRY REJECTED — no position taken !!")
        return
    entry_oid = orders[0].get("OrderID")
    stop_oid = orders[1].get("OrderID") if len(orders) > 1 else None
    target_oid = orders[2].get("OrderID") if len(orders) > 2 else None
    print(f"entry_oid={entry_oid} stop_oid={stop_oid} target_oid={target_oid}")
    if not entry_oid:
        return

    fill_price = None
    for s in range(60):
        time.sleep(1)
        pos = get_position(token)
        if pos and abs(int(pos.get("Quantity", 0) or 0)) >= QTY:
            fill_price = float(pos.get("AveragePrice", 0) or 0)
            print(f"\nFILLED @ {fill_price} after {s+1}s")
            break
    if fill_price is None:
        print("ENTRY DID NOT FILL — cancelling stop+target")
        if stop_oid: ts_api("DELETE", f"/orderexecution/orders/{stop_oid}", token)
        if target_oid: ts_api("DELETE", f"/orderexecution/orders/{target_oid}", token)
        return

    try:
        time.sleep(1)
        at_pos_1 = get_balance(token)
        print_balance("AT-POSITION immediate", at_pos_1)

        time.sleep(5)
        at_pos_2 = get_balance(token)
        print_balance("AT-POSITION +5s settled", at_pos_2)

        print(f"\nWatching P&L — close at +${PROFIT_TAKE_DOLLARS:.0f} | "
              f"bail at -${MAX_LOSS_DOLLARS:.0f} | hard timeout {MAX_HOLD_SECONDS}s:")
        exit_reason = "timeout"
        t0 = time.time()
        while time.time() - t0 < MAX_HOLD_SECONDS:
            time.sleep(1)
            pos = get_position(token)
            if not pos:
                print(f"  t+{int(time.time()-t0)}s | position GONE (stop hit?)")
                exit_reason = "stop_hit"
                break
            unr = float(pos.get("UnrealizedProfitLoss", 0) or 0)
            elapsed = int(time.time() - t0)
            print(f"  t+{elapsed}s | unrealized P&L: ${unr:+.2f}")
            if unr >= PROFIT_TAKE_DOLLARS:
                print(f"  ✓ profit hit +${PROFIT_TAKE_DOLLARS:.0f} — closing")
                exit_reason = "profit_take"
                break
            if unr < -MAX_LOSS_DOLLARS:
                print(f"  !! safety: loss > ${MAX_LOSS_DOLLARS:.0f} — flattening")
                exit_reason = "safety_bail"
                break
        else:
            print(f"  ⏰ hard timeout {MAX_HOLD_SECONDS}s reached — closing")

        if exit_reason == "stop_hit":
            print("\nStop already filled, no manual close needed")
            if target_oid:
                print(f"  cancel target {target_oid}: {ts_api('DELETE', f'/orderexecution/orders/{target_oid}', token)}")
        else:
            print(f"\n>>> MANUAL CLOSE ({exit_reason}) — cancel stop+target, market close <<<")
            if stop_oid:
                print(f"  cancel stop {stop_oid}: {ts_api('DELETE', f'/orderexecution/orders/{stop_oid}', token)}")
            if target_oid:
                print(f"  cancel target {target_oid}: {ts_api('DELETE', f'/orderexecution/orders/{target_oid}', token)}")

            close = {
                "AccountID": ACCOUNT, "Symbol": SYMBOL, "Quantity": str(QTY),
                "OrderType": "Market", "TradeAction": "Buy",
                "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
            }
            cr = ts_api("POST", "/orderexecution/orders", token, close)
            print(f"  market close resp: {cr}")

            for s in range(30):
                time.sleep(1)
                pos = get_position(token)
                if not pos or abs(int(pos.get("Quantity", 0) or 0)) == 0:
                    print(f"FLAT after {s+1}s")
                    break

        time.sleep(2)
        post = get_balance(token)
        print_balance("POST-CLOSE", post)

        print("\n" + "=" * 72)
        print("VERDICT")
        print("=" * 72)
        im = at_pos_2.get("InitialMargin")
        dtm = at_pos_2.get("DayTradeMargin")
        print(f"InitialMargin at position : ${im}")
        print(f"DayTradeMargin at position: ${dtm}")
        print(f"Day-rate expected (3×265) : ${3*265}")
        print(f"Overnight expected (3×2499): ${3*2499}")
        try:
            im_f = float(im or 0)
            if im_f < 1500:
                print("\n[VERDICT] ✓ DAY-RATE — atomic FIXED the margin issue")
            elif im_f > 5000:
                print("\n[VERDICT] ✗ OVERNIGHT RATE — atomic did NOT fix margin")
            else:
                print("\n[VERDICT] ? AMBIGUOUS — between rates, inspect manually")
        except Exception:
            print("[VERDICT] ? cannot parse InitialMargin")

        pnl_change = (float(post.get("RealizedPnL") or 0) -
                      float(pre.get("RealizedPnL") or 0))
        print(f"\nRealized P&L change: ${pnl_change:+.2f}")
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt — emergency flatten")
        flatten_emergency(token, stop_oid, target_oid)
        raise
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e!r} — emergency flatten")
        flatten_emergency(token, stop_oid, target_oid)
        raise


if __name__ == "__main__":
    main()
