"""
SIM test: does TS v3 accept a NORMAL ordergroup with mixed sides for futures?

Submits one atomic ordergroup to SIM2609239F:
  - Buy 1 MES Market   (entry)
  - Sell 1 MES StopMarket  (stop, far below)
  - Sell 1 MES Limit    (target, far above)

If accepted (200) → all 3 orders hit TS atomically → potential intraday-margin fix.
If rejected (400 "same side" or similar) → NORMAL has the same restriction as BRK.

Then queries balance + cancels everything to leave SIM clean.

Run pre-market: python _tmp_test_normal_ordergroup.py
"""
import os, json, time, requests, calendar
from datetime import date, timedelta

SIM_BASE = "https://sim-api.tradestation.com/v3"
SIM_ACCOUNT_ID = "SIM2609239F"


def _auto_mes_symbol() -> str:
    """Front-month MES (e.g. MESM26), rolling ~8 days before expiry."""
    today = date.today()
    months = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]
    for m, code in months:
        c = calendar.Calendar(firstweekday=calendar.MONDAY)
        fridays = [d for d in c.itermonthdates(today.year, m) if d.month == m and d.weekday() == 4]
        third_fri = fridays[2]
        if today <= third_fri - timedelta(days=8):
            return f"MES{code}{str(today.year)[-2:]}"
    return f"MESH{str(today.year + 1)[-2:]}"


def get_token() -> str:
    """Get TS access token from refresh token (read env vars from Railway)."""
    client_id = os.environ.get("TS_CLIENT_ID")
    refresh = os.environ.get("TS_REFRESH_TOKEN")
    if not client_id or not refresh:
        raise SystemExit(
            "Missing TS_CLIENT_ID / TS_REFRESH_TOKEN env vars.\n"
            "Fetch from Railway: railway variables -s 0dtealpha --json\n"
            "Then set them: $env:TS_CLIENT_ID='...'; $env:TS_REFRESH_TOKEN='...'"
        )
    resp = requests.post(
        "https://signin.tradestation.com/oauth/token",
        data={"grant_type": "refresh_token", "client_id": client_id, "refresh_token": refresh},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def main():
    token = get_token()
    mes = _auto_mes_symbol()
    print(f"\n=== SIM ordergroup test ===")
    print(f"Account: {SIM_ACCOUNT_ID}")
    print(f"Symbol: {mes}")

    # Get current ES/MES price for sensible stop/target
    quote = requests.get(
        f"{SIM_BASE}/marketdata/quotes/{mes}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()
    last = float(quote.get("Quotes", [{}])[0].get("Last", 0)) or 7400.0
    print(f"Current {mes} last: {last}")

    # Wide stop/target so neither triggers during the test
    stop_px = round((last - 50.0) * 4) / 4  # 50 pts below
    tgt_px = round((last + 50.0) * 4) / 4  # 50 pts above

    payload = {
        "Type": "NORMAL",  # atomic group, no special handling
        "Orders": [
            {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": mes,
                "Quantity": "1",
                "OrderType": "Market",
                "TradeAction": "Buy",
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            },
            {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": mes,
                "Quantity": "1",
                "OrderType": "StopMarket",
                "StopPrice": str(stop_px),
                "TradeAction": "Sell",
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            },
            {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": mes,
                "Quantity": "1",
                "OrderType": "Limit",
                "LimitPrice": str(tgt_px),
                "TradeAction": "Sell",
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            },
        ],
    }
    print(f"\nPayload:\n{json.dumps(payload, indent=2)}\n")

    resp = requests.post(
        f"{SIM_BASE}/orderexecution/ordergroups",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    print(f"=== RESPONSE ===")
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.text}\n")

    if resp.status_code != 200:
        print("❌ NORMAL ordergroup REJECTED")
        print("   → TS does not support atomic Buy+Sell+Sell ordergroup for futures.")
        print("   → Plan B: call TS Trade Desk with this exact error.")
        return

    print("✅ NORMAL ordergroup ACCEPTED")
    print("   → Now check balance to see which margin rate applies.\n")

    # Check balance immediately
    time.sleep(2)
    bal = requests.get(
        f"{SIM_BASE}/brokerage/accounts/{SIM_ACCOUNT_ID}/balances",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()
    b = bal.get("Balances", [{}])[0]
    detail = b.get("BalanceDetail", {})
    print(f"=== BALANCE AFTER FILL ===")
    print(f"BuyingPower:       {b.get('BuyingPower')}")
    print(f"CashBalance:       {b.get('CashBalance')}")
    print(f"InitialMargin:     {detail.get('InitialMargin')}")
    print(f"DayTradeMargin:    {detail.get('DayTradeMargin')}")
    print(f"MaintenanceMargin: {detail.get('MaintenanceMargin')}")
    print(f"RequiredMargin:    {detail.get('RequiredMargin')}\n")

    # Cleanup: cancel any working orders and flatten
    print("=== CLEANUP ===")
    orders = requests.get(
        f"{SIM_BASE}/brokerage/accounts/{SIM_ACCOUNT_ID}/orders",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()
    for o in orders.get("Orders", []):
        if o.get("Status") in ("OPN", "ACK", "DON"):
            oid = o.get("OrderID")
            requests.delete(
                f"{SIM_BASE}/orderexecution/orders/{oid}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            print(f"  cancelled order {oid}")

    pos = requests.get(
        f"{SIM_BASE}/brokerage/accounts/{SIM_ACCOUNT_ID}/positions",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()
    for p in pos.get("Positions", []):
        qty = int(p.get("Quantity", 0))
        if qty != 0:
            close_side = "Sell" if qty > 0 else "Buy"
            close_payload = {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": p.get("Symbol"),
                "Quantity": str(abs(qty)),
                "OrderType": "Market",
                "TradeAction": close_side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            }
            close_resp = requests.post(
                f"{SIM_BASE}/orderexecution/orders",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=close_payload,
                timeout=15,
            )
            print(f"  flatten {p.get('Symbol')} {close_side} {abs(qty)}: {close_resp.status_code}")

    print("\nDone.")


if __name__ == "__main__":
    main()
