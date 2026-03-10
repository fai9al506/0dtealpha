"""Pull full account statements, positions, and balances for both SIM accounts."""
import os, json, requests

resp = requests.post("https://signin.tradestation.com/oauth/token", data={
    "grant_type": "refresh_token",
    "client_id": os.environ["TS_CLIENT_ID"],
    "client_secret": os.environ["TS_CLIENT_SECRET"],
    "refresh_token": os.environ["TS_REFRESH_TOKEN"],
})
token = resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

FUTURES_SIM = "SIM2609239F"
OPTIONS_SIM = "SIM2609238M"

for acct, label in [(FUTURES_SIM, "FUTURES SIM"), (OPTIONS_SIM, "OPTIONS SIM")]:
    print(f"\n{'='*60}", flush=True)
    print(f"=== {label} ({acct}) ===", flush=True)
    print(f"{'='*60}", flush=True)

    # 1. BALANCES
    print("\n--- BALANCES ---", flush=True)
    resp = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/balances",
        headers=headers
    )
    if resp.status_code == 200:
        bal_data = resp.json()
        # Print ALL balance fields
        balances = bal_data.get("Balances", [])
        if isinstance(balances, list) and balances:
            b = balances[0]
        elif isinstance(balances, dict):
            b = balances
        else:
            b = bal_data
        for k, v in sorted(b.items()) if isinstance(b, dict) else []:
            if v and v != "0" and v != 0:
                print(f"  {k}: {v}", flush=True)
    else:
        print(f"  Error: {resp.status_code} {resp.text[:200]}", flush=True)

    # 2. POSITIONS
    print("\n--- OPEN POSITIONS ---", flush=True)
    resp = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/positions",
        headers=headers
    )
    if resp.status_code == 200:
        pos_data = resp.json()
        positions = pos_data.get("Positions", [])
        if not positions:
            print("  No open positions", flush=True)
        for p in positions:
            print(f"\n  Symbol: {p.get('Symbol')}", flush=True)
            print(f"  Quantity: {p.get('Quantity')}", flush=True)
            print(f"  AveragePrice: {p.get('AveragePrice')}", flush=True)
            print(f"  Last: {p.get('Last')}", flush=True)
            print(f"  MarketValue: {p.get('MarketValue')}", flush=True)
            print(f"  TodaysProfitLoss: {p.get('TodaysProfitLoss')}", flush=True)
            print(f"  UnrealizedProfitLoss: {p.get('UnrealizedProfitLoss')}", flush=True)
            print(f"  UnrealizedProfitLossPercent: {p.get('UnrealizedProfitLossPercent')}", flush=True)
            print(f"  UnrealizedProfitLossQty: {p.get('UnrealizedProfitLossQty')}", flush=True)
            print(f"  LongShort: {p.get('LongShort')}", flush=True)
            print(f"  ConversionRate: {p.get('ConversionRate')}", flush=True)
            # Print ALL fields
            for k, v in sorted(p.items()):
                if k not in ('Symbol','Quantity','AveragePrice','Last','MarketValue',
                             'TodaysProfitLoss','UnrealizedProfitLoss','UnrealizedProfitLossPercent',
                             'UnrealizedProfitLossQty','LongShort','ConversionRate'):
                    if v:
                        print(f"  {k}: {v}", flush=True)
    else:
        print(f"  Error: {resp.status_code} {resp.text[:200]}", flush=True)

    # 3. ORDERS (all, with full detail)
    print("\n--- ALL ORDERS (last 7 days) ---", flush=True)
    resp = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/orders",
        headers=headers,
        params={"since": "2026-03-01", "pageSize": "600"}
    )
    if resp.status_code == 200:
        orders = resp.json().get("Orders", [])
        # Group by date
        by_date = {}
        for o in orders:
            dt = o.get("OpenedDateTime", "")[:10]
            by_date.setdefault(dt, []).append(o)

        for dt in sorted(by_date.keys()):
            day_orders = by_date[dt]
            # Count statuses
            statuses = {}
            for o in day_orders:
                s = o.get("StatusDescription", "?")
                statuses[s] = statuses.get(s, 0) + 1
            print(f"\n  {dt}: {len(day_orders)} orders - {statuses}", flush=True)

            # Print filled orders with detail
            for o in sorted(day_orders, key=lambda x: x.get("ClosedDateTime", x.get("OpenedDateTime", ""))):
                status = o.get("StatusDescription", "?")
                legs = o.get("Legs", [{}])
                leg = legs[0] if legs else {}
                ts_close = o.get("ClosedDateTime", o.get("OpenedDateTime", ""))[:19]
                sym = leg.get("Symbol", "?")
                act = leg.get("BuyOrSell", "?")
                qty = leg.get("ExecQuantity", leg.get("QuantityOrdered", "?"))
                exec_price = leg.get("ExecPrice", "N/A")
                commission = o.get("CommissionFee", 0)
                otype = o.get("OrderType", "?")
                oid = o.get("OrderID", "?")
                limit_p = o.get("LimitPrice", "")
                stop_p = o.get("StopPrice", "")
                group = o.get("GroupName", "")
                reject_reason = o.get("RejectReason", "")

                line = f"    {ts_close}  {status:10}  {act:4}  {qty:>3}x  {sym:24}  exec@{exec_price}  {otype}"
                if limit_p: line += f"  lmt={limit_p}"
                if stop_p: line += f"  stp={stop_p}"
                if commission: line += f"  comm=${commission}"
                if group: line += f"  grp={group}"
                if reject_reason: line += f"  REJECT: {reject_reason}"
                print(line, flush=True)
    else:
        print(f"  Error: {resp.status_code} {resp.text[:200]}", flush=True)

    # 4. HISTORICAL ORDERS for March 6 specifically
    print(f"\n--- MARCH 6 ORDER DETAILS ---", flush=True)
    resp2 = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/historicalorders",
        headers=headers,
        params={"since": "2026-03-06", "pageSize": "600"}
    )
    if resp2.status_code == 200:
        hist = resp2.json()
        orders = hist.get("Orders", hist.get("HistoricalOrders", []))
        print(f"  Historical orders endpoint: {len(orders)} orders", flush=True)
        if orders:
            for o in orders[:3]:
                print(f"  Sample: {json.dumps(o, default=str)[:500]}", flush=True)
    elif resp2.status_code == 404:
        print("  Historical orders endpoint not available", flush=True)
    else:
        print(f"  Error: {resp2.status_code} {resp2.text[:200]}", flush=True)
