"""Authoritative broker pull for 210VYX65 on 2026-06-02: positions, orders, realized P&L."""
import os, requests, json
from datetime import datetime, timezone, timedelta

AUTH = "https://signin.tradestation.com"
BASE = "https://api.tradestation.com/v3"
ACCT = "210VYX65"
CID, SEC, RTOK = os.getenv("TS_CLIENT_ID"), os.getenv("TS_CLIENT_SECRET"), os.getenv("TS_REFRESH_TOKEN")

r = requests.post(f"{AUTH}/oauth/token", data={
    "grant_type": "refresh_token", "refresh_token": RTOK,
    "client_id": CID, "client_secret": SEC,
    "scope": "openid profile MarketData ReadAccount Trade OptionSpreads offline_access",
}, timeout=20)
r.raise_for_status()
TOK = r.json()["access_token"]
H = {"Authorization": f"Bearer {TOK}"}

print("=" * 100)
print("CURRENT POSITIONS on", ACCT)
print("=" * 100)
p = requests.get(f"{BASE}/brokerage/accounts/{ACCT}/positions", headers=H, timeout=15).json()
for pos in p.get("Positions", []):
    print(f"  {pos.get('Symbol')}  qty={pos.get('Quantity')}  {pos.get('LongShort')}  "
          f"avg={pos.get('AveragePrice')}  open_pnl={pos.get('UnrealizedProfitLoss')}")
if not p.get("Positions"):
    print("  FLAT (no positions)")

print("\n" + "=" * 100)
print("BALANCES — RealizedProfitLoss (day truth)")
print("=" * 100)
b = requests.get(f"{BASE}/brokerage/accounts/{ACCT}/balances", headers=H, timeout=15).json()
for bal in b.get("Balances", []):
    d = bal.get("BalanceDetail", {})
    print(f"  CashBalance={bal.get('CashBalance')}  RealizedPnL={d.get('RealizedProfitLoss')}  "
          f"UnrealizedPnL={d.get('UnrealizedProfitLoss')}")

print("\n" + "=" * 100)
print("HISTORICAL ORDERS (MES today, chronological)")
print("=" * 100)
since = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%m-%d-%Y")
h = requests.get(f"{BASE}/brokerage/accounts/{ACCT}/historicalorders?since={since}&pageSize=600",
                 headers=H, timeout=20).json()
orders = h.get("Orders", [])
def et(s):
    if not s: return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%H:%M:%S")
    except Exception:
        return s
rows = []
for o in orders:
    leg = (o.get("Legs") or [{}])[0]
    sym = leg.get("Symbol", "")
    if "MES" not in sym.upper():
        continue
    opened = o.get("OpenedDateTime", "")
    # only today (ET date)
    try:
        odt = datetime.fromisoformat(opened.replace("Z", "+00:00")).astimezone(timezone(timedelta(hours=-4)))
        if odt.strftime("%Y-%m-%d") != "2026-06-02":
            continue
    except Exception:
        pass
    rows.append((opened, o, leg))
rows.sort(key=lambda x: x[0])
for opened, o, leg in rows:
    fill = ""
    for k in ("ExecutionPrice", "FilledPrice", "AverageFilledPrice"):
        if leg.get(k) or o.get(k):
            fill = leg.get(k) or o.get(k); break
    print(f"  oid={o.get('OrderID'):>12}  {et(opened)}  {leg.get('BuyOrSell'):4s} "
          f"{leg.get('QuantityOrdered')}x  {o.get('OrderType'):11s}  status={o.get('Status'):4s}  "
          f"fill={fill or '-':>9}  limit={o.get('LimitPrice') or '-'}  stop={o.get('StopPrice') or '-'}  "
          f"closed={et(o.get('ClosedDateTime',''))}")
