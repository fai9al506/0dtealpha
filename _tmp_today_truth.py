"""Broker truth + DB trade log for TODAY (both TSRT accounts)."""
import os, requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

AUTH = "https://signin.tradestation.com"
BASE = "https://api.tradestation.com/v3"
ACCTS = {"210VYX65": "LONG", "210VYX91": "SHORT"}
CID, SEC, RTOK = os.getenv("TS_CLIENT_ID"), os.getenv("TS_CLIENT_SECRET"), os.getenv("TS_REFRESH_TOKEN")
ET = ZoneInfo("America/New_York")
today = datetime.now(ET).date()
print("ET now:", datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %A"), "| trade date:", today)

r = requests.post(f"{AUTH}/oauth/token", data={
    "grant_type": "refresh_token", "refresh_token": RTOK,
    "client_id": CID, "client_secret": SEC,
    "scope": "openid profile MarketData ReadAccount Trade OptionSpreads offline_access",
}, timeout=20)
r.raise_for_status()
TOK = r.json()["access_token"]
H = {"Authorization": f"Bearer {TOK}"}

def etstr(s):
    if not s: return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return dt.astimezone(ET).strftime("%H:%M:%S")
    except Exception: return s

since = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%m-%d-%Y")
for acct, label in ACCTS.items():
    print("\n" + "="*90)
    print(f"ACCOUNT {acct} ({label})")
    print("="*90)
    b = requests.get(f"{BASE}/brokerage/accounts/{acct}/balances", headers=H, timeout=15).json()
    for bal in b.get("Balances", []):
        d = bal.get("BalanceDetail", {})
        print(f"  CashBalance={bal.get('CashBalance')}  RealizedPnL={d.get('RealizedProfitLoss')}  UnrealPnL={d.get('UnrealizedProfitLoss')}")
    p = requests.get(f"{BASE}/brokerage/accounts/{acct}/positions", headers=H, timeout=15).json()
    poss = p.get("Positions", [])
    if poss:
        for pos in poss:
            print(f"  OPEN: {pos.get('Symbol')} qty={pos.get('Quantity')} {pos.get('LongShort')} avg={pos.get('AveragePrice')} open_pnl={pos.get('UnrealizedProfitLoss')}")
    else:
        print("  FLAT")
    # filled MES orders today
    for endpoint in ["historicalorders", "orders"]:
        url = f"{BASE}/brokerage/accounts/{acct}/{endpoint}"
        if endpoint == "historicalorders":
            url += f"?since={since}&pageSize=600"
        h = requests.get(url, headers=H, timeout=20).json()
        for o in h.get("Orders", []):
            opened = o.get("OpenedDateTime","")
            try:
                od = datetime.fromisoformat(opened.replace("Z","+00:00")).astimezone(ET).date()
            except Exception:
                od = None
            if od != today: continue
            status = o.get("Status","")
            if status not in ("FLL","FPR"): continue
            for leg in o.get("Legs", []):
                sym = leg.get("Symbol","")
                if "MES" not in sym.upper(): continue
                print(f"   [{endpoint[:4]}] {etstr(opened)} {leg.get('BuyOrSell'):>4} {leg.get('ExecQuantity')} {sym} @ {leg.get('ExecutionPrice')}  ({o.get('OrderType')})")
