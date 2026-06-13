"""Pull current TSRT P&L from both accounts + state of any open positions."""
import os, requests, json, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"
ACCOUNTS = ["210VYX65", "210VYX91"]


def refresh_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def ts_get(path, token):
    r = requests.get(TS_BASE + path,
                     headers={"Authorization": f"Bearer {token}"}, timeout=10)
    return r.json() if r.text else None


def fmt_money(v):
    if v is None: return "  N/A  "
    try:
        v = float(v)
        sign = "+" if v >= 0 else "-"
        return f"{sign}${abs(v):>8.2f}"
    except Exception:
        return f"  {v}  "


print(f"=== TSRT STATUS @ {datetime.now(ET).strftime('%H:%M:%S ET %Y-%m-%d')} ===\n")

token = refresh_token()
total_realized = 0.0
total_unrealized = 0.0

for acct in ACCOUNTS:
    label = "LONGS " if acct == "210VYX65" else "SHORTS"
    bal = ts_get(f"/brokerage/accounts/{acct}/balances", token)
    if bal:
        b = bal.get("Balances", [{}])
        b = b[0] if isinstance(b, list) else b
        detail = b.get("BalanceDetail", {}) or {}
        cash = b.get("CashBalance")
        bp = b.get("BuyingPower")
        rpnl = detail.get("RealizedProfitLoss")
        upnl = detail.get("UnrealizedProfitLoss")
        im = detail.get("InitialMargin")
        dtm = detail.get("DayTradeMargin")
        print(f"[{label} {acct}]")
        print(f"  Cash:           {fmt_money(cash)}    BuyingPower: {fmt_money(bp)}")
        print(f"  Realized today: {fmt_money(rpnl)}    Unrealized: {fmt_money(upnl)}")
        print(f"  InitialMargin:  {fmt_money(im)}    DayTradeMargin: {fmt_money(dtm)}")
        if rpnl is not None:
            total_realized += float(rpnl)
        if upnl is not None:
            total_unrealized += float(upnl)

    pos = ts_get(f"/brokerage/accounts/{acct}/positions", token)
    positions = (pos or {}).get("Positions", []) or []
    if positions:
        for p in positions:
            sym = p.get("Symbol")
            qty = p.get("Quantity")
            avg = p.get("AveragePrice")
            mkt = p.get("MarketValue") or p.get("Last")
            unr = p.get("UnrealizedProfitLoss")
            ldn = p.get("LongShort")
            print(f"  POSITION: {sym} {ldn} qty={qty} avg={avg} mark={mkt} unrealized={fmt_money(unr)}")
    else:
        print("  POSITION: FLAT")
    print()

print(f"=== COMBINED ===")
print(f"  Realized today:   {fmt_money(total_realized)}")
print(f"  Unrealized open:  {fmt_money(total_unrealized)}")
print(f"  Net day:          {fmt_money(total_realized + total_unrealized)}")

print()
print("=== Today's real_trade_orders ===")
c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()
cur.execute("""
    SELECT setup_log_id, state
    FROM real_trade_orders
    WHERE created_at >= CURRENT_DATE
    ORDER BY setup_log_id
""")
rows = cur.fetchall()
if not rows:
    print("  (none yet)")
for sid, state in rows:
    if isinstance(state, str):
        state = json.loads(state)
    setup = state.get("setup_name")
    dir_ = state.get("direction")
    atomic = state.get("atomic_bracket")
    qty = state.get("quantity") or 1
    fill = state.get("fill_price")
    close = state.get("close_fill_price")
    status = state.get("status")
    reason = state.get("close_reason", "")
    path = "[ATOMIC]" if atomic else "[SEQ]"
    print(f"  lid={sid} {path} {setup} {dir_} {qty}x fill={fill} close={close} status={status} reason={reason}")

cur.close(); c.close()
