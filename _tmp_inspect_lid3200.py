"""Find lid=3200's exit and confirm no orphan position."""
import os, json, requests, socket
import urllib3.util.connection as urllib3_cn
def _ipv4(): return socket.AF_INET
urllib3_cn.allowed_gai_family = _ipv4

TS_BASE = "https://api.tradestation.com/v3"

def refresh_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    }, timeout=60); r.raise_for_status()
    return r.json()["access_token"]


def ts_get(p, t):
    r = requests.get(TS_BASE + p, headers={"Authorization": f"Bearer {t}"}, timeout=60)
    return r.json() if r.text else None


def main():
    t = refresh_token()
    acct = "210VYX65"

    # Current positions
    print("Current positions on 210VYX65:")
    pos = ts_get(f"/brokerage/accounts/{acct}/positions", t) or {}
    for p in pos.get("Positions") or []:
        print(f"  {p}")
    if not pos.get("Positions"):
        print("  (none — account is FLAT)")
    print()

    # Balance detail
    bd = ts_get(f"/brokerage/accounts/{acct}/balances", t) or {}
    for b in bd.get("Balances") or []:
        det = b.get("BalanceDetail") or {}
        print(f"Balance: Cash={b.get('CashBalance')} BP={b.get('BuyingPower')} "
              f"Realized={det.get('RealizedProfitLoss')} Unrealized={b.get('UnrealizedProfitLoss')}")
    print()

    # All MES Buy orders today (to count entries)
    live = ts_get(f"/brokerage/accounts/{acct}/orders?pageSize=600", t) or {}
    hist = ts_get(f"/brokerage/accounts/{acct}/historicalorders?since=05-21-2026&pageSize=600", t) or {}
    all_o = (live.get("Orders") or []) + (hist.get("Orders") or [])
    by_oid = {str(o.get("OrderID")): o for o in all_o}

    buys = []
    for o in by_oid.values():
        legs = (o.get("Legs") or [{}])[0]
        if legs.get("BuyOrSell") != "Buy":
            continue
        if "MES" not in str(legs.get("Symbol", "")):
            continue
        cd = o.get("ClosedDateTime", "") or ""
        if not cd.startswith("2026-05-22"):
            continue
        if o.get("Status") != "FLL":
            continue
        buys.append(o)
    buys.sort(key=lambda o: o.get("ClosedDateTime") or "")
    print(f"FILLED Buy orders today on 210VYX65 ({len(buys)} entries):")
    for o in buys:
        legs = (o.get("Legs") or [{}])[0]
        print(f"  OID={o.get('OrderID')} fill={legs.get('ExecutionPrice')} "
              f"closed={o.get('ClosedDateTime')}")
    print()

    # All MES Sell orders today
    sells = []
    for o in by_oid.values():
        legs = (o.get("Legs") or [{}])[0]
        if legs.get("BuyOrSell") != "Sell":
            continue
        if "MES" not in str(legs.get("Symbol", "")):
            continue
        cd = o.get("ClosedDateTime", "") or ""
        if not cd.startswith("2026-05-22"):
            continue
        if o.get("Status") != "FLL":
            continue
        sells.append(o)
    sells.sort(key=lambda o: o.get("ClosedDateTime") or "")
    print(f"FILLED Sell orders today on 210VYX65 ({len(sells)} exits):")
    for o in sells:
        legs = (o.get("Legs") or [{}])[0]
        print(f"  OID={o.get('OrderID')} type={o.get('OrderType'):12s} fill={legs.get('ExecutionPrice')} "
              f"closed={o.get('ClosedDateTime')}")


if __name__ == "__main__":
    main()
