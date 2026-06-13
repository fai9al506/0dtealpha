"""Comprehensive S200 netting-guard matrix. Each case asserts whether
_flatten_position(lid) should place a market sell (close) or skip."""
import app.real_trader as rt

def run_case(name, lids, close_lid, broker_qty, expect_sell):
    posted = []
    bn = {"q": broker_qty}
    rt._validate_account_direction = lambda a, l: True
    rt._get_broker_position = lambda a, expect_position=False: (
        {"qty": bn["q"], "long_short": "Long", "symbol": "MESM26"} if bn["q"] > 0 else None)
    rt._get_order_fill_price = lambda oid, a: None
    rt._backfill_ghost_fill = lambda o: None
    rt._alert = lambda m: None
    rt._day_line = lambda a: ""
    rt._persist_order = lambda lid: None
    def _api(method, path, body, acct):
        if method == "POST" and "orderexecution/orders" in path:
            posted.append(body); bn["q"] -= int(body["Quantity"])
            return {"Orders": [{"OrderID": "1", "Error": None}]}
        return {}
    rt._ts_api = _api
    rt._active_orders = {}
    for lid, ts in lids:
        rt._active_orders[lid] = {
            "setup_log_id": lid, "account_id": "210VYX65", "direction": "long",
            "status": "filled", "quantity": 1, "fill_price": 7600.0,
            "stop_order_id": f"s{lid}", "ts_placed": ts, "setup_name": "SC"}
    rt._active_orders[close_lid]["closing_in_progress"] = True
    rt._flatten_position(rt._active_orders[close_lid])
    got_sell = len(posted) == 1
    ok = (got_sell == expect_sell)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: sold={got_sell} expected={expect_sell}")
    return ok

T = "2026-06-02T"
allok = True
# 3 longs, broker net 3 (all real). Closing ANY lid should sell (all hold a contract).
allok &= run_case("3 open net3, close oldest",  [(1,T+"09:00"),(2,T+"10:00"),(3,T+"11:00")], 1, 3, True)
allok &= run_case("3 open net3, close middle",  [(1,T+"09:00"),(2,T+"10:00"),(3,T+"11:00")], 2, 3, True)
allok &= run_case("3 open net3, close newest",  [(1,T+"09:00"),(2,T+"10:00"),(3,T+"11:00")], 3, 3, True)
# Today's bug: 3 tracked but net2 (oldest already gone). Redundant close of OLDEST -> skip.
allok &= run_case("3 tracked net2, close oldest(gone)", [(1,T+"09:00"),(2,T+"10:00"),(3,T+"11:00")], 1, 2, False)
# net2, close a survivor (newest) -> sell.
allok &= run_case("3 tracked net2, close newest(live)", [(1,T+"09:00"),(2,T+"10:00"),(3,T+"11:00")], 3, 2, True)
# Naked hole: 2 tracked, net1 (older stale). Close NEWER live one -> MUST sell.
allok &= run_case("2 tracked net1, close newer(live)", [(1,T+"09:00"),(2,T+"10:00")], 2, 1, True)
# 2 tracked, net1, close OLDER(gone) -> skip.
allok &= run_case("2 tracked net1, close older(gone)", [(1,T+"09:00"),(2,T+"10:00")], 1, 1, False)
# Single lid, net1 -> sell (baseline unchanged).
allok &= run_case("1 tracked net1, close it", [(1,T+"09:00")], 1, 1, True)
# Single lid, net0 (already flat) -> no sell (broker_pos None -> earlier return).
allok &= run_case("1 tracked net0 flat", [(1,T+"09:00")], 1, 0, False)

print("\n" + ("ALL PASS" if allok else "SOME FAILED"))
