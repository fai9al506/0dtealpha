"""AUDIT: stress the S200 netting guard for the NAKED-POSITION hole.

Scenario B (the bug): 2 longs A(old) + B(new), net=1 at broker because A's stop
ALREADY filled (A's contract gone) but the bot still marks A 'filled' (poll lag).
Now B resolves -> _flatten_position(B). B's contract is the live one; it MUST be
closed. A correct guard closes B (1 market sell). The buggy subtraction guard sees
other_open=A=1, net=1 -> max_closeable=0 -> SKIPS -> B left NAKED (stop already
cancelled at top of _flatten_position).
"""
import app.real_trader as rt

posted = []
broker_net = {"qty": 1}  # only B's contract remains (A's stop already filled)
rt._validate_account_direction = lambda a, l: True
rt._get_broker_position = lambda a, expect_position=False: (
    {"qty": broker_net["qty"], "long_short": "Long", "symbol": "MESM26"} if broker_net["qty"] > 0 else None)
rt._get_order_fill_price = lambda oid, a: None
rt._backfill_ghost_fill = lambda o: None
rt._alert = lambda m: None
rt._day_line = lambda a: ""
rt._persist_order = lambda lid: None
def _api(method, path, body, acct):
    if method == "POST" and "orderexecution/orders" in path:
        posted.append(body); broker_net["qty"] -= int(body["Quantity"])
        return {"Orders": [{"OrderID": "1", "Error": None}]}
    return {}
rt._ts_api = _api

def mk(lid, fill, ts):
    return {"setup_log_id": lid, "account_id": "210VYX65", "direction": "long",
            "status": "filled", "quantity": 1, "fill_price": fill,
            "stop_order_id": f"s{lid}", "ts_placed": ts, "setup_name": "Skew Charm"}
# A older (09:00), B newer (10:00). A is stale (its contract already gone at broker).
rt._active_orders = {
    111: mk(111, 7600, "2026-06-02T13:00:00"),   # A (old, stale)
    222: mk(222, 7610, "2026-06-02T14:00:00"),   # B (new, live contract)
}

rt._active_orders[222]["closing_in_progress"] = True
rt._flatten_position(rt._active_orders[222])   # close B

sells = len(posted)
print(f"market sells placed closing B: {sells}  (broker_net now {broker_net['qty']})")
if sells == 1 and broker_net["qty"] == 0:
    print("PASS: B was closed -> no naked position.")
else:
    print("FAIL: B was SKIPPED -> NAKED POSITION (stop already cancelled, contract open).")
