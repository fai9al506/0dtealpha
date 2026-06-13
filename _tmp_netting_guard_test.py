"""Prove the S198 netting guard would have prevented the 2026-06-02 incident.

Scenario: 3 concurrent longs on 210VYX65 (net 3 at broker). S131 closes 3465
(Market Sell #1 -> net 2). A redundant outcome-resolution cycle re-fires
close_trade(3465) -> 2nd _flatten_position. OLD code would POST a 2nd Market
Sell (eating 3468 via FIFO). NEW guard must SKIP it.
"""
import app.real_trader as rt

# --- mock broker surface ---
posted_market_sells = []
broker_net = {"qty": 3}  # mutable: net contracts at broker

rt._validate_account_direction = lambda acct, is_long: True
rt._get_broker_position = lambda acct, expect_position=False: (
    {"qty": broker_net["qty"], "long_short": "Long", "symbol": "MESM26"}
    if broker_net["qty"] > 0 else None)
rt._get_order_fill_price = lambda oid, acct: None  # stops not filled (cancelled)
rt._backfill_ghost_fill = lambda o: ("close_fill_price", 7608.0)
rt._alert = lambda m: None
rt._day_line = lambda acct: ""
rt._persist_order = lambda lid: None

def _fake_ts_api(method, path, body, acct):
    if method == "POST" and "orderexecution/orders" in path:
        leg = {"BuyOrSell": body["TradeAction"], "QuantityOrdered": body["Quantity"]}
        posted_market_sells.append(body)
        broker_net["qty"] -= int(body["Quantity"])  # market sell reduces net
        return {"Orders": [{"OrderID": "999", "Error": None}]}
    return {}
rt._ts_api = _fake_ts_api

# --- seed 3 concurrent longs ---
def mk(lid, fill, ts):
    return {"setup_log_id": lid, "account_id": "210VYX65", "direction": "long",
            "status": "filled", "quantity": 1, "fill_price": fill,
            "stop_order_id": f"stop{lid}", "ts_placed": ts, "setup_name": "Skew Charm"}
rt._active_orders = {
    3465: mk(3465, 7601.5, "2026-06-02T13:45:06"),
    3468: mk(3468, 7605.5, "2026-06-02T14:07:35"),
    3469: mk(3469, 7609.25, "2026-06-02T14:16:37"),
}

print("Initial broker net:", broker_net["qty"])

# Sell #1: S131 closes 3465 (legit first close)
rt._active_orders[3465]["closing_in_progress"] = True
rt._flatten_position(rt._active_orders[3465])
rt._active_orders[3465]["status"] = "closed"
rt._active_orders[3465].pop("closing_in_progress", None)
print(f"After S131 close(3465): broker_net={broker_net['qty']} market_sells={len(posted_market_sells)}")

# Sell #2: redundant outcome-resolution re-fires close_trade(3465) -> flatten again
rt._active_orders[3465]["closing_in_progress"] = True
rt._flatten_position(rt._active_orders[3465])
print(f"After redundant close(3465): broker_net={broker_net['qty']} market_sells={len(posted_market_sells)}")

# --- assertions ---
assert len(posted_market_sells) == 1, f"GUARD FAILED: {len(posted_market_sells)} market sells (expected 1)"
assert broker_net["qty"] == 2, f"GUARD FAILED: net={broker_net['qty']} (expected 2 — 3468 & 3469 intact)"
assert rt._active_orders[3468]["status"] == "filled", "3468 wrongly disturbed"
assert rt._active_orders[3469]["status"] == "filled", "3469 wrongly disturbed"
print("\n✅ PASS: redundant close was SKIPPED by netting guard.")
print("   3468 (DD) and 3469 NOT eaten. Net stays 2. No ghost, no spam.")
print("   OLD behaviour would have posted 2 market sells -> net 1 -> 3468 ghost.")
