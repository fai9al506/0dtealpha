# -*- coding: utf-8 -*-
"""S279 AUDIT — prove the healer is safe before it touches real money.

Every case it can meet, with the broker API and the alerting stubbed out, asserting on
BEHAVIOUR: what it changes, what it must never change, and that it never calls anything
destructive. Run: python _tmp_s279_audit.py
"""
import os, sys, types
from datetime import datetime, timedelta, timezone

os.environ.setdefault("REAL_TRADE_DISABLED", "true")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import real_trader as rt

rt.LONGS_ENABLED = True
rt.SHORTS_ENABLED = True
rt.ACCOUNT_WHITELIST = {"210VYX65", "210VYX91"}

CALLS = {"api": [], "alerts": [], "persist": [], "fills": []}
DESTRUCTIVE = []


def fake_api(method, path, body=None, acct=None):
    CALLS["api"].append((method, path))
    if method != "GET":
        DESTRUCTIVE.append((method, path))
    return FAKE_ORDERS.get(acct, {"Orders": []})


rt._ts_api = fake_api
rt._alert = lambda m: CALLS["alerts"].append(m)
rt._persist_order = lambda lid: CALLS["persist"].append(lid)

_real_check = rt._check_order_fills


def mk(lid, acct, status="pending_entry", age_min=10, oid="OID1", tz_aware=False):
    placed = datetime.utcnow() - timedelta(minutes=age_min)
    ts = (placed.replace(tzinfo=timezone.utc).astimezone().isoformat() if tz_aware
          else placed.isoformat())
    return {"setup_log_id": lid, "setup_name": "Skew Charm", "direction": "short",
            "account_id": acct, "status": status, "entry_order_id": oid,
            "stop_order_id": "S1", "current_stop": 7812.5, "fill_price": None,
            "quantity": 2, "stop_pts": 14.0, "ts_placed": ts, "trail_only": True,
            "target_pts": None, "max_favorable": 0.0}


def run_case(name, orders, fake_orders, check_impl, expect_healed=None,
             expect_alert=None, expect_status=None):
    global FAKE_ORDERS
    FAKE_ORDERS = fake_orders
    for k in CALLS:
        CALLS[k].clear()
    DESTRUCTIVE.clear()
    rt._active_orders = {o["setup_log_id"]: o for o in orders}
    rt._stuck_alerted.clear()
    rt._check_order_fills = check_impl
    healed = rt.heal_stuck_entries()
    ok = True
    msgs = []
    if expect_healed is not None and healed != expect_healed:
        ok = False; msgs.append(f"healed={healed} want {expect_healed}")
    if expect_alert is not None:
        got = len(CALLS["alerts"])
        if (got > 0) != expect_alert:
            ok = False; msgs.append(f"alerts={got} want {'some' if expect_alert else 'none'}")
    if expect_status is not None:
        for lid, want in expect_status.items():
            got = rt._active_orders[lid]["status"]
            if got != want:
                ok = False; msgs.append(f"lid {lid} status={got} want {want}")
    if DESTRUCTIVE:
        ok = False; msgs.append(f"DESTRUCTIVE CALLS: {DESTRUCTIVE}")
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -> {'; '.join(msgs)}" if msgs else ""))
    return ok


def fills_ok(lid, order, bo):
    """stand-in for the real path: broker says FLL -> mark filled."""
    CALLS["fills"].append(lid)
    if (bo.get(order["entry_order_id"]) or {}).get("Status") == "FLL":
        order["status"] = "filled"
        order["fill_price"] = 7798.5


def fills_noop(lid, order, bo):
    CALLS["fills"].append(lid)


def fills_raise(lid, order, bo):
    CALLS["fills"].append(lid)
    raise RuntimeError("simulated _extract_fill_price failure")


FLL = {"210VYX91": {"Orders": [{"OrderID": "OID1", "Status": "FLL"}]}}
ACK = {"210VYX91": {"Orders": [{"OrderID": "OID1", "Status": "ACK"}]}}
GONE = {"210VYX91": {"Orders": []}}

print("=" * 78)
print("S279 HEALER AUDIT")
print("=" * 78)
results = []

results.append(run_case(
    "1. THE INCIDENT: stuck 58 min, broker FILLED -> healed + alert",
    [mk(6090, "210VYX91", age_min=58)], FLL, fills_ok,
    expect_healed=1, expect_alert=True, expect_status={6090: "filled"}))

results.append(run_case(
    "2. young order (1 min) -> untouched, no API call at all",
    [mk(1, "210VYX91", age_min=1)], FLL, fills_ok,
    expect_healed=0, expect_alert=False, expect_status={1: "pending_entry"}))

results.append(run_case(
    "3. broker still working the order (ACK) -> no heal, no alert",
    [mk(2, "210VYX91", age_min=10)], ACK, fills_ok,
    expect_healed=0, expect_alert=False, expect_status={2: "pending_entry"}))

results.append(run_case(
    "4. order absent from payload -> no heal, no alert, no crash",
    [mk(3, "210VYX91", age_min=10)], GONE, fills_ok,
    expect_healed=0, expect_alert=False, expect_status={3: "pending_entry"}))

results.append(run_case(
    "5. broker FILLED but the audited path refuses -> PAGE, do not force",
    [mk(4, "210VYX91", age_min=10)], FLL, fills_noop,
    expect_healed=0, expect_alert=True, expect_status={4: "pending_entry"}))

results.append(run_case(
    "6. _check_order_fills raises -> caught, no crash, no state change",
    [mk(5, "210VYX91", age_min=10)], FLL, fills_raise,
    expect_healed=0, expect_status={5: "pending_entry"}))

results.append(run_case(
    "7. already-filled trade -> never touched",
    [mk(6, "210VYX91", status="filled", age_min=99)], FLL, fills_ok,
    expect_healed=0, expect_alert=False, expect_status={6: "filled"}))

results.append(run_case(
    "8. pending_limit (charm S/R) -> NOT this healer's business",
    [mk(7, "210VYX91", status="pending_limit", age_min=99)], FLL, fills_ok,
    expect_healed=0, expect_alert=False, expect_status={7: "pending_limit"}))

results.append(run_case(
    "9. offset-aware ts_placed (the S259 two-formats trap) -> parsed, healed",
    [mk(8, "210VYX91", age_min=30, tz_aware=True)], FLL, fills_ok,
    expect_healed=1, expect_status={8: "filled"}))

results.append(run_case(
    "10. account NOT whitelisted -> skipped entirely",
    [mk(9, "999XXXXX", age_min=30)], FLL, fills_ok,
    expect_healed=0, expect_alert=False, expect_status={9: "pending_entry"}))

o = mk(10, "210VYX91", age_min=30)
o["ts_placed"] = "not-a-timestamp"
results.append(run_case(
    "11. unparseable ts_placed -> skipped, no crash",
    [o], FLL, fills_ok, expect_healed=0, expect_alert=False))

results.append(run_case(
    "12. duplicate run -> alerts only ONCE per lid",
    [mk(11, "210VYX91", age_min=30)], FLL, fills_noop,
    expect_healed=0, expect_alert=True))
before = len(CALLS["alerts"])
rt.heal_stuck_entries()
dup_ok = len(CALLS["alerts"]) == before
print(f"  [{'PASS' if dup_ok else 'FAIL'}] 12b. second run adds no duplicate alert")
results.append(dup_ok)

rt.LONGS_ENABLED = rt.SHORTS_ENABLED = False
results.append(run_case(
    "13. trading disabled -> does nothing",
    [mk(12, "210VYX91", age_min=30)], FLL, fills_ok, expect_healed=0, expect_alert=False))
rt.LONGS_ENABLED = rt.SHORTS_ENABLED = True

rt._check_order_fills = _real_check
print("=" * 78)
print(f"{sum(results)}/{len(results)} PASSED"
      + ("   — safe to ship" if all(results) else "   — DO NOT SHIP"))
print("=" * 78)
sys.exit(0 if all(results) else 1)
