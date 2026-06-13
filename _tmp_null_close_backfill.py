"""S179.b — Backfill close_fill_price for historical lids that were never repaired.

The Sunday FIFO backfill skipped 8 lids because their days had count-mismatch
(closed lids != broker FLL exits). This script does lid-by-lid recovery by
scanning broker /historicalorders for the right exit fill:

  - Pull all FLL orders for the lid's account on the lid's date
  - Filter to exit-side (Sell on longs acct, Buy on shorts acct), MES symbol
  - Sort by ClosedDateTime
  - Find the first FLL order that satisfies:
      * direction = exit-of-entry (Sell for long, Buy for short)
      * ClosedDateTime > entry time
      * If lid has stop_order_id stored → match exactly
      * Else: use closest-time-after-entry
  - Write close_fill_price + audit fields

Affected lids per 2026-05-27 audit:
  3112 (2026-05-21) SC long
  3110 (2026-05-21) DD long
  2937 (2026-05-18) SC long
  2911 (2026-05-18) VIX Div long
  2871 (2026-05-15) SC long
  2830 (2026-05-15) SC long
  2728 (2026-05-12) SC long
  2696 (2026-05-12) SC long
"""
import os, json, requests, psycopg2, socket
import urllib3.util.connection as urllib3_cn
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"
DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

TARGET_LIDS = [3112, 3110, 2937, 2911, 2871, 2830, 2728, 2696]


def refresh_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    }, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def ts_get(p, t):
    r = requests.get(TS_BASE + p, headers={"Authorization": f"Bearer {t}"}, timeout=60)
    return r.json() if r.text else None


def parse_dt(s):
    if not s: return None
    s = s.replace("Z", "+00:00")
    try: return datetime.fromisoformat(s)
    except Exception: return None


def main():
    token = refresh_token()
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Pull lid state
    cur.execute("""
        SELECT setup_log_id, state, created_at FROM real_trade_orders
        WHERE setup_log_id = ANY(%s)
        ORDER BY created_at
    """, (TARGET_LIDS,))
    rows = cur.fetchall()
    print(f"Found {len(rows)} target lids\n")

    # Group by (acct, date) to minimize TS API calls
    by_key = {}
    for sid, state, created in rows:
        if isinstance(state, str):
            state = json.loads(state)
        acct = state.get("account_id")
        d = created.date()
        key = (acct, d)
        by_key.setdefault(key, []).append((sid, state, created))

    # Pull TS historicalorders once per (acct, date)
    orders_cache = {}
    for (acct, d), lids in by_key.items():
        since = (d - timedelta(days=1)).strftime("%m-%d-%Y")
        data = ts_get(
            f"/brokerage/accounts/{acct}/historicalorders?since={since}&pageSize=600",
            token,
        )
        ods = (data or {}).get("Orders") or []
        # Filter to today + FLL only
        ods_today = [o for o in ods if (o.get("ClosedDateTime") or "").startswith(d.isoformat())]
        orders_cache[(acct, d)] = ods_today
        print(f"[{acct} {d}] pulled {len(ods_today)} FLL orders for the day")

    # Per-lid backfill
    for sid, state, created in rows:
        if isinstance(state, str):
            state = json.loads(state)
        acct = state.get("account_id")
        direction = state.get("direction")
        is_long = direction in ("long", "bullish")
        exit_side = "Sell" if is_long else "Buy"
        entry_oid = str(state.get("entry_order_id") or "")
        stop_oid = str(state.get("stop_order_id") or "")
        target_oid = str(state.get("target_order_id") or "")
        fill_price = state.get("fill_price")
        d = created.date()

        if state.get("close_fill_price") is not None:
            print(f"\nlid={sid}: already has close_fill_price={state['close_fill_price']}, skip")
            continue

        ods = orders_cache.get((acct, d), [])
        entry_order = next((o for o in ods if str(o.get("OrderID")) == entry_oid), None)
        entry_time = None
        if entry_order:
            entry_time = parse_dt(entry_order.get("OpenedDateTime")) or parse_dt(entry_order.get("ClosedDateTime"))

        print(f"\n=== lid={sid} acct={acct} {state.get('setup_name')} {direction} ===")
        print(f"  entry oid={entry_oid} fill={fill_price} time={entry_time}")
        print(f"  stop oid={stop_oid}  target oid={target_oid}")

        # Strategy 1: if stop_order_id stored AND it's FLL → use it
        if stop_oid:
            stop_order = next((o for o in ods if str(o.get("OrderID")) == stop_oid and o.get("Status") == "FLL"), None)
            if stop_order:
                legs = stop_order.get("Legs") or [{}]
                fp = legs[0].get("ExecutionPrice") or stop_order.get("FilledPrice")
                if fp:
                    fp = float(fp)
                    print(f"  ✓ STOP fired: oid={stop_oid} price={fp}")
                    _persist(cur, sid, state, fp, str(stop_oid), "stop_filled_recovered")
                    conn.commit()
                    continue

        # Strategy 2: target_order_id same
        if target_oid:
            target_order = next((o for o in ods if str(o.get("OrderID")) == target_oid and o.get("Status") == "FLL"), None)
            if target_order:
                legs = target_order.get("Legs") or [{}]
                fp = legs[0].get("ExecutionPrice") or target_order.get("FilledPrice")
                if fp:
                    fp = float(fp)
                    print(f"  ✓ TARGET filled: oid={target_oid} price={fp}")
                    _persist(cur, sid, state, fp, str(target_oid), "target_filled_recovered")
                    conn.commit()
                    continue

        # Strategy 3: scan all FLL exit-side orders post-entry
        candidates = []
        for o in ods:
            if o.get("Status") != "FLL":
                continue
            if str(o.get("OrderID")) in (entry_oid, stop_oid, target_oid):
                # Already handled or is the entry itself
                if str(o.get("OrderID")) == entry_oid:
                    continue  # skip entry
            legs = o.get("Legs") or [{}]
            leg = legs[0]
            if leg.get("BuyOrSell") != exit_side:
                continue
            if "MES" not in str(leg.get("Symbol", "")):
                continue
            ot = parse_dt(o.get("ClosedDateTime")) or parse_dt(o.get("OpenedDateTime"))
            if entry_time and ot and ot < entry_time:
                continue
            fp = leg.get("ExecutionPrice") or o.get("FilledPrice")
            if fp is None or float(fp) <= 0:
                continue
            candidates.append({
                "oid": str(o.get("OrderID")),
                "fp": float(fp),
                "ot": ot,
                "type": o.get("OrderType"),
            })

        candidates.sort(key=lambda x: x["ot"] or datetime.max.replace(tzinfo=timezone.utc))

        if not candidates:
            print(f"  ✗ no exit candidates found — manual review needed")
            continue

        # Pick closest-time-after-entry
        c = candidates[0]
        # Sanity: distance from entry
        diff = abs(c["fp"] - float(fill_price)) if fill_price else 0
        if diff > 50:
            print(f"  ⚠️ candidate too far from entry (|diff|={diff:.2f}) — skipping, manual review")
            for cd in candidates[:5]:
                print(f"      oid={cd['oid']} fp={cd['fp']} type={cd['type']} time={cd['ot']}")
            continue

        print(f"  ✓ FALLBACK FIFO: oid={c['oid']} price={c['fp']} type={c['type']} time={c['ot']}")
        if len(candidates) > 1:
            print(f"      (skipped {len(candidates)-1} later candidates)")
        _persist(cur, sid, state, c["fp"], c["oid"], "ghost_backfill_v3")
        conn.commit()

    cur.close()
    conn.close()
    print("\n=== Backfill complete ===")


def _persist(cur, sid, state, fp, oid, method):
    if "close_fill_price_pre_fifo_reconcile" not in state:
        state["close_fill_price_pre_fifo_reconcile"] = None
    state["close_fill_price"] = fp
    state["fifo_close_oid"] = oid
    state["fifo_reconciled_at"] = datetime.now(ET).isoformat()
    state["fifo_backfill_method"] = method
    cur.execute("UPDATE real_trade_orders SET state = %s WHERE setup_log_id = %s",
                (json.dumps(state), sid))


if __name__ == "__main__":
    main()
