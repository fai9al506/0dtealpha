"""Override lid=3200's close_fill_price to 7494.25 (FIFO truth)."""
import os, json, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
ET = ZoneInfo("America/New_York")

conn = psycopg2.connect(DB); cur = conn.cursor()

# Read current state
cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id = 3200")
state = cur.fetchone()[0]
print(f"BEFORE: close_fill_price = {state.get('close_fill_price')}")
print(f"        ghost_backfilled_at = {state.get('ghost_backfilled_at')}")
print(f"        ghost_backfilled_oid = {state.get('ghost_backfilled_oid')}")

# Archive the wrong value
state["close_fill_price_BAD_OID_MATCH"] = state.get("close_fill_price")  # 7487.75
state["close_fill_price_BAD_OID"] = state.get("ghost_backfilled_oid")  # 1267803220 = lid=3201's stop
# Set correct FIFO-derived value
state["close_fill_price"] = 7494.25
state["ghost_backfilled_at"] = datetime.now(ET).isoformat()
state["ghost_backfilled_method"] = "FIFO_manual_2026-05-22"
state["ghost_backfilled_note"] = (
    "Auto-script matched OID 1267803220 (lid=3201's stop) by closest-time-after-entry. "
    "Wrong because broker uses FIFO. TRUE FIFO match for buy@7495.75 (lid=3200): "
    "1st in-flight sell after FIFO depletes earlier buys = market sell OID 1267805956 @ 7494.25 at 19:27 UTC. "
    "P&L = (7494.25 - 7495.75) * 5 = -$7.50."
)
state["ghost_backfilled_oid"] = "1267805956"

cur.execute("UPDATE real_trade_orders SET state = %s WHERE setup_log_id = 3200",
            (json.dumps(state),))
conn.commit()
print(f"\nAFTER:  close_fill_price = {state['close_fill_price']}  (was {state['close_fill_price_BAD_OID_MATCH']})")
print(f"        method = {state['ghost_backfilled_method']}")
cur.close(); conn.close()
