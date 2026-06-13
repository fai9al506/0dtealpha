"""S217 trail-fix behavior test: verify update_stop() skips the MES wrong-side
market-exit ONLY in S131 internal mode, and preserves original behavior otherwise."""
import sys
sys.stdout.reconfigure(encoding='utf-8'); sys.path.insert(0,'.')
from app import real_trader as rt

calls={"close":0}
def fake_close(sid, reason): calls["close"]+=1; print(f"   close_trade({sid},{reason}) CALLED")
def setup(initial_realign_done, spx_enabled):
    calls["close"]=0
    rt._active_orders = {1: {"status":"filled","current_stop":7324.0,"stop_order_id":"OID",
        "account_id":"ACCT","setup_name":"Skew Charm","direction":"short",
        "initial_realign_done":initial_realign_done,"fill_price":7310.0,"quantity":1}}
    rt._validate_account_direction = lambda *a,**k: True
    rt._get_current_mes_price = lambda: 7320.0   # SHORT: market 7320, new_stop 7304 below = wrong-side
    rt._spx_exit_enabled = lambda: spx_enabled
    rt.close_trade = fake_close
    rt._alert = lambda *a,**k: None
    rt._persist_order = lambda *a,**k: None

print("Test A: S131 mode (initial_realign_done=True, spx_exit ON) + wrong-side short")
setup(True, True); rt.update_stop(1, 7304.0)
A = (calls["close"]==0)
print(f"   close_trade called: {calls['close']}  -> {'PASS (suppressed)' if A else 'FAIL'}")

print("Test B: first realign (initial_realign_done=False, spx ON) + wrong-side")
setup(False, True); rt.update_stop(1, 7304.0)
B = (calls["close"]==1)
print(f"   close_trade called: {calls['close']}  -> {'PASS (original behavior kept)' if B else 'FAIL'}")

print("Test C: S131 disabled (spx_exit OFF) + wrong-side")
setup(True, False); rt.update_stop(1, 7304.0)
C = (calls["close"]==1)
print(f"   close_trade called: {calls['close']}  -> {'PASS (original behavior kept)' if C else 'FAIL'}")

print("\nRESULT:", "ALL PASS" if (A and B and C) else "FAIL")
sys.exit(0 if (A and B and C) else 1)
