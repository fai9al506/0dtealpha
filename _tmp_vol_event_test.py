"""S209 vol_event_alert end-to-end test against real DB data (Telegram intercepted)."""
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

NY = ZoneInfo("America/New_York")
url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
engine = create_engine(url, pool_pre_ping=True)

from app import vol_event_alert as va

sent = []
va._send = lambda msg: (sent.append(msg), print(f"--- CAPTURED ALERT ---\n{msg}\n"))[1] or True

va.init(engine)

# clean any test keys from previous runs so the test is deterministic
with engine.begin() as conn:
    conn.execute(text("""
        DELETE FROM vol_event_alerts
        WHERE key IN ('intraday-2026-06-05','intraday-2026-03-09','confirmed-2026-03-06')
    """))

# 1. Friday Jun 5 16:09 ET — corr 3.53, down day -> INTRADAY alert expected
va.check(_test_now=datetime(2026, 6, 5, 16, 9, tzinfo=NY),
         _test_snap_before="2026-06-05 20:10:00+00")
assert len(sent) == 1 and "VOL EVENT LIKELY" in sent[0], f"T1 FAIL: {len(sent)} sent"
print("T1 PASS: intraday alert fired for Jun 5\n")

# 2. same again -> dedup, no new alert
va.check(_test_now=datetime(2026, 6, 5, 16, 14, tzinfo=NY),
         _test_snap_before="2026-06-05 20:10:00+00")
assert len(sent) == 1, f"T2 FAIL: dedup broken, {len(sent)} sent"
print("T2 PASS: dedup works\n")

# 3. Mon Mar 9 16:05 ET snapshot (has vixEvents for Mar 6) -> CONFIRMED alert
va.check(_test_now=datetime(2026, 3, 9, 16, 5, tzinfo=NY),
         _test_snap_before="2026-03-09 20:11:00+00")
confirmed = [m for m in sent if "VOL EVENT CONFIRMED" in m]
assert confirmed, "T3 FAIL: no confirmed alert"
assert "6,822" in confirmed[0], f"T3 FAIL: target missing: {confirmed[0]}"
print("T3 PASS: confirmed alert fired for Mar 6 event\n")

# 4. weekend guard
n = len(sent)
va.check(_test_now=datetime(2026, 6, 6, 12, 0, tzinfo=NY))  # Saturday
assert len(sent) == n, "T4 FAIL: weekend guard broken"
print("T4 PASS: weekend guard\n")

# 5. stale-snapshot guard: Monday 10:00 but latest snapshot = Friday -> no intraday
va.check(_test_now=datetime(2026, 6, 8, 10, 0, tzinfo=NY),
         _test_snap_before="2026-06-05 20:10:00+00")
intraday_after = [m for m in sent if "VOL EVENT LIKELY" in m]
assert len(intraday_after) == 1, f"T5 FAIL: stale snapshot fired intraday alert"
print("T5 PASS: stale-snapshot guard\n")

# cleanup test dedup keys (so live behavior is untouched; past dates anyway)
with engine.begin() as conn:
    conn.execute(text("""
        DELETE FROM vol_event_alerts
        WHERE key IN ('intraday-2026-06-05','intraday-2026-03-09','confirmed-2026-03-06')
    """))

print(f"ALL 5 TESTS PASS ({len(sent)} alerts captured, 0 real Telegram sends)")
