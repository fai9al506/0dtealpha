# -*- coding: utf-8 -*-
"""S293 AUDIT — the per-setup day breaker, every case, before it sees real money."""
import os, sys
from datetime import date

os.environ.setdefault("REAL_TRADE_DISABLED", "true")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import real_trader as rt

import datetime as _dt
D1 = None          # None -> the code's own ET today, as in production
D2 = _dt.date(2026, 7, 31)   # a DIFFERENT day, for the roll test
res = []


def reset():
    rt._stop_streaks.clear()
    rt._stop_streak_day["d"] = None
    rt._breaker_alerted.clear()


def check(name, cond):
    res.append(cond)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")


print("=" * 74)
print("S293 DAY BREAKER AUDIT")
print("=" * 74)

# 1. the incident: two full stops in a row -> blocked
reset()
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
check("1a. one stop does NOT block", rt._day_breaker_check("Skew Charm", False) == 0)
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
check("1b. two stops in a row DOES block", rt._day_breaker_check("Skew Charm", False) == 2)

# 2. the other direction and other setups are untouched
check("2a. the LONG side is unaffected", rt._day_breaker_check("Skew Charm", True) == 0)
check("2b. a different setup is unaffected", rt._day_breaker_check("AG Short", False) == 0)

# 3. a winner clears the streak
reset()
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, +8.0, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
check("3. a WINNER between two stops clears it", rt._day_breaker_check("Skew Charm", False) == 0)

# 4. a small loss is not a stop
reset()
rt.note_trade_closed("Skew Charm", False, -3.0, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, -5.0, 14.0, today=D1)
check("4. small losses are NOT stop-outs", rt._day_breaker_check("Skew Charm", False) == 0)

# 5. slippage tolerance - a stop that fills slightly worse still counts
reset()
rt.note_trade_closed("Skew Charm", False, -14.25, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, -13.8, 14.0, today=D1)
check("5. stop fills within a quarter point still count", rt._day_breaker_check("Skew Charm", False) == 2)

# 6. new day clears everything
reset()
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
rt._roll_streak_day(D2)   # simulate the ET date changing
check("6. a NEW DAY clears the block", rt._day_breaker_check("Skew Charm", False) == 0)

# 7. fail-open on junk input
reset()
rt.note_trade_closed("Skew Charm", False, None, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, -14.0, None, today=D1)
rt.note_trade_closed(None, False, -14.0, 14.0, today=D1)
check("7. missing data never blocks and never raises", rt._day_breaker_check("Skew Charm", False) == 0)

# 8. kill switch
reset()
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
rt.note_trade_closed("Skew Charm", False, -14.0, 14.0, today=D1)
os.environ["DAY_BREAKER_ENABLED"] = "false"
check("8a. kill switch disables it", rt._day_breaker_check("Skew Charm", False) == 0)
os.environ["DAY_BREAKER_ENABLED"] = "true"
check("8b. and re-enables it", rt._day_breaker_check("Skew Charm", False) == 2)

# 9. DD Exhaustion has a different stop (12) - threshold follows the setup
reset()
rt.note_trade_closed("DD Exhaustion", True, -12.0, 12.0, today=D1)
rt.note_trade_closed("DD Exhaustion", True, -12.0, 12.0, today=D1)
check("9. uses each setup's OWN stop distance", rt._day_breaker_check("DD Exhaustion", True) == 2)

# 10. a 14-pt loss on a setup whose stop is 20 is NOT a stop-out
reset()
rt.note_trade_closed("DD Exhaustion", True, -14.0, 20.0, today=D1)
rt.note_trade_closed("DD Exhaustion", True, -14.0, 20.0, today=D1)
check("10. a partial loss on a wider stop does not count", rt._day_breaker_check("DD Exhaustion", True) == 0)

# 11. the _stamp hook fires exactly once and feeds the streak
reset()
order = {"direction": "short", "setup_name": "Skew Charm", "fill_price": 7404.0,
         "close_fill_price": 7418.0, "stop_pts": 14.0}
rt._stamp(order, "exit_fill_et")
rt._stamp(order, "exit_fill_et")   # second call must not double-count
check("11. the exit hook counts a stop-out exactly once",
      rt._stop_streaks.get(("Skew Charm", False), 0) == 1)

# 12. a target/trail win through the same hook clears the streak
order2 = {"direction": "short", "setup_name": "Skew Charm", "fill_price": 7404.0,
          "target_fill_price": 7390.0, "stop_pts": 14.0}
rt._stamp(order2, "exit_fill_et")
check("12. a win through the same hook clears it",
      rt._stop_streaks.get(("Skew Charm", False), 0) == 0)

# 13. an exit with no price recorded must not crash or count
order3 = {"direction": "short", "setup_name": "Skew Charm", "fill_price": 7404.0,
          "stop_pts": 14.0}
rt._stamp(order3, "exit_fill_et")
check("13. an exit with no price is ignored safely",
      rt._stop_streaks.get(("Skew Charm", False), 0) == 0)

print("=" * 74)
print(f"{sum(res)}/{len(res)} PASSED" + ("   — safe to ship" if all(res) else "   — DO NOT SHIP"))
print("=" * 74)
sys.exit(0 if all(res) else 1)
