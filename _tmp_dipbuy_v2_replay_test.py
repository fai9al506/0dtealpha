"""Equivalence test: replay cached 30s paths through app.dipbuy_detector state machine
(DB stubbed) and check v2 fires match the backtest engine on identical input."""
import pickle, sys
from datetime import time as dtime

sys.path.insert(0, ".")
from app import dipbuy_detector as db

with open("_tmp_dipbuy_paths.pkl", "rb") as f:
    paths = pickle.load(f)

fired = []
def fake_fire(et, entry, vix, v2=False):
    db._state["v2_fired" if v2 else "fired"] = True
    fired.append(("v2" if v2 else "v1", et, entry))

db._fire = fake_fire
db._engine = object()          # truthy; never used since _fire is stubbed

# patch module-internal reference: _detect looks up _fire globally in module dict
import types
db._detect.__globals__["_fire"] = fake_fire

test_days = [d for d in sorted(paths) if d.isoformat() >= "2026-05-26"]
for d in test_days:
    db._reset_day(d)
    db._state["date"] = d
    for et, sp in paths[d]:
        db._track_outcomes(et, sp)   # no-op, no open trades
        db._detect(et, sp, None)

print("module replay fires:")
for v, et, px in fired:
    print(f"  {v}  {et}  @{px:.2f}")

# reference: backtest engine v2 (d8 c3 p8) on same days
def bt_v2(path):
    sess_high = -1e9; in_dip = False; lo = 1e9; hold = 0
    for i, (et, sp) in enumerate(path):
        if et.time() > dtime(11, 30): return None
        sess_high = max(sess_high, sp)
        if not in_dip:
            if sp <= sess_high - 8.0:
                in_dip = True; lo = sp; hold = 0
        else:
            lo = min(lo, sp)
            if sp >= lo + 3.0:
                hold += 1
                if hold > 8:
                    return (et, sp)
            else:
                hold = 0
    return None

print("\nbacktest engine v2 fires (reference):")
for d in test_days:
    r = bt_v2(paths[d])
    if r: print(f"  v2  {r[0]}  @{r[1]:.2f}")
