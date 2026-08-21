"""S233 gate: prove the decomposed rule filter == app.live_filter.passes_v16 with off=empty."""
import os
os.environ["VPB_REAL_TRADE_ENABLED"] = "true"
from sqlalchemy import create_engine, text
from app.live_filter import passes_v16, load_gaps, COLS
from _tmp_s233_rules import passes, WHITELIST

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps = load_gaps(c)
    rows = c.execute(text(f"SELECT {COLS} FROM setup_log WHERE ts>='2026-02-01' ORDER BY ts")).mappings().all()

diff = 0; n = 0; both = 0
for r in rows:
    if r["setup_name"] not in WHITELIST:
        continue
    n += 1
    a = passes_v16(r, gaps)
    b, why = passes(r, gaps)
    if a:
        both += 1
    if a != b:
        diff += 1
        if diff <= 12:
            print(f"  DIFF id={r['id']} {r['setup_name']} {r['direction']} live={a} rules={b} why={why} "
                  f"grade={r['grade']} para={r['paradigm']} align={r['greek_alignment']} ts={r['ts']}")
print(f"\nchecked {n} rows (whitelist setups, GEX Long excluded)   live_pass_true={both}   DIFFS={diff}")
print("PARITY OK" if diff == 0 else "*** PARITY FAILED ***")
