import os, sys, pandas as pd
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"]); conn=eng.connect()
gaps=lf.load_gaps(conn)
rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
    f"WHERE (ts AT TIME ZONE 'America/New_York')::date >= '2026-06-16' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
sbpass=[r for r in base if lf.passes_v16_sb(r,gaps)]
blocked=[r for r in base if not lf.passes_v16_sb(r,gaps)]
bp=sum(float(r['outcome_pnl']) for r in base); sp=sum(float(r['outcome_pnl']) for r in sbpass); blk=sum(float(r['outcome_pnl']) for r in blocked)
print(f"Gate-active era (Jun-16+), V16-base = {len(base)} trades, {bp:+.1f} pts")
print(f"  SB gate TOOK:    {len(sbpass)} trades, {sp:+.1f} pts")
print(f"  SB gate BLOCKED: {len(blocked)} trades, {blk:+.1f} pts  <- these were filtered OUT (gate working)")
print(f"  => gate removed {len(blocked)} trades worth {blk:+.1f} pts from the book")
