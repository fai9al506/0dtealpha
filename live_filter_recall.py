# -*- coding: utf-8 -*-
"""Backfill setup_log.live_pass for ANALYSIS (so any study is just: WHERE live_pass=true).

Filter logic lives in the CANONICAL module app/live_filter.py (shared with app/darkmate.py).
This script just stamps the column. Validated: 920 trades / +3408.1 pts (all-time Feb 2026+).

USAGE:  python live_filter_recall.py        # re-stamp setup_log.live_pass
RECALL: SELECT * FROM setup_log WHERE live_pass=true
On a filter change (V17): edit app/live_filter.py, then re-run this.
"""
import os
from sqlalchemy import create_engine, text
from app.live_filter import backfill_live_pass, LIVE_VER  # canonical


if __name__ == "__main__":
    eng = create_engine(os.environ['DATABASE_URL'])
    n = backfill_live_pass(eng)
    with eng.connect() as c:
        pts = c.execute(text("SELECT COALESCE(SUM(outcome_pnl),0) FROM setup_log WHERE live_pass=true AND outcome_pnl IS NOT NULL")).scalar()
    print(f"setup_log.live_pass stamped: {n} trades (ver={LIVE_VER}), pts {float(pts):+.1f}")
    print("Recall:  SELECT * FROM setup_log WHERE live_pass=true   |  TARGET 920 / +3408.1")
