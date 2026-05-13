-- S55: MES-driven trail simulation — DB schema migration.
--
-- ⚠️ DO NOT RUN DURING MARKET HOURS (9:30-16:00 ET).
-- ALTER TABLE acquires an ACCESS EXCLUSIVE lock on setup_log; if the live
-- chain-snapshot writer is in the middle of an UPDATE, this will wait or
-- block setup-outcome resolution. Safe window: after 16:10 ET on a trading
-- day, or any time on a weekend.
--
-- Adds three columns to setup_log:
--   mes_sim_outcome_pnl    — float pts, MES-walk simulated P&L
--   mes_sim_outcome_result — text (WIN/LOSS/EXPIRED)
--   mes_sim_max_fav        — float pts, MES-walk MFE
--
-- These columns are populated by app/mes_sim_backfill.py
-- (live: from _check_setup_outcomes ➜ compute_mes_sim_outcome+write).
-- (historical: _tmp_s55_backfill_runner.py).
--
-- Run order:
--   1. Apply this migration (after 16:10 ET).
--   2. Run _tmp_s55_backfill_runner.py to populate historical rows.
--   3. git push so live writes start happening on next session's new trades.
--
-- Rollback (only if needed):
--   ALTER TABLE setup_log DROP COLUMN IF EXISTS mes_sim_outcome_pnl;
--   ALTER TABLE setup_log DROP COLUMN IF EXISTS mes_sim_outcome_result;
--   ALTER TABLE setup_log DROP COLUMN IF EXISTS mes_sim_max_fav;

BEGIN;

ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS mes_sim_outcome_pnl NUMERIC;
ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS mes_sim_outcome_result TEXT;
ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS mes_sim_max_fav NUMERIC;

-- Index optional: query workload is portal dropdown filter + S55 reports;
-- both currently scan a few hundred rows max. Skip until volume justifies it.

COMMIT;

-- Verify:
--   SELECT column_name, data_type FROM information_schema.columns
--   WHERE table_name='setup_log' AND column_name LIKE 'mes_sim_%';
-- Expected 3 rows.
