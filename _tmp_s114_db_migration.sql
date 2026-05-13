-- S114: real_trader silent skip telemetry
-- Adds skip_reason column to setup_log so the dashboard / audit scripts
-- can attribute every V14-pass signal that DID NOT result in a real trade.
--
-- Defensive design: the application code (app/real_trader.py and app/main.py)
-- wraps every write to this column in try/except so a missing column does NOT
-- break the trade flow. Run this migration any time post-deploy.
--
-- Safe to re-run.

ALTER TABLE setup_log
    ADD COLUMN IF NOT EXISTS real_trade_skip_reason TEXT;

-- Helpful index for the S108/S113-style audits that filter by skip_reason.
CREATE INDEX IF NOT EXISTS idx_setup_log_real_trade_skip_reason
    ON setup_log (real_trade_skip_reason)
    WHERE real_trade_skip_reason IS NOT NULL;
