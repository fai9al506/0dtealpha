# Eval Trader Performance Audit + Improvement Brief
**For: VPS Claude session (running on user's VPS, owns eval_trader)**
**From: Main session, 2026-05-27 EOD**
**Trigger: User requested parallel work to improve eval trader performance**

---

## Context you need

- **Main session just shipped** S186 (be_trigger=None crash fix in `app/main.py`, commit `592b3d7`). Affects portal outcome tracker, not eval directly — but Railway redeploy happened post-market.
- **Main session audited TSRT May 2026 performance.** Headline: pre-V16 (May 1-17) lost -$676 at 1 MES, post-V16 (May 18+) made +$820. V16 worked. ES Absorption is the worst remaining setup at -$28 over 23 post-V16 trades.
- **S183 cap=2 stacking trial** is RUNNING on REAL eval through Tue 2026-06-02. Day 1 was 2026-05-27. Revert if 3-day cumulative < -$400.
- **S185 root cause** (eval state corruption pre-Wed open) is still UNINVESTIGATED. Defenses (4-item hardening) live but source unknown.
- **TRUSTED state snapshot** is the new safety mechanism — verify it's being updated daily.

## Your scope (eval trader = E2T 25K TCP account)

The eval is SEPARATE from TSRT. Different account (E2T not TS), different filter (refined whitelist from S182 + S183 cap=2 trial flags), different scale (qty=2-3 MES, cap=2 stacking enabled). Your job is to make this eval **pass to funded** by reaching +$1,750 net profit on the $25K balance.

Last known status (from memory, may be stale — VERIFY):
- Balance ~ $25,452 (+$416 total)
- Daily P&L day-by-day must stay > -$200
- Trail-DD cushion ~ $1,463
- Need ~$1,300 more to pass

## Tasks for you (ranked by leverage)

### Task 1 — Daily P&L reconciliation report (HIGH leverage, do every EOD)
Build a daily report that VPS Claude sends to user each day at 16:15 ET via Telegram. Should contain:
- Trades fired today: count, per-setup breakdown, per-direction
- Per-trade outcomes: setup_name | entry_time | direction | fill | exit | real_pnl | reason
- Daily P&L: gross + net of commissions
- Cumulative P&L: 5-day rolling (for S183 trial revert check)
- Trail-DD cushion vs floor
- Counterfactual: "what would cap=1 have made today?" (for S183 trial comparison)
- Blocked signals: count by reason (`[refined] BLOCKED`, `BLOCKED: cluster floor`, `BLOCKED: stack cap`, `BLOCKED: drawdown floor`)
- Beacon echo: confirm TRUSTED state snapshot was updated

This is the primary trial-monitoring tool. Without it the S183 trial result is opaque.

### Task 2 — Eval vs TSRT divergence analysis (MEDIUM leverage)
Pull 2026-05-21 to 2026-05-27 trade data for both eval and TSRT. For each signal that TSRT took:
- Did eval also take it? If not, why (refined whitelist rejection, smart-ban, cap, etc.)
- If both took it: real P&L gap (eval - TSRT)
- Refined whitelist effectiveness: trades EVAL took that TSRT didn't (and outcome)

This validates whether the S182 refined whitelist is helping or just under-trading. Target: identify whether eval is leaving money on the table by being TOO selective.

### Task 3 — S185 root cause forensic (HIGH leverage if recurring risk)
The state corruption source is unknown. The CORRUPTED-* artifacts are preserved. Investigate:
1. Compare CORRUPTED state vs verified pre-corruption state — what fields differ exactly?
2. Search file system mtimes around the corruption window (17:00-18:18 ET 2026-05-26) for any process touching state files.
3. Check if wscript launcher has any quirk (env vars, working dir resolution).
4. Run the "shadow state hash logger" recommended in S185 action items — add SHA hash log to every save/load.

If root cause not found in 2 sessions, declare "monitoring only" and rely on TRUSTED defense.

### Task 4 — ES Absorption eval performance (MEDIUM leverage)
Per main session audit, ES Abs is bleeding -$28 over 23 post-V16 trades on TSRT. Check on eval:
- Is ES Abs enabled on eval (per S182 refined whitelist it should be longs-only)
- What's eval's ES Abs P&L?
- Are there paradigm-specific subsets that work?
- Recommend: keep, tighten, or kill ES Abs on eval

### Task 5 — Pre-build the cap=2 graduation criteria (LOW leverage, prep work)
S183 trial ends Tue 2026-06-02. If positive, we ship cap=2 permanently. Pre-build:
- The "promote to permanent" config diff
- The "graduate to TSRT real" companion change (if eval cap=2 wins, should TSRT cap also raise?)
- Documentation update for CLAUDE.md memory

---

## What you should NOT do without main-session approval

- Do not modify `eval_trader_config.json` filter rules during the S183 trial (only revert if criteria met)
- Do not restart eval_trader unless eval is dead AND no other option (state-corruption risk)
- Do not push to `main` branch — coordinate via this pending/ file or commit on a feature branch
- Do not flip `WATCHDOG_AUTO_RESTART_EVAL=true` (S185 defense, stays off)
- Do not run any code that uses `os.kill` or `taskkill` on the eval PID (state risk)

## Communication protocol

- Update this file with findings under a "## VPS Session Log" section at the bottom (append, don't overwrite)
- Daily diagnostic → Telegram to user (highest signal)
- Decisions needed → write a question to this file's bottom; main session checks each new session
- Urgent blockers → Telegram to user directly

## Files of interest (read-only or careful-edit)

- `eval_trader.py` — main loop, ComplianceGate, StackingTracker
- `eval_trader_config.json` — REAL eval config (S183 trial flags live)
- `eval_trader_config_sim.json` — SIM A/B control (no S183 changes)
- `eval_trader_state.json` — current state (DO NOT manually edit)
- `eval_trader_state.TRUSTED.json` — defense snapshot (update daily post-EOD)
- `eval_trader_position.json` — current position
- `eval_trader_api_state.json` — signal poll state
- `app/main.py` — Railway-side setup detector (do not modify from VPS)

## Success criteria (1-2 weeks from today)

By 2026-06-03 or earlier:
- Daily diagnostic report shipping reliably every EOD
- S183 trial verdict reached (revert / extend / graduate)
- ES Abs eval verdict (keep / tighten / kill)
- S185 root cause: found OR declared "monitoring-only via defenses"
- Net eval P&L progress: ideally +$500 to +$1,000 toward the $1,750 pass target

---

## VPS Session Log
*(Append findings here. Format: `### YYYY-MM-DD HH:MM ET — finding`)*

