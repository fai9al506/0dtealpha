# Plan: Eval Trader — Poll Railway API Instead of Telegram

## Status: READY TO IMPLEMENT (saved Feb 23, 2026)

## Context
The eval_trader (`eval_trader.py`) runs locally on the work PC and needs to receive setup signals from the Railway-hosted 0DTE Alpha service. Currently it polls Telegram, but **Telegram API is blocked on the work PC network** (SSL handshake reset on every attempt). General HTTPS works fine (Google, httpbin). Solution: add a simple API endpoint to Railway that serves setup signals as JSON, and modify eval_trader to poll that instead.

## Changes

### 1. Add `/api/eval/signals` endpoint to `app/main.py`

**Auth:** Use a simple API key via `EVAL_API_KEY` env var (checked in Authorization header). Add this path to auth middleware with custom key check — no session cookie needed.

**Endpoint logic:**
- Query param: `since_id=N` (return setup_log entries with `id > N`)
- Returns two arrays:
  - `signals` — new setup entries (with all fields eval_trader needs)
  - `outcomes` — entries where `outcome_result IS NOT NULL` and `id > since_id`
- Limit to today's entries only (no stale signals from past days)
- Include `stop_level` and `target_level` computed from the same logic as outcome tracking

**Response shape:**
```json
{
  "signals": [
    {
      "id": 123,
      "ts": "2026-02-23T10:30:45-05:00",
      "setup_name": "AG Short",
      "direction": "short",
      "grade": "A",
      "score": 78,
      "spot": 6150.25,
      "target": 6125.0,
      "lis": 6160.0,
      "paradigm": "GEX Pure",
      "bofa_stop_level": null,
      "bofa_target_level": null,
      "abs_es_price": null,
      "stop_level": 6170.25,
      "target_level": 6125.0
    }
  ],
  "outcomes": [
    {
      "id": 120,
      "setup_name": "ES Absorption",
      "outcome_result": "WIN",
      "outcome_pnl": 10.5
    }
  ]
}
```

**Key fields for eval_trader by setup type:**
- GEX Long/AG Short: `spot`, `target` (Volland target for msg_target_pts)
- BofA Scalp: `spot`, `bofa_stop_level`, `bofa_target_level`
- ES Absorption: `abs_es_price` (entry is ES price, not SPX)
- DD Exhaustion: `spot` (trail-only, no target)
- Paradigm Reversal: `spot`

### 2. Modify `eval_trader.py` — Replace TelegramPoller with APIPoller

**New class `APIPoller`:**
- Polls `https://<railway-url>/api/eval/signals?since_id=N` every 2 seconds
- Tracks `last_id` — starts at 0, updates to max(id) from response
- Persists `last_id` to state file for crash recovery
- Converts API response directly to signal/outcome dicts (no regex parsing needed)

**Changes to `main()` loop:**
- Replace `TelegramPoller` with `APIPoller`
- Signal path: API returns structured JSON → directly map to signal dict (no `parse_signal()` needed)
- Outcome path: API returns outcome entries → map to outcome dict (no `parse_outcome()` needed)
- `parse_signal()` and `parse_outcome()` remain for backward compatibility but aren't called in API mode

**New config fields:**
- `railway_api_url`: Railway service URL (e.g. `https://0dtealpha-production.up.railway.app`)
- `eval_api_key`: API key (matches `EVAL_API_KEY` env var on Railway)
- `signal_source`: `"api"` (default) or `"telegram"` (legacy fallback)

### 3. Set `EVAL_API_KEY` env var on Railway

Generate a random key, set it on the `0dtealpha` service, and put the same key in `eval_trader_config.json`.

## Files Modified

| File | Change |
|------|--------|
| `app/main.py` | Add `/api/eval/signals` endpoint (~40 lines), add `EVAL_API_KEY` check to auth middleware |
| `eval_trader.py` | Add `APIPoller` class (~40 lines), update `main()` to use it, add config fields |
| `eval_trader_config.json` | Add `railway_api_url`, `eval_api_key`, `signal_source` fields |

## Verification

1. **Local dry-run:** Start eval_trader, confirm it connects to Railway URL and gets `{"signals":[], "outcomes":[]}`
2. **Auth test:** Hit the endpoint without API key → 401. With key → 200.
3. **Live test at market open:** When a setup fires, confirm eval_trader receives it via API and places NT8 OIF order.
4. **Outcome test:** When Railway resolves a trade (WIN/LOSS), confirm eval_trader receives it and closes position.

## DB Impact
None — reads existing `setup_log` table only.
