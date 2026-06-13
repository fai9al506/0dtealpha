# MES Auto-Roll Fix — Spec (2026-06-12)

## Incident
2026-06-12 (~10:00 ET): 3 real shorts (lids 3961 Skew Charm, 3962/3963 ES Absorption) each
had their **protective stop REJECTED BY EXCHANGE** → bare-position failsafe market-closed
each. Day −$39.85 (contained). Real trading KILLED + June pin set.

## Root cause — the auto-roll fired ~8 days too early
`real_trader._auto_mes_symbol()` (line 29) and `eval_trader.current_mes_symbol()` (line 163)
both roll **`rollover = expiry - timedelta(days=8)`**. June MES expires **2026-06-19**, so
on **2026-06-11** they rolled to the next quarterly = **September (MESU26)** (MES has no
monthly contracts, so "next" jumps a full quarter).

But the **actual market had NOT rolled** — Sierra Chart + TradingView still showed ES ~7400
(June), and the bot's own price feed (`signal_es_price`) was June/cash ~7396. So:

| lid | signal_es_price (stop calc, JUNE feed) | fill (SEPT order) | stop placed | result |
|---|---|---|---|---|
| 3961 | 7389 | 7452 | 7408 | rejected |
| 3962 | 7398 | 7459 | 7411 | rejected |
| 3963 | 7396 | 7459 | 7409 | rejected |

September trades **~+62 over SPX** (cost-of-carry). Orders went to September (~7459) while
stops were computed off the June feed (~7396) → every short's protective **buy-stop landed
~60pt BELOW the live September market → "stop through market" → rejected.**

**The 8-day window is too aggressive** — ES/MES liquidity (per Sierra/TV) hadn't moved to
September yet; the real roll completes ~1 day before expiry, on volume, not a fixed date.

## Immediate fix (DONE)
- `REAL_TRADE_MES_SYMBOL=MESM26` (Railway env) — env pin bypasses `_auto_mes_symbol()`
  (since `_es_env != "auto"`). Orders → June MES = market = feed.
- Eval (user, on VPS): set `nt8_mes_symbol = "MES 06-26"` in `eval_trader_config.json`.
- Real trading stays DISABLED (`REAL_TRADE_DISABLED=true`) until the roll fix + a clean test.

## Durable fix — roll WHEN THE MARKET ROLLS, not a fixed date

### Option 1 (BEST) — volume-based roll (matches Sierra/TV exactly)
Roll to the next contract only when **its session volume exceeds the current contract's**.
That's how data providers pick the front month, so the order symbol always matches the
feed/chart the user sees. Never gets ahead of the market.

- New helper (real_trader.py): `_front_by_volume(api_get, cur, nxt) -> str`
  - `q = api_get(f"/marketdata/quotes/{cur},{nxt}").json()["Quotes"]`
  - `vol = {x["Symbol"]: float(x.get("Volume") or 0) for x in q}`
  - `return nxt if vol.get(nxt,0) > vol.get(cur,0) else cur`
- Resolution must move from **import-time** (line 59) to **runtime** (api_get only exists
  after `init()`): add `get_mes_symbol()` that caches the resolved symbol and **re-checks
  once per day at/after market open** (or on first trade of the day). Replace the module
  global `MES_SYMBOL` usages with `get_mes_symbol()`.
- Mirror in `eval_trader.current_mes_symbol()` (it has its own quote access via the API
  poller / NT8 — or compute the candidate pair and let NT8's continuous resolve; simplest:
  same TS volume check against the API the eval already polls).

### Option 2 (SIMPLE fallback) — roll at/just-before expiry
One-liner: `rollover = expiry - timedelta(days=1)` (roll the day before expiry) — or roll
the **day after expiry**. Much closer to the real roll; never jumps a week early. Lower
robustness than volume-based (still date-based) but trivial + low-risk. Use if Option 1
can't be done before next quarter.

### Option 3 (most robust, if feed contract is exposable) — order symbol = feed contract
Derive the traded MES contract from whatever contract the ES price feed is quoting, so order
and stop-calc can NEVER diverge (the exact failure here). Requires the feed to expose its
contract month.

## Recommendation
Ship **Option 1 (volume-based)** in both `real_trader.py` + `eval_trader.py`. If time-boxed,
ship **Option 2** now and upgrade to Option 1 before Sept roll. Either way: **never let the
order symbol roll ahead of the price feed again** (add an assertion: if
`abs(fill_price - signal_es_price) > 25`, refuse the trade + alert — a cheap guard that would
have caught this on trade #1).

## Roll-forward plan for the manual pin
- Keep `REAL_TRADE_MES_SYMBOL=MESM26` until **after 2026-06-19** (June expiry) AND Sierra/TV
  show September as front. Then either un-pin (if Option 1 deployed) or re-pin `MESU26`.

## Test before re-enabling
1. With Option-1 code: confirm `get_mes_symbol()` returns **MESM26** today (June still higher
   volume), flips to MESU26 only when Sept volume > June.
2. Paper/SIM a short: confirm the stop places at `fill_price + stop_pts` (June-space) WITHOUT
   exchange rejection.
3. Re-enable real trading (`REAL_TRADE_DISABLED=false` + direction switches) only after a
   clean stop placement.

**Deploy:** AFTER 16:10 ET (live trading code). Files: `app/real_trader.py`, `eval_trader.py`.
