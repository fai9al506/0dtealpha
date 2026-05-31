# SPEC for VPS_Claude — Eval 2-Account Split (LONG / SHORT)

**Author:** PC_Claude (main session) 2026-05-31
**Owner to implement:** VPS_Claude (eval_trader runs on VPS)
**Goal:** Split the eval into TWO direction-isolated accounts to (a) stop the
reversal-whipsaw that loses winning trades, and (b) match TSRT's capture.

---

## 0. WHY (background — read first)

The single combined eval account **reverses** on opposite signals — it can't hold
a long and a short at once. On trend/chop days it whipsaws: a short signal closes
the winning long (or flips into a losing short), so it misses the move TSRT keeps.
That's why **last Wed TSRT made +$300 but eval lost -$100**.

Fix = two accounts, each rides ONE direction, full capture, no reversal.

**Backtest evidence (PC session, 2026-05-31, eval-exact stops, SPX 2-min walk):**
- Long vs short daily P&L are anti-correlated (−0.40) and **complementary by regime**:
  - March (choppy): SHORT +2,285p, LONG +797p
  - Apr/May (grind): LONG +1,261/+1,446p, SHORT +334/+401p
  - **Both streams positive EVERY month Mar–May.**
- **Rolling-start test** (start eval on each of 63 Mar–May days, run to pass/bust):
  **54 PASS / 0 BUST** on both streams at both 1 MES and 3 MES, *with the −$200
  daily floor*. Median days-to-pass = 10 (the E2T minimum); worst = 18 (1 MES).
- **The −$200 daily floor is MANDATORY.** Without it (hard −$550 limit only),
  clustered bad days breach the $1,500 trailing DD and both accounts BUST.

---

## 1. ⚠️ FIRST: confirm account sizes (they differ!)

| Account ID | Role | Size (from ID) | Status |
|---|---|---|---|
| `falde5482tcp25d114592` | **LONG** | **25K TCP** | existing eval, bal $25,334.93, ~10 cal-days to billing |
| `falde5482tcp50d180023` | **SHORT** | **50K TCP** (`tcp50d`) | freshly purchased |

**ACTION:** Verify both sizes in the E2T dashboard before configuring. The short
account appears to be a **50K TCP — different rules**. Confirm the exact 50K TCP
numbers (do NOT trust my estimates below — verify):

| Param | 25K TCP (confirmed by user) | 50K TCP (VERIFY) |
|---|---|---|
| Start balance | $25,000 | $50,000 |
| Profit goal | $1,750 | ~$3,000 ? |
| EOD trailing DD | $1,500 | ~$2,000 ? |
| Daily loss limit | $550 | ~$1,100 ? |
| Max contracts | 3 | ~6 ? |
| Min trading days | 10 | 10 |

If the short is actually 25K, use the 25K column for both.

---

## 2. Infra: run TWO eval_trader instances

Each instance = its own config + state files + NT8 account. Same Railway signal
source (`/api/eval/signals`); each filters to its direction via `allowed_directions`.

Create (copy from `eval_trader_config.json`):
- `eval_trader_config_long.json`  → `nt8_account_id: "falde5482tcp25d114592"`
- `eval_trader_config_short.json` → `nt8_account_id: "falde5482tcp50d180023"`

Each instance needs distinct state files (so they don't clobber each other) — e.g.
launch with a per-instance suffix, or run from two folders. State files to isolate:
`eval_trader_state*.json`, `eval_trader_api_state*.json`, `eval_trader_position*.json`.
(Confirm how the current launcher names them; the `_sierra`/`_real` suffixes suggest
multi-instance is already supported — reuse that pattern.)

**Each instance must have its own `_seen_signals` / api_state** so both can emit the
same underlying signal independently (long instance takes the long leg, short the short).

---

## 3. Config per account

### LONG account (`eval_trader_config_long.json`, 25K)
```jsonc
"nt8_account_id": "falde5482tcp25d114592",
"qty": 1,                          // START 1 MES (see §5 for billing-speed option)
"daily_loss_floor": -200,          // MANDATORY protective floor (NOT the $550 hard limit)
"e2t_starting_balance": 25000,
"e2t_eod_trailing_drawdown": 1500,
"e2t_daily_loss_limit": 550,
"e2t_daily_pnl_cap": 525,          // keep (optional self-cap)
"no_new_trades_after_et": "15:30",
"flatten_time_et": "15:50",
"setup_rules": {
   // LONG legs of the TSRT/V16 whitelist, allowed_directions ["long"] on each:
   "ES Absorption":   {"enabled": true, "stop": 8,  "allowed_directions": ["long"]},
   "DD Exhaustion":   {"enabled": true, "stop": 12, "qty": 2, "allowed_directions": ["long"], "allowed_paradigms": ["BOFA-PURE"]},
   "Skew Charm":      {"enabled": true, "stop": 14, "allowed_directions": ["long"]},
   "VIX Divergence":  {"enabled": true, "stop": 8,  "allowed_directions": ["long"]},   // GEX-paradigm gate already on Railway
   "Vanna Pivot Bounce": {"enabled": true, "stop": 8, "target": 10, "allowed_directions": ["long"]},
   "AG Short":        {"enabled": false},   // shorts OFF on the long account
   // all others false
}
```

### SHORT account (`eval_trader_config_short.json`, 50K — adjust to verified 50K rules)
```jsonc
"nt8_account_id": "falde5482tcp50d180023",
"qty": 1,                          // START 1 MES
"daily_loss_floor": -300,          // scale to 50K (still very safe vs ~$2000 DD); confirm
"e2t_starting_balance": 50000,
"e2t_eod_trailing_drawdown": 2000, // VERIFY
"e2t_daily_loss_limit": 1100,      // VERIFY
"setup_rules": {
   // SHORT legs:
   "AG Short":        {"enabled": true, "stop": 12, "allowed_directions": ["short"]},
   "Skew Charm":      {"enabled": true, "stop": 14, "allowed_directions": ["short"]},
   "DD Exhaustion":   {"enabled": true, "stop": 12, "allowed_directions": ["short"]},   // (DD shorts: confirm paradigm gate)
   "Vanna Pivot Bounce": {"enabled": true, "stop": 8, "target": 10, "allowed_directions": ["short"]},
   "ES Absorption":   {"enabled": false},   // longs OFF on the short account
   // all others false
}
```

**Trail params** (`_TRAIL_PARAMS` in eval_trader.py) stay as-is — they're already
direction-agnostic and per-setup. No change needed.

**Reversal logic:** with each account single-direction, opposite signals simply
won't appear (filtered out), so the reversal path never fires. Good. But confirm
`PositionTracker` won't choke if it only ever sees one direction (it shouldn't).

---

## 4. Whitelist note

The backtest that showed 0 busts used the **full TSRT/V16 whitelist split by
direction** (above). Current eval only enables AG-short + ES-Abs-long + DD-long.
Enabling the SC/VIX/VPB legs is what produces the ~50p/day stream pace and the
~10-day pass timeline. If you keep the narrow current set, the timeline stretches.
Recommend the full split (matches TSRT). Monitor first week.

---

## 5. Timeline reality + speed-vs-billing (IMPORTANT — read)

- **Min 10 TRADING days is a hard E2T gate.** You cannot pass before 10 trading days
  no matter the profit or size.
- **LONG (25K, billing in ~10 CALENDAR days ≈ ~7 trading days):** Check
  `eval_trader_state` for trading-days-elapsed. If it already has ≥3–5 trading days
  logged, +5–7 more lands near billing — TIGHT but possible. It only needs **+$1,415**
  more (already +$335). To make the billing window, consider running the LONG at
  **2 MES** (it has a full $1,500 DD cushion + proven stream; −$200 floor still caps
  risk). 3 MES is faster but the −$200 floor = ~one-losing-trade-and-done (fragile).
  **If it has <5 trading days logged, it likely CANNOT hit min-10 before billing** —
  decide whether to renew one cycle.
- **SHORT (50K, fresh = 0 trading days):** **Cannot pass within 10 calendar days** —
  needs 10 *trading* days (~2 calendar weeks) AND a higher (~$3,000) goal. It's freshly
  purchased with a full billing cycle, so no rush. Expect ~2.5–3.5 weeks.

**Honest answer to "pass both in 10 days":** LONG = possible-but-tight (depends on
trading-days-already-logged; size up to 2 MES to help). SHORT = no (min-10-trading-days
+ 50K's bigger goal). Plan for SHORT to finish ~1–2 weeks after LONG.

---

## 6. Verify / monitor
- After config: confirm each instance connects to the RIGHT NT8 account and only
  fires its direction (watch first few signals in logs/Telegram).
- Daily: confirm the −$200 (long) / −$300 (short) floor halts trading on bad days.
- Confirm trading-days counter increments on each account (for the min-10 gate).
- Report EOD per-account P&L + trading-days-elapsed so we track pass progress.

## 7. Safety / revert
- Keep the protective daily floor ON both accounts at all times (it's the only thing
  preventing a trailing-DD bust per the backtest).
- If either account has 2 consecutive days < −$200 (long) / < −$300 (short), pause and
  ping PC session — the regime may have shifted.
- Caveats on the backtest: SPX 2-min walk (real MES fills ~2pt different, S55);
  3-MES capture is optimistic (model doesn't simulate first-loss stop-out).

— end spec —
