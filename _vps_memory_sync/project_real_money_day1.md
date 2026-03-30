---
name: Real Money Day 1 Results
description: First live trading day Mar 24 2026 — 7 trades, -$213.60, bugs fixed, system validated
type: project
---

## Day 1 Real Money — Mar 24, 2026

**Config:** SC only, V11 A+/A/B, 1 MES, direction-routed (210VYX65 longs, 210VYX91 shorts)

**Result:** 7 trades, 2W/5L, -$213.60 total (including commissions)

**Why:** Mar 24 was the 2nd worst SC day of March (portal: 3W/5L, -32.6 pts). Only 6 of 16 March days were negative.

**How to apply:** This is within expected variance for a 78% WR system. Don't adjust strategy based on one day.

### Real Trades
| # | LogID | Dir | Fill | Exit | P&L |
|---|-------|-----|------|------|-----|
| 1 | 1141 | SHORT | 6581.0 | SL | -$75 |
| 2 | 1150 | SHORT | 6599.5 | SL | ~-$64 |
| 3 | 1169 | LONG | 6641.75 | SL | -$74 |
| 4 | 1172 | LONG | 6625.25 | SL | -$73 |
| 5 | 1176 | LONG | 6609.75 | TP | +$51 |
| 6 | 1185 | LONG | 6614.0 | TP | +$54 |
| 7 | 1194 | LONG | 6624.0 | closed | ? |

### Bugs Found & Fixed (all committed to Git)
1. **Cancel-verify** — stop/target counterpart cancel was fire-and-forget. Now verifies + 3 retries + Telegram alert
2. **Account check 404** — live TS API returns 404 on `/accounts/{id}`, switched to `/balances`
3. **Format string None crash** — `result.get("key", 0)` returns None when key exists with None value
4. **Grade variable undefined** — `grade` not defined in `_check_setup_outcomes`, cost us SC #1179 WIN (+$43.50)
5. **Missing SQL bind params** — setups with missing fields couldn't log → blocked real trades
6. **Cooldown date serialization** — `json.dumps(default=str)`
7. **Service reverting on restart** — local `railway up` deploys lost on restart. Fixed by committing to Git.

### Key Lesson
**Never deploy during market hours.** 5+ deploys caused Rithmic disconnects, missed signals, position tracking gaps. All bugs were fixable pre-market with a dry run.
