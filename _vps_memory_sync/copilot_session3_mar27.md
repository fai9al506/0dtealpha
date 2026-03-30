---
name: Copilot Session 3 — Mar 27 Post-Market Analysis
description: Full day review of Mar 27 2026 trend-down day. 53 signals, V12 filter analysis, Discord vs system, divergence findings, concrete improvement suggestions.
type: project
---

# Copilot Session 3 — Mar 27, 2026 Post-Market Review

## Market Summary
- **SPX:** ~6473 prev close → 6438 open → 6372 low (2:35 PM) → ~6375 close = **-98 pts (-1.5%)**
- **VIX:** 29.66 open → 31.34 high → ~31 close. **Breached 30.**
- **Day type:** Trend down. Every bounce was fake. Lower highs all day: 6438→6442→6435→6418→6413→6394→6375.
- **Paradigm:** BofA-LIS → BOFA-MESSY → GEX-LIS (3 flips in 1 hour, then GEX-LIS rest of day)
- **LIS:** Started 6413/6460, dropped to 6340 by close (Phoenix's exact target)

## All Signals (53 total from DB)

### By Setup
| Setup | Trades | W | L | E | WR | PnL |
|-------|--------|---|---|---|----|-----|
| ES Absorption | 16 | 7 | 6 | 3 | 44% | +27.0 |
| SB2 Absorption | 12 | 4 | 8 | 0 | 33% | -44.0 |
| Skew Charm | 12 | 2 | 10 | 0 | 17% | -117.7 |
| DD Exhaustion | 10 | 2 | 8 | 0 | 20% | -46.3 |
| GEX Long | 1 | 1 | 0 | 0 | 100% | +9.8 |
| BofA Scalp | 1 | 1 | 0 | 0 | 100% | +10.0 |
| VIX Compression | 1 | 0 | 1 | 0 | 0% | -20.0 |
| **TOTAL** | **53** | **17** | **33** | **3** | **32%** | **-186.2** |

### By Direction (unfiltered)
| Direction | Trades | W | L | WR | PnL |
|-----------|--------|---|---|----|-----|
| **Bearish (shorts)** | 17 | 9 | 6 | 53% | **+32.0** |
| **Bullish (ES/SB2)** | 11 | 2 | 8 | 18% | -49.0 |
| **Long (SC/DD/GEX/BofA)** | 25 | 6 | 19 | 24% | -164.2 |

**Key insight: Shorts were profitable (+32 pts). Longs were a disaster (-213 pts combined).**

## V12 Filtered Trades

**V12 filter logic (from code):**
- SC Longs: exempt from VIX gate, grade A/B only (C/LOG blocked)
- Other longs: VIX <= 22 OR overvix >= +2 required (VIX was 28-31 all day → BLOCKED)
- Shorts whitelist: SC, AG, DD(align!=0) ONLY → ES Abs/SB2 shorts = `return False`
- SC/DD shorts blocked when paradigm = GEX-LIS (paradigm was GEX-LIS most of day → blocks SC/DD shorts)
- V12 gap: |gap| > 30 → block first 30 min

### V12 Passed Trades (SC Long only — everything else blocked)
| Time | ID | Setup | Grade | Spot | VIX | Result | PnL |
|------|-----|-------|-------|------|-----|--------|-----|
| 09:45 | 1290 | SC Long | A | 6438 | 29.66 | LOSS | -14.0 |
| 10:15 | 1298 | SC Long | B | 6423 | 29.81 | LOSS | -14.0 |
| 10:49 | 1304 | SC Long | B | 6420 | 29.72 | WIN | +8.4 |
| 11:50 | 1316 | SC Long | B | 6435 | 28.94 | LOSS | -14.0 |

**V12 filtered: 4 trades, 1W/3L, -33.6 pts**

### V12 Blocked But PROFITABLE
| Time | ID | Setup | Grade | Direction | Result | PnL | Why Blocked |
|------|-----|-------|-------|-----------|--------|-----|-------------|
| 11:06 | 1306 | ES Abs | B | bearish | WIN | +10 | Not in shorts whitelist |
| 11:32 | 1312 | ES Abs | B | bearish | WIN | +10 | Not in shorts whitelist |
| 13:17 | 1326 | ES Abs | B | bearish | WIN | +10 | Not in shorts whitelist |
| 10:18 | 1300 | SB2 | B | bearish | WIN | +5 | Not in shorts whitelist |
| 14:24 | 1333 | SB2 | B | bearish | WIN | +5 | Not in shorts whitelist |
| **TOTAL BLOCKED WINNERS** | | | | | | **+40** | |

**If V12 had allowed ES Abs/SB2 bearish (B+ grade):**
- +40 from bearish winners, -16 from bearish B-grade losers (1305 -8, 1324 -8)
- Net bearish B-grade: +24 pts
- Combined V12 would be: -33.6 + 24 = **-9.6 pts** (vs -33.6 actual)

## Real Trader Performance
- **210VYX65 (longs):** RealizedPnL = **-$179.80** (3 stops + 1 trail win, SC only)
- **210VYX91 (shorts):** RealizedPnL = **$0** (no shorts fired — V12 + GEX-LIS blocked SC/DD shorts)
- **Combined:** **-$179.80**

## Copilot Analysis vs Reality

### What I Got Right
1. **"Bounce is real but temporary"** (11:00 AM analysis) — Correct. 6412→6442 bounce was fake, exactly as LordHelmet predicted.
2. **"Price at lower LIS is support test"** (10:00 AM) — LIS held briefly (6413) then broke at 1:36 PM. Support was temporary.
3. **"DD is schizophrenic, unreliable"** (10:57 AM) — Correct. DD flipped 8+ times. Not actionable.
4. **"2 PM charm window is key"** — Charm reached $1.08B but couldn't overcome selling. Correctly identified the timing even though the outcome was bearish.
5. **"R3 beach ball at extreme DD"** (2:04 PM) — DD hit +$10.4B combined, warned of snap-back risk. Small bounce did occur.

### What I Got Wrong
1. **Initial bias "Slight Long, conviction 3"** (10:00 AM) — Wrong. DD +$4.2B was misleading. Should have weighted SVB (1.57) higher.
2. **"Vol crush thesis"** (11:32 AM) — Believed Apollo's vol seller signals + Friday pattern. Vol sellers LOST. VIX went 29→31. Dead wrong.
3. **"SPX DD = institutions bullish = bottom"** (11:55 AM divergence analysis) — SPX DD was +$4.9B but institutions pulled back to -$6.4B by afternoon. The divergence resolved BEARISH not bullish.
4. **Over-relied on DD combined** — DD combined was useless all day (8 flips). Should have switched to price action + SVB earlier.
5. **Didn't call "trend day" early enough** — SVB at 1.57 at open was the clearest signal. I didn't flag it as a trend indicator until 2:04 PM when SVB was 1.73.

### Copilot Score
| Dimension | Score | Notes |
|-----------|-------|-------|
| Direction | 4/10 | Called long initially, corrected to short by 10:45. Too slow. |
| Timing | 5/10 | Identified key levels (6486 ES, LIS, 6400) correctly. Too late to short. |
| Actionability | 6/10 | "Sit on hands" advice (10:57) was correct. Conditional playbook was useful. |
| Mechanics | 7/10 | SVB/DD/charm analysis was deep. Correctly identified divergences. |
| **Overall** | **5.5/10** | Better mechanics than Mar 25, but still too slow to call direction. SVB was staring me in the face. |

## Discord Expert Scorecard (Full Day — 536 msgs)
| Expert | Call | Time | Accuracy | Notes |
|--------|------|------|----------|-------|
| **LordHelmet** | "fake rally then more pain" | 09:09 | **10/10** | Perfect call. Every bounce was a sell. |
| **LordHelmet** | "macro will trump dealer hedging" | 13:09 | **10/10** | Key thesis: headlines overrode all Greek positioning. |
| **LordHelmet** | "VX futures and VIX divergence" | 15:26 | **N/A** | Can't verify yet. VX Futures data comes Apr 1. |
| **Phoenix** | 6340 target, VIX 30 | overnight | **9/10** | LIS hit 6340. VIX hit 31.28. Near-perfect. |
| **Phoenix** | "6360, we can bounce now" | 15:55 | **8/10** | 6360 held intraday. AH hit 6320 though. |
| **Dark Matter** | "sell bounces into resistance" | 09:16 | **8/10** | Correct bias. Also: "more downside but bounce first" at 12:41. |
| **Yahya** | "GIV doubles at 6400" | 09:20 | **8/10** | 6400 broke, VIX went 29→31. |
| **Yahya** | "CTAs max short, no more selling in uptape" | 15:54 | **TBD** | Look-ahead for Monday. |
| **Apollo** | 6486 ES = key resistance | 09:18 | **9/10** | Exact rejection level confirmed. |
| **Apollo** | "Vol seller jumping in" | 10:16 | **2/10** | Vol sellers LOST. VIX 29→31. Wrong read. |
| **Apollo** | "DD positive + overvixing = rally to 6450" | 14:27 | **3/10** | Rally never materialized. More selling instead. |
| **Apollo** | "6400 last hope on ES" | 15:10 | **8/10** | 6400 ES broke, confirmed trend continuation. |
| **Zack** | "Not overvixxed yet, not vol event" | 14:32 | **9/10** | Correct — Wizard also said no vol signal. Even at VIX 31+. |
| **Wizard** | "We never hit a vol signal" | 14:08 | **9/10** | Even at overvix 2.18, Wizard says NOT a vol event. Critical for our filter. |
| **Wizard** | "If Iran says unconditional surrender → moon" | 13:53 | **N/A** | Conditional — didn't trigger. |
| **TheEdge** | "Flipping 638-640 massive for DD" | 13:41 | **10/10** | Explained exact DD whiplash mechanism. |
| **Pro H. Bido** | "Positive delta, price didn't move" | 15:12 | **9/10** | Buyers absorbed by sellers = trend continuation signal. |
| **Abcdefg** | "Positioned for DD flip+Vanna slide" | 15:17 | **5/10** | Mechanics right, but it never triggered intraday. |

## Key Divergences Observed

### 1. SVB (Spot-Vol Beta) — STRONGEST SIGNAL, NOT USED
- **Open:** SVB 1.57 (extreme abnormal — vol rising WITH price)
- **Peak:** SVB 2.18 (2:35 PM, most extreme all day)
- **Meaning:** SVB > 1 = stressed regime, vol buyers winning = trend day
- **Our system doesn't filter on SVB at all.**
- **Suggestion:** SVB > 1.5 at open or in first 30 min = "trend day warning" flag

### 2. DD Combined — USELESS IN CHOP
- Flipped sign 8+ times in one session
- Ranged from +$13.3B to -$8.4B
- SPX and SPY DD were in opposite directions most of the day
- **DD is not actionable when it flips > 3x in 2 hours**

### 3. SPX vs SPY DD — Wizard's "Retail vs Institutional"
- SPX DD (institutional) started +$1.9B, ended -$6.4B
- SPY DD (retail) swung wildly: +$2.4B → -$13.3B → +$14B → -$7.9B
- The institutional read (SPX DD) going negative at 10:45 was a clear short signal — I missed it

### 4. VIX Dropping While SPX Flat (10:00-11:30)
- VIX went 29.66 → 28.94 while SPX was flat at 6420-6440
- Apollo saw vol sellers at 10:16 and 10:21
- This was a FALSE signal — vol sellers lost, VIX resumed rising
- **Lesson: Vol seller signals without VX Futures data are unreliable**

### 5. Overvix Crossed +2 at the WORST Time
- Overvix hit +2.03 at 2:35 PM (VIX 31.28)
- Our V12 filter treats overvix >= +2 as "market overvixed, allow longs"
- But this was during the steepest sell-off — price at 6377
- **On trend days, overvix +2 means "trending hard" not "ready to mean-revert"**

## Suggestions for System Improvement

### S1: SVB Trend Day Detector (REVISED — Copilot Rule Only, NOT filter)
**Rule:** SVB level alone doesn't predict long performance (Mar 3 SVB 3.82 = best long day, Mar 13 SVB -0.68 = worst long day).
**What works instead:** Check if shorts are winning too. "Both sides winning = range = longs OK. Only shorts winning = trend down = sit out longs."
**Action:** Copilot tracks intraday short outcomes. If first 2-3 shorts all WIN and first 2-3 longs all LOSE → "trend day detected, reduce long conviction."
**NOT a code filter** — SVB range is too noisy for automated gates.
**Evidence:** 15 VIX>25 days analyzed. SVB has no clean threshold for blocking longs.

### S2: Add ES Absorption Bearish to V12 Shorts Whitelist (MEDIUM PRIORITY)
**Current:** V12 `return False` for all shorts except SC/AG/DD → ES Abs bearish always blocked.
**Proposed:** Add ES Absorption (grade B+) to shorts whitelist, subject to same paradigm checks.
**Evidence:** Mar 27: ES Abs bearish B-grade = 3W/1L, +22 pts (blocked by V12). Best-performing setup today.
**Risk:** Need multi-day backtest. ES Abs has only 12 V12-filtered trades historically.
**Implementation:** Add `if setup_name == "ES Absorption": return True` in the shorts section.

### S3: DD Instability Detector — Copilot Rule (LOW-CODE)
**Rule (R32):** When DD combined flips sign > 3 times in 2 hours → DD is noise, ignore it.
**Action:** Copilot flags "DD unreliable today." Weight price action and SVB instead.
**Evidence:** Mar 27: 8+ DD flips. Every bias call based on DD was reversed within 30 min.

### S4: SPX DD as Institutional Bias — Copilot Rule (LOW-CODE)
**Rule (R33):** When SPX DD and SPY DD diverge in sign, weight SPX DD (institutions) over combined.
**Refinement:** SPX DD going negative = institutions short → bearish signal regardless of SPY.
**Evidence:** Mar 27: SPX DD went -$683M at 10:45 (institutions turned short). Price dropped 55 pts after. I missed this because I was looking at combined DD.
**Note:** Wizard of Ops confirmed "SPY is retail, SPX is institutional" on Mar 27.

### S5: WITHDRAWN — V12 Overvix Gate Is Correct As-Is (revised Mar 28)
**Original proposal:** Block longs when overvix +2 AND SVB > 1.5.
**WITHDRAWN because:** Comprehensive analysis (15 VIX>25 days, all Discord history) showed:
- Mar 3 (SVB 3.82) and Mar 5 (SVB 2.69) were BEST long days despite extreme SVB. S5 would have blocked +294 pts of winners.
- Mar 13 (SVB -0.68) was WORST long day despite negative SVB. SVB level is not the differentiator.
- S5 net impact: save 33 pts (Mar 27) but lose 294 pts (Mar 3,5) = **-$261 worse**.
**Why V12 is already correct:**
- SC catches bottoms via VIX exemption (Mar 9: 11W/1L, +152 pts)
- Non-SC longs are VIX-gated (blocks most long signals on high-VIX days)
- Overvix +2 correctly unlocks longs when the spring is compressed — the issue isn't the gate, it's that Mar 27 wasn't a vol event (Wizard/Zack confirmed)
**The real bottom signal (from Discord experts):**
1. Overvix peaks >= +2 (compression)
2. VIX starts dropping / goes red (Phoenix's signal)
3. SVB flips negative (vol sellers take over)
4. Shorts start LOSING (reversal confirmed)
→ That's when SC is already firing and all longs should follow. V12 handles this naturally.
**Future consideration (NOT a filter change):** Track intraday VIX direction. If overvix peaked +2 AND VIX now dropping > 1pt from session high → Telegram alert "VOL RELEASE signal" for discretionary longs.

### S6: SC Long Consecutive Loss Breaker (MEDIUM PRIORITY)
**Rule:** After 2 consecutive SC long losses in same session → increase cooldown to 60 min.
**Evidence:** Mar 27: SC longs fired 4 times in 2 hours, 3 lost. Each loss was -14 pts (full SL).
**Alternative:** After 2 SC long losses, require grade A+ or A to fire next (upgrade grade gate temporarily).
**Backtest needed:** Check if 2-loss streaks followed by 3rd loss is statistically significant across all dates.

### S7: Paradigm Stability as Conviction Signal — Copilot Rule (LOW-CODE)
**Rule (R34):** Count paradigm flips in 2 hours. If > 2 → "paradigm unstable, level-to-level only."
**Evidence:** Mar 27: BofA-LIS → BOFA-MESSY → GEX-LIS in 1 hour. Every paradigm-based bias call was wrong.
**Action:** Copilot reduces conviction to max 2/5 when paradigm is flickering.

### S8: "Macro Trumps Dealer Hedging" — Copilot Rule (NEW from Discord PM session)
**Source:** LordHelmet 13:09 ET: "I think macro will 'trump' dealer hedging."
**Rule (R36):** When headline/macro forces are dominant (Iran, tariffs, oil shock, bank earnings), Volland Greek positioning is OVERRIDDEN.
**Detection:** Paradigm instability (R34) + SVB > 1.0 + VIX > 28 = "macro regime." Flag all Greek-based calls with "macro override risk."
**Evidence:** Mar 27: DD +$4.2B at 10:00 → ignored. Charm $1B+ at 2 PM → overwhelmed. Every Volland-based bias wrong because macro dominated.
**Confidence:** HIGH (LordHelmet + multiple experts, validated Mar 27)

### S9: DD Whiplash Mechanism — Understanding Not Filter (NEW from Discord PM)
**Source:** TheEdge 13:41 ET: "Flipping the 638, 639 and 640 is massive for DD."
**Explanation:** SPY DD whiplash is MECHANICAL — spot crossing ATM strikes flips DD by billions instantly. Not real sentiment.
**Apply:** When SPY DD swings > $10B, check if spot crossed 2-3 consecutive $1 SPY strikes. If yes = "mechanical DD flip, discount."
**Confidence:** HIGH (TheEdge explained, validated by 8 DD flips Mar 27)

### S10: CTA Exhaustion as Next-Day Signal (NEW from Discord PM)
**Source:** Yahya 15:54 ET: "CTAs are max short after today. No more selling in an uptape."
**Rule:** After >1% down day when CTAs reach max short, mechanical selling exhausted → bullish next session.
**Apply:** Flag: "CTA selling likely exhausted — watch for gap-up Monday." Use SPX >1% down + VIX >30 as proxy.
**Confidence:** MEDIUM (need more data points)

## Validated / Invalidated Rules from Mar 27

### Validated
- **Rule 22 (RE-VALIDATED):** "Fewer trades = higher P&L." System fired 53 signals; V12 caught only 4, still lost. Apollo took 0 trades today.
- **Rule 31 (VALIDATED):** "VIX rising while SPX drops = vol buyside = trend." SVB confirmed this mechanically. VIX went 29→31 with SPX -98 pts.
- **Rule 7 (VALIDATED):** Paradigm flickering (3 flips in 1 hour) = chop zone, no edge.
- **Rule 10 (VALIDATED):** Charm is EOD force. Charm reached $1B+ at 2 PM but couldn't overcome selling pressure on a trend day.

### Invalidated
- **Apollo's vol seller call (Rule 29 proxy):** Vol sellers at 10:16 did NOT cap the move. Without VX Futures, we cannot reliably detect vol flow. Wait for Apr 1 data.

### New Rules to Add
- **R32:** DD flips > 3x in 2h = DD noise. Ignore, trade price action + SVB.
- **R33:** SPX DD (institutional) > SPY DD (retail) for bias. SPX DD negative = bearish.
- **R34:** Paradigm flips > 2 in 2h = unstable, conviction max 2/5.
- **R35:** SVB > 1.5 = trend day. Reduce long conviction. Don't trust bounces.
