---
name: Copilot Market Rules — Self-Improving Trading Playbook
description: Accumulated tactical rules for market bias, entry timing, DD mechanics, paradigm interpretation, and Discord-derived insights. Updated after each session by comparing calls vs reality. Read at EVERY session start.
type: feedback
---

# Copilot Market Rules

Last updated: 2026-03-28 (Session 3 + Mar 27 post-market analysis)
Sessions tracked: 3
Running score: 5.5/10 (avg: S1=5, S2=6, S3=5.5)

---

## A. DD (DELTA DECAY) RULES

### RULE 1: Don't wait for DD confirmation on gap+paradigm days
**Source:** Mar 25 — Apollo shorted at 09:48 and banked 20 pts while I was "waiting for DD to confirm"
**Why:** Gap-up + MESSY/AG paradigm is already a strong structural signal. DD confirmation adds conviction but costs 15-20 pts of the move.
**Apply:** If gap > +30 AND paradigm is bearish (SIDIAL-MESSY, AG-*), call SHORT bias immediately. Don't say "wait for DD." Say "SHORT bias, DD will likely confirm — entry window NOW." Conviction 4/5.
**Confidence:** HIGH (validated Mar 25)

### RULE 2: "Sell pops while DD stays negative" (LordHelmet)
**Source:** Discord Mar 25, 11:53 ET. Also Abcdefg: "trade WITH DD after 10:00."
**Why:** When DD is negative, every bounce is a selling opportunity because dealers are hedging short. Multiple Discord experts agree: DD direction after 10:00 AM is the PRIMARY short-term bias filter.
**Apply:** DD < -$1B after 10:00 = "sell pops until DD flips positive." DD > +$1B = "buy dips until DD flips negative." If DD opposes your setup = level-to-level only (tight target).
**Confidence:** HIGH (LordHelmet, Abcdefg, TheEdge all use this)

### RULE 3: Beach ball effect — extreme DD creates snap-back
**Source:** Apollo Mar 25: "anytime you push under, all the DD flips." LordHelmet: "like pushing a beach ball underwater."
**Why:** When price pushes through strikes, those strikes' DD contribution FLIPS sign. The act of going lower makes DD less bearish. This creates a natural snap-back.
**Apply:** |DD| > $10B = EXTREME — warn "beach ball risk, don't chase, snap-back likely." |DD| > $5B = strong conviction but still building. |DD| < $1B = no directional signal from DD.
**Confidence:** HIGH (Apollo explained mechanics, validated Mar 25: DD went -$14B then snapped to +$3.7B)

### RULE 4: Sum SPX + SPY DD for net imbalance (Apollo)
**Source:** Apollo Mar 23: "I like to do the Sum{SPY,SPX} to get a net imbalance." Combined -$6B preceded 60pt drop.
**Why:** Single-source DD misses the full picture. When SPX and SPY offset = neutral. When they align = strong conviction.
**Apply:** Always report Combined DD. When combined > ±$5B = strong signal. When SPX and SPY diverge in sign = mixed signal, reduce conviction.
**Confidence:** HIGH (Apollo validated live Mar 23)

### RULE 5: DD widget vs DD exposure chart — INVERTED signs
**Source:** Apollo Mar 23: DD widget +bullish/-bearish. DD exposure CHART is INVERTED (+bearish/-bullish).
**Apply:** Always clarify which DD source. Our system uses the widget convention (+bullish/-bearish).
**Confidence:** HIGH

---

## B. PARADIGM RULES

### RULE 6: Paradigm-specific entry windows (from Volland User Guide)
**Source:** Volland User Guide pp. 26-30, backtested success rates.
**Apply:**
- **BofA:** Range-bound. Iron condors at LIS. Best window 10:05-10:35 AM (93-100% WR). Declines after 2 PM.
- **GEX:** Bullish trend to target. Best 10:30-11:05 AM (54-62% targets). Frequently transitions to BofA after 1 PM — take profits early.
- **AG (Anti-GEX):** Bearish trend to target. Best 11:00-11:35 AM (50-70% targets). Converts to BofA late afternoon.
- **Sidial:** Mean-revert to single target. Extremely tight range. Usually appears as transition after GEX/AG target hit.
- **Messy:** UNPREDICTABLE. SIT OUT or reduce size. Strike-by-strike analysis required.
**Confidence:** HIGH (from official Volland guide with backtested rates)

### RULE 7: Paradigm settling vs flickering
**Source:** Mar 25 — paradigm flickered SIDIAL-MESSY ↔ AG-TARGET ↔ AG-PURE.
**Why:** A SETTLING paradigm (MESSY→AG-PURE) = conviction building. FLICKERING = chop zone, reduce size.
**Apply:** Track paradigm trajectory. Settling into one type = increase conviction. Flickering = "paradigm unstable, level-to-level only." Report the trajectory, not just the current value.
**Confidence:** MEDIUM (need more sessions)

### RULE 8: Paradigm subtypes matter more than paradigm
**Source:** Skew Charm grading v2 (Mar 22). Discord experts.
**Why:** GOOD subtypes (SIDIAL, GEX-PURE, AG-TARGET, BofA-LIS) = 84% WR. BAD subtypes (GEX-LIS, AG-LIS) = 45% WR.
**Apply:** Always report paradigm subtype. Flag BAD subtypes as "toxic paradigm subtype — reduce conviction."
**Confidence:** HIGH (backtested on 210 trades)

### RULE 9: Negative gamma above spot = explosive upside fuel (but needs catalyst)
**Source:** Wizard of Ops Mar 20: "negative gamma above, any bullish momentum can spark a chain reaction." Apollo, LordHelmet confirm.
**Why:** Negative gamma = lack of dealer friction. Price can accelerate quickly through negative gamma zones. But needs ignition (vol crush, headline, DD flip).
**Apply:** When paradigm is GEX and there's negative gamma above spot, call "explosive upside potential IF catalyst arrives (vol crush, DD flip positive)."
**Confidence:** HIGH (multiple experts agree)

---

## C. CHARM RULES

### RULE 10: Charm is primarily an EOD force
**Source:** Volland User Guide pp. 23-26.
**Why:** Pre-2 PM charm has MINIMAL effect (delta & gamma dominate). 2-4 PM = charm effects MAXIMIZE (dealer rebalancing into expiry). Within 1-2 hours of expiry = exponentially strongest.
**Apply:** Don't weight charm heavily in morning bias. After 2 PM, charm direction becomes the dominant force. Report charm separately for AM vs PM bias.
**Confidence:** HIGH (from Volland guide)

### RULE 11: Charm + paradigm integration
**Source:** Volland User Guide.
**Apply:**
- BofA (negative charm below, positive above) = range reversals at LIS.
- GEX (negative charm both sides) = follow target up, charm neutral point = bullish limit.
- AG (positive charm both sides) = follow target down, charm neutral point = bearish limit.
**Confidence:** HIGH

### RULE 12: Skew must compress before charm works
**Source:** Apollo (Discord Part 1.1): "Bearish charm does NOT work in elevated skew."
**Apply:** When skew is elevated, don't rely on charm-driven moves. Must wait for skew compression first. Flag "charm blocked by elevated skew" when applicable.
**Confidence:** HIGH (Apollo's framework)

---

## D. VANNA RULES

### RULE 13: Vanna support is CONDITIONAL on fixed-strike vol
**Source:** Apollo Mar 21: "when fixed strike vols come down, that is where the support really plays out." Miyaka: "note the vols at key strikes at open and watch when price approaches if that vol is rising or dropping."
**Why:** Positive vanna below spot does NOT automatically mean support. Only supports if IV at those strikes is STABLE or FALLING. Rising IV negates vanna support.
**Apply:** When citing vanna support, caveat: "holds IF IV at those strikes is stable/falling." If VIX is rising, warn "vanna support may not hold — IV rising."
**Confidence:** HIGH (Apollo + Miyaka consensus)

### RULE 14: Vanna stochastic at extremes = momentum
**Source:** Volland White Paper pp. 6-8.
**Apply:** Vanna stochastic at ±1 = maximum vega sensitivity to spot = strongest momentum. At 0 = IV controls, mean reversion. Rising stochastic = building momentum.
**Confidence:** MEDIUM (theoretical, need live validation)

---

## E. VOL SELLING / "THE WORM" RULES

### RULE 15: Vol sellers can override 0DTE positioning (Apollo)
**Source:** Apollo Mar 25: "0dte is bearish but vol guys keep selling it." Also Mar 24: "IF vol flips long, then 40 pt dip."
**Why:** Vol sellers (the "worm") actively suppress realized volatility. When vol sellers are active, they can cap moves even when 0DTE positioning says otherwise. The FLIP from vol selling to vol buying is the KEY catalyst signal.
**Apply:** I can't see the worm directly. But I should note: "vol selling may override this positioning — watch order flow for confirmation." Flag when VIX drops despite bearish positioning = vol sellers active.
**Limitation:** No worm data. This is the biggest gap in my analysis.
**Confidence:** HIGH (Apollo's primary edge)

---

## F. ENTRY TIMING RULES

### RULE 16: The retest entry pattern (most repeatable)
**Source:** Volland User Guide, Discord experts.
**Apply:** 1) Identify key level. 2) Wait for BREAKOUT. 3) Enter on RETEST of that level. 4) Tight stop beyond retest failure. 5) BE at 10-15 pts profit.
**Why:** Filters fakeouts. Dealers have committed. Retests show genuine follow-through.
**Confidence:** HIGH

### RULE 17: Avoid the open (9:30-10:00)
**Source:** Multiple Discord experts (Simple Jack, Miyaka, Volland guide).
**Why:** Binary event, 50/50, noise premium too high. Destroys accounts on prop firms. Wait for structure.
**Exception:** Apollo trades the open — but he has order flow (worm) that I don't. Without order flow, avoid.
**Confidence:** HIGH

### RULE 18: Wizard's "2 PM reversal" pattern on panic put days
**Source:** Wizard of Ops Mar 20: panic put buying → LIS drops all morning → puts decay → 2 PM reversal.
**Apply:** When morning shows massive put buying (DD shifts bearish sharply at open due to puts), note: "panic put pattern — watch for 2 PM ET reversal as puts decay."
**Confidence:** MEDIUM (one detailed example, needs more validation)

### RULE 19: Early target hit = take profits
**Source:** Abcdefg Mar 25: "if we hit the target this early I usually close early-ish. I find if we hit target early it usually misses."
**Apply:** If Volland target is hit before noon, flag "target hit early — profit-taking advised, holding usually fails."
**Confidence:** MEDIUM (one source)

---

## G. EXIT & RISK MANAGEMENT RULES

### RULE 20: VIX rising = cut longs (LordHelmet)
**Source:** LordHelmet Mar 20: "I'll cut them in profit if VX creeps back up."
**Apply:** When tracking a long position and VIX starts rising, flag "VIX rising — consider taking profits on longs."
**Confidence:** MEDIUM

### RULE 21: No overnight exposure in headline-driven markets
**Source:** Dragonboys Mar 24: "this is all so much news driven and not comfortable carrying anything overnight."
**Apply:** During geopolitical/headline-heavy periods (Trump, Iran, tariffs), flag "headline-driven market — avoid overnight exposure."
**Confidence:** HIGH (common sense + expert consensus)

### RULE 22: Fewer trades = higher P&L
**Source:** LordHelmet Mar 20: "in 2026, the fewer trades I make, the higher my P/L." Miyaka: content with few trades/day at small size.
**Apply:** Don't push for more signals. Quality over quantity. If system has 0 signals and I have no strong bias = "no trade is a good trade today."
**Confidence:** HIGH (multiple experts)

### RULE 23: Don't fight DD with the worm absent
**Source:** LordHelmet Mar 25: "im flat, but this is too grindy for me with the DD-" — sits out rather than fighting DD.
**Apply:** When DD opposes bias and I can't see order flow confirmation, recommend sitting out rather than forcing a trade.
**Confidence:** HIGH

---

## H. OPTIONS STRATEGY AWARENESS

### RULE 24: Quarterly put decay as hidden support
**Source:** Zack Mar 25: "a lot of the support is the decay flow from that march quarterly put strike" at 6475.
**Apply:** Near quarterly expirations (March, June, Sep, Dec), large put OI at key strikes creates support via decay flow. Once quarterly rolls off, market becomes more volatile. Flag near quarterly opex.
**Confidence:** MEDIUM (one detailed observation)

### RULE 25: When skew is undervalued, go long vega (Wizard)
**Source:** Wizard of Ops Mar 20: "Skew is way undervalued. Long gamma, long vega." Options mispriced by ~10 VIX points vs dealer positioning.
**Apply:** When Volland shows dealer positioning that implies much higher vol than current IV, flag "options look cheap vs dealer positioning — long vega may be appropriate."
**Confidence:** HIGH (Wizard's specialty, validated by VIX spike that followed)

---

## I. COMMUNICATION RULES (Self-Improvement)

### RULE 26: Express conviction level 1-5
**Source:** Self-assessment Mar 25.
**Apply:**
- 5 = "Strong [direction]. Enter now."
- 4 = "Lean [direction]. Wait for first pullback."
- 3 = "Slight [direction] bias. Level-to-level only."
- 2 = "No edge. Stay flat."
- 1 = "Conflicting signals. Avoid."

### RULE 27: Lead with the framework, not the data
**Source:** Self-assessment Mar 25. My analysis lags by 30-60 min.
**Apply:** At session start, give CONDITIONAL playbook: "Today: if X happens do Y, if Z happens do W." User can act in real-time without waiting for me.

### RULE 28: Don't mix "might" language with directional calls
**Source:** Self-assessment Mar 25. "Bias is short but might bounce" = useless.
**Apply:** Separate the call from the risk: "BIAS: Short (conviction 4). RISK: DD beach ball above $10B. ACTION: sell pops, stop above [level]."

### RULE 32: DD flips > 3x in 2 hours = DD is noise
**Source:** Mar 27 — DD combined flipped sign 8+ times. Every DD-based bias call reversed within 30 min.
**Why:** Headline-driven markets create DD whiplash as strikes go ATM/OTM rapidly. Combined DD becomes random noise.
**Apply:** Track DD sign flips. If > 3 in 2 hours: "DD unreliable — ignore for bias, use price action + SVB instead." Don't cite DD conviction when it's flickering.
**Confidence:** HIGH (Mar 27: 8 flips, every DD call wrong)

### RULE 33: SPX DD (institutional) outweighs SPY DD (retail)
**Source:** Wizard of Ops Mar 27: "SPY is retail sentiment, SPX is institutional." Pro H. Bido asked about SPX/SPY DD divergence.
**Why:** When SPX and SPY DD diverge massively, combined DD is misleading. SPX DD reflects institutional hedging = smarter flow. SPY DD reflects retail panic.
**Apply:** When |SPX DD - SPY DD| > $5B: report separately. Weight SPX DD for bias. "SPX DD negative = institutions short = bearish regardless of SPY." Combined DD only valid when both align.
**Confidence:** MEDIUM (1 day evidence, Wizard framework)

### RULE 34: Paradigm flips > 2 in 2 hours = unstable regime
**Source:** Mar 27 — BofA-LIS → BOFA-MESSY → GEX-LIS in 1 hour. All paradigm-based calls wrong.
**Why:** Rapid paradigm flips mean Volland's model is straddling a boundary. No single paradigm is reliable.
**Apply:** Track paradigm changes. > 2 flips in 2 hours = "paradigm unstable, max conviction 2/5, level-to-level only."
**Confidence:** MEDIUM (validated Mar 25 + Mar 27)

### RULE 35: SVB level alone doesn't predict long success (REVISED from Mar 28 research)
**Source:** Comprehensive analysis of ALL high-VIX days (Mar 3-27). Original rule "SVB > 1.5 = block longs" was WRONG.
**Evidence:** Mar 3 (SVB 3.82, longs +139 pts) and Mar 5 (SVB 2.69, longs +155 pts) were the BEST long days despite extreme SVB. Mar 13 (SVB -0.68, longs -261 pts) was the WORST despite negative SVB.
**What actually differentiates good vs bad long days:**
- **Shorts also winning = RANGE day** → longs OK (Mar 3, 5: both sides +)
- **Shorts getting destroyed = CLEAN REVERSAL** → longs GREAT (Mar 9: shorts -168, longs +152)
- **Only shorts winning = TREND DOWN** → longs terrible (Mar 12, 27: shorts +, longs -)
**Apply:** Don't use SVB level for filtering. Instead check: "are shorts also winning today?" If yes = range (longs fine). If shorts losing = reversal (go aggressive). If only shorts winning = trend (sit out longs).
**Confidence:** HIGH (validated across 15 VIX>25 days, Mar 3-27)

### RULE 36: Macro trumps dealer hedging
**Source:** LordHelmet Mar 27 13:09 ET: "I think macro will 'trump' dealer hedging." Validated by entire Mar 27 session.
**Why:** When headline/macro forces dominate (Iran, tariffs, oil, bank earnings), Volland's Greek-based positioning is overridden. DD can be strongly bullish while price drops 100 pts. Charm $1B+ can push up while market ignores it.
**Apply:** When macro is dominant (paradigm unstable R34 + SVB > 1.0 + VIX > 28), caveat ALL Greek-based calls: "Macro override risk — Greek positioning may not hold." Reduce conviction on Volland-based signals.
**Confidence:** HIGH (LordHelmet, multiple experts, validated Mar 27: DD/charm/paradigm all wrong)

### RULE 37: DD whiplash is mechanical at ATM SPY strikes
**Source:** TheEdge Mar 27 13:41 ET: "Flipping the 638, 639 and 640 is massive for DD."
**Why:** SPY DD swings billions when spot crosses consecutive $1 strikes because ATM puts/calls flip dealer hedging. It's mechanical, not sentiment. SPY has millions of contracts at each $1 strike.
**Apply:** When SPY DD swings > $10B in one cycle and spot just crossed 2-3 consecutive SPY strikes → "mechanical DD flip, not real. Discount."
**Confidence:** HIGH (TheEdge explained mechanics, dauma confirmed: "Wasn't this -14B two hours ago?")

### RULE 29: "Vol buyside" = strongest bearish catalyst (Apollo)
**Source:** Apollo Mar 26, 10:10 ET: "Vol is buyside as well" + 2 ES sellers. Preceded 90-pt SPX drop.
**Why:** Vol sellers suppress realized vol (range-bound). When vol FLIPS to buyside, institutional traders are buying protection = trending day incoming. The flip is the catalyst, not the positioning.
**Apply:** VX Futures via CFE/Rithmic coming Apr 1 — will give direct vol flow visibility. Until then, use VIX proxy: VIX rising while price flat/up = vol buying underway. Flag "vol appears buyside — trending day likely."
**Confidence:** HIGH (Apollo's primary signal, validated Mar 26)

### RULE 30: Visible liquidity pools PULL price in trends (Apollo)
**Source:** Apollo Mar 26, 13:20 ET. Dragonboys asked if liquidity below acts as support. Apollo: "id almost say inverse — Sellers are strong. That is liquidity. pulling price."
**Why:** In a trending day, visible bid/offer liquidity gets swept. Market makers see the liquidity and route to it. What looks like support in range = magnet in trend.
**Apply:** When paradigm is AG or trending, large bid clusters below are TARGETS not SUPPORT. "Liquidity at [level] will likely be swept before any bounce."
**Confidence:** HIGH (Apollo validated live, matched AG target hit)

### RULE 31: Vol sell-side = range, Vol buy-side = trend
**Source:** Apollo Mar 26: vol sell-side at 11:21 = "tricky spot by open right now" (range). Vol buyside at 10:10 = trending.
**Why:** Vol sellers cap realized vol by selling into moves. Vol buyers want realized vol to expand = they LET it trend. The regime (sell vs buy) determines whether positioning plays (range) or trends break through.
**Apply:** Until I can see vol flow, use VIX behavior as proxy: VIX dropping while SPX drops = vol sell-side (range likely). VIX rising while SPX drops = vol buyside (trend likely).
**Confidence:** MEDIUM (proxy-based, need VX Futures for direct observation)

---

## SESSION SCORES

| Date | Direction | Timing | Actionability | Mechanics | Overall | Key Learning |
|------|-----------|--------|---------------|-----------|---------|-------------|
| 2026-03-25 | 7/10 | 3/10 | 4/10 | 5/10 | **5/10** | Don't wait for DD at open. Beach ball. Sell pops while DD-. |
| 2026-03-26 | 8/10 | 6/10 | 5/10 | 6/10 | **6/10** | System nailed direction (all shorts). Vol buyside = trend signal. Liquidity = pull not support. |
| 2026-03-27 | 4/10 | 5/10 | 6/10 | 7/10 | **5.5/10** | Called long initially—wrong. SVB 1.57 was the signal. DD useless (8 flips). Vol seller call wrong. Corrected to short too late. |

---

## PRE-MARKET CHECKLIST (Use every session)

1. **Gap size** — > +30 = block longs (V12). > -30 = block shorts?
2. **Paradigm + subtype** — which regime? settling or flickering?
3. **DD Combined (SPX+SPY)** — direction + magnitude. > $5B = strong.
4. **Charm** — mostly matters after 2 PM. Flag if extreme.
5. **VIX + Overvix** — elevated? overvixed?
6. **Quarterly opex proximity** — near quarterly = hidden support from put decay
7. **Headlines active?** — if yes, flag "headline market, avoid overnight"
8. **Give conviction 1-5 and conditional playbook**

## THINGS I STILL CAN'T DO
- [ ] Order flow / worm / vol seller-buyer flips — **VX Futures via CFE/Rithmic ETA Apr 1** (Rules 29/30/31 depend on this)
- [ ] CL / VX multi-asset correlation
- [ ] Fixed-strike vol tracking for vanna support validation
- [ ] Real-time continuous monitoring (structural: I check when asked)
- [ ] Discord live feed integration

## VALIDATED RULES
- **Rule 1 (VALIDATED Mar 26):** Don't wait for DD — first signals at 09:49-10:06 were all shorts, all correct
- **Rule 2 (VALIDATED Mar 26):** "Sell pops while DD-" — DD stayed negative, every short was a winner
- **Rule 7 (VALIDATED Mar 27):** Paradigm flickering = chop zone. 3 flips in 1 hour, no paradigm call worked.
- **Rule 10 (VALIDATED Mar 27):** Charm is EOD force. $1B+ at 2 PM but couldn't overcome trend-day selling.
- **Rule 22 (RE-VALIDATED Mar 27):** Fewer trades = higher P&L. 53 signals, V12 filtered to 4, still lost. Apollo took 0.
- **Rule 25 (VALIDATED Mar 26):** Wizard long vega paying with VIX >20 all March
- **Rule 31 (VALIDATED Mar 27):** VIX rising while SPX drops = trend day. SVB confirmed mechanically (1.57→2.18).

## INVALIDATED
- **Apollo vol seller proxy (Rule 29):** Vol seller call at 10:16 Mar 27 did NOT cap the move. Without VX Futures, vol flow is unreliable. Wait for Apr 1 data.

## VALIDATE NEXT SESSION
- Rule 32: Does DD instability (>3 flips) reliably predict "no edge" days?
- Rule 33: Does SPX DD negative reliably predict bearish regardless of combined?
- Rule 35: Does SVB > 1.5 at open reliably predict trend days? (2/2 so far)
- Rule 36: Does macro override Greek positioning on next headline day?
- Yahya's CTA exhaustion: does max short → Monday gap up?
- First SC/AG signal direction as trend predictor
