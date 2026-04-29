---
name: Copilot Market Rules — Self-Improving Trading Playbook
description: Accumulated tactical rules for market bias, entry timing, DD mechanics, paradigm interpretation, and Discord-derived insights. Updated after each session by comparing calls vs reality. Read at EVERY session start.
type: feedback
---

# Copilot Market Rules

Last updated: 2026-04-06 (Session 5: Apr 2 Liberation Day)
Sessions tracked: 5
Running score: 5.2/10 (avg: S1=5, S2=6, S3=5.5, S4=4, S5=5.5)

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

### RULE 38: Vanna vacuum = acceleration zone
**Source:** Dark Matter Mar 30-31: "there is NO vanna in this zone... moves will ACCELERATE very quickly"
**Why:** When vanna exposure is near zero within ±15 pts of spot, there is no dealer cushion. Moves extend faster and further in both directions because dealers don't need to hedge gamma/vanna.
**Apply:** Check per-strike vanna near spot. If sparse/zero = expect extended moves. Don't fade strong trends in vanna vacuum. Go WITH the move or stay out.
**Confidence:** HIGH (validated Mar 31: +3.4% off globex lows, Dark Matter called 6525 ES target and nailed it)

### RULE 39: Charm zero-crossing = afternoon pivot
**Source:** Dark Matter Mar 27: "if we're still at the charm zero-crossing (6,445 SPX) by 2 PM, the resolution will be sharp and directional"
**Why:** The SPX level where per-strike charm flips sign acts as an anchor. After 2 PM when 0DTE theta accelerates, price resolves directionally away from this level.
**Apply:** Compute charm zero-crossing level. Afternoon trades near this level = higher conviction for directional move.
**Confidence:** MEDIUM (1 observation, mechanically sound)

### RULE 40: DD sum for macro bias — extreme levels are high conviction
**Source:** Apollo Mar 27: "DD over -7B, firmly bear." Yahya Z Mar 31: DD hit $17.5B — dealers HAD to buy into close.
**Why:** Aggregate DD (SPX+SPY sum) represents total dealer hedging pressure. At extremes (>$10B or <-$5B), the flow becomes mechanical — dealers MUST buy/sell regardless of tape.
**Apply:** DD > +$10B = strong buy pressure into close (conviction 4+). DD < -$5B = strong sell pressure. Per-strike DD is for levels, aggregate DD is for bias.
**Confidence:** HIGH (Apollo + Yahya Z, validated Mar 27 + Mar 31)

### RULE 41: Vol event near-miss (overvix 1.8-2.0) still actionable
**Source:** Wizard Mar 31: "if .02 VIX points makes a difference between taking a trade or not... the concept is still pretty active. Maybe just don't hold the trade for as long."
**Why:** The vol event threshold (overvix >= 2.0) is not binary. At 1.8-2.0, the same mean-reversion dynamics exist, just slightly weaker.
**Apply:** Overvix 1.8-2.0 = same thesis as vol event, but shorter holding period. Don't go full size, don't hold for EOD — take profits at first target.
**Confidence:** HIGH (Wizard is the vol event creator)

### RULE 42: CTA max positioning = selling exhaustion
**Source:** Yahya Z Mar 27: "CTAs are max short after today. So no more selling in an uptape."
**Why:** When systematic/CTA sellers are fully positioned (max short), there is no more mechanical selling pressure. Any catalyst flips the tape because the sellers are done.
**Apply:** Track CTA positioning (SqzMe, community). At max short = bounce incoming on any positive catalyst. Don't initiate new shorts.
**Confidence:** MEDIUM (structural, but CTA data is not real-time for us)

### RULE 43: Negative vanna above spot → breakout cascade when breached
**Source:** BigBill Mar 31: "Negative Vanna above spot = potential breakout if things can get moving higher. It flips when you pass the strike."
**Why:** Negative vanna above spot creates resistance. But when price moves through those strikes, vanna flips positive = becomes tailwind. Each strike crossed converts headwind to tailwind, creating acceleration.
**Apply:** Check weekly/monthly vanna above spot. If large negative cluster exists above, a break through creates cascade effect — don't fade the breakout.
**Confidence:** HIGH (BigBill + indyicon confirmed with opex vanna charts, validated Mar 31)

### RULE 44: Multi-day vol assessment > single-day SVB
**Source:** jk23 Mar 31: "Vol tends to play out over multiple days. I wouldn't take 1 single day by itself. When looking at the relationship between spot and vol it needs to be assessed over a multi day period."
**Why:** Two weeks of overvixing means vol mean-reverts DOWN (bullish), even if today's SVB shows undervixed. Single-day SVB reads are noise without multi-day context.
**Apply:** Before calling SVB regime, check 5-day VIX trend. If VIX has been falling for 3+ days, lean bullish regardless of today's reading. If VIX has been rising 3+ days, lean bearish.
**Confidence:** MEDIUM (conceptually sound, needs validation)

### RULE 45: JPM collar now on CME ES options (structural change)
**Source:** Zack Mar 31: "they are using a packaged product through CME. Won't be on SPX." Yahya Z: "JPM starts using ES options to roll their collar."
**Why:** JPM's quarterly collar was visible in SPX OI/flow. Now it's on ES options via CME packaged product, invisible to SPX-focused analysis.
**Apply:** Don't reference SPX OI for JPM collar impact anymore. Track effects indirectly via ES delta, DD, and quarter-end flow patterns.
**Confidence:** HIGH (Zack confirmed, structural change effective Q1 2026 roll)

### RULE 46: Macro headlines override Greeks — track CL/crude in geopolitical regime
**Source:** Apr 1 — jaytech887: "only thing ES cares about is CL." Trump Iran ceasefire tweet caused HOD spike at 12:47 ET. Iran denial caused PM selloff. Copilot had ZERO macro awareness, scored 1/10 on macro.
**Why:** When geopolitical events are active (Iran, Middle East, tariffs) AND CL > $100, headline-driven algos dominate price. Greek-based DD/charm/paradigm readings become secondary. The Trump tweet single-handedly created the HOD.
**Apply:** At session start, ask "any headlines today?" Check if CL crude is elevated (>$95). If yes, flag "headline regime — Greeks are secondary, headlines drive direction." Reduce Greek-based conviction by 1 level. Watch financial news feeds.
**Confidence:** HIGH (Apr 1: Iran headline caused HOD spike, denial caused PM selloff. Validated R36 macro override.)

### RULE 47: Pre-speech vol buying is NOT charm decay
**Source:** Apr 1 — calling.margin: "I wonder if vol is being bought ahead of the address tonight." VIX rose 23.6→25 in afternoon. Copilot attributed it to charm/DD mechanics.
**Why:** Known presidential/Fed speeches after market hours cause vol buying 2-3 hours before close. VIX rising in this context is EVENT POSITIONING, not directional selling. Charm was -$3B at close but price was bouncing — the VIX rise was vol buyers, not Greeks.
**Apply:** When scheduled speech/event exists after close, VIX rising 2-3 hours before = "vol positioning for event risk, not directional signal." Don't conflate VIX rise with bearish momentum in this context.
**Confidence:** HIGH (Apr 1: Trump 9PM speech, vol bought all afternoon, apollobix confirmed "BuyVol.exe")

### RULE 48: Mean reversion is the base case on GEX-PURE chop days
**Source:** Apr 1 — a_b_cdef: "Feels mean reverting today. 6585 mean?" SPX closed 6574 (close to midpoint). Simple mean reversion beat all Greek analysis.
**Why:** On GEX-PURE days without paradigm flip before 2 PM, dealer friction (positive gamma) naturally pushes price back toward the mean. Complex Greek analysis overcomplicates what is a simple range reversion. The day's VWAP or opening range midpoint is a better target than Volland's.
**Apply:** If GEX-PURE paradigm holds into afternoon without flipping, default bias = mean reversion to day's midpoint. Don't overthink directional calls. Conviction 3/5 max for directional trades.
**Confidence:** MEDIUM (Apr 1 validated: SPX high 6608, low 6554, mid=6581, close 6574. Need more samples.)

### RULE 49: JPM Q2 collar levels — structural quarterly reference
**Source:** Apr 1 — jrestrepo posted collar details: Q2 = 5,210/6,180 put spread vs 6,865 short call. "Executed at 4pm close to match quarterly NAV."
**Why:** JPM's collar is the single largest structural options position in the market. The short call at 6865 creates a ceiling — above that level, JPM hedging becomes a headwind. The 6180 put spread creates a floor for deep corrections.
**Apply:** Reference quarterly collar levels in weekly brief: Q2 ceiling = 6865 SPX (short call), Q2 floor = 6180/5210 (put spread). Above 6865 = "approaching JPM collar headwind." Below 6180 = "entering JPM protection zone."
**Confidence:** HIGH (jrestrepo posted exact terms, confirmed by community)

### RULE 50: DD positive is the DEFAULT at VIX 22-26 — only negative is signal
**Source:** Apr 1 — LordHelmet: "DDs are always positive." DD was +$1B to +$8B all morning despite price action going both ways.
**Why:** In moderate VIX (22-26), put hedging structurally dominates → DD is positive by default. Treating DD+ as "bullish" in this regime is misleading — it's just the baseline. Only when DD goes NEGATIVE in this VIX range is it meaningful (means institutional selling overwhelmed structural put hedging).
**Apply:** At VIX 22-26: DD positive = "neutral/default, no signal." DD negative = "unusual, bearish signal — institutions overwhelmed structural hedging." Adjust R2/R3 thresholds: DD+ needs to be >$5B to mean anything, DD- at any level is noteworthy.
**Confidence:** MEDIUM (Apr 1 observation, LordHelmet quip. Need multi-day validation at this VIX level.)

---

## SESSION SCORES

| Date | Direction | Timing | Actionability | Mechanics | Overall | Key Learning |
|------|-----------|--------|---------------|-----------|---------|-------------|
| 2026-03-25 | 7/10 | 3/10 | 4/10 | 5/10 | **5/10** | Don't wait for DD at open. Beach ball. Sell pops while DD-. |
| 2026-03-26 | 8/10 | 6/10 | 5/10 | 6/10 | **6/10** | System nailed direction (all shorts). Vol buyside = trend signal. Liquidity = pull not support. |
| 2026-03-27 | 4/10 | 5/10 | 6/10 | 7/10 | **5.5/10** | Called long initially—wrong. SVB 1.57 was the signal. DD useless (8 flips). Vol seller call wrong. Corrected to short too late. |
| 2026-04-01 | 4/10 | 5/10 | 5/10 | 6/10 | **4/10** | Morning bearish bias wrong (gave user's "sell" thesis too much weight). DD floor call at 9:48 correct. PM sell + paradigm flip called correctly. ZERO macro awareness (Iran headlines, CL, Trump speech). |
| 2026-04-02 | 6/10 | 5/10 | 5/10 | 6/10 | **5.5/10** | Morning short correct (DD -$2.65B, SIDIAL-EXTREME, gap -78). Warned beach ball but didn't LEAD with it. V-reversal +89 pts from LOD. DD flipped to +$16.4B. 4 paradigm flips = unstable. CVD -4K vs +89 rally = absorption. |

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
- [ ] Order flow / worm / vol seller-buyer flips — VX Futures via CFE/Rithmic (need to check if enabled)
- [ ] CL / VX multi-asset correlation — **CRITICAL GAP** (R46: CL drives ES on headline days, Apr 1 proved)
- [ ] Fixed-strike vol tracking for vanna support validation
- [ ] Real-time continuous monitoring (structural: I check when asked)
- [ ] Discord live feed integration
- [ ] Macro headline monitoring (R46: biggest blind spot, scored 1/10 Apr 1)

## VALIDATED RULES
- **Rule 1 (VALIDATED Mar 26):** Don't wait for DD — first signals at 09:49-10:06 were all shorts, all correct
- **Rule 2 (VALIDATED Mar 26):** "Sell pops while DD-" — DD stayed negative, every short was a winner
- **Rule 3 (RE-VALIDATED Apr 2):** Beach ball effect. DD -$2.65B → +$16.4B on gap-down Liberation Day. SPX rallied +89 pts from LOD 6,483. Textbook snap-back from extreme.
- **Rule 5 (RE-VALIDATED Apr 1):** DD instability. 8+ magnitude flips ($5B+ swings). DD was noise all day. Confirmed R32.
- **Rule 7 (VALIDATED Mar 27):** Paradigm flickering = chop zone. 3 flips in 1 hour, no paradigm call worked.
- **Rule 10 (RE-VALIDATED Apr 1):** Charm is EOD force. -$3B charm at close dominated PM selling from 2 PM on.
- **Rule 22 (RE-VALIDATED Mar 27):** Fewer trades = higher P&L. 53 signals, V12 filtered to 4, still lost. Apollo took 0.
- **Rule 25 (VALIDATED Mar 26):** Wizard long vega paying with VIX >20 all March
- **Rule 31 (VALIDATED Mar 27):** VIX rising while SPX drops = trend day. SVB confirmed mechanically (1.57→2.18).
- **Rule 36 (VALIDATED Apr 1):** Macro override. Iran ceasefire tweet → HOD spike. Iran denial → PM selloff. Greeks were secondary.

## INVALIDATED
- **Apollo vol seller proxy (Rule 29):** Vol seller call at 10:16 Mar 27 did NOT cap the move. Without VX Futures, vol flow is unreliable. Wait for Apr 1 data.

## VALIDATE NEXT SESSION
- Rule 32: VALIDATED Apr 1+2. Move to validated list next cleanup.
- Rule 34: VALIDATED Apr 2 (4 paradigm flips = unstable, no paradigm call reliable). Move to validated list next cleanup.
- Rule 40: RE-VALIDATED Apr 2. DD +$16.4B = forced buying into close, SPX rallied +89 from LOD. Extreme DD is the signal.
- Rule 46: CL/crude correlation — next headline day, track CL vs ES tick-by-tick
- Rule 48: Mean reversion — next GEX-PURE day, check if midpoint is better target than Volland target
- Rule 50: DD+ default at VIX 22-26 — check multi-day if DD+ is persistent regardless of direction
- Yahya's CTA exhaustion: Apr 1 gapped up +35 from Mar 31 (CTA max short). PARTIALLY VALIDATED — gap up occurred but didn't sustain intraday.
- Rule 43: Vanna cascade — still needs clean breakout above negative vanna cluster
- Rule 38: Vanna vacuum — still needs low-vanna day
