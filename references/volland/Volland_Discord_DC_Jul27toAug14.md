# Volland Discord — curated extract, 2026-07-27 → 2026-08-14

Window #10. Sources: `⛅│volland-daytrading-central` (4,383 msgs), `📨┃0dte-alerts` (28),
`⚛️┃dark-matter-trade` weekly plans 7_20 / 8_3 / 8_10.
Mined 2026-08-14 (session 123). Prior window: Jun 21 → Jul 27.

**Market context:** violent V-bottom off the 7/29 FOMC (Warsh) flush at SPX ~7,294, then a
near-vertical melt-up to 7,800+ by 8/13. Every fade lost. Every dip-buy won.

> **yahyaz, 8/4 6:14 PM:** "everyone trying to fade this intraday will eventually be right in X
> amount of time but will have missed days of straight rippers. No one can call tops, no person,
> no service"

---

## ⭐ THE FOUR TO BUILD FIRST

### 1. GATE vs WALL — the missing continuation setup

The highest-value idea in the corpus. Fully specified, and every input is already in our DB.

A level near spot is either a **WALL** (dealers net-LONG gamma → pushes stall, fade it) or a
**GATE** (dealers net-SHORT gamma → hedging chases, it accelerates). **Trading a gate as a wall is
what killed everyone in this window** — and it is exactly what our GEX Long / AG Short / Skew Charm
logic does by construction.

> **darkmattertrade, 8/10 plan:** "The door is SPX 7,800 / ES 7,825, the one place near the market
> where the hedging book is short gamma (−$28.1M at that gate). It starts nothing on its own, but
> once price accepts above it, hedging chases instead of absorbing and the move runs."

> "The polarity map alternates rather than running smooth — positive from SPX 7,700 up to 7,790,
> negative at the 7,800 gate (−$28.1M), positive again at the 7,820 catcher (+$7.6M) and the 7,850
> magnet (+$15.4M), negative at the 7,875 pocket (−$7.7M)…"

**Trigger (verbatim):** "The tell for a break UP is a session close above SPX 7,800 that then holds
SPX 7,820 on the retest — that is the catcher, positive on every horizon, and it is what turns a
poke into travel."
**Invalidation:** "a full session close beneath SPX 7,700 that is not reclaimed in that same session
OR the following session."
**The hard rule:** "**A wick through either edge is not a tell; a close is.**"
**Sizing:** "dealer gamma is net-short here, so whichever side moves first gets pushed rather than
absorbed — the stop and the target both arrive faster than the levels suggest. Take the smaller size
and make the trigger prove itself before adding."

Repeated as a prohibition three times across two weeks: *"Don't short SPX 7,800 on the first touch —
it is this week's gate, not a ceiling."*

**Build:** classify each strike within ~1% of spot as wall (net-long dealer gamma) or gate
(net-short) from `volland_exposure_points` + chain GEX; require a CLOSE beyond a gate plus a hold of
the next positive-gamma strike on retest. This is the exact inverse of our existing fade logic.

### 2. VIX/VIX3M RATIO as a regime gate — a named study, thresholds given

We store VIX and VIX3M and use `overvix = VIX − VIX3M`. The room uses the **ratio**, with levels.

> **bigbill8887, 8/13 3:30 PM:** "Lower readings (e.g. <.8) historically have been a prudent time to
> start reducing leverage, closing positions etc. Also it is @Apollo study."
> **8/13 4:32 PM:** "Approaching .8 (from above) and below more or less means further upside will be
> difficult. Approaching 1 (from below) and above generally means we are due for a bounce."

> **apollobix, 8/7 7:39 PM** (asked whether to act intraday): "**I always wait till the close to get
> confirmation.**"

**Dissent worth testing — otc4313, 8/13:** "the value in this ratio is paying attn to the
**direction of change more than an absolute value**, such as 0.785 coming from 0.82 vs coming from
0.725" and "why not look at **VVIX/VIX or VIX9D/VIX**?"

Live: VIX/VIX3M closed **0.785** on 8/13 ("spicy" — yahyaz).
⚠️ Cross-check against S248 — a prior VIX/VIX3M "regime gate" turned out to be a NULL-coercion bug
in my own query, so this needs a clean re-test, not a re-run.

### 3. RASCHKE 4-DAY GAP RULE — cleanest multi-day rule in the corpus, testable tonight

> **Linda Raschke** (posted by toto2229, 8/7): "Learned this rule on the floor 45 years ago and it
> still holds true **90% of the time** (or so it seems): 'If a stock does not trade into its GAP area
> the next 4 days, it can continue in the direction of the gap for 2 weeks.'"

Testable on daily OHLC alone. Natural seed for the swing book.
**Disagreement — gammahivey, 8/7:** "I'm careful with gaps below price on bounces like these; there
are a ton that never fill!" (his work is on NDX). apollobix: "Futures gaps are different I think."

### 4. LBAF / LAAF at the IB extreme + CVD divergence — reuses what we already compute

Look-Below-And-Fail (long) / Look-Above-And-Fail (short) at the initial-balance extreme, confirmed by
delta divergence and a volume spike, targeting the **opposite IB extreme**.

> **disciple3, 8/11:** "es lbaf" / "bullish div" / "**but ib low valid now to ib high**" ← the target
> **disciple3, 8/6:** "very nice LAAF ib high with divergence short, all you needed"

Everything needed is in `vps_es_range_bars`. Note this targets the opposite IB extreme rather than
our fixed +10 pt.

---

## OTHER MECHANIZABLE IDEAS

| # | Idea | Who | Detectable? |
|---|---|---|---|
| 5 | **Vanna sign × vol direction**: "Positive Vanna + vol up = dealer selling pressure / Negative Vanna + vol up = dealer buying pressure — just needed to watch vanna" | otc4313 8/13 | Yes — Volland vanna + VIX. Contradicts the naive "positive vanna = magnet" read. |
| 6 | **Out-of-bounds line = 1.5 × opening straddle from the pin.** "a push past it forces immediate hedging rather than absorption" | darkmattertrade | Yes — ATM straddle from the 09:30 chain; pin = peak same-day gamma strike. **This is the switch that tells a momentum detector it is allowed to fire.** |
| 7 | **Buy the 5-min opening-candle retest.** "you guys know if you just buy the 5m opening candle retest you are profitable over time / cause shorting loses most of the time" | disciple3 8/7 | Yes. No stop/target given, but a complete entry rule. |
| 8 | **Volume veto on shorts**: "no high volume topwick no short for me" (stated 5×) | disciple3 | Yes — same shape as our existing ES Absorption 1.5× gate. No threshold ever given. |
| 9 | **"Vol runs one day ahead"** — undervix today → upside tomorrow. "Today's upside from yesterday's strong undervixing." | wizardofops 8/13 | Yes — `overvix` exists; the new part is using YESTERDAY's close as a next-day directional signal. One-line backtest. |
| 10 | **Pyramid on pullback (B.E.R.)** — "buy every red candle", add on each level reclaimed | disciple3 | Yes, as a sizing rule. Note he posted the opposite (add on green) on 7/29 and never reconciled them. |
| 11 | **Negative-vanna air pockets as a swing map.** "look for negative total vanna or pockets of negative Vanna that we can slide into… if we fell below 7700 there is not a lot of support and we can slide lower" | bigbill8887 8/13 | Yes — per-strike vanna above/below spot. "Distance to next negative-vanna pocket" is a direct build. |
| 12 | **Vanna expiry roll-off as a swing calendar.** "The next big negative vanna is september and December" / "Late August is when the Sept vanna will get spicy" | wizardofops | Yes — total vanna expiring on date X, and the change the day after each expiry. |

**Specified but NOT reproducible:** otc4313's 76%-WR "angle of ascent" system (needs Murrey Math
lines, not in DB); apollobix's limit-order-book overnight signal (needs full DOM); otc4313's 0DTE
**DAG** continuation gate (the scraper captures charm / vanna ×4 / gamma ×4 / deltaDecay — **not DAG**).

---

## 🎲 THE "GOLDEN GATE" — biggest claimed edge, worst specified

Cited constantly, **correct every time it was called**, and never defined despite two members asking.

> **disciple3, 8/2:** "Atr level completion percentages based on 10 years of spx back testing… we
> opened the golden gate on the opening candle and **91% of the time we have extension to the third**
> in that case before close."
> **7/31:** "we hit it on open, **80% to complete before 10, 91% in general today**"

Called live with the target price on six dated sessions: 7/29 (7379, short), 7/31 (7491), 8/3 (7545),
8/4 (7657), 8/12 (7778), 8/13 (7800).

The cleanest test — 7/31, after SPX had fallen ~100 pts away from the target:
> "You believed in the 91% stat. @LordHelmet didn't. Even as we hit 7400 the 91% chance to hit 7491
> stood until close lol. Astounding. Casual 100pt spx long from lis support"

**Unresolved.** Asked twice (meow334641 8/3, mark487. 8/1), never answered. Inferable: a ladder of
ATR-derived levels off a daily anchor; "opening the gate" = the opening candle reaching level 1;
claim is level 1 → level 3 before the close, 91%. **Six dated calls with target prices are enough to
recover the spec by grid search.** Highest upside / highest uncertainty item in the window.

---

## 🚨 IDEAS THAT CONTRADICT OUR CURRENT SETUPS

1. **LIS is a pivot, not a fade level.**
   > **l0rd.helmet, 8/13:** "**The LIS means that dealers have to go with the move to hedge.** So it's
   > a pivot."
   > **yahyaz, 8/13:** "**LIS = PT when to the upside / LIS = local low when to the downside**"

   Our setups treat LIS as support/magnet to fade around (±5 pts). This says on an up-move it is a
   *target*, on a down-move a *local low*. Directly testable, and it bears on GEX Long + BofA Scalp.

2. **Delta Decay > vanna > charm for 0DTE — a direct instruction.**
   > **wizardofops, 8/3:** "**Vanna has less of an impact on 0DTE. My recommendation if doing 0DTE is
   > use Delta Decay**, it is a much more accurate look at the hedging dealers need to do at the end
   > of the day." Asked if DD until 2pm then charm: "**I'd say if anything the other way around, but
   > I'd always use DD.**"

3. **Late DD data can be deadly.** ⚠️ check our DD Exhaustion late-session behaviour.
   > **tommyp53, 8/5:** "heavy calls suddenly appeared on DD at 1558… it is the delay from the
   > exchange to subscribers (Volland)… **late data for 0 dte can be deadly**"

4. **Swing-holding period must match the term-structure row it was read from.**
   > **darkmattertrade, 8/10:** "the SPX 7,700 cushion recommends a 5-to-10-day holding window, and
   > that thesis window maps onto the two-week row rather than the weekly one — so the DTE to trade
   > it with is the 8/21 monthly contract, not this Friday's. **Trading that thesis on a weekly DTE
   > would put the option's expiry inside the holding period the read itself asks for.**"

---

## 📄 REFERENCED DOCUMENTS — to fetch

| Document | Why it matters |
|---|---|
| **Volland User Guide, May 2026 edition** — `https://vol.land/VollandUserGuide_May26.pdf` | **Highest value.** Posted by mrswizardofops in answer to "is there any literature on how to correctly use DD?" — contains the Delta Decay section behind wizardofops' "always use DD for 0DTE" instruction. **We hold the Dec-2025 edition; this is newer.** |
| **Wizard of Ops free Substack** | Referenced 5× — FOMC fade rule, the 7500–7525 peak-till-opex call, a "dealer volatility" post. |
| **Cem Karsan — "The World Is Short S&P Calls"** `youtu.be/W4qsUqpyAOQ` | The continuation mechanism for spot-up/vol-up. wizardofops explicitly disputes it (see disagreements). |
| **OddStats seasonal/gap studies (3)** | All reproducible from daily OHLC + VIX. Monday gap-up + VIX 15-20 after a Thu/Fri drop: 9 occurrences, only 1 faded. |
| **Linda Raschke** — gap rule + a 15-period pit-session EMA stat ("first time in 35 years") | Both testable on daily data. |
| **sqzme / SqueezeMetrics newsletter** | Quoted verbatim 4×, including a 0DTE band read: "up at the Mid band (around 7615) we get over $75mm in selling all at once… stop out that trade pretty quickly (by 7630?)". They puked their 7200 puts on 8/7. |
| Volland **Institutional Skew** (paid tier) | wizardofops: "Volland Institutional Skew was a savior." Not in our data. |

---

## ⚔️ DISAGREEMENTS = TESTABLE FORKS

1. **Spot-up/vol-up: reversal or continuation?** wizardofops says reversal, and gave the only
   probability triplet of the window — *"about **60% chance of retrace, 15% chance of massive
   breakout, or 25% chance of flat**"*. **The 15% branch hit.** He said so himself: *"I have never
   seen this before… I have no prior occasions of this happening in the dataset."* Cem says
   continuation into a blow-off. attackofthejax splits it: 8/3 was ATM/downside vol staying bid, 8/4
   was an upside-skew reprice — *"if the upside skew continues to reprice higher then there is
   continued vanna dealer buying pressure but if it compresses they'll sell deltas."*
2. **Gap fill:** Raschke's 4-day continuation vs gammahivey's "a ton never fill" (NDX).
3. **LIS:** fade level vs trend pivot / price target.
4. **VIX/VIX3M:** absolute level vs direction of change vs use VVIX/VIX or VIX9D/VIX instead.

---

## ⚠️ THE ROOM'S OWN VERDICT ON VOLLAND IN THIS REGIME

Recorded because our entire system is Volland-driven:

> **disciple3, 8/4:** "**dont analyze, dont open volland, just buy**"
> **disciple3, 8/4:** "please put the vanna chart down, fraud is much stronger"
> **yahyaz, 8/13:** "save some of your pc's memory and no need to open volland"
> **disciple3, 8/12:** "**i have an OR/sweep model that is blowing anything i can come up with on
> volland away tho**"
> **lateralus_05051, 8/13:** "It doesnt seem like these lines in sand are really lines in the sand.
> We just blow thru them."

Counterweight — who kept it and made money: wizardofops (Institutional tier, +2% for the month),
darkmattertrade (whose 8/10 pin and floor calls were correct all week), apollobix, and mark487.'s
+120% Volland-sourced debit spread.

**Read this as regime-conditional, not as a verdict on the data.** Levels stop being levels when
dealer gamma is net-short — which is precisely what the gate/wall classifier (#1) exists to detect.

---

## WHO MADE MONEY, AND HOW

| Trader | Method | Result |
|---|---|---|
| **disciple3** | 1 MNQ scalps, long-only, buy-every-dip, copied across ~15–20 prop accounts | Biggest earner by frequency. "1200 on each today. 1-2mnq. **why did trading get so easy when sizing down**". Blew Phidias accounts **twice** trying to short. |
| **otc4313** | ES/NQ order flow + Murrey Math, R-multiples | +8R→+10.25R (7/27); first five-figure day in 3 months (7/29). |
| **yahyaz** | 0DTE momentum long scalps funding a bleeding left-tail swing put book | "Slow bleed on the swings while intraday brrrrr / Net green" |
| **wizardofops** | Long calls + long vega, flat before vacation | +2% for the month. His directional reversal calls on 8/3–8/4 were **wrong** and he said so. |
| **Losers, uniformly** | Everyone short — dauma8500, jk232323, dragonboys, zackhoski, sqzme | — |

**Sizing lesson that matches our own S244 finding** (more positions beat bigger positions):
disciple3's whole edge was 1 MNQ × 20 accounts, and toto2229's note on why props push the opposite —
*"props want you to oversize… 'Maximum of 2 minis per account' - you'll blow the F up trading 2 minis."*

**Discipline rule for when we add a swing book — yahyaz, 8/4:**
> "as long as you dont let the swing position affect your intraday trading, its all fine. **the second
> you stop believing in upside because you have puts bleeding on the other account, its over you lost**"

---

---
---

# PART 2 — DARK MATTER WEEKLY PLANS (7_20 · 8_3 · 8_10), GRADED

Graded against **our own `spx_ohlc_1m`** (390 bars/session, complete RTH), not against a narrative.
8/14 has no bars in the DB yet, so week 3 is graded on 4 of 5 sessions.

⚠️ **His ES↔SPX basis changes every week** — stated as +40 (7/20), +37 (7/22), +30 (8/3), +25 (8/10),
+22 (8/14). His ES and SPX levels are **not interchangeable across documents.**

## Scoreboard

| | Week 7/20 | Week 8/3 | Week 8/10 |
|---|---|---|---|
| Regime read | ✅ RANGE | ✅ transition→long | ✅✅ range→upside break |
| Direction | ✅ short, −0.60% | ✅ long, **+3.55%** | ✅ long, +0.56% |
| Levels | ceiling ✅ on closes / floor ❌ | 7,600 magnet ✅✅ | ✅✅ all of them |
| Swing / structural | ✅ withheld correctly | ✅✅ 7,300 paid; 7,600 reclassified | ✅✅ **+74 pt 2-day hold** |
| Same-day setups | 1 win / 3 losses | Grade A **LOST**; posted short **LOST** | Grade A **WON** big |

**The honest pattern: his swing and structural work is where the money is. His intraday setups are
roughly a coin flip.** Grade A went **1–1** across the corpus, so the letter grade carried no
observable information — **do not port the grading scheme.**

### The three standout calls

1. **8/3 — the best call in the corpus.** *"the overhead 7,600 call-wall dissolved (vanna collapsed
   66–80%) → 7,600 now a magnet on a break, not a wall to fade."* 8/3 closed **7599.97 — 0.03 pts
   from the magnet**, then ran to 7757.89 the next session. A Greek regime change read correctly
   ahead of a 260-pt move.
2. **The pin, twice.** 8/10 pin 7,750 → closed **7749.36** (error **0.64 pts**). 8/12, on CPI day,
   closed **7748.71** (1.29 pts).
3. **8/11 LONG B, Grade A** — entry 7,702–7,726, stop 7,676, T1 7,750, T2 7,800. Filled at the top
   of the zone 13:16 on 8/11, stop never approached, T1 hit 8/12, T2 hit 8/13. **+74 pts on 50 pts
   of risk over a 2-session hold.** The best trade in the corpus — **and it was a swing, not a scalp.**

### ⚠️ A design lesson from his 7,400 gate — from our own data

His rule: *"the lower shelf stays unnamed as a target until the market proves it by closing a full
session beneath SPX 7,400 — it has been named three times before and reached none of them."*

The gate correctly suppressed **three false starts**. It then fired on **7/29** — the only close
below 7,400 in the whole window (7320.23) — on the **same session** that ES printed 7,324.25 low =
SPX 7,294 at his +30 basis. **The gate and the destination arrived on the same bar.** A mechanical
"short on a daily close below 7,400, target 7,300" would have entered at 7320 with the target 20 pts
away.

**A pure close-confirmed swing gate is safe but can be too slow to be tradeable.** Pair it with the
week-over-week Greek decay (below) as the early warning.

### His execution failures — port the method, not the setups

- **8/4 LONG A** was posted at 7:24am quoting *"SPX 7,620"* with an entry zone of **7,575–7,600** —
  20–45 pts *below* the price in the same message. It never filled (8/4 low 7629.49). **If we
  mechanize a setup generator, assert the entry zone is reachable from spot.**
- **8/3 posted Setup 1** was SHORT 7,530 stop 7,540 — and the Anchor two paragraphs later says
  *"Don't re-short the 7,515-7,530 cap now that it has broken."* Same document, opposite
  instructions. Stop hit within minutes.
- **8/5 "trending, leaning long, high conviction"** was issued at 7,775.58 — **18 pts below the high
  of the entire advance.** The trend label arrived at the top.
- **8/14 update** says *"targeting 7900spx with stops at 7850 and 7875spx"* — those sit **above** a
  long entry near 7,750. They are scale-out levels mislabelled as stops.

---

## THE METHOD, AS AN ALGORITHM

```
STEP 1 — SIZE GOVERNOR (runs FIRST, overrides everything)
  SVB < 0        → HALF SIZE, mean-reversion ONLY, momentum/cascade VETOED
  SVB ≈ 0 (±0.1) → small-to-half, tight stops, veto LIFTED
  SVB > 0        → normal
  Then stack further caps: own recent accuracy, weak breadth, catalyst proximity.

STEP 2 — CLASSIFY REGIME
  +gamma, timeframes NOT aligned                    → RANGE, two-sided
  +gamma, timeframes aligned, −gamma slice at edge  → RANGE TRANSITIONING TO BREAKOUT
  gamma softening + a cap broken and reheld         → IN TRANSITION, tilt
  all timeframes aligned + weight below > above     → TRENDING

STEP 3 — SET THE LEAN FROM WEIGHT, NOT FROM THE CHART
  Sum dealer weight above vs below spot within ~3%.
  Price sitting UNDER its own structure → lean is UP into it, however bearish the chart looks.

STEP 4 — BUILD THE LADDER (4 tags per rung)
  price in BOTH SPX and ES with the basis stated
  + dominant second-order Greek and signed $ notional
  + term-stack tag [0][W][2W][M][all]
  + role from a FIXED vocabulary:
      +gamma  → wall / pin / cap        (fade INTO it)
      −gamma  → gate / door             (trade the BREAK)
      +vanna  → magnet                  (price pulled toward)
      −vanna  → pocket / repellent      (take profit INTO, never target THROUGH)
      charm 0-cross → the 0DTE pin
  Delta is POWER only — how violent, NEVER direction.

STEP 5 — INVALIDATE ON CLOSES, NEVER WICKS
  State the timeframe: 15-minute / session / daily. Sometimes two-session.
  Gate every deep target behind a confirmed close, and track how often it has been named and missed.

STEP 6 — PICK THE DTE FROM THE TERM STRUCTURE, NOT FROM CONVICTION
```

### What changed vs the method we had on file

| We believed | These three weeks show |
|---|---|
| "Low vol → fade the box at HALF size" | **Half size is triggered by SVB SIGN, not by VIX level.** Week 3 (VIX **14.59**, bottom decile) got a full plan with a Grade A long; week 1 (VIX 18.3 but **SVB −0.12**) got half size. |
| "Magnets invert above ~67th pct VIX" | He says 67th pct is **below** the line: *"which is below the threshold where that polarity inverts."* **The threshold is higher than 67 and never fired in three weeks — uncalibrated. Do not build this yet.** |
| Levels are a static map | **NEW — a level's ROLE changes week to week, and that change is the highest-value signal.** |
| Delta is one of the Greeks | **NEW — delta demoted to POWER only.** In week 1 it was the dominant tag at most rungs; by week 3 it is stripped out: *"Dealer delta is POWER, never a driver."* |
| — | **NEW — an accuracy ledger that feeds SIZE only.** *"Direction comes from the structure; size comes from that record."* |
| — | **NEW — DTE must match the thesis window** (see below). |

---

## 🚀 THE FADE → CONTINUATION SWITCH — four gates, all four required

This is the concrete recipe for the momentum setup we do not have.

**Gate 1 — SVB must not be negative. An absolute veto that outranks a valid signal.**
> 7/23: *"no breakout-chase setups (negative spot-vol reading) ... which pauses every trend-following
> play — **no breakout-chase setups appear in this plan even though the upside magnet at SPX 7,600 is
> real.**"*

**Gate 2 — a DOOR must exist: a negative-gamma (permissive) slice at the range edge.**
A **long-gamma edge = fade it. A short-gamma edge = trade the break.** Same price, opposite trade,
decided entirely by the sign.

**Gate 3 — a CATCHER beyond the door:** *"Positive in both gamma and vol-sensitivity on every
horizon, so a break has something to land on."*

**Gate 4 — timeframe continuity aligned.**

| Element | Rule |
|---|---|
| **Confirmation** | 15-minute close, not a wick. *"A wick through either edge is not a tell; a close is."* |
| **Entry** | **Almost never the breakout itself — the first pullback into a defined shelf after acceptance.** *"buy pullbacks into the 7,610-7,700 delta cushion toward 7,800, don't chase the extended tape at the highs"* |
| **Target** | **Into the pockets, never through them.** *"Don't set a target on the far side of SPX 7,950 — a target beyond an acceleration pocket either never fills or fills on a gap you cannot work."* |
| **Stop** | The reclaimed level / the catcher. **Stop TYPE differs by horizon:** intraday = price stop; swing = **daily-close** stop. |
| **Size on a break** | Smaller. *"whichever side moves first gets pushed rather than absorbed — the stop and the target both arrive faster than the levels suggest."* |

## 📅 THE SWING BOOK — run separately from the weekly plan

He carries ~2 multi-week theses at a time, each with a level, a confirmation count, a gate, and a
prescribed DTE.

| Rule | Verbatim |
|---|---|
| **Swing eligibility comes from TERM STRUCTURE, not conviction** | *"what survives Friday is what a swing trade can lean on"* · *"The near-spot levels at 7,475 and 7,440 are the opposite: they wipe at Friday's close and should be traded with short-dated options, not carried."* |
| **A target is not named until a close proves it** | see the 7,400 gate above |
| **DTE is prescribed per thesis** | *"scale-in-and-roll (28–42 DTE)"* · *"a 5-to-10-day holding window... the DTE to trade it with is the 8/21 monthly contract, not this Friday's"* |
| **Add at the defended level on a retest — never inside profit** | *"positions get added at the defended level on a re-test that holds, not inside the profit zone."* |
| **Catalysts override structure on holds** | *"Anything held past Wednesday morning is held into the inflation print, not into structure."* |

---

## 🔧 WHAT WE CAN MECHANIZE — ranked, with the input named

| # | Step | Our input | Value |
|---|---|---|---|
| **a** | **SIZE governor + momentum veto** | `volland_snapshots.payload->statistics->spot_vol_beta` (already captured ~2 min; `vol_event_alert.py` already reads it) | **Highest.** ⚠️ **Our note says "SVB as a trade filter was REFUTED 2026-05-30" — but that tested it as an ENTRY filter. This is a SIZE multiplier + a momentum-only veto. Different test. Must be re-run, not assumed dead.** |
| **c** | **DOOR + CATCHER detector — the missing momentum trigger** | Volland per-strike gamma + vanna multi-expiry (already on `/darkmate-fw`) | Scan strikes above spot within ~1.5 ADR → first strike where gamma flips **negative** while those below are positive = **door**. Next strike positive in **both** gamma and vanna on **all** expiries = **catcher**. Both present → breakout ARMED. |
| **i** | **Week-over-week level delta** | Our stored `volland_exposure_points` history | **Second highest.** His single best call was *"vanna collapsed 66–80%"* — a percentage change on a time series **we already store and currently never compute.** |
| **h** | **Swing eligibility + DTE** | Volland exposure tagged `[0][W][2W][M][all]` | Weight in `[0]/[W]` → intraday only. Weight in `[M]/[all]` → swing-eligible, DTE = that window. **Mechanical core of the missing swing setup: never hold against a level that expires inside your hold.** |
| **m** | **Acceptance test** | **ES 5-pt range bars + CVD** | His rule is "a 15-min close, not a wick." Ours can be better: N consecutive range-bar closes beyond the level **with CVD confirming**. **This is the one place our data beats his** — and it reuses the ES Absorption divergence machinery, inverted. |
| **e** | 0DTE pin + its intraday **migration** | 0DTE chain Greeks @30s | charm 0-cross ∩ vanna 0-cross ∩ 0DTE gamma peak. He was within 0.64 and 1.29 pts. **A migrating pin is itself a momentum tell.** |
| **f** | Out-of-bounds band | ATM opening straddle from the 09:31 snapshot | pin ± 1.0–1.5 × opening straddle. A ready-made **mean-reversion kill switch**: inside = MR valid, outside = dealers hedge WITH the move. |
| **j** | Asymmetry / lean | Per-strike weight within ±3% of spot | Σ above vs Σ below. `/darkmate-fw` already computes `gamma_below − gamma_above` — this is that, generalised. |
| **b** | Gamma sign: aggregate vs at-spot | GEX + Volland gamma per strike | Disagreement = his "accelerant pocket". This is `gex_state` generalised, and v7 already proved a gamma-state gate has edge. |
| **d** | Level-role classifier | Volland per-strike by expiry | Deterministic lookup, all inputs present. |
| **g** | Vanna:gamma dominance ratio | Volland aggregate $ | ratio > ~20 → read the map vanna-first. He quotes 28:1 and 40:1. |
| **k** | Breadth proxy | 23-stock GEX scanner + 6-name tech basket | He uses breadth only as a caution modifier, **never a bias flip**, so a proxy suffices. |
| **l** | VIX inversion switch | VIX + VIX3M | **DO NOT BUILD YET — threshold never stated, never fired in 3 weeks, all evidence is on one side of it.** |

### ❌ Cannot be automated

The regime **label** (unstated, inconsistent weights — on 8/3 the header says *"Fade-the-edges week,
not chase-the-breakout"* and the Anchor says *"the fade is off"*, same document); the accuracy-ledger
override; catalyst interpretation; choosing between two contradicting readings of the same strike;
and **the A/B/C grades, which carried no information in this sample.**

### 🎯 Concrete build recommendation

**MOMENTUM = (c) + (a) + (m).** Arm a breakout only when a short-gamma **door** exists at the range
edge, a positive-everything **catcher** sits beyond it, **SVB ≥ 0**, and ES range bars + CVD confirm
**acceptance** rather than a wick. Enter on the **first pullback to the catcher**, not the break.
Target the next positive-vanna magnet; **take profit into the negative-vanna pocket, never through
it.** Stop at the reclaimed gate.

**SWING = (h) + (i) + a close-confirmed gate.** Trade only levels whose weight lives in `[M]/[all]`;
size the option DTE to that window; refuse to name a target until a daily close through the gate;
add at the defended level on a retest that holds. **Pair the gate with (i) as the early warning** —
see the 7/29 lesson above.

Both fit inside the existing `/darkmate-fw` data model, which already pulls multi-expiry gamma +
vanna per strike near spot.

---

## WHERE THE ROOM WAS JUST NOISE

No systematic single-name / stock-GEX work at all — **our 23-stock scanner is ahead of what this room
does on stocks.** No quantified swing study beyond Raschke + OddStats. mark487.'s question of whether
"a greater DAG cumulative overpowers a less cumulative charm" was asked twice and never answered.
Roughly half the message volume is prop-firm logistics, cars, and memes.
