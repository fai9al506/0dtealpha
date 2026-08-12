# Exelza — "GEX Dealer Positioning" · From Reading to Decision

**Source:** 19-page Arabic beginner's guide by Exelza (`exelza.com`), supplied by the user
2026-08-11. Original images: `C:\Users\Faisa\Downloads\GEX guidance\6043893548647649734..752.jpg`.
**This file:** faithful English transcription of the full text, so the material is searchable and
citable without re-reading the images. Figure descriptions are summarised; all rules, thresholds
and tables are reproduced verbatim in meaning.

**Our test of it:** `S244_GEX_FRAMEWORK_STUDY.md` (what held up on our book, what didn't).
**Our implementation:** `app/gex_state.py` + portal GEX tab. Memory:
`research_gex_state_v7_gamma_support`.

**Contents:** Ch.1 reading the page · Ch.2 the pre-trade plan · Ch.3 combining tools ·
Ch.4 signal strength vs reliability + full glossary · Ch.5 advanced search

---

## The core idea in a minute

When you buy an option (Call or Put), the counterparty is usually the **market maker (dealer)**.
The dealer does not want to bet on the direction of the stock, so they **hedge**: they buy or sell
actual shares to stay neutral. Every time price moves, they are forced to adjust that hedge.

**GEX (Gamma Exposure) measures exactly this: how many shares will the dealer be forced to buy or
sell if price moves?** It matters because their hedging — by sheer size — moves the market itself.

### The Golden Rule

- **Positive GEX** → dealers **sell into rallies and buy into dips** → they act as a **shock
  absorber** → the market tends to hold together inside a range.
- **Negative GEX** → dealers **buy into rallies and sell into dips** → they act as an
  **amplifier** → moves become violent and accelerating.

---

## Chapter 1 — Understanding the GEX Dealer Positioning page

### The six top cards (read on a real SPY snapshot)

Snapshot used: NET GEX 1297.5B positive · ANALYSIS SPOT $772.93 · ZERO GAMMA $772.13 ·
MAX GAMMA $773.00 · PUT WALL $520.00 (−32.72%) · CALL WALL $780.00 (+0.91%) ·
NET DEX 18.0B (Call 23.1B / Put −5.1B) · SIGNAL: HIGH VOLATILITY / BUY, "Previous session" badge.

| Card | Value in the snapshot | What it means for a beginner |
|---|---|---|
| **NET GEX** | 1297.5B positive | Dealers are in "calming" mode — expect price cohesion, not collapse. |
| **ANALYSIS SPOT** | $772.93 | The price the analysis is built on, with the source time. Below it, **Live Spot** for comparison. |
| **ZERO GAMMA** | $772.13 | The break-even line: above it the market is "calm", below it, it flips to "volatile". **The most important level on the page.** |
| **MAX GAMMA** | $773.00 | The strike with the largest gamma — usually acts as a **magnet** pulling price, especially near expiry. |
| **PUT WALL** | $520.00 (−32.72%) | The biggest put concentration = **expected support**. The percentage tells you how far it is from the current price. |
| **CALL WALL** | $780.00 (+0.91%) | The biggest call concentration = **expected resistance** (in this snapshot, less than 1% away). |
| **NET DEX** | 18.0B (Call 23.1B / Put −5.1B) | Directional flow tilt: positive = buying pressure in the options market, negative = selling pressure. |

### The SIGNAL card — how it is actually computed

The page does not guess; it applies fixed rules that combine **the price's position relative to
the levels** and **the direction of DEX**:

- Price **very close to Zero Gamma (less than 1%)** → `HIGH VOLATILITY`: a high-chop zone. The
  direction (BUY or SELL) is taken from Net DEX.
- **Positive GEX and price between the two walls** → `MEAN REVERSION`: expect a sideways range.
- **Positive GEX and price above the Call Wall with positive DEX** → `BREAKOUT TEST`: price is
  testing the resistance break with buying support.
- Other states such as `SUPPORT`, `SQUEEZE`, `CHOPPY` — full glossary in Chapter 4.

### Worked figure — QQQ, price on the knife's edge

Spot $720.86, Zero Gamma $721.15 — only **0.04%** above, so the page fired `HIGH VOLATILITY`; and
because Net DEX was positive (5.7B) the tilt came out **BUY**. Red bars = net negative gamma at
those strikes; blue dots = the DEX value at each strike. Note that the **weekly** call wall (green
line ≈ $729.5) differs from the card wall ($700) — **each expiry window has its own walls.**

### Worked figure — MSFT, a real breakout test

Spot $506.40, **above** the $500 call wall (the dotted green line), with positive GEX and positive
DEX → `BREAKOUT TEST / BUY`. Note the huge green bar at $500 — **that stacking is what makes the
wall real.** Zero Gamma at $497.52 has become support: as long as price is above it, the regime is
"calm".

### Reading the "Gamma Exposure by Strike" chart

- **Each horizontal bar = a strike.** Bar length = the size of dealer positioning there. A very
  long bar = a level the market will defend or stall at.
- The **Net** button shows the net (green = positive gamma, red = negative). The **Call / Put**
  button separates them: green for calls, red for puts.
- **Blue dots above the bars** = the DEX value at each strike.
- **Dotted lines:** green = Call Wall, yellow = Zero Gamma; the **solid line = current price**.
- **This Week / Next Week / This Month / Next Month** tabs filter by expiry. The zero week has the
  strongest effect on today's move; the monthly matters most for the bigger picture.

### Important beginner warnings

- **The walls move daily** — do not build a whole week's plan on today's wall.
- **Watch the data age** — `Source age` at the top tells you how old the analysis is, and a
  `Previous session` badge means it is last session's data.
- **GEX is context, not a buy button** — the signal tells you the nature of the field; you add
  your own analysis, the news, and capital management.
- **Level strength shows in the chart** — a wall built on a **single huge bar** (like $780 in the
  SPY figure) is stronger than a wall built on scattered positioning.

---

## Chapter 2 — The pre-trade plan: before you buy a Call or Put

Think of it as a **pilot's pre-flight check** — ordered steps, each answering one question.

### Step 0 — Is the data fresh?

Look at the top of the page: `Source age`. Open market and a data age in minutes = you are reading
the live state. A `Previous session` badge = you are reading the past session's map — excellent for
**pre-open planning**, but redo the check after the open, because the walls move.

### Step 1 — Determine the nature of the field: is buying options even a good idea today?

- **Big positive NET GEX** → a suppressed, range-prone market. A **hard environment for option
  buyers**: price is slow and time value (theta) eats your contract day after day.
- **Negative NET GEX** → an explosive market that amplifies moves. **The best environment for a
  Call or Put buyer**: one strong move pays for the contract and more.
- **Price glued to Zero Gamma** → a potential explosion zone in either direction. Option buyers
  love it, **on condition of waiting for the resolution.**

### Step 2 — Determine your initial direction: Call or Put?

| What you see | Tilt |
|---|---|
| SIGNAL shows **BUY** + Net DEX **positive** | preference for **Call** trades |
| SIGNAL shows **SELL** + Net DEX **negative** | preference for **Put** trades |
| The signal and DEX **conflict** | **do not force the trade** — wait for clarity |

Remember: this is a *preference*, not a guarantee — you are choosing to swim **with** the dealer
hedging current instead of against it.

### Step 3 — Choose the right expiry tab before reading the walls

A common beginner mistake: you buy a **weekly** contract and read the **month's** walls. Your
contract is this week → read the chart on **This Week**. Your contract extends over weeks →
**This Month / Next Month**. The QQQ figure illustrates it: the week's wall at ≈$729.5 while the
card wall is at $700 — **the one that concerns you is the wall of your contract's window.**

### Step 4 — Call decisions: when is it right, and where is it a trap?

**✓ When is a Call trade valid?**
- **A confirmed bounce off the Put Wall:** real money came in to defend support, and your target
  is clear.
- **A steady breakout above the Call Wall with positive DEX** — the MSFT case: buying is correct
  as long as price holds above $500, with a clear invalidation point beneath it.
- **A close above Zero Gamma after chop** — the QQQ case: instead of buying immediately, wait for
  confirmation above $721.15, and then the target is the week's wall.

**✗ The two common Call traps**
- **Buying a Call while price is glued to the Call Wall from below** — the SPY case ($772.93 with
  the wall at $780): dealers will sell into the rally, and the remaining space is often not worth
  the price.
- **A 0DTE contract with price stuck at Max Gamma on expiry day** — the **"pinning"** phenomenon
  nails price in place and melts your contract's value.

### Step 5 — Put decisions: the same logic mirrored

**✓ When is a Put trade valid?**
- **Breaking Zero Gamma downward with negative DEX:** the strongest Put scenario — the regime
  flips from calm to amplifying and the drop accelerates.
- **A failed breakout at the Call Wall** (a bearish rejection off it): the trade targets a return
  to mid-range.

**✗ The two common Put traps**
- **Buying a Put while price is directly above the Put Wall:** you are buying a fall *toward* a
  defended support — the profit space is short.
- **Buying a Put in strong positive GEX with price mid-range:** every dip will be bought
  automatically, and theta works against you.

### Step 6 — Before executing, define three numbers from the page itself

1. **Entry:** at / after level confirmation (a bounce off a wall, or holding above a broken level).
2. **Target:** the next wall in your direction (or the largest bar in the chart before it).
3. **Invalidation (stop loss):** behind the level you built the idea on.

> **The molten-zone rule.** If the signal is `HIGH VOLATILITY`, **widen your stop or reduce
> contract size** — the chop around Zero Gamma hunts tight stops in both directions before it
> resolves.

### Integrated example combining all the steps — the MSFT scenario

1. Data is from the previous session → this is a plan for tomorrow's open, to be reviewed after
   the open.
2. NET GEX positive (74.7B) → the move will not be a collapse; a disciplined trade, not a gamble.
3. Signal `BREAKOUT TEST / BUY` and DEX positive → preference for a Call.
4. My contract is weekly → I read the **This Week / Next Week** walls.
5. **The plan:** buy a Call **only if it opens and holds above $500** — target the **$508–$510**
   zone (Max Gamma at $510), invalidation on a close below $500. If it opens below $500 → **the
   idea is cancelled and I do not chase it**; instead I watch whether $500 turns into resistance
   (at which point the idea flips in favour of a Put toward Zero Gamma at $497.5).

---

## Chapter 3 — Combining contract momentum, options radars, and the gamma scanner

> **Golden rule: each tool answers a different question, and a good trade needs all three answers
> together.**

| Tool | The question it answers | Its role in the trade |
|---|---|---|
| **Contract momentum** | *What is moving right now?* | **Candidate screening** — contracts whose price and volume are accelerating this moment |
| **Options radars** (options flow, confluence) | *Who is moving it, and how much evidence do you have?* | **Verification** — is there big money behind the move? does independent evidence converge? |
| **Gamma scanner (GEX)** | *Where are we on the map?* | **Environment and levels** — does the terrain allow the trade to reach its target? |

> **Simple analogy.** Contract momentum is the **alarm** that alerts you something is happening;
> options flow is the **camera** that shows you who is behind it; the gamma scanner is the **map**
> that tells you whether the road ahead is open or blocked by a wall.

### Stage 1 — Detection: start from the radar, not from your opinion

The beginner who starts with *"I feel stock X will rise"* then looks for evidence to confirm their
opinion. The correct approach is to **start neutral**: open contract momentum and let it nominate
what is actually moving now. Instead of thousands of stocks, you have a handful of live contracts.
Note the **contract direction** (Call or Put?) and whether **the signal repeats** — a contract that
reappears with rising momentum is more credible than one that appears once.

### Stage 2 — Verification: is there real money behind the move?

- **Options flow:** do you see huge trades (block trades, consecutive buying) in the same direction
  as the momentum signal? Momentum backed by large trades is far more credible than momentum on
  small trades.
- **Confluence radar:** a ready-made summary of the whole idea — each chip is an independent piece
  of evidence (flow, block trades, multiple contracts, the option chain, news…) with a strength out
  of 100. The more evidence converges, the higher the quality of the opportunity.

> **Beginner rule: one piece of evidence = watch. Two or more independent pieces = an opportunity
> that deserves a map check.**

### Stage 3 — The map: examine the environment in the gamma scanner

1. **What is the regime?** Positive GEX (absorbs moves) or negative (amplifies them)? Momentum
   signals complete most beautifully in a **negative** environment or near Zero Gamma.
2. **Does DEX agree with the signal direction?** A Call signal with positive Net DEX = the current
   is with you.
3. **Where is the nearest wall in my direction?** *The reward question:* a wall **1% away = tiny
   profit space**; an open road to the next wall = respectable space.
4. **Where am I relative to Zero Gamma?** Above it (stable), glued to it (expect violent chop), or
   below it (an explosive regime)?

### Stage 4 — The decision: a simple matrix

| Momentum + Flow | Gamma scanner | Decision |
|---|---|---|
| ✓ both agree | ✓ supports (space + DEX agrees) | **A first-class trade** — execute with a plan |
| ✓ both agree | ✗ contradicts (near a wall / opposite DEX) | **Wait** — either it breaks the wall and you enter, or you save yourself a loss |
| ✓ momentum only, no supporting flow | whatever it is | **Usually ignore** — momentum without big money behind it fades quickly |
| ✗ no signals | ✓ a beautiful environment | **Watch only** — an excellent map without a trigger is not a trade |

### Full-agreement example — the MSFT scenario

1. Contract momentum shows acceleration in MSFT Calls.
2. Options flow confirms: large buy trades on Calls, and the confluence radar gathers more than
   one piece of evidence.
3. Gamma scanner: signal `BREAKOUT TEST / BUY`, price $506.40 holding above the $500 wall, and DEX
   positive.
4. **Decision:** a Call trade with a clear plan — entry as long as it is above $500, target the
   $510 zone (Max Gamma), cancellation on a close below $500.

### The contradiction example that saves you — the SPY scenario

1. Suppose the radar fired a Call signal on SPY.
2. The flow looks supportive (Net DEX = +18B).
3. **But the map says:** price $772.93 and the call wall at $780 — **less than 1% of space**, and
   dealers will sell at the wall.
4. **The smart decision: do not chase.** Set an alert at $780; if it breaks and holds, the trade
   turns into a real breakout, and if it bounces off it, you have saved yourself from buying the top.

### The three beginner mistakes when combining tools

1. **Entering from the radar directly without a map** — the momentum signal says "now", but you
   cannot see the wall that will stop the move. *A radar without GEX = speed without brakes.*
2. **Reverse-engineering:** building a conviction from GEX and then waiting for any signal to
   confirm it — you will accept weak signals because they suit your whim. **Let the radar nominate
   first, always.**
3. **Ignoring expiry-window matching** — a signal on a weekly contract is examined on **This Week**
   walls, not the month's walls.

> **Operational tip.** Use the ★ star feature on interesting contracts from contract momentum and
> options flow — they gather into a review list, and you then examine them on the gamma scanner in
> one batch instead of chasing each signal the moment it appears.

---

## Chapter 4 — The strongest signals: movement power ≠ reliability

The most common beginner question: *"Is SQUEEZE the strongest signal?"* The answer: **yes in terms
of the violence of the movement, but "strongest" in the sense of most reliable is something else
entirely.** Confusing the two meanings is the biggest mistake a beginner makes.

### Why is SQUEEZE the most violently bullish?

Its conditions: **negative GEX + price above the Call Wall + positive DEX.** Three forces pushing
the same direction at once:

1. **Negative GEX** → dealers chase the move: the more price rises, the more they are forced to buy
   more shares to hedge.
2. **Price above the call wall** → the last big resistance has fallen, and those positioned against
   the rise are trapped.
3. **Positive DEX** → a continuous buyer flow feeding the cycle.

The result is a **self-feeding chain**: a rise → forced buying from dealers → a stronger rise →
more buying. And for the Call buyer this is the best environment: the move is large and implied
volatility (IV) rises with it, so the contract profits from direction and volatility together.

### The deceptive twin: FAILED SQUEEZE

Exactly the same position (above the call wall with negative GEX) **but DEX has begun to fade** →
the danger of a sharp reversal. **The difference between the two signals is DEX only — and this is
why you never read position without flow.**

### The bearish twin everyone forgets: ACCELERATION

The same explosive force but downward: **negative GEX + price below the Put Wall + negative DEX** —
every drop forces the market maker to sell, so the collapse accelerates. **Big crashes historically
happen in this state specifically.** For the Put buyer it is the exact equivalent of SQUEEZE.

### The full educational ranking

| Level | Signals | Movement energy | Reliability for a beginner |
|---|---|---|---|
| **Explosive** | `SQUEEZE` (up) / `ACCELERATION` (down) | The highest — the move feeds itself | **Medium:** rare, fast, and flips without warning — for the experienced and with small size |
| **Conditionally strong** | `BREAKOUT TEST` / `BREAKDOWN TEST` | High if the break holds | **Good with confirmation** — holding above/below the wall is the judge (like MSFT above $500) |
| **Energy without direction** | `HIGH VOLATILITY` | High but in both directions | A compressed spring near Zero Gamma — strong for whoever waits for the resolution, **a trap for whoever rushes** |
| **Quiet reliable** | `SUPPORT` / `RESISTANCE` / `MEAN REVERSION` | Low — limited bounces | **The highest reliability:** dealer hedging actually defends the level, but the move is small and theta is against the contract buyer |
| **Cautionary** | `FAILED SQUEEZE` / `SHORT COVER BOUNCE` / `CHOPPY` | Volatile | **Not beginner entry signals** — either a sharp reversal risk or chaos without a system |

### Conceptual summary

- You ask *"where is the most violent movement?"* → **SQUEEZE / ACCELERATION** (the negative GEX
  regime generally).
- You ask *"where is the highest success rate for a quiet trade?"* → **SUPPORT / MEAN REVERSION**
  in strong positive GEX — but it is a better environment for **contract sellers** than buyers.
- **The best balance for a beginner option buyer?** Usually `BREAKOUT TEST` / `BREAKDOWN TEST`
  **after confirmation**: respectable movement + a clear invalidation point + less violent than the
  squeeze.

### Three practical rules for measuring the real "strength" of any signal

1. **Agreement is more important than the name:** a signal that DEX agrees with, and that repeats
   across more than one expiry window (**This Week and This Month together**), is stronger than an
   isolated "explosive" signal.
2. **Watch the transitions, not the moments:** the strongest moments are the transition —
   `HIGH VOLATILITY` at Zero Gamma is **the waiting room** out of which comes either `SQUEEZE`
   (an upward break) or `ACCELERATION` (a downward break). Whoever watches the room enters early
   with a plan.
3. **Greater strength requires more discipline, not less:** in negative-GEX signals the move can
   reverse with the same violence — smaller contract size, quick partial profit-taking, and do not
   turn a squeeze trade into an "investment" if it slows down.

### The full glossary — all the page's signals in one table

| Signal | Conditions | Tilt | Meaning |
|---|---|---|---|
| `HIGH VOLATILITY` | price less than 1% away from Zero Gamma | per DEX | high-chop zone — wait for the resolution |
| `MEAN REVERSION` | positive GEX + price between the walls | per DEX | a sideways range — moves are absorbed |
| `RESISTANCE` | positive GEX + above Call Wall + DEX negative or zero | **sell** | dealers sell in the face of the rise |
| `BREAKOUT TEST` | positive GEX + above Call Wall + DEX positive | **buy** | a breakout test supported by buying flow |
| `SUPPORT` | positive GEX + below Put Wall + DEX positive or zero | **buy** | dealers buy the dip at support |
| `BREAKDOWN TEST` | positive GEX + below Put Wall + DEX negative | **sell** | a support-break test with selling pressure |
| `SQUEEZE` | negative GEX + above Call Wall + DEX positive | **buy** | an upward squeeze — forced buying that feeds itself |
| `FAILED SQUEEZE` | negative GEX + above Call Wall + DEX fading | **sell** | a squeeze that lost its fuel — sharp reversal risk |
| `ACCELERATION` | negative GEX + below Put Wall + DEX negative | **sell** | an accelerating drop — forced selling that feeds itself |
| `SHORT COVER BOUNCE` | negative GEX + below Put Wall + DEX positive | **buy** | a short-covering bounce — fast and short-lived |
| `CHOPPY` | negative GEX + price between the walls | per DEX | random chop without a clear system — **the most dangerous environment for a beginner** |

---

## Chapter 5 — Advanced search (Find in GEX): scan the whole market in one press

Before this feature, the gamma scanner page answered one question: *"what is the state of stock
X?"* — you had to know the stock first and then examine it. **Advanced search flips the question:**
*"show me all the stocks whose state is such-and-such"* — you write the conditions, and the
platform examines the entire supported universe (more than a thousand symbols) in seconds and
returns the matching opportunities list. You had a map for one stock; now you own **an artificial
satellite that sweeps all the maps at once.**

### Worked figure — the "breakout hunter" recipe

Filters: `Bias = Buy`, `This Week` window, the breakout and squeeze signals, and price near the
Call Wall within 1%. **The result: 19 matching opportunities out of 1,074 scanned symbols — the
filter cut 98% of the noise.** The ordering is by nearest to the wall first: AUR above its wall by
0.14%, then BX by 0.15%. **The first row is the most urgent.** Press any row and the symbol opens
in the full gamma scanner page.

### Understanding the filters one by one

| Filter | What it means | Usage tip |
|---|---|---|
| **Day** | US trading day — `Today` for live opportunities, or a historical range up to 31 days | The historical range is a **treasure for learning**: "show me all last month's SQUEEZE signals", then review what the stocks did afterwards |
| **Symbol** | A specific symbol prefix (optional) | Leave it `ANY` when hunting; use it for reviewing a particular stock |
| **Regime** | GEX regime: positive (absorbing) or negative (amplifying) | Remember Chapter 4: the most violent opportunities live in the **negative** regime |
| **Bias** | Signal direction: Buy / Sell / Neutral | The first filter you set — are you looking for a Call or a Put? |
| **Expiry window** | The expiry window the walls are read from — `Auto` chooses the primary window like the page | **Match it with your contract** (Chapter 2's rule): weekly contracts → `This Week` |
| **Signal** | Multi-select from the eleven signals (Chapter 4's glossary) | Here you determine **the type of prey** — see the recipes below |
| **Price near level + Within %** | Price within X% of a specific level: Call Wall, Put Wall or Zero Gamma | **The most powerful filter in the tool** — it turns the list from "stocks with a signal" into "stocks at the decision moment now" |

> **A reassuring quality guarantee.** The scan applies **the same rules as the page itself**: any
> incomplete data generation is automatically excluded from its signals — you cannot hunt a signal
> that the gamma scanner page would refuse to display.

### Reading the results table

- **Matches / Symbols scanned:** how many opportunities matched your conditions out of the total
  symbols examined.
- **Smart ordering:** with the proximity filter, results are arranged **nearest to the level
  first** — the top of the list is the most urgent.
- **The columns:** the signal and its tilt, the analysis price, NET GEX, the walls and Zero Gamma
  with the percentage distance of each from the price.
- **Reads / Last read (ET):** how many times that symbol's data was read today and when the last
  read was (New York time) — **a "freshness" column**: a recent read = the row actually reflects
  the current state.

### Four ready hunting recipes

| Recipe | Settings | What does it hunt? |
|---|---|---|
| **Breakout hunter** | `Bias = Buy` + `BREAKOUT TEST` + `SQUEEZE` + near **Call Wall** within 1% | Stocks testing a break of their resistance at this moment with buying support |
| **Bounce hunter** | `Bias = Buy` + `SUPPORT` + near **Put Wall** within 1% | Stocks that have just touched their defended support — buying a dip with a clear target and a near stop |
| **Explosion hunter** | `HIGH VOLATILITY` + near **Zero Gamma** within 0.5% | Stocks poised on the trigger — Chapter 4's compressed spring that will explode in one of the directions |
| **Downside hunter** | `Bias = Sell` + `BREAKDOWN TEST` + `ACCELERATION` + near **Put Wall** within 1% | The mirror of the first recipe — stocks breaking their supports backed by selling pressure (Put trades) |

### Updating the funnel: you now have a discovery engine

Contract momentum answers *"what is moving now?"* — a **momentary** engine that monitors the move
at the time it happens. Advanced search answers *"where are structures ready for the explosion?"* —
**a map engine that monitors the structure before the move.** The practical routine:

1. **Before the open or shortly after:** run a recipe or two → a short watch list (put a ★ on the
   candidates).
2. **During the session:** watch contract momentum and options flow — **the appearance of momentum
   on a name from your list = the meeting of the engines**, and it is the strongest signal on the
   whole platform.
3. **Before execution:** press the row to open the stock in the gamma scanner and apply Chapter 2's
   check (entry, target, cancellation).

> **The golden rule of advanced search: advanced search nominates and does not decide** — the short
> list is the starting point, and the individual examination on the full page is what makes the
> trade.

---

## The guide's own closing summary

> The radar says **"when"**, the flow says **"who"**, the gamma scanner says **"where"**, and
> advanced search says **"where do I look at all"** — and you do not enter a trade except when the
> answers agree.
>
> And SQUEEZE is indeed the queen of bullish violence, but **the "strongest signal" in truth is any
> signal in which the three rules agree: position relative to the walls + GEX regime + DEX
> direction — and which holds after confirmation. The name alone is never enough.**

*Exelza — `exelza.com` · `x.com/exelza_com` · `t.me/exelza_com` · `youtube.com/@exelza_staff`.
The original states: this guide is educational content for understanding the platform's tools, and
is not a recommendation to buy or sell any financial asset.*

---

## How this maps to OUR system (added 2026-08-11)

Tested against 5,629 fired trades, Jan–Aug 2026, no lookahead. Full evidence:
`S244_GEX_FRAMEWORK_STUDY.md`.

**Held up on SPX 0DTE:**
- **The reliability ranking is real.** Taking trades in the direction the framework recommends:
  quiet-reliable +3.07 pt/trade > explosive +2.11 > cautionary +1.67 > conditional +0.16. Its
  warning that BREAKOUT TEST is a coin flip without confirmation reproduced exactly (50.4% WR).
- **Zero Gamma is the most valuable idea in the document.** Trading with the regime (long above the
  flip, short below) = 55.8% WR / +4,622 pt; against it 48.3% / −185 pt.
- **SUPPORT is our best long state** — the candidate gate for **GEX Long v7 ("Gamma Support")**.
- **The conjunction matters, as the guide insists.** `spot < put_wall` alone is worth almost
  nothing (+0.77 pt/trade); combined with the GEX and DEX conditions it is +3.26.

**Did NOT transfer to SPX 0DTE:**
- **The "wall within 1% = no room" Call trap.** Longs with <5 pt of headroom did 50.0% WR, no worse
  than the middle buckets. Our 10-pt target and trailing exit are a different game from the guide's
  weekly option buyer.
- **The 1% Zero Gamma band.** On SPX 1% ≈ 77 pt, which swallowed 43% of all snapshots. We rescaled
  to 8 pt; results are insensitive across 0–20 pt.
- **Net DEX on its own** is regime-flipping (helps below VIX 19, inverts above VIX 21). Only usable
  inside the conjunction.

**Structural difference to remember:** the guide's Call/Put Wall are **chain-wide** maxima. Our
chain window is ±100 pt, so ours mean "the dominant wall within ±100 pt". For 0DTE with 10-pt
targets the near-spot walls are the ones that matter, but it is not literally the same quantity.
