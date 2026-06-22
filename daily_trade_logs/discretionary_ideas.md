# 📓 Discretionary Trade-Idea Journal

User's own market reads (vanna/gamma framework, Dark Mate FW map). Each idea logged as a
hypothetical trade with thesis + levels + due-date. Claude resolves the outcome (actual SPX
vs targets/stop) → HIT / MISS / PENDING. Goal: measure which discretionary reads actually pay,
by setup type. Not real money — a forward-log of the human edge alongside the mechanical one.

**Why we save the WHY:** every idea records its **Basis / framework** (e.g. vanna-magnet, multi-expiry
vanna stack, GEX-support). Once a basis accumulates enough winning logs, we promote it into a real
**setup** — 0DTE or swing depending on the horizon that paid. The reasoning is the asset, not the row.

**How to use:** fire an idea in chat ("log: ..."); Claude appends a row here. Review on due
dates or on request. Outcomes pulled from `chain_snapshots` (SPX) / Volland.

---

## #1 · 2026-06-12 — VANNA SWING (long)
- **Basis / framework:** **Vanna-magnet read** — −vanna below = thin/repelling floor, +vanna above = magnets price is drawn toward.
- **Thesis:** −vanna at 7350 below = thin support, price pushed up away from it; +vanna magnets at **7500** and **7575** above = upside targets (price drawn toward them).
- **Direction / horizon:** LONG · swing (multi-day, ~1–2 wk)
- **Entry (ref):** ~7390 (spot at idea time)
- **Targets:** 7500, then 7575
- **Stop / invalidation:** below **7350** (−vanna cascade risk) — *user to confirm exact stop*
- **Vol caveat:** valid IF VIX calm/falling; **inverts** (7500/7575 → resistance, downside accelerant) if VIX spikes. VIX ~20 at entry.
- **Due-date / check:** by ~2026-06-20 (end of next week) — *adjust if needed*
- **Status:** ✅ CLOSED — **HIT (both targets)**
- **Result:** ✅ **HIT.** Entry day Jun 12 ran 7366→7456 (held above 7350 — invalidation never threatened). Gapped up over the weekend: **Jun 15 low 7535.7 / high 7576.5** → **7500 cleared** (gapped through) and **7575 hit** (7576.5). Stop 7350 never touched (window low 7366.4 on entry day). Both targets reached within ~3 days. Vanna-magnet read was correct; VIX stayed calm so no inversion. _(source: `chain_snapshots`, Jun 12–20 ET)_

---
## #2 · 2026-06-12 — GEX-SUPPORT BOUNCE (long, REAL — Mohammed manual acct, 1 ES) ✅ CLOSED
- **Thesis:** market **rejected at TS GEX support 7370** (rejection area ~7363); paradigm **GEX-LIS**, LIS ~7357 just below → support confluence, bounce long.
- **Direction / size:** LONG · **1 ES** · manual (Mohammed 25k TCP account)
- **Entry:** 7375
- **Target:** 7450 (upside)
- **Exit:** ~7384 (**early, on purpose**) — to protect the **25k TCP consistency rule** (don't let one day be too large a % of total profit; E2T 30% rule)
- **Result:** ✅ **+$500** (~+9 pts on 1 ES). Thesis (GEX-support bounce) worked; left the 7450 target on the table by choice for consistency.
- **Follow-up:** 🎯 **TARGET HIT** — SPX ran to **7456 (high, 11:26 ET)**, clean through the 7450 target. Read was **dead-on** (GEX-support bounce). Entry 7375 → 7456 = **+81 pts** of move; captured **+9** ($+500) by exiting at 7384 for consistency. ~+72 pts ($~+3,600 on 1 ES) left on the table — but that's the *correct* call for the 25k TCP (a +$3,600 day would blow the E2T 30% consistency rule). **Discipline cost points but protected the account — good trade by the rules.**

---
## #3 · 2026-06-22 — VANNA CONTINUATION (long)
- **Basis / framework:** **Multi-expiry vanna view** — 0DTE + weekly + monthly + aggregate all agree on +vanna magnets above (the cross-expiry stack is what gives conviction, not a single tenor).
- **Thesis:** **7500 now acts as support** (prior target → flipped to floor); upside vanna-magnet continuation toward stacked +vanna magnets above.
- **Direction / horizon:** LONG · 7550 intraday, 7575/7600 swing
- **Entry (ref):** ~7520 (spot at idea time, 09:42 ET)
- **Targets:** **7550 (today)**, then **7575**, then **7600 (soon, before end of month)**
- **Stop / invalidation:** below **7500** (support flip fails) — *user to confirm exact stop*
- **Vol caveat:** valid IF VIX calm/falling; **inverts** (targets → resistance / downside accelerant) if VIX spikes.
- **Due-date / check:** 7550 by EOD **2026-06-22**; 7575/7600 by **2026-06-30**
- **Status:** 🟡 PENDING
- **Suggested option expression (logged 09:54 ET, spot 7523, VIX 16.5 — to grade vs actual entry later):**
  - *Intraday 7550 (0DTE, REAL chain):* slightly-OTM **7530 call @ $9.90** (or 7540 @ $6.10). Est ~+60–120% if 7550 tags with time left.
  - *Swing 7575/7600 by Jun 30 (MODEL, IV 0.13):* **call debit spread 7550/7600 exp ~Jun 30, ~$18.75 debit → +167% if 7600 prints by expiry** (+64% if 7600 tags early w/ 3d left). Preferred over naked call (which only ~+40% at 7600 — overpays for capped tail).
  - *Rule applied (from #1):* uncapped/large target → naked OTM call; **capped/modest target → debit spread to the target strike.**
- **Result:** _(to be filled — which targets hit before a break of 7500? note path + vol regime; then compare REAL fills vs the suggested expression above)_

---
## #4 · 2026-06-22 — GEX PIN/MAGNET (long)
- **Basis / framework:** **GEX paradigm** — big gamma bars on BOTH puts and calls (heavy dealer positioning) → price pinned/magnetised toward the GEX wall above.
- **Thesis:** flat price + VIX falling under a GEX regime → drift up into 7500 magnet.
- **Direction / horizon:** LONG · intraday (today)
- **Entry (ref):** ~7485 (spot 14:22 ET; user said ~7475)
- **Target:** **7500** (today)
- **Stop / invalidation:** *user to confirm* (flat/VIX-down thesis breaks if VIX turns up or price loses the GEX support below)
- **Vol caveat:** valid while VIX falling (17.0 at entry); GEX pin weakens if VIX spikes.
- **Due-date / check:** EOD 2026-06-22
- **Status:** ❌ CLOSED — **MISS**
- **Result:** ❌ **MISS.** After the 14:22 log SPX high was only **7485.2** (never tagged 7500), drifted to 7461 low, **closed 7470**. VIX ticked **up** 17.0→17.35 — the "VIX falling" premise broke, GEX pin failed to lift. 0DTE 7480/7500/7520 fly → expired ~worthless. Lesson: GEX-pin-up read needs VIX actually falling to confirm; flat/rising VIX = no lift. **Mechanism (data-checked): 7500 WAS the wall (mass 9964, correct read) — but a gamma wall ABOVE spot is a ceiling/magnet only when price is NEAR it; it can't PULL price up 20 pts. Below sat a put-gamma shelf (7475/7480) that slid price down. GEX wall above = needs a VIX-down/flow catalyst to climb into; no catalyst → price pins at the lower gamma, not the target.** _(source: `chain_snapshots`)_

<!-- new ideas appended below -->
