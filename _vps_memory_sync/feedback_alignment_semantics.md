---
name: Greek alignment is RELATIVE to trade direction
description: alignment +3 = all Greeks WITH the trade, -3 = all AGAINST. NOT fixed bullish/bearish.
type: feedback
---

Greek alignment (`greek_alignment` in setup_log, computed by `_compute_greek_alignment()`) is RELATIVE to the trade direction, NOT a fixed bullish/bearish scale.

- `+3` = all 3 Greeks aligned WITH the trade direction
- `+1` = 2 with, 1 against
- `-1` = 1 with, 2 against
- `-3` = all 3 Greeks AGAINST the trade direction

**Why:** The user got confused because sometimes we said "align=-1 is anti-aligned" and other times "align=-3 is bearish." The alignment score depends on the trade direction — a short with align=+3 means all Greeks are bearish (aligned with the short), while a long with align=+3 means all Greeks are bullish (aligned with the long).

**How to apply:** When analyzing alignment, ALWAYS clarify relative to what direction. Never say "align=-1 is bearish" — instead say "align=-1 means 2 Greeks are against the trade." For DD Exhaustion (contrarian setup), anti-alignment (lower values) = stronger signal. For directional setups (GEX Long, AG Short), higher alignment = better.
