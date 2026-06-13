"""Compare portal V16 view vs TSRT (real_trader) effective behavior.

V16 portal purpose (per feedback_portal_v16_mirrors_tsrt.md):
  "What did TSRT actually fire today?" — must be an EXACT MIRROR.

This script finds DISCREPANCIES by cross-referencing:
  setup_log (what fired)
  real_trade_orders (what real_trader placed)
  real_trade_skip_reason (why real_trader skipped)

Rules a real_trader fire must satisfy (current env, 2026-05-20):
  - setup in {Skew Charm, AG Short, Vanna Pivot Bounce, ES Absorption, DD Exhaustion}
  - DD Exhaustion: LONG only (main.py:5430 _dd_short_block)
  - VIX Divergence: BLOCKED (VIX_DIV_REAL_TRADE_ENABLED=false)
  - GEX Long: BLOCKED (GEX_LONG_V3_REAL_TRADE_ENABLED=false)
  - Plus V14 quality gates (paradigm, alignment, time, vanna, etc.)

For each setup_log signal in last N days, check:
  A. Did it land in real_trade_orders?  (placed = yes)
  B. If not, what skip_reason?
  C. Cross-reference: does V16 portal say "passed" but real_trader skipped, OR
     vice versa? Those are the discrepancies.
"""
import os, psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
LOOKBACK_DAYS = 14

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

since = (datetime.now(ET) - timedelta(days=LOOKBACK_DAYS)).date()

# Pull all signals + whether they placed + skip reasons
cur.execute("""
    SELECT
      sl.id, sl.ts, sl.setup_name, sl.direction, sl.grade,
      sl.paradigm, sl.greek_alignment, sl.real_trade_skip_reason,
      sl.notified, sl.vix, sl.outcome_pnl,
      sl.vanna_cliff_side, sl.vanna_peak_side,
      EXISTS(SELECT 1 FROM real_trade_orders rto WHERE rto.setup_log_id = sl.id) AS placed
    FROM setup_log sl
    WHERE sl.ts::date >= %s
    ORDER BY sl.ts
""", (since,))
all_rows = cur.fetchall()
print(f"Loaded {len(all_rows)} signals since {since} ({LOOKBACK_DAYS}d lookback)")


# TSRT effective whitelist (current env)
TSRT_SETUPS = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"}

# Categorize each signal
def tsrt_would_fire(name, direction):
    """Simplified: would real_trader at least pass the whitelist + dispatch gate?
    Does NOT include _passes_live_filter logic (that's the V14/V16 filter)."""
    if name not in TSRT_SETUPS:
        return False, "not_in_tsrt_whitelist"
    if name == "DD Exhaustion" and direction not in ("long", "bullish"):
        return False, "dd_short_block"
    return True, None


# 1. Signals that real_trader placed (sanity check)
placed_setups = {}
skipped_with_reason = {}
no_reason = []
not_in_whitelist = []
dd_short_signals = []
vix_div_signals = []
gex_long_signals = []

for r in all_rows:
    sid, ts, name, dir_, grade, para, align, skip, notified, vix, pnl, vc, vp, placed = r
    if placed:
        placed_setups[(name, dir_)] = placed_setups.get((name, dir_), 0) + 1
    elif skip:
        skipped_with_reason.setdefault(skip, []).append((sid, name, dir_))
    else:
        no_reason.append((sid, ts, name, dir_, grade, para))

    if name not in TSRT_SETUPS:
        not_in_whitelist.append((sid, ts, name, dir_, placed))
    if name == "DD Exhaustion" and dir_ not in ("long", "bullish"):
        dd_short_signals.append((sid, ts, dir_, placed, skip))
    if name == "VIX Divergence":
        vix_div_signals.append((sid, ts, dir_, placed, skip))
    if name == "GEX Long":
        gex_long_signals.append((sid, ts, dir_, placed, skip))

print()
print("=" * 70)
print("WHAT REAL_TRADER PLACED (last 14d)")
print("=" * 70)
for (name, dir_), cnt in sorted(placed_setups.items(), key=lambda x: -x[1]):
    print(f"  {name:<25} {dir_:<10} {cnt} placed")

print()
print("=" * 70)
print("BLOCKED — skip_reason populated (top 15 reasons)")
print("=" * 70)
reasons_sorted = sorted(skipped_with_reason.items(), key=lambda x: -len(x[1]))[:15]
for reason, lids in reasons_sorted:
    print(f"  {reason:<35} {len(lids):>4} signals")

print()
print("=" * 70)
print("!! NOT IN TSRT WHITELIST — should never have been placed")
print("=" * 70)
non_whitelist_placed = [r for r in not_in_whitelist if r[4]]
if non_whitelist_placed:
    print(f"!! BUG !!  {len(non_whitelist_placed)} signals NOT in TSRT whitelist but placed:")
    for sid, ts, name, dir_, _ in non_whitelist_placed[:10]:
        print(f"    lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} {name} {dir_}")
else:
    print("  OK no leakage — all out-of-whitelist signals correctly NOT placed")

print()
print("=" * 70)
print("DD SHORTS — should be blocked, V16 portal MUST NOT show as passed")
print("=" * 70)
dd_short_placed = [s for s in dd_short_signals if s[3]]
print(f"  Total DD shorts fired:   {len(dd_short_signals)}")
print(f"  DD shorts placed (BUG):  {len(dd_short_placed)}")
dd_short_blocked = [s for s in dd_short_signals if not s[3]]
print(f"  DD shorts blocked:       {len(dd_short_blocked)}")
if dd_short_blocked[:5]:
    print("  Recent blocked DD shorts (sample):")
    for sid, ts, dir_, _, skip in dd_short_blocked[:5]:
        print(f"    lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} skip_reason={skip}")
print("  --> V16 PORTAL CHECK: query DB to see if v16 view shows any of these as PASSED.")

print()
print("=" * 70)
print("VIX DIVERGENCE — should be blocked (env false)")
print("=" * 70)
vix_div_placed = [s for s in vix_div_signals if s[3]]
print(f"  Total VIX Div fired:   {len(vix_div_signals)}")
print(f"  VIX Div placed (BUG):  {len(vix_div_placed)}")
if vix_div_placed:
    for sid, ts, dir_, _, skip in vix_div_placed[:5]:
        print(f"    LEAK! lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} {dir_} skip={skip}")

print()
print("=" * 70)
print("GEX LONG — should be blocked (env false)")
print("=" * 70)
gex_long_placed = [s for s in gex_long_signals if s[3]]
print(f"  Total GEX Long fired:   {len(gex_long_signals)}")
print(f"  GEX Long placed (BUG):  {len(gex_long_placed)}")
if gex_long_placed:
    for sid, ts, dir_, _, skip in gex_long_placed[:5]:
        print(f"    LEAK! lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} {dir_} skip={skip}")

print()
print("=" * 70)
print("!! Signals with NO skip_reason but ALSO not placed — silent failures?")
print("=" * 70)
print(f"  count={len(no_reason)} (first 10)")
for sid, ts, name, dir_, grade, para in no_reason[:10]:
    print(f"    lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} {name} {dir_} g={grade} p={para}")

print()
print("=" * 70)
print("V16 SETTING SANITY")
print("=" * 70)
print("Real_trader.py:483 _allowed base = {Skew Charm, AG Short, Vanna Pivot Bounce, ES Absorption}")
print("                + DD Exhaustion (env DD_EXHAUSTION_REAL_TRADE_ENABLED=true)")
print("                + VIX Divergence (env VIX_DIV_REAL_TRADE_ENABLED=false --> EXCLUDED)")
print("                + GEX Long (env GEX_LONG_V3_REAL_TRADE_ENABLED=false --> EXCLUDED)")
print("Main.py:5430 _dd_short_block = (DD Exhaustion and direction != long/bullish)")
print()
print("Portal V16:main.py:13476 _v16Allowed = {SC, AG Short, VPB, ES Abs, DD Exhaustion}")
print("            +v16DDAdmit() admits DD long with align in {0,1,2}, good paradigm, !grade C, !vix>=22")
print("            !! BUT _v16Allowed includes 'DD Exhaustion' without direction filter — DD SHORTS")
print("               could fall through v16DDAdmit() to V14 base filter and PASS portal v16.")
print("               TSRT blocks them via _dd_short_block. POTENTIAL DISCREPANCY.")

cur.close(); c.close()
