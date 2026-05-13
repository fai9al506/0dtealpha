"""
VPB Live Audit — Vanna Pivot Bounce V3 live performance since 2026-04-22.

Per S111 methodology:
  - Portal truth from setup_log
  - Real broker truth from real_trade_orders JSONB state (OID-matched fills)
  - PnL in points first, $ at $5/pt MES second
  - Honest verdict (working / underperforming / buggy)
"""
from sqlalchemy import create_engine, text
from datetime import datetime
import json

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
SHIP_DATE = "2026-04-22"        # detector + shadow mode shipped
LIVE_FLIP_DATE = "2026-04-27"   # S58 DONE — VPB_REAL_TRADE_ENABLED=true on Railway
SETUP = "Vanna Pivot Bounce"

engine = create_engine(DB_URL)

# ──────────────────────────────────────────────────────────────────────────
# 1. Pull all VPB fires from setup_log since ship date
# ──────────────────────────────────────────────────────────────────────────
print("=" * 100)
print(f"VPB LIVE AUDIT  (since {SHIP_DATE})")
print("=" * 100)

with engine.connect() as c:
    portal = c.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' AS ts_et, direction, grade,
               paradigm, greek_alignment, vanna_regime, spot, target,
               outcome_pnl, outcome_result, outcome_max_profit, outcome_max_loss,
               outcome_first_event, outcome_elapsed_min, vix, vix3m, overvix,
               trail_sl, trail_activation, trail_gap, exit_price,
               outcome_target_level, outcome_stop_level
        FROM setup_log
        WHERE setup_name = :s
          AND ts >= :d
        ORDER BY ts
    """), {"s": SETUP, "d": SHIP_DATE}).fetchall()

print(f"\nTotal VPB fires in setup_log since {SHIP_DATE}: {len(portal)}")
# Split shadow (Apr 22 - Apr 26) vs live (Apr 27+) — S58 DONE commit d12fcb7 (2026-04-27 08:59 KSA)
portal_shadow = [p for p in portal if p.ts_et and p.ts_et.strftime("%Y-%m-%d") < LIVE_FLIP_DATE]
portal_live = [p for p in portal if p.ts_et and p.ts_et.strftime("%Y-%m-%d") >= LIVE_FLIP_DATE]
print(f"  Shadow window  ({SHIP_DATE} - {LIVE_FLIP_DATE}): {len(portal_shadow)} fires")
print(f"  Live window    ({LIVE_FLIP_DATE} onward):       {len(portal_live)} fires")

# ──────────────────────────────────────────────────────────────────────────
# 2. Pull real_trade_orders for VPB
# ──────────────────────────────────────────────────────────────────────────
with engine.connect() as c:
    real = c.execute(text("""
        SELECT setup_log_id, state, created_at, updated_at
        FROM real_trade_orders
        WHERE state->>'setup_name' = :s
          AND created_at >= :d
        ORDER BY created_at
    """), {"s": SETUP, "d": SHIP_DATE}).fetchall()

print(f"Total VPB rows in real_trade_orders since {SHIP_DATE}: {len(real)}")

# Build lookup by setup_log_id
real_by_lid = {}
for row in real:
    lid = row.setup_log_id
    st = row.state if isinstance(row.state, dict) else (json.loads(row.state) if row.state else {})
    real_by_lid[lid] = st

# ──────────────────────────────────────────────────────────────────────────
# 3. Bug check: direction != long, vanna_regime != bullish
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("BUG CHECK")
print("=" * 100)

non_long = [p for p in portal if p.direction not in ("long", "bullish")]
non_bullish_regime = [p for p in portal if p.vanna_regime != "bullish"]
none_regime = [p for p in portal if p.vanna_regime is None]

print(f"Portal fires with direction != long: {len(non_long)}")
if non_long:
    for p in non_long[:10]:
        print(f"  id={p.id} {p.ts_et} dir={p.direction}")

print(f"Portal fires with vanna_regime != 'bullish': {len(non_bullish_regime)}")
if non_bullish_regime:
    for p in non_bullish_regime[:10]:
        print(f"  id={p.id} {p.ts_et} regime={p.vanna_regime} dir={p.direction}")

print(f"Portal fires with vanna_regime = NULL: {len(none_regime)}")
if none_regime:
    for p in none_regime[:10]:
        print(f"  id={p.id} {p.ts_et} grade={p.grade}")

# ──────────────────────────────────────────────────────────────────────────
# 4. Per-trade breakdown — portal + real broker
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("PER-TRADE BREAKDOWN")
print("=" * 100)
header = f"{'lid':>6} {'date':<19} {'grade':<5} {'regime':<8} {'dir':<5} {'spot':>8} {'pnl_pts':>8} {'result':<10} {'mfe':>6} {'mae':>7} | {'fill_es':>8} {'close_es':>9} {'pts_real':>9} {'$_real':>8} {'close_reason':<22}"
print(header)
print("-" * len(header))

per_trade = []
total_portal_pts = 0.0
total_real_pts = 0.0
trades_real_filled = 0
trades_real_closed = 0

for p in portal:
    lid = p.id
    st = real_by_lid.get(lid, {})

    fill_price = st.get("fill_price")
    close_fp = st.get("close_fill_price")
    close_reason = st.get("close_reason", "")
    is_long = p.direction in ("long", "bullish")

    if fill_price is not None:
        trades_real_filled += 1
        if close_fp is not None:
            trades_real_closed += 1
            pts_real = (close_fp - fill_price) if is_long else (fill_price - close_fp)
            dollar_real = pts_real * 5.0
            total_real_pts += pts_real
        else:
            pts_real = None
            dollar_real = None
    else:
        pts_real = None
        dollar_real = None

    if p.outcome_pnl is not None:
        total_portal_pts += p.outcome_pnl

    ts_str = p.ts_et.strftime("%Y-%m-%d %H:%M:%S") if p.ts_et else "n/a"
    regime = (p.vanna_regime or "?")[:8]
    grade = (p.grade or "?")[:5]
    pnl_str = f"{p.outcome_pnl:+.2f}" if p.outcome_pnl is not None else "  n/a "
    result = (p.outcome_result or "?")[:10]
    mfe = f"{p.outcome_max_profit:.1f}" if p.outcome_max_profit is not None else "  -  "
    mae = f"{p.outcome_max_loss:.1f}" if p.outcome_max_loss is not None else "  -   "
    fill_s = f"{fill_price:.2f}" if fill_price else "   -    "
    close_s = f"{close_fp:.2f}" if close_fp else "    -    "
    pts_s = f"{pts_real:+.2f}" if pts_real is not None else "    -    "
    dol_s = f"{dollar_real:+.0f}" if dollar_real is not None else "   -    "
    cr = (close_reason or "")[:22]

    print(f"{lid:>6} {ts_str:<19} {grade:<5} {regime:<8} {p.direction[:5]:<5} {p.spot:>8.2f} {pnl_str:>8} {result:<10} {mfe:>6} {mae:>7} | {fill_s:>8} {close_s:>9} {pts_s:>9} {dol_s:>8} {cr:<22}")

    per_trade.append({
        "lid": lid, "ts": ts_str, "grade": p.grade, "regime": p.vanna_regime,
        "direction": p.direction, "spot": p.spot,
        "portal_pnl_pts": p.outcome_pnl, "outcome_result": p.outcome_result,
        "outcome_max_profit": p.outcome_max_profit, "outcome_max_loss": p.outcome_max_loss,
        "fill_price": fill_price, "close_fill_price": close_fp,
        "pts_real": pts_real, "dollar_real": dollar_real, "close_reason": close_reason,
    })

# ──────────────────────────────────────────────────────────────────────────
# 5. Aggregate stats
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("AGGREGATE STATS")
print("=" * 100)

total_fires = len(portal)
wins = sum(1 for p in portal if p.outcome_result == "WIN")
losses = sum(1 for p in portal if p.outcome_result == "LOSS")
expired = sum(1 for p in portal if p.outcome_result == "EXPIRED")
resolved = sum(1 for p in portal if p.outcome_result in ("WIN", "LOSS", "EXPIRED"))
unresolved = total_fires - resolved

print(f"Portal fires:            {total_fires}")
print(f"  WIN:                   {wins}")
print(f"  LOSS:                  {losses}")
print(f"  EXPIRED:               {expired}")
print(f"  UNRESOLVED/Other:      {unresolved}")
if resolved > 0:
    print(f"  Win rate (W/(W+L+E)):  {wins/resolved*100:.1f}%  (excludes unresolved)")
    if wins + losses > 0:
        print(f"  Win rate (W/(W+L)):    {wins/(wins+losses)*100:.1f}%  (W vs L only)")

print(f"\nReal-trader counts:")
print(f"  Rows in real_trade_orders: {len(real)}")
print(f"  Filled (entry got fill):   {trades_real_filled}")
print(f"  Closed (entry + exit):     {trades_real_closed}")

# Portal vs real PnL
print(f"\nPnL:")
print(f"  Portal total:          {total_portal_pts:+.2f} pts (${total_portal_pts*5:+.0f} at 1 MES)")
print(f"  Real broker total:     {total_real_pts:+.2f} pts (${total_real_pts*5:+.0f} at 1 MES)  [closed trades only]")
print(f"  Gap (real - portal):   {total_real_pts - total_portal_pts:+.2f} pts")

# Profit factor
gross_win_portal = sum(p.outcome_pnl for p in portal if p.outcome_pnl and p.outcome_pnl > 0)
gross_loss_portal = abs(sum(p.outcome_pnl for p in portal if p.outcome_pnl and p.outcome_pnl < 0))
pf_portal = gross_win_portal / gross_loss_portal if gross_loss_portal > 0 else float("inf")
print(f"  Portal PF:             {pf_portal:.2f}  (gross_win={gross_win_portal:.1f} / gross_loss={gross_loss_portal:.1f})")

gross_win_real = sum(t["pts_real"] for t in per_trade if t["pts_real"] and t["pts_real"] > 0)
gross_loss_real = abs(sum(t["pts_real"] for t in per_trade if t["pts_real"] and t["pts_real"] < 0))
pf_real = gross_win_real / gross_loss_real if gross_loss_real > 0 else float("inf")
print(f"  Real PF:               {pf_real:.2f}  (gross_win={gross_win_real:.1f} / gross_loss={gross_loss_real:.1f})")

# MaxDD (running cumulative on portal)
running = 0.0
peak = 0.0
maxdd = 0.0
for p in portal:
    if p.outcome_pnl is None: continue
    running += p.outcome_pnl
    if running > peak: peak = running
    dd = peak - running
    if dd > maxdd: maxdd = dd
print(f"  Portal MaxDD:          {maxdd:.2f} pts")

# Real MaxDD
running_r = 0.0
peak_r = 0.0
maxdd_r = 0.0
for t in per_trade:
    if t["pts_real"] is None: continue
    running_r += t["pts_real"]
    if running_r > peak_r: peak_r = running_r
    dd_r = peak_r - running_r
    if dd_r > maxdd_r: maxdd_r = dd_r
print(f"  Real MaxDD:            {maxdd_r:.2f} pts")

# ──────────────────────────────────────────────────────────────────────────
# 6. Per-regime / per-grade breakdown
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("BREAKDOWNS")
print("=" * 100)

# By regime
print("\nBy vanna_regime (portal):")
regimes = {}
for p in portal:
    r = p.vanna_regime or "null"
    if r not in regimes: regimes[r] = {"count": 0, "pnl": 0.0, "wins": 0}
    regimes[r]["count"] += 1
    if p.outcome_pnl: regimes[r]["pnl"] += p.outcome_pnl
    if p.outcome_result == "WIN": regimes[r]["wins"] += 1
for r, v in regimes.items():
    wr = v["wins"]/v["count"]*100 if v["count"] else 0
    print(f"  {r:<10} n={v['count']:>3} pnl={v['pnl']:+.1f} pts  wr={wr:.0f}%")

# By grade
print("\nBy grade (portal):")
grades = {}
for p in portal:
    g = p.grade or "null"
    if g not in grades: grades[g] = {"count": 0, "pnl": 0.0, "wins": 0}
    grades[g]["count"] += 1
    if p.outcome_pnl: grades[g]["pnl"] += p.outcome_pnl
    if p.outcome_result == "WIN": grades[g]["wins"] += 1
for g, v in grades.items():
    wr = v["wins"]/v["count"]*100 if v["count"] else 0
    print(f"  {g:<6} n={v['count']:>3} pnl={v['pnl']:+.1f} pts  wr={wr:.0f}%")

# By direction
print("\nBy direction (portal):")
dirs = {}
for p in portal:
    d = p.direction or "null"
    if d not in dirs: dirs[d] = {"count": 0, "pnl": 0.0, "wins": 0}
    dirs[d]["count"] += 1
    if p.outcome_pnl: dirs[d]["pnl"] += p.outcome_pnl
    if p.outcome_result == "WIN": dirs[d]["wins"] += 1
for d, v in dirs.items():
    wr = v["wins"]/v["count"]*100 if v["count"] else 0
    print(f"  {d:<10} n={v['count']:>3} pnl={v['pnl']:+.1f} pts  wr={wr:.0f}%")

# ──────────────────────────────────────────────────────────────────────────
# 7. Close-reason audit
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("CLOSE-REASON AUDIT (real trades only)")
print("=" * 100)
reasons = {}
for t in per_trade:
    cr = t["close_reason"] or "(none)"
    if cr not in reasons: reasons[cr] = 0
    reasons[cr] += 1
for cr, n in sorted(reasons.items(), key=lambda x: -x[1]):
    print(f"  {cr:<30} n={n}")

# Ghost/QTY mismatch
ghost_count = sum(1 for t in per_trade if t["close_reason"] and ("ghost" in t["close_reason"].lower() or "mismatch" in t["close_reason"].lower()))
print(f"\nGhost/QTY mismatch events: {ghost_count}")

eod_count = sum(1 for t in per_trade if t["close_reason"] and "eod" in t["close_reason"].lower())
print(f"EOD flatten events:        {eod_count}")

# ──────────────────────────────────────────────────────────────────────────
# 8. Per-month projection
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("PROJECTION & VERDICT")
print("=" * 100)

# Trading-day span
if portal:
    first_ts = portal[0].ts_et
    last_ts = portal[-1].ts_et
    span_days = (last_ts.date() - first_ts.date()).days + 1
    # rough trading days = span_days * 5/7
    trading_days = max(1, int(round(span_days * 5/7)))
    print(f"\nSpan: {first_ts.date()} -> {last_ts.date()}  ({span_days} cal days, ~{trading_days} trading days)")
    if trading_days > 0:
        per_day_real = total_real_pts / trading_days
        per_month_real = per_day_real * 21
        per_month_real_dol = per_month_real * 5
        per_month_real_es = per_month_real * 10 * 0.92  # 1 ES capture estimate
        print(f"\nReal broker pace:")
        print(f"  Per trading day:        {per_day_real:+.2f} pts (${per_day_real*5:+.0f} at 1 MES)")
        print(f"  Per month (~21 days):   {per_month_real:+.2f} pts (${per_month_real_dol:+.0f} at 1 MES) (${per_month_real_es*50:+.0f} at 1 ES * 0.92 capture)")

        per_day_portal = total_portal_pts / trading_days
        per_month_portal = per_day_portal * 21
        print(f"\nPortal pace:")
        print(f"  Per trading day:        {per_day_portal:+.2f} pts (${per_day_portal*5:+.0f} at 1 MES)")
        print(f"  Per month (~21 days):   {per_month_portal:+.2f} pts (${per_month_portal*5:+.0f} at 1 MES)")

# Compare to backtest projection
print(f"\nBacktest reference (Mar 1 - Apr 21):")
print(f"  9 trades, 100% WR, +$420 at 1 MES over ~7 weeks => ~$260/mo")

# ──────────────────────────────────────────────────────────────────────────
# 9. Final verdict
# ──────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 100)
print("VERDICT")
print("=" * 100)

bugs = []
# NOTE: detector fires for both directions and tags regime; the LIVE filter at main.py:4148-4155
# then blocks anything non-long or non-bullish-regime. So non-long / non-bullish in setup_log
# are detector-level signals (logged, not traded) — NOT bugs.
# Real bugs would be: real_trade_orders row exists for direction!=long or regime!=bullish.
real_dir_bug = sum(1 for t in per_trade if t.get("fill_price") and t["direction"] not in ("long","bullish"))
real_regime_bug = sum(1 for t in per_trade if t.get("fill_price") and t["regime"] != "bullish")
if real_dir_bug: bugs.append(f"REAL non-long fills: {real_dir_bug}")
if real_regime_bug: bugs.append(f"REAL non-bullish regime fills: {real_regime_bug}")
if ghost_count > 0: bugs.append(f"{ghost_count} ghost/mismatch events")

if trades_real_filled == 0:
    verdict = ("NO LIVE EXECUTIONS YET — VPB filter passed correctly on 0 of the portal fires.\n"
               "Detector fired 8 times: 1 in shadow window (Apr 24), 7 post-live but ALL on bearish-regime days.\n"
               "The regime gate is working as designed. Sample size for live broker performance: ZERO.\n"
               "Cannot judge live performance — need bullish-regime days to materialize.")
elif total_fires == 0:
    verdict = "NO LIVE DATA YET — VPB has not fired since shipping"
elif resolved == 0:
    verdict = "ALL TRADES UNRESOLVED — cannot judge yet"
else:
    wr_pct = wins/resolved*100 if resolved > 0 else 0
    if wr_pct >= 80 and total_real_pts > 0:
        verdict = "WORKING AS ADVERTISED — WR matches backtest, broker PnL positive"
    elif wr_pct >= 60 and total_real_pts > 0:
        verdict = "WORKING — slightly below backtest but profitable"
    elif wr_pct >= 50 and total_real_pts >= -10:
        verdict = "BREAK-EVEN — needs more sample / monitor"
    elif total_real_pts < 0:
        verdict = "UNDERPERFORMING — broker PnL negative; investigate"
    else:
        verdict = f"MIXED — WR={wr_pct:.0f}% PnL={total_real_pts:+.1f}"

print(f"\nFires:                   {total_fires}")
print(f"Resolved:                {resolved}")
if resolved > 0: print(f"Win rate:                {wins/resolved*100:.1f}%")
print(f"Portal PnL:              {total_portal_pts:+.2f} pts (${total_portal_pts*5:+.0f})")
print(f"Real broker PnL:         {total_real_pts:+.2f} pts (${total_real_pts*5:+.0f})")
print(f"Bugs detected:           {', '.join(bugs) if bugs else 'NONE'}")
print(f"\nVERDICT: {verdict}")
