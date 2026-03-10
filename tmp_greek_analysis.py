"""Retroactive Greek context analysis on historical setup_log trades.

Joins setup_log with volland_exposure_points and volland_snapshots at each
signal's timestamp to compute vanna_all, vanna_weekly, vanna_monthly,
spot_vol_beta, and greek_alignment — then analyzes WR by each dimension.
"""
import os
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

# Step 1: Pull all trades with outcomes
with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.max_plus_gex, s.paradigm,
               s.outcome_result, s.outcome_pnl
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.id
    """)).mappings().all()

print(f"Total trades with outcomes: {len(trades)}")
print()

# Step 2: For each trade, retroactively compute Greek context
enriched = []
with engine.begin() as conn:
    for t in trades:
        trade_ts = t["ts_et"]
        tid = t["id"]

        # Vanna ALL: latest volland_exposure_points <= trade timestamp
        vanna_all = None
        row = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'ALL'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'ALL'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if row and row["total"] is not None:
            vanna_all = float(row["total"])

        # Vanna THIS_WEEK
        vanna_weekly = None
        row = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIS_WEEK'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'THIS_WEEK'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if row and row["total"] is not None:
            vanna_weekly = float(row["total"])

        # Vanna THIRTY_NEXT_DAYS
        vanna_monthly = None
        row = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if row and row["total"] is not None:
            vanna_monthly = float(row["total"])

        # Spot-vol-beta from volland_snapshots
        svb_correlation = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics'->'spot_vol_beta' IS NOT NULL
              AND ts <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": trade_ts}).mappings().first()
        if snap:
            import json
            payload = snap["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                stats = payload.get("statistics", {})
                svb = stats.get("spot_vol_beta", {})
                if isinstance(svb, dict) and svb.get("correlation") is not None:
                    try:
                        svb_correlation = float(svb["correlation"])
                    except (ValueError, TypeError):
                        pass

        # Aggregated charm from same snapshot
        agg_charm = None
        if snap:
            payload = snap["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                stats = payload.get("statistics", {})
                charm_val = stats.get("aggregatedCharm")
                if charm_val is not None:
                    try:
                        agg_charm = float(charm_val)
                    except (ValueError, TypeError):
                        pass

        # Compute greek_alignment
        direction = t["direction"]
        is_long = direction in ("long", "bullish")
        alignment = 0
        if agg_charm is not None:
            alignment += 1 if (agg_charm > 0) == is_long else -1
        if vanna_all is not None:
            alignment += 1 if (vanna_all > 0) == is_long else -1
        spot = t["spot"]
        max_plus_gex = t["max_plus_gex"]
        if spot and max_plus_gex:
            gex_bullish = spot <= max_plus_gex
            alignment += 1 if gex_bullish == is_long else -1

        enriched.append({
            **dict(t),
            "vanna_all": vanna_all,
            "vanna_weekly": vanna_weekly,
            "vanna_monthly": vanna_monthly,
            "svb_correlation": svb_correlation,
            "agg_charm": agg_charm,
            "greek_alignment": alignment,
        })

print(f"Enriched {len(enriched)} trades with Greek context")
print()

# ============================================================
# ANALYSIS
# ============================================================

def wr_stats(trades_list, label=""):
    """Compute WR, PnL, count for a list of trades."""
    if not trades_list:
        return {"n": 0, "wr": 0, "pnl": 0, "wins": 0, "losses": 0}
    wins = [t for t in trades_list if t["outcome_result"] == "WIN"]
    losses = [t for t in trades_list if t["outcome_result"] == "LOSS"]
    pnl = sum(t["outcome_pnl"] or 0 for t in trades_list)
    wr = len(wins) / len(trades_list) * 100 if trades_list else 0
    return {"n": len(trades_list), "wr": wr, "pnl": pnl, "wins": len(wins), "losses": len(losses)}


def print_bucket_analysis(title, buckets):
    """Print a formatted table of bucket analysis."""
    print(f"\n{'='*70}")
    print(f" {title}")
    print(f"{'='*70}")
    print(f"  {'Bucket':<25} {'N':>4}  {'WR':>6}  {'PnL':>8}  {'W/L':>7}  {'Avg':>6}")
    print(f"  {'-'*25} {'-'*4}  {'-'*6}  {'-'*8}  {'-'*7}  {'-'*6}")
    for label, stats in buckets:
        if stats["n"] == 0:
            continue
        avg = stats["pnl"] / stats["n"] if stats["n"] else 0
        print(f"  {label:<25} {stats['n']:>4}  {stats['wr']:>5.1f}%  {stats['pnl']:>+7.1f}  "
              f"{stats['wins']}/{stats['losses']:<4}  {avg:>+5.1f}")


# ---- 1. Greek Alignment (overall) ----
align_buckets = defaultdict(list)
for t in enriched:
    align_buckets[t["greek_alignment"]].append(t)

buckets = []
for a in sorted(align_buckets.keys()):
    buckets.append((f"Alignment = {a:+d}", wr_stats(align_buckets[a])))
print_bucket_analysis("GREEK ALIGNMENT (all setups)", buckets)


# ---- 2. Greek Alignment per setup ----
setup_names = sorted(set(t["setup_name"] for t in enriched))
for sn in setup_names:
    st = [t for t in enriched if t["setup_name"] == sn]
    if len(st) < 5:
        continue
    align_buckets = defaultdict(list)
    for t in st:
        align_buckets[t["greek_alignment"]].append(t)
    buckets = []
    for a in sorted(align_buckets.keys()):
        buckets.append((f"Alignment = {a:+d}", wr_stats(align_buckets[a])))
    print_bucket_analysis(f"GREEK ALIGNMENT — {sn} ({len(st)} trades)", buckets)


# ---- 3. Vanna ALL direction ----
def vanna_dir(v):
    if v is None:
        return "N/A"
    return "Positive" if v > 0 else "Negative" if v < 0 else "Zero"

vanna_buckets = defaultdict(list)
for t in enriched:
    vanna_buckets[vanna_dir(t["vanna_all"])].append(t)
buckets = [(label, wr_stats(vanna_buckets[label])) for label in ["Positive", "Negative", "Zero", "N/A"]]
print_bucket_analysis("VANNA ALL DIRECTION (all setups)", buckets)

# Per-setup vanna analysis
for sn in setup_names:
    st = [t for t in enriched if t["setup_name"] == sn]
    if len(st) < 5:
        continue
    vb = defaultdict(list)
    for t in st:
        vb[vanna_dir(t["vanna_all"])].append(t)
    buckets = [(label, wr_stats(vb[label])) for label in ["Positive", "Negative", "Zero", "N/A"] if vb[label]]
    print_bucket_analysis(f"VANNA ALL — {sn} ({len(st)} trades)", buckets)


# ---- 4. Vanna Weekly vs Monthly divergence ----
def vanna_divergence(weekly, monthly):
    if weekly is None or monthly is None:
        return "N/A"
    if (weekly > 0) != (monthly > 0):
        return "Divergent"
    return "Aligned"

div_buckets = defaultdict(list)
for t in enriched:
    div_buckets[vanna_divergence(t["vanna_weekly"], t["vanna_monthly"])].append(t)
buckets = [(label, wr_stats(div_buckets[label])) for label in ["Aligned", "Divergent", "N/A"]]
print_bucket_analysis("VANNA WEEKLY vs MONTHLY DIVERGENCE", buckets)


# ---- 5. Spot-Vol-Beta ----
def svb_bucket(svb):
    if svb is None:
        return "N/A"
    if svb < -0.5:
        return "Strong neg (<-0.5)"
    elif svb < 0:
        return "Weak neg (-0.5 to 0)"
    elif svb < 0.5:
        return "Weak pos (0 to 0.5)"
    else:
        return "Strong pos (>0.5)"

svb_buckets = defaultdict(list)
for t in enriched:
    svb_buckets[svb_bucket(t["svb_correlation"])].append(t)
order = ["Strong neg (<-0.5)", "Weak neg (-0.5 to 0)", "Weak pos (0 to 0.5)", "Strong pos (>0.5)", "N/A"]
buckets = [(label, wr_stats(svb_buckets[label])) for label in order if svb_buckets[label]]
print_bucket_analysis("SPOT-VOL-BETA (all setups)", buckets)

# Per-setup SVB
for sn in setup_names:
    st = [t for t in enriched if t["setup_name"] == sn]
    if len(st) < 5:
        continue
    sb = defaultdict(list)
    for t in st:
        sb[svb_bucket(t["svb_correlation"])].append(t)
    buckets = [(label, wr_stats(sb[label])) for label in order if sb[label]]
    print_bucket_analysis(f"SPOT-VOL-BETA — {sn} ({len(st)} trades)", buckets)


# ---- 6. Charm direction vs trade direction ----
def charm_alignment(charm, direction):
    if charm is None:
        return "N/A"
    is_long = direction in ("long", "bullish")
    if (charm > 0) == is_long:
        return "Aligned"
    return "Opposed"

charm_buckets = defaultdict(list)
for t in enriched:
    charm_buckets[charm_alignment(t["agg_charm"], t["direction"])].append(t)
buckets = [(label, wr_stats(charm_buckets[label])) for label in ["Aligned", "Opposed", "N/A"]]
print_bucket_analysis("CHARM vs TRADE DIRECTION (all setups)", buckets)

for sn in setup_names:
    st = [t for t in enriched if t["setup_name"] == sn]
    if len(st) < 5:
        continue
    cb = defaultdict(list)
    for t in st:
        cb[charm_alignment(t["agg_charm"], t["direction"])].append(t)
    buckets = [(label, wr_stats(cb[label])) for label in ["Aligned", "Opposed", "N/A"] if cb[label]]
    print_bucket_analysis(f"CHARM ALIGNMENT — {sn} ({len(st)} trades)", buckets)


# ---- 7. Multi-factor: alignment + SVB combined ----
print(f"\n{'='*70}")
print(f" MULTI-FACTOR: Alignment + SVB Combined")
print(f"{'='*70}")
print(f"  {'Alignment':<12} {'SVB':<22} {'N':>4}  {'WR':>6}  {'PnL':>8}  {'Avg':>6}")
print(f"  {'-'*12} {'-'*22} {'-'*4}  {'-'*6}  {'-'*8}  {'-'*6}")

combos = defaultdict(list)
for t in enriched:
    a = t["greek_alignment"]
    s = svb_bucket(t["svb_correlation"])
    combos[(a, s)].append(t)

for (a, s), trades_list in sorted(combos.items(), key=lambda x: (-wr_stats(x[1])["pnl"])):
    st = wr_stats(trades_list)
    if st["n"] < 3:
        continue
    avg = st["pnl"] / st["n"]
    print(f"  {a:>+3}          {s:<22} {st['n']:>4}  {st['wr']:>5.1f}%  {st['pnl']:>+7.1f}  {avg:>+5.1f}")


# ---- 8. Summary: best/worst combos ----
print(f"\n{'='*70}")
print(f" TOP FILTERS (min 5 trades)")
print(f"{'='*70}")

# Collect all interesting filters
all_filters = []

# Alignment filters
for a in sorted(align_buckets.keys()):
    for sn in setup_names:
        st = [t for t in align_buckets[a] if t["setup_name"] == sn]
        if len(st) >= 5:
            s = wr_stats(st)
            all_filters.append((f"{sn} @ alignment={a:+d}", s))

# SVB filters
for label in order:
    if label == "N/A":
        continue
    for sn in setup_names:
        st = [t for t in svb_buckets.get(label, []) if t["setup_name"] == sn]
        if len(st) >= 5:
            s = wr_stats(st)
            all_filters.append((f"{sn} @ SVB={label}", s))

# Sort by avg PnL
all_filters.sort(key=lambda x: x[1]["pnl"] / x[1]["n"] if x[1]["n"] else 0, reverse=True)

print(f"\n  BEST (by avg PnL):")
print(f"  {'Filter':<45} {'N':>4}  {'WR':>6}  {'PnL':>8}  {'Avg':>6}")
print(f"  {'-'*45} {'-'*4}  {'-'*6}  {'-'*8}  {'-'*6}")
for label, s in all_filters[:10]:
    avg = s["pnl"] / s["n"]
    print(f"  {label:<45} {s['n']:>4}  {s['wr']:>5.1f}%  {s['pnl']:>+7.1f}  {avg:>+5.1f}")

print(f"\n  WORST (by avg PnL):")
print(f"  {'Filter':<45} {'N':>4}  {'WR':>6}  {'PnL':>8}  {'Avg':>6}")
print(f"  {'-'*45} {'-'*4}  {'-'*6}  {'-'*8}  {'-'*6}")
for label, s in all_filters[-10:]:
    avg = s["pnl"] / s["n"]
    print(f"  {label:<45} {s['n']:>4}  {s['wr']:>5.1f}%  {s['pnl']:>+7.1f}  {avg:>+5.1f}")

print(f"\n{'='*70}")
print(f" DATA COVERAGE")
print(f"{'='*70}")
has_vanna = sum(1 for t in enriched if t["vanna_all"] is not None)
has_svb = sum(1 for t in enriched if t["svb_correlation"] is not None)
has_charm = sum(1 for t in enriched if t["agg_charm"] is not None)
print(f"  Vanna ALL:    {has_vanna}/{len(enriched)} ({has_vanna/len(enriched)*100:.0f}%)")
print(f"  SVB:          {has_svb}/{len(enriched)} ({has_svb/len(enriched)*100:.0f}%)")
print(f"  Charm:        {has_charm}/{len(enriched)} ({has_charm/len(enriched)*100:.0f}%)")
print(f"  Full context: {sum(1 for t in enriched if t['vanna_all'] is not None and t['svb_correlation'] is not None and t['agg_charm'] is not None)}/{len(enriched)}")
