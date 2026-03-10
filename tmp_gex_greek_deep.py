"""Deep dive: GEX Long performance with vs without Greek filter."""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.max_plus_gex, s.paradigm,
               s.outcome_result, s.outcome_pnl, s.ts::date as trade_date,
               s.vanna_all, s.spot_vol_beta, s.greek_alignment
        FROM setup_log s
        WHERE s.setup_name = 'GEX Long'
          AND s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.id
    """)).mappings().all()

# Enrich with charm alignment (need charm from volland)
enriched = []
with engine.begin() as conn:
    for t in trades:
        ts = t["ts_et"]
        agg_charm = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics' IS NOT NULL
              AND ts<=(:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": ts}).mappings().first()
        if snap:
            p = snap["payload"]
            if isinstance(p, str): p = json.loads(p)
            if isinstance(p, dict):
                st = p.get("statistics", {})
                cv = st.get("aggregatedCharm")
                if cv is not None:
                    try: agg_charm = float(cv)
                    except: pass

        charm_aligned = None
        if agg_charm is not None:
            is_long = t["direction"] in ("long", "bullish")
            charm_aligned = (agg_charm > 0) == is_long

        vanna_dir = None
        if t["vanna_all"] is not None:
            vanna_dir = "positive" if t["vanna_all"] > 0 else "negative"

        enriched.append({**dict(t), "agg_charm": agg_charm, "charm_aligned": charm_aligned,
                         "vanna_dir": vanna_dir})

print(f"GEX Long trades: {len(enriched)}\n")

# ---- Helper ----
def stats(tl, label=""):
    if not tl: return None
    n = len(tl)
    w = sum(1 for t in tl if t["outcome_result"] == "WIN")
    l = sum(1 for t in tl if t["outcome_result"] == "LOSS")
    pnl = sum(t["outcome_pnl"] or 0 for t in tl)
    wr = w/n*100
    gw = sum(t["outcome_pnl"] for t in tl if t["outcome_result"] == "WIN" and t["outcome_pnl"])
    gl = abs(sum(t["outcome_pnl"] for t in tl if t["outcome_result"] == "LOSS" and t["outcome_pnl"]))
    pf = gw/gl if gl > 0 else float('inf')
    return {"n": n, "w": w, "l": l, "pnl": pnl, "wr": wr, "pf": pf}

# ---- BASELINE: All GEX Long ----
b = stats(enriched)
print("=" * 80)
print(" GEX LONG — ALL TRADES (no filter)")
print("=" * 80)
print(f"  Trades: {b['n']} | WR: {b['wr']:.1f}% | W/L: {b['w']}/{b['l']} | PnL: {b['pnl']:+.1f} | PF: {b['pf']:.2f}")

# ---- FILTER: Alignment >= +1 (what optimal filter does) ----
filt = [t for t in enriched if t["greek_alignment"] >= 1]
blocked = [t for t in enriched if t["greek_alignment"] < 1]
f = stats(filt)
bl = stats(blocked)
print(f"\n{'='*80}")
print(f" GEX LONG — WITH GREEK FILTER (alignment >= +1)")
print(f"{'='*80}")
print(f"  Passed: {f['n']} | WR: {f['wr']:.1f}% | W/L: {f['w']}/{f['l']} | PnL: {f['pnl']:+.1f} | PF: {f['pf']:.2f}")
print(f"  Blocked: {bl['n']} | WR: {bl['wr']:.1f}% | W/L: {bl['w']}/{bl['l']} | PnL: {bl['pnl']:+.1f}")
print(f"  PnL saved: {abs(bl['pnl']):+.1f} pts")

# ---- Trade-by-trade detail ----
print(f"\n{'='*80}")
print(f" TRADE-BY-TRADE DETAIL")
print(f"{'='*80}")
print(f"  {'ID':>4} {'Date':>12} {'Grade':>6} {'Align':>6} {'Charm':>8} {'Vanna':>9} {'SVB':>7} {'Result':>7} {'PnL':>8} {'Filter':>8}")
print(f"  {'-'*4} {'-'*12} {'-'*6} {'-'*6} {'-'*8} {'-'*9} {'-'*7} {'-'*7} {'-'*8} {'-'*8}")

for t in enriched:
    align = t["greek_alignment"]
    charm = "aligned" if t["charm_aligned"] == True else "opposed" if t["charm_aligned"] == False else "N/A"
    vanna = t["vanna_dir"] or "N/A"
    svb = f"{t['spot_vol_beta']:+.2f}" if t["spot_vol_beta"] is not None else "N/A"
    result = t["outcome_result"]
    pnl = t["outcome_pnl"] or 0
    passed = "PASS" if align >= 1 else "BLOCK"
    date_str = str(t["trade_date"])
    print(f"  {t['id']:>4} {date_str:>12} {t['grade']:>6} {align:>+5} {charm:>8} {vanna:>9} {svb:>7} {result:>7} {pnl:>+7.1f} {passed:>8}")

# ---- Breakdown by alignment ----
print(f"\n{'='*80}")
print(f" BY ALIGNMENT SCORE")
print(f"{'='*80}")
for a in sorted(set(t["greek_alignment"] for t in enriched)):
    sub = [t for t in enriched if t["greek_alignment"] == a]
    s = stats(sub)
    print(f"  Alignment {a:>+2}: {s['n']:>3} trades | WR: {s['wr']:>5.1f}% | PnL: {s['pnl']:>+7.1f} | PF: {s['pf']:>5.2f}")

# ---- Breakdown by charm ----
print(f"\n{'='*80}")
print(f" BY CHARM ALIGNMENT")
print(f"{'='*80}")
for label in ["aligned", "opposed", "N/A"]:
    if label == "aligned": sub = [t for t in enriched if t["charm_aligned"] == True]
    elif label == "opposed": sub = [t for t in enriched if t["charm_aligned"] == False]
    else: sub = [t for t in enriched if t["charm_aligned"] is None]
    if not sub: continue
    s = stats(sub)
    print(f"  Charm {label:<8}: {s['n']:>3} trades | WR: {s['wr']:>5.1f}% | PnL: {s['pnl']:>+7.1f} | PF: {s['pf']:>5.2f}")

# ---- Breakdown by vanna ----
print(f"\n{'='*80}")
print(f" BY VANNA ALL DIRECTION")
print(f"{'='*80}")
for label in ["positive", "negative", "N/A"]:
    if label == "N/A": sub = [t for t in enriched if t["vanna_dir"] is None]
    else: sub = [t for t in enriched if t["vanna_dir"] == label]
    if not sub: continue
    s = stats(sub)
    print(f"  Vanna {label:<8}: {s['n']:>3} trades | WR: {s['wr']:>5.1f}% | PnL: {s['pnl']:>+7.1f} | PF: {s['pf']:>5.2f}")

# ---- Breakdown by SVB ----
print(f"\n{'='*80}")
print(f" BY SPOT-VOL-BETA")
print(f"{'='*80}")
def svb_bucket(v):
    if v is None: return "N/A"
    if v < -0.5: return "Strong neg"
    if v < 0: return "Weak neg"
    if v < 0.5: return "Weak pos"
    return "Strong pos"

for label in ["Strong neg", "Weak neg", "Weak pos", "Strong pos", "N/A"]:
    sub = [t for t in enriched if svb_bucket(t["spot_vol_beta"]) == label]
    if not sub: continue
    s = stats(sub)
    print(f"  SVB {label:<11}: {s['n']:>3} trades | WR: {s['wr']:>5.1f}% | PnL: {s['pnl']:>+7.1f} | PF: {s['pf']:>5.2f}")

# ---- Dollar impact ----
print(f"\n{'='*80}")
print(f" DOLLAR IMPACT (3 MES = SAR 56.25/pt)")
print(f"{'='*80}")
dpp = 3 * 5 * 3.75  # 3 MES x $5 x 3.75 SAR/USD
print(f"  GEX Long WITHOUT filter: {b['pnl']:>+7.1f} pts = SAR {b['pnl']*dpp:>+10,.0f}")
print(f"  GEX Long WITH filter:    {f['pnl']:>+7.1f} pts = SAR {f['pnl']*dpp:>+10,.0f}")
print(f"  Improvement:             {f['pnl']-b['pnl']:>+7.1f} pts = SAR {(f['pnl']-b['pnl'])*dpp:>+10,.0f}")
print(f"  Blocked trades saved:    {abs(bl['pnl']):>+7.1f} pts = SAR {abs(bl['pnl'])*dpp:>+10,.0f}")

# ---- Should we keep or disable GEX Long? ----
print(f"\n{'='*80}")
print(f" VERDICT: KEEP OR DISABLE GEX LONG?")
print(f"{'='*80}")
if f['pnl'] > 0:
    print(f"  WITH Greek filter: GEX Long is PROFITABLE ({f['pnl']:+.1f} pts, {f['wr']:.0f}% WR, PF {f['pf']:.2f})")
    print(f"  KEEP IT ON with the filter. It went from the worst setup to a contributor.")
else:
    print(f"  Even WITH Greek filter: GEX Long is still negative ({f['pnl']:+.1f} pts)")
    print(f"  Consider DISABLING until more data confirms the filter works.")
print(f"\n  Without filter: {b['pnl']:+.1f} pts (TOXIC)")
print(f"  With filter:    {f['pnl']:+.1f} pts")
print(f"  Filter turns GEX Long from SAR {b['pnl']*dpp:+,.0f} to SAR {f['pnl']*dpp:+,.0f}")
