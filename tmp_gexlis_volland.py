"""
Can Volland metrics (charm, DD hedging) distinguish GEX-LIS winners from losers?
Goal: keep #995 (+20.5 WIN) while blocking today's 5 losers.
"""
import sqlalchemy as sa
import os
from collections import defaultdict

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    # Get ALL SC/DD shorts with V9-SC filter
    all_shorts = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date,
               outcome_max_profit, outcome_max_loss,
               vanna_all, vanna_weekly, vanna_monthly, spot_vol_beta
        FROM setup_log
        WHERE setup_name IN ('Skew Charm', 'DD Exhaustion')
          AND direction IN ('short', 'bearish')
          AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    def passes_v9sc(r):
        al = r.greek_alignment or 0
        if r.direction in ("long", "bullish"):
            if al < 2: return False
            if r.setup_name == "Skew Charm": return True
            if r.vix is not None and r.vix > 22:
                ov = r.overvix if r.overvix is not None else -99
                if ov < 2: return False
            return True
        else:
            if r.setup_name in ("Skew Charm", "AG Short"): return True
            if r.setup_name == "DD Exhaustion" and al != 0: return True
            return False

    live_shorts = [r for r in all_shorts if passes_v9sc(r)]

    def parse_dollar(s):
        if s is None: return None
        if isinstance(s, (int, float)): return float(s)
        s = str(s).replace("$", "").replace(",", "").strip()
        try: return float(s)
        except: return None

    # Enrich with volland data
    enriched = []
    for r in live_shorts:
        vs = c.execute(sa.text("""
            SELECT ts,
                   payload->'statistics'->>'aggregatedCharm' as agg_charm,
                   payload->'statistics'->>'delta_decay_hedging' as dd_hedging
            FROM volland_snapshots
            WHERE ts <= :trade_ts AND ts::date = :trade_date
              AND payload->'statistics' IS NOT NULL
              AND payload->'statistics'->>'aggregatedCharm' IS NOT NULL
            ORDER BY ts DESC LIMIT 1
        """), {"trade_ts": r.ts, "trade_date": r.trade_date}).fetchone()

        charm = parse_dollar(vs.agg_charm) if vs else None
        dd = parse_dollar(vs.dd_hedging) if vs else None
        enriched.append({"r": r, "charm": charm, "dd": dd})

    def stats(subset, label):
        if not subset:
            print(f"  {label}: 0 trades")
            return 0
        tlist = [t["r"] for t in subset]
        w = sum(1 for t in tlist if t.outcome_result == 'WIN')
        lo = sum(1 for t in tlist if t.outcome_result == 'LOSS')
        ex = sum(1 for t in tlist if t.outcome_result == 'EXPIRED')
        pnl = round(sum(t.outcome_pnl or 0 for t in tlist), 1)
        wr = round(w/(w+lo)*100,1) if w+lo > 0 else 0
        # MaxDD
        running = peak = max_dd = 0
        for t in sorted(tlist, key=lambda x: x.ts):
            running += t.outcome_pnl or 0
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd
        print(f"  {label}: {len(tlist)}t, {w}W/{lo}L/{ex}E, WR={wr}%, PnL={pnl:+.1f}, MaxDD={max_dd:.1f}")
        return pnl

    # ---- GEX-LIS DETAIL ----
    print("="*130)
    print("GEX-LIS TRADES: CHARM & DD AT TIME OF FIRE")
    print("="*130)

    gex_lis = [t for t in enriched if t["r"].paradigm == "GEX-LIS"]
    for t in gex_lis:
        r = t["r"]
        p = r.outcome_pnl or 0
        mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
        charm_s = f"{t['charm']/1e6:+.0f}M" if t['charm'] is not None else "n/a"
        dd_s = f"{t['dd']/1e9:+.2f}B" if t['dd'] is not None else "n/a"
        charm_sign = "POS" if t['charm'] and t['charm'] > 0 else ("NEG" if t['charm'] and t['charm'] < 0 else "n/a")
        dd_sign = "POS" if t['dd'] and t['dd'] > 0 else ("NEG" if t['dd'] and t['dd'] < 0 else "n/a")
        print(f"  #{r.id:4d} {r.trade_date} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f} mxP={mxp:>6s} | charm={charm_s:>8s} [{charm_sign}] dd={dd_s:>8s} [{dd_sign}] | vix={r.vix:.1f}")

    # ---- GEX-LIS: CHARM BREAKDOWNS ----
    print(f"\n{'='*130}")
    print("GEX-LIS SHORTS: BY CHARM SIGN")
    print("="*130)
    charm_neg = [t for t in gex_lis if t["charm"] is not None and t["charm"] < 0]
    charm_pos = [t for t in gex_lis if t["charm"] is not None and t["charm"] >= 0]
    charm_na = [t for t in gex_lis if t["charm"] is None]
    stats(charm_neg, "Charm NEGATIVE (bearish pressure — good for shorts)")
    stats(charm_pos, "Charm POSITIVE (bullish support — bad for shorts)")
    stats(charm_na, "Charm N/A")

    # ---- GEX-LIS: DD BREAKDOWNS ----
    print(f"\n{'='*130}")
    print("GEX-LIS SHORTS: BY DD HEDGING")
    print("="*130)
    dd_pos = [t for t in gex_lis if t["dd"] is not None and t["dd"] > 0]
    dd_neg = [t for t in gex_lis if t["dd"] is not None and t["dd"] < 0]
    stats(dd_pos, "DD POSITIVE (dealers long — bullish = bad for shorts)")
    stats(dd_neg, "DD NEGATIVE (dealers short — bearish = good for shorts)")

    # ---- COMBINED FILTER TESTS ----
    print(f"\n{'='*130}")
    print("COMBINED FILTERS: SC/DD SHORTS (ALL paradigms)")
    print("="*130)

    filters = {
        "Baseline V9-SC": lambda t: True,
        "Block GEX-LIS (all)": lambda t: t["r"].paradigm != "GEX-LIS",
        "Block GEX-LIS when charm >= 0": lambda t: not (t["r"].paradigm == "GEX-LIS" and t["charm"] is not None and t["charm"] >= 0),
        "Block GEX-LIS when charm > -100M": lambda t: not (t["r"].paradigm == "GEX-LIS" and (t["charm"] is None or t["charm"] > -100_000_000)),
        "Block GEX-LIS when dd > 0": lambda t: not (t["r"].paradigm == "GEX-LIS" and t["dd"] is not None and t["dd"] > 0),
        "Allow GEX-LIS ONLY if charm < 0": lambda t: t["r"].paradigm != "GEX-LIS" or (t["charm"] is not None and t["charm"] < 0),
        "Allow GEX-LIS ONLY if charm < 0 AND dd < 0": lambda t: t["r"].paradigm != "GEX-LIS" or (t["charm"] is not None and t["charm"] < 0 and t["dd"] is not None and t["dd"] < 0),
        "Allow GEX-LIS ONLY if charm < -100M": lambda t: t["r"].paradigm != "GEX-LIS" or (t["charm"] is not None and t["charm"] < -100_000_000),
        "Allow GEX-LIS ONLY if charm < -200M": lambda t: t["r"].paradigm != "GEX-LIS" or (t["charm"] is not None and t["charm"] < -200_000_000),
    }

    for fname, fn in filters.items():
        kept = [t for t in enriched if fn(t)]
        blocked = [t for t in enriched if not fn(t)]
        print(f"\n--- {fname} ---")
        stats(kept, "KEPT")
        if blocked:
            stats(blocked, "BLOCKED")

    # ---- TODAY: which filter keeps #995 and blocks losers? ----
    print(f"\n{'='*130}")
    print("TODAY'S GEX-LIS TRADES: FILTER COMPARISON")
    print("="*130)
    today_gex = [t for t in enriched if t["r"].trade_date.month == 3 and t["r"].trade_date.day == 20
                 and t["r"].paradigm == "GEX-LIS"]
    for t in today_gex:
        r = t["r"]
        p = r.outcome_pnl or 0
        charm_s = f"{t['charm']/1e6:+.0f}M" if t['charm'] is not None else "n/a"
        dd_s = f"{t['dd']/1e9:+.2f}B" if t['dd'] is not None else "n/a"
        checks = {
            "!GL": r.paradigm != "GEX-LIS",
            "GL+charm>=0": not (t["charm"] is not None and t["charm"] >= 0),
            "GL_ok_charm<0": t["charm"] is not None and t["charm"] < 0,
            "GL_ok_charm<-100M": t["charm"] is not None and t["charm"] < -100_000_000,
            "GL_ok_charm<-200M": t["charm"] is not None and t["charm"] < -200_000_000,
        }
        flags = " | ".join(f"{k}={'PASS' if v else 'BLOCK'}" for k, v in checks.items())
        print(f"  #{r.id} {r.setup_name:15s} {r.outcome_result:8s} {p:+6.1f}pts charm={charm_s:>8s} dd={dd_s:>8s} | {flags}")

    # ---- FULL SYSTEM IMPACT ----
    print(f"\n{'='*130}")
    print("FULL V9-SC SYSTEM IMPACT (all setups)")
    print("="*130)

    all_live = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, ts::date as trade_date
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts
    """)).fetchall()

    all_v9sc = [r for r in all_live if passes_v9sc(r)]
    base_pnl = sum(r.outcome_pnl or 0 for r in all_v9sc)

    # For charm-based filter, need to enrich GEX-LIS trades
    gl_ids = {t["r"].id: t for t in gex_lis}

    for fname in ["Block GEX-LIS (all)", "Allow GEX-LIS ONLY if charm < 0",
                   "Allow GEX-LIS ONLY if charm < -200M"]:
        filt_pnl = 0
        for r in all_v9sc:
            is_sc_dd_short = (r.setup_name in ("Skew Charm", "DD Exhaustion") and
                              r.direction in ("short", "bearish"))
            if is_sc_dd_short and r.paradigm == "GEX-LIS":
                t = gl_ids.get(r.id)
                if t is None:
                    continue  # no volland data
                if fname == "Block GEX-LIS (all)":
                    continue
                elif fname == "Allow GEX-LIS ONLY if charm < 0":
                    if t["charm"] is not None and t["charm"] < 0:
                        filt_pnl += r.outcome_pnl or 0
                elif fname == "Allow GEX-LIS ONLY if charm < -200M":
                    if t["charm"] is not None and t["charm"] < -200_000_000:
                        filt_pnl += r.outcome_pnl or 0
            else:
                filt_pnl += r.outcome_pnl or 0
        print(f"  {fname:45s}: {filt_pnl:+.1f} pts (vs base {base_pnl:+.1f}, diff {filt_pnl-base_pnl:+.1f})")
