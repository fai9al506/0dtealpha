"""Hunt for 90% WR filter using real option prices."""
import sys, io, json, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta

C_DELTA, C_ASK, C_BID, STRIKE, P_DELTA, P_ASK, P_BID = 4, 7, 5, 10, 16, 12, 14
e = create_engine(os.environ['DATABASE_URL'])

with e.connect() as conn:
    trades = conn.execute(text("""
        SELECT id, setup_name, direction, grade, spot, ts,
               outcome_result, outcome_pnl, outcome_elapsed_min,
               greek_alignment, score
        FROM setup_log
        WHERE ts >= '2026-03-01' AND ts < '2026-04-01'
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    def passes_v7ag(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        if dirn in ('long', 'bullish'):
            return align is not None and align >= 2
        else:
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    filtered = [r for r in trades if passes_v7ag(r)]

    def get_option_pnl(r):
        ts_entry = r['ts']
        is_long = r['direction'] in ('long', 'bullish')
        elapsed = r['outcome_elapsed_min'] or 0
        opt_type = 'call' if is_long else 'put'
        entry_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_entry - timedelta(minutes=2),
            "t2": ts_entry + timedelta(minutes=2),
            "ts": ts_entry}).mappings().first()
        if not entry_snap:
            return None
        strikes = entry_snap['rows']
        if isinstance(strikes, str):
            strikes = json.loads(strikes)
        best_strike, best_ask, best_dd = None, None, 999
        for sd in strikes:
            if len(sd) < 17:
                continue
            try:
                delta = float(sd[C_DELTA] if opt_type == 'call' else sd[P_DELTA])
                ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
            except (TypeError, ValueError):
                continue
            if ask <= 0:
                continue
            diff = abs(abs(delta) - 0.30)
            if diff < best_dd:
                best_dd = diff
                best_strike = sd[STRIKE]
                best_ask = ask
        if not best_strike:
            return None
        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)
        exit_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_exit - timedelta(minutes=2),
            "t2": ts_exit + timedelta(minutes=2),
            "ts": ts_exit}).mappings().first()
        if not exit_snap:
            return None
        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str):
            exit_strikes = json.loads(exit_strikes)
        for sd in exit_strikes:
            if len(sd) < 17:
                continue
            if float(sd[STRIKE]) == best_strike:
                try:
                    exit_bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
                    return (exit_bid - best_ask) * 100
                except (TypeError, ValueError):
                    return None
        return None

    print("Loading real option prices...", flush=True)
    enriched = []
    for r in filtered:
        opt_pnl = get_option_pnl(r)
        if opt_pnl is None:
            continue
        enriched.append({**dict(r), 'opt_pnl': opt_pnl})
    print(f"Got {len(enriched)} trades with prices")
    print()

    days_count = len(set(r['ts'].strftime('%Y-%m-%d') for r in enriched))

    def ef(name, tlist):
        if len(tlist) < 3:
            print(f"  {name:<55s} {len(tlist):>3d}t  (too few)")
            return
        wins = sum(1 for t in tlist if t['opt_pnl'] > 0)
        wr = wins / len(tlist) * 100
        total_pnl = sum(t['opt_pnl'] for t in tlist)
        avg_day = len(tlist) / days_count
        mark = " <== HIGH WR" if wr >= 85 else ""
        print(f"  {name:<55s} {len(tlist):>3d}t {avg_day:>4.1f}/d WR:{wr:>5.1f}% PnL:${total_pnl/10:>+7,.0f}(SPY){mark}")

    print("=" * 110)
    print("HUNTING FOR 90%+ WIN RATE (option WR = closed higher than entry)")
    print("=" * 110)

    print("\n--- BY SETUP ---")
    for s in ['Skew Charm', 'DD Exhaustion', 'AG Short', 'ES Absorption', 'BofA Scalp', 'GEX Long', 'Paradigm Reversal']:
        ef(s, [t for t in enriched if t['setup_name'] == s])

    print("\n--- BY SETUP + DIRECTION ---")
    for s in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
        for d in ['long', 'short']:
            dm = ('long', 'bullish') if d == 'long' else ('short', 'bearish')
            ef(f"{s} {d.upper()}", [t for t in enriched if t['setup_name'] == s and t['direction'] in dm])

    print("\n--- BY SETUP + GRADE ---")
    for s in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
        for g in ['A+', 'A', 'A-Entry']:
            ef(f"{s} grade={g}", [t for t in enriched if t['setup_name'] == s and t['grade'] == g])

    print("\n--- BY SETUP + ALIGNMENT ---")
    for s in ['Skew Charm', 'DD Exhaustion']:
        for a in [3, 2, -1, -2, -3]:
            ef(f"{s} align={a}", [t for t in enriched if t['setup_name'] == s and t['greek_alignment'] == a])

    print("\n--- SC DIRECTION + ALIGNMENT ---")
    ef("SC LONG align=3", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['direction'] in ('long', 'bullish') and t['greek_alignment'] == 3])
    ef("SC LONG align>=2", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['direction'] in ('long', 'bullish') and (t['greek_alignment'] or 0) >= 2])
    ef("SC SHORT align<=-2", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['direction'] not in ('long', 'bullish') and (t['greek_alignment'] or 0) <= -2])
    ef("SC SHORT align=-3", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['direction'] not in ('long', 'bullish') and t['greek_alignment'] == -3])

    print("\n--- TIME FILTERS ---")
    ef("SC before noon ET (UTC<17)", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['ts'].hour < 17])
    ef("SC 10:00-12:00 ET (UTC 15-17)", [t for t in enriched if t['setup_name'] == 'Skew Charm' and 15 <= t['ts'].hour < 17])
    ef("SC 10:00-13:00 ET (UTC 15-18)", [t for t in enriched if t['setup_name'] == 'Skew Charm' and 15 <= t['ts'].hour < 18])
    ef("SC after 12:00 ET (UTC>=17)", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['ts'].hour >= 17])

    print("\n--- PORTAL PTS FILTERS ---")
    ef("Portal WIN trades (pts hit target)", [t for t in enriched if t['outcome_result'] == 'WIN'])
    ef("Portal WIN >= 10 pts", [t for t in enriched if t['outcome_result'] == 'WIN' and (t['outcome_pnl'] or 0) >= 10])
    ef("Portal pnl >= 8 pts (all setups)", [t for t in enriched if (t['outcome_pnl'] or 0) >= 8])
    ef("Portal pnl >= 10 pts (all setups)", [t for t in enriched if (t['outcome_pnl'] or 0) >= 10])
    ef("SC portal WIN", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['outcome_result'] == 'WIN'])

    print("\n--- COMBO HIGH-WR CANDIDATES ---")
    ef("SC LONG before noon", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['direction'] in ('long', 'bullish') and t['ts'].hour < 17])
    ef("SC LONG align=3 before noon", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['direction'] in ('long', 'bullish') and t['greek_alignment'] == 3 and t['ts'].hour < 17])
    ef("SC + portal WIN (option P&L on portal WINs)", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['outcome_result'] == 'WIN'])
    ef("AG SHORT align=-3", [t for t in enriched if t['setup_name'] == 'AG Short' and t['greek_alignment'] == -3])
    ef("SC grade=A + LONG", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['grade'] == 'A' and t['direction'] in ('long', 'bullish')])
    ef("SC grade=A+ + LONG", [t for t in enriched if t['setup_name'] == 'Skew Charm' and t['grade'] == 'A+' and t['direction'] in ('long', 'bullish')])
