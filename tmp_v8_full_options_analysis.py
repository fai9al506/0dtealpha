"""V8 Full Options Analysis — Feb 5 to Mar 13, deep per-setup breakdown."""
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
               greek_alignment, score, vix, overvix
        FROM setup_log
        WHERE ts >= '2026-02-05' AND ts < '2026-03-14'
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    print(f"Total trades Feb 5 - Mar 13: {len(trades)}")

    # Check which setups exist in each period
    feb_trades = [t for t in trades if t['ts'].month == 2]
    mar_trades = [t for t in trades if t['ts'].month == 3]
    feb_setups = set(t['setup_name'] for t in feb_trades)
    mar_setups = set(t['setup_name'] for t in mar_trades)
    print(f"\nFeb setups ({len(feb_trades)} trades): {sorted(feb_setups)}")
    print(f"Mar setups ({len(mar_trades)} trades): {sorted(mar_setups)}")

    def passes_v8(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        vix_val, ov_val = r['vix'], r['overvix']
        if dirn in ('long', 'bullish'):
            if align is None or align < 2: return False
            if vix_val is not None and vix_val > 26:
                ov = ov_val if ov_val is not None else -99
                if ov < 2: return False
            return True
        else:
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    def passes_v7ag(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        if dirn in ('long', 'bullish'):
            return align is not None and align >= 2
        else:
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    # Also test unfiltered
    def passes_all(r):
        return True

    v8_trades = [r for r in trades if passes_v8(r)]
    v7ag_trades = [r for r in trades if passes_v7ag(r)]
    print(f"\nV8 filtered: {len(v8_trades)}")
    print(f"V7+AG filtered: {len(v7ag_trades)}")
    print(f"Unfiltered: {len(trades)}")

    def get_option_prices(r):
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
        if not entry_snap: return None
        strikes = entry_snap['rows']
        if isinstance(strikes, str): strikes = json.loads(strikes)
        best_strike, best_ask, best_bid, best_delta, best_dd = None, None, None, None, 999
        for sd in strikes:
            if len(sd) < 17: continue
            try:
                delta = float(sd[C_DELTA] if opt_type == 'call' else sd[P_DELTA])
                ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
                bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
            except (TypeError, ValueError): continue
            if ask <= 0: continue
            diff = abs(abs(delta) - 0.30)
            if diff < best_dd:
                best_dd = diff
                best_strike = float(sd[STRIKE])
                best_ask = ask
                best_bid = bid
                best_delta = abs(delta)
        if not best_strike: return None
        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)
        exit_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_exit - timedelta(minutes=2),
            "t2": ts_exit + timedelta(minutes=2),
            "ts": ts_exit}).mappings().first()
        if not exit_snap: return None
        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str): exit_strikes = json.loads(exit_strikes)
        for sd in exit_strikes:
            if len(sd) < 17: continue
            try:
                if float(sd[STRIKE]) == best_strike:
                    exit_bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
                    return {
                        'strike': best_strike, 'entry_ask': best_ask, 'entry_bid': best_bid,
                        'exit_bid': exit_bid, 'entry_delta': best_delta,
                        'pnl_per_contract': (exit_bid - best_ask) * 100,
                        'pnl_pct': (exit_bid - best_ask) / best_ask * 100 if best_ask > 0 else 0,
                        'capital_per_trade': best_ask * 100,
                        'opt_type': opt_type, 'elapsed_min': elapsed,
                    }
            except (TypeError, ValueError): continue
        return None

    # Enrich all trades
    print("\nLoading real option prices for ALL trades...", flush=True)
    all_enriched = []
    no_price = 0
    for r in trades:
        opt = get_option_prices(r)
        if opt is None:
            no_price += 1
            continue
        all_enriched.append({**dict(r), **opt})
    print(f"Got {len(all_enriched)} trades with prices, {no_price} skipped")

    # Split by filter
    v8_e = [t for t in all_enriched if passes_v8(t)]
    v7ag_e = [t for t in all_enriched if passes_v7ag(t)]

    def analyze_group(name, tlist, show_daily=False, show_top=False):
        if not tlist:
            print(f"\n  {name}: 0 trades")
            return {}
        wins = [t for t in tlist if t['pnl_per_contract'] > 0]
        losses = [t for t in tlist if t['pnl_per_contract'] <= 0]
        total_pnl = sum(t['pnl_per_contract'] for t in tlist)
        gross_win = sum(t['pnl_per_contract'] for t in wins)
        gross_loss = abs(sum(t['pnl_per_contract'] for t in losses))
        pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
        wr = len(wins) / len(tlist) * 100
        avg_win = gross_win / len(wins) if wins else 0
        avg_loss = gross_loss / len(losses) if losses else 0
        # MaxDD
        cum = 0; peak = 0; max_dd = 0
        for t in sorted(tlist, key=lambda x: x['ts']):
            cum += t['pnl_per_contract']
            if cum > peak: peak = cum
            dd = peak - cum
            if dd > max_dd: max_dd = dd
        days = sorted(set(t['ts'].strftime('%Y-%m-%d') for t in tlist))
        num_days = len(days)
        avg_entry = sum(t['entry_ask'] for t in tlist) / len(tlist)
        total_cap = sum(t['capital_per_trade'] for t in tlist)

        print(f"\n  {name}")
        print(f"  {'─'*60}")
        print(f"  Trades: {len(tlist)} ({len(wins)}W/{len(losses)}L) | WR: {wr:.1f}%")
        print(f"  P&L: ${total_pnl:>+,.0f} | PF: {pf:.2f} | MaxDD: ${max_dd:>,.0f}")
        print(f"  Avg win: ${avg_win:>,.0f} | Avg loss: -${avg_loss:>,.0f}")
        print(f"  Days: {num_days} | P&L/day: ${total_pnl/num_days:>+,.0f}")
        print(f"  Avg entry prem: ${avg_entry:.2f} | Total capital deployed: ${total_cap:>,.0f}")

        if show_daily:
            print(f"\n  Daily P&L:")
            daily = defaultdict(list)
            for t in tlist:
                daily[t['ts'].strftime('%Y-%m-%d')].append(t)
            cum_pnl = 0
            for d in sorted(daily.keys()):
                dt = daily[d]
                dp = sum(t['pnl_per_contract'] for t in dt)
                dw = sum(1 for t in dt if t['pnl_per_contract'] > 0)
                cum_pnl += dp
                cap = sum(t['capital_per_trade'] for t in dt)
                avg_vix = sum(float(t['vix']) for t in dt if t['vix']) / max(1, sum(1 for t in dt if t['vix']))
                print(f"    {d}  {len(dt):>2d}t {dw:>2d}W  ${dp:>+8,.0f}  cum=${cum_pnl:>+9,.0f}  cap=${cap:>7,.0f}  VIX={avg_vix:.1f}")

        if show_top:
            print(f"\n  Top 5 Winners:")
            for t in sorted(tlist, key=lambda x: x['pnl_per_contract'], reverse=True)[:5]:
                print(f"    #{t['id']:<5d} {t['setup_name']:<20s} {t['direction']:<6s} "
                      f"${t['entry_ask']:.2f}→${t['exit_bid']:.2f} ${t['pnl_per_contract']:>+7,.0f} ({t['pnl_pct']:>+.0f}%) {t['elapsed_min']:.0f}m")
            print(f"\n  Top 5 Losers:")
            for t in sorted(tlist, key=lambda x: x['pnl_per_contract'])[:5]:
                print(f"    #{t['id']:<5d} {t['setup_name']:<20s} {t['direction']:<6s} "
                      f"${t['entry_ask']:.2f}→${t['exit_bid']:.2f} ${t['pnl_per_contract']:>+7,.0f} ({t['pnl_pct']:>+.0f}%) {t['elapsed_min']:.0f}m")

        return {'pnl': total_pnl, 'trades': len(tlist), 'wr': wr, 'pf': pf,
                'max_dd': max_dd, 'days': num_days, 'avg_entry': avg_entry}

    # ========== OVERALL COMPARISON ==========
    print(f"\n{'='*70}")
    print(f" OVERALL COMPARISON — Feb 5 to Mar 13 (Real Option Prices)")
    print(f"{'='*70}")

    r_all = analyze_group("UNFILTERED (all trades)", all_enriched)
    r_v7 = analyze_group("V7+AG", v7ag_e)
    r_v8 = analyze_group("V8 (V7+AG + VIX Gate)", v8_e, show_daily=True, show_top=True)

    # ========== PER-SETUP DEEP ANALYSIS ==========
    print(f"\n{'='*70}")
    print(f" PER-SETUP ANALYSIS (V8 filtered, real option prices)")
    print(f"{'='*70}")

    by_setup = defaultdict(list)
    for t in v8_e:
        by_setup[t['setup_name']].append(t)

    for sn in sorted(by_setup.keys()):
        st = by_setup[sn]
        analyze_group(f"  {sn}", st, show_top=True)

        # Direction breakdown
        for dirn in ['long', 'bullish', 'short', 'bearish']:
            dt = [t for t in st if t['direction'] == dirn]
            if not dt: continue
            dw = sum(1 for t in dt if t['pnl_per_contract'] > 0)
            dp = sum(t['pnl_per_contract'] for t in dt)
            dwr = dw / len(dt) * 100
            print(f"      {dirn}: {len(dt)}t {dwr:.0f}% WR ${dp:>+,.0f}")

        # By grade
        by_grade = defaultdict(list)
        for t in st:
            by_grade[t['grade'] or 'None'].append(t)
        if len(by_grade) > 1:
            print(f"    By grade:")
            for g in sorted(by_grade.keys()):
                gt = by_grade[g]
                gw = sum(1 for t in gt if t['pnl_per_contract'] > 0)
                gp = sum(t['pnl_per_contract'] for t in gt)
                gwr = gw / len(gt) * 100
                print(f"      {g}: {len(gt)}t {gwr:.0f}% WR ${gp:>+,.0f}")

        # By alignment
        by_align = defaultdict(list)
        for t in st:
            by_align[t['greek_alignment']].append(t)
        if len(by_align) > 1:
            print(f"    By alignment:")
            for a in sorted(by_align.keys()):
                at = by_align[a]
                aw = sum(1 for t in at if t['pnl_per_contract'] > 0)
                ap = sum(t['pnl_per_contract'] for t in at)
                awr = aw / len(at) * 100
                print(f"      align={a:>+3d}: {len(at):>3d}t {awr:>5.0f}% WR ${ap:>+8,.0f}")

        # By time of day
        by_hour = defaultdict(list)
        for t in st:
            by_hour[t['ts'].hour].append(t)
        print(f"    By hour:")
        for h in sorted(by_hour.keys()):
            ht = by_hour[h]
            hw = sum(1 for t in ht if t['pnl_per_contract'] > 0)
            hp = sum(t['pnl_per_contract'] for t in ht)
            hwr = hw / len(ht) * 100
            print(f"      {h:02d}:00: {len(ht):>3d}t {hwr:>5.0f}% WR ${hp:>+8,.0f}")

        # Avg hold time winners vs losers
        w_hold = [t['elapsed_min'] for t in st if t['pnl_per_contract'] > 0 and t['elapsed_min']]
        l_hold = [t['elapsed_min'] for t in st if t['pnl_per_contract'] <= 0 and t['elapsed_min']]
        if w_hold and l_hold:
            print(f"    Avg hold: winners={sum(w_hold)/len(w_hold):.0f}min losers={sum(l_hold)/len(l_hold):.0f}min")

    # ========== FEB vs MAR COMPARISON ==========
    print(f"\n{'='*70}")
    print(f" FEB vs MAR COMPARISON (V8 filtered)")
    print(f"{'='*70}")

    feb_v8 = [t for t in v8_e if t['ts'].month == 2]
    mar_v8 = [t for t in v8_e if t['ts'].month == 3]

    analyze_group("February (V8)", feb_v8)
    analyze_group("March (V8)", mar_v8)

    # Per-setup Feb vs Mar
    print(f"\n  Per-setup Feb vs Mar:")
    all_setups = sorted(set(t['setup_name'] for t in v8_e))
    print(f"  {'Setup':<22s} {'Feb Trades':>10s} {'Feb WR':>7s} {'Feb P&L':>9s} {'Mar Trades':>10s} {'Mar WR':>7s} {'Mar P&L':>9s}")
    print(f"  {'-'*22} {'-'*10} {'-'*7} {'-'*9} {'-'*10} {'-'*7} {'-'*9}")
    for sn in all_setups:
        ft = [t for t in feb_v8 if t['setup_name'] == sn]
        mt = [t for t in mar_v8 if t['setup_name'] == sn]
        fw = sum(1 for t in ft if t['pnl_per_contract'] > 0) / len(ft) * 100 if ft else 0
        fp = sum(t['pnl_per_contract'] for t in ft)
        mw = sum(1 for t in mt if t['pnl_per_contract'] > 0) / len(mt) * 100 if mt else 0
        mp = sum(t['pnl_per_contract'] for t in mt)
        print(f"  {sn:<22s} {len(ft):>10d} {fw:>6.0f}% ${fp:>+8,.0f} {len(mt):>10d} {mw:>6.0f}% ${mp:>+8,.0f}")

    # ========== VIX REGIME COMPARISON ==========
    print(f"\n{'='*70}")
    print(f" VIX REGIME ANALYSIS (V8 filtered)")
    print(f"{'='*70}")

    for vix_lo, vix_hi, label in [(0, 18, "VIX < 18"), (18, 20, "VIX 18-20"),
                                    (20, 22, "VIX 20-22"), (22, 24, "VIX 22-24"),
                                    (24, 26, "VIX 24-26"), (26, 99, "VIX 26+")]:
        vt = [t for t in v8_e if t['vix'] and vix_lo <= float(t['vix']) < vix_hi]
        if not vt: continue
        vw = sum(1 for t in vt if t['pnl_per_contract'] > 0)
        vp = sum(t['pnl_per_contract'] for t in vt)
        vwr = vw / len(vt) * 100
        avg_entry = sum(t['entry_ask'] for t in vt) / len(vt)
        print(f"  {label:<12s}: {len(vt):>3d}t  {vwr:>5.1f}% WR  ${vp:>+9,.0f}  avg_prem=${avg_entry:.2f}")

    # ========== CAPITAL & INCOME PROJECTIONS (FULL PERIOD) ==========
    print(f"\n{'='*70}")
    print(f" CAPITAL & INCOME (V8, full period)")
    print(f"{'='*70}")

    daily = defaultdict(list)
    for t in v8_e:
        daily[t['ts'].strftime('%Y-%m-%d')].append(t)

    day_caps = []
    for d in sorted(daily.keys()):
        dt = daily[d]
        cap = sum(t['capital_per_trade'] for t in dt)
        pnl = sum(t['pnl_per_contract'] for t in dt)
        day_caps.append({'date': d, 'cap': cap, 'pnl': pnl, 'trades': len(dt)})

    num_days = len(day_caps)
    total_pnl = sum(d['pnl'] for d in day_caps)
    max_cap = max(d['cap'] for d in day_caps)
    avg_cap = sum(d['cap'] for d in day_caps) / num_days
    worst_day = min(d['pnl'] for d in day_caps)
    best_day = max(d['pnl'] for d in day_caps)
    winning_days = sum(1 for d in day_caps if d['pnl'] > 0)

    print(f"\n  SPX (1 contract per signal):")
    print(f"    Trading days: {num_days}")
    print(f"    Total P&L: ${total_pnl:>+,.0f}")
    print(f"    Avg daily P&L: ${total_pnl/num_days:>+,.0f}")
    print(f"    Best day: ${best_day:>+,.0f}")
    print(f"    Worst day: ${worst_day:>+,.0f}")
    print(f"    Winning days: {winning_days}/{num_days} ({winning_days/num_days*100:.0f}%)")
    print(f"    Max daily capital: ${max_cap:>,.0f}")
    print(f"    Avg daily capital: ${avg_cap:>,.0f}")

    spy_factor = 0.1
    spy_total = total_pnl * spy_factor
    spy_daily = spy_total / num_days
    spy_max_cap = max_cap * spy_factor
    spy_worst = worst_day * spy_factor
    spy_monthly = spy_daily * 21

    print(f"\n  SPY (1 contract per signal, SPX/10):")
    print(f"    Total P&L: ${spy_total:>+,.0f}")
    print(f"    Avg daily P&L: ${spy_daily:>+,.0f}")
    print(f"    Monthly projection: ${spy_monthly:>+,.0f}")
    print(f"    Max daily capital: ${spy_max_cap:>,.0f}")
    print(f"    Worst day: ${spy_worst:>+,.0f}")
    print(f"    Account needed (comfortable): ${spy_max_cap + abs(spy_worst):>,.0f}")

    # Growth projection with full-period data
    acct_per_qty = spy_max_cap + abs(spy_worst)
    print(f"\n  Growth projection (starting $4,000, conservative 75% performance):")
    balance = 4000.0
    pnl_per = spy_monthly * 0.75
    for month in range(1, 13):
        qty = max(1, int(balance // acct_per_qty))
        qty = min(qty, 50)
        m_pnl = pnl_per * qty
        balance += m_pnl
        print(f"    Month {month:>2d}: {qty:>3d} SPY  ${m_pnl:>+10,.0f}  bal=${balance:>12,.0f}")

print("\nDone.")
