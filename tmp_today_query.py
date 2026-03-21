import sqlalchemy as sa, os

e = sa.create_engine(os.environ['DATABASE_URL'])
with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, score, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix, charm_limit_entry,
               outcome_max_profit, outcome_max_loss, outcome_stop_level,
               outcome_target_level, lis, target
        FROM setup_log
        WHERE ts::date = '2026-03-20'
        ORDER BY ts
    """)).fetchall()

    # Apply actual V9-SC filter logic
    def passes_v9sc(name, direction, align, vix, overvix):
        if name == "VIX Compression":
            return False
        is_long = direction in ("long", "bullish")
        al = align or 0
        if is_long:
            if al < 2:
                return False
            if name == "Skew Charm":
                return True
            if vix is not None and vix > 22:
                ov = overvix if overvix is not None else -99
                if ov < 2:
                    return False
            return True
        else:
            if name in ("Skew Charm", "AG Short"):
                return True
            if name == "DD Exhaustion" and al != 0:
                return True
            return False

    live = []
    blocked = []
    for r in rows:
        al = r.greek_alignment or 0
        if passes_v9sc(r.setup_name, r.direction, al, r.vix, r.overvix):
            live.append(r)
        else:
            blocked.append(r)

    print(f"Total: {len(rows)} | Live: {len(live)} | Blocked: {len(blocked)}")
    print()

    print(f"=== V9-SC LIVE ({len(live)}) ===")
    t = 0
    for r in live:
        p = r.outcome_pnl or 0
        t += p
        res = r.outcome_result or 'OPEN'
        mxp = f"{r.outcome_max_profit:+.1f}" if r.outcome_max_profit is not None else "n/a"
        mxl = f"{r.outcome_max_loss:+.1f}" if r.outcome_max_loss is not None else "n/a"
        print(f"#{r.id} {r.setup_name:20s} {r.direction:5s} al={r.greek_alignment or 0:+d} {res:8s} pnl={p:+.1f} mxP={mxp} mxL={mxl} spot={r.spot} vix={r.vix} t={str(r.ts)[11:16]} charm={r.charm_limit_entry}")
    print(f"LIVE total: {t:+.1f} pts")

    print(f"\n=== BLOCKED BY V9-SC ({len(blocked)}) ===")
    bt = 0
    for r in blocked:
        p = r.outcome_pnl or 0
        bt += p
        res = r.outcome_result or 'OPEN'
        is_long = r.direction in ("long", "bullish")
        reason = ""
        al = r.greek_alignment or 0
        if is_long:
            if al < 2: reason = f"align {al:+d}<+2"
            elif r.vix and r.vix > 22: reason = f"VIX {r.vix:.1f}>22"
        else:
            reason = f"{r.setup_name} short not whitelisted"
        print(f"#{r.id} {r.setup_name:20s} {r.direction:7s} al={al:+d} {res:8s} pnl={p:+.1f} [{reason}]")
    print(f"BLOCKED total: {bt:+.1f} pts")

    # Breakdown
    print(f"\n=== LIVE BY SETUP ===")
    from collections import defaultdict
    by = defaultdict(lambda: {'n': 0, 'pnl': 0, 'w': 0, 'l': 0, 'o': 0})
    for r in live:
        s = by[r.setup_name]
        s['n'] += 1
        s['pnl'] += r.outcome_pnl or 0
        if r.outcome_result == 'WIN': s['w'] += 1
        elif r.outcome_result == 'LOSS': s['l'] += 1
        else: s['o'] += 1
    for name, s in sorted(by.items(), key=lambda x: x[1]['pnl']):
        print(f"  {name:20s}: {s['n']}t {s['w']}W/{s['l']}L/{s['o']}O  {s['pnl']:+.1f} pts")
