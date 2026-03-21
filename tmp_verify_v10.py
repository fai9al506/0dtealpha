"""Verify V9-SC vs V10 for today Mar 20 — match portal numbers."""
import sqlalchemy as sa
import os

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT id, setup_name, direction, grade, greek_alignment,
               outcome_result, outcome_pnl, ts, spot, paradigm,
               vix, overvix
        FROM setup_log
        WHERE ts::date = '2026-03-20'
          AND grade != 'LOG'
        ORDER BY ts
    """)).fetchall()

    def passes_v9sc(r):
        if r.setup_name == "VIX Compression": return False
        al = r.greek_alignment or 0
        is_long = r.direction in ("long", "bullish")
        if is_long:
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

    def passes_v10(r):
        if not passes_v9sc(r): return False
        is_short = r.direction in ("short", "bearish")
        if is_short and r.setup_name in ("Skew Charm", "DD Exhaustion") and r.paradigm == "GEX-LIS":
            return False
        return True

    v9 = [r for r in rows if passes_v9sc(r)]
    v10 = [r for r in rows if passes_v10(r)]

    # Include ALL outcomes (WIN, LOSS, EXPIRED, and NULL/OPEN)
    print("=== ALL trades today (no grade=LOG) ===")
    print(f"Total: {len(rows)}")

    def show(trades, label):
        resolved = [r for r in trades if r.outcome_result in ('WIN', 'LOSS', 'EXPIRED')]
        opens = [r for r in trades if r.outcome_result is None]
        w = sum(1 for r in resolved if r.outcome_result == 'WIN')
        lo = sum(1 for r in resolved if r.outcome_result == 'LOSS')
        ex = sum(1 for r in resolved if r.outcome_result == 'EXPIRED')
        pnl = sum(r.outcome_pnl or 0 for r in resolved)

        print(f"\n=== {label} ===")
        print(f"SHOWN: {len(resolved)} resolved + {len(opens)} open = {len(trades)} total")
        print(f"{w}W/{lo}L/{ex}E")
        wr = round(w/(w+lo)*100) if w+lo > 0 else 0
        print(f"WR: {wr}%")
        print(f"PnL: {pnl:+.1f}")

        print(f"\nDetail:")
        for r in trades:
            res = r.outcome_result or "OPEN"
            p = r.outcome_pnl or 0
            v10_status = "PASS" if passes_v10(r) else "BLOCK(GL)"
            print(f"  #{r.id} {r.setup_name:20s} {r.direction:7s} {res:8s} {p:+6.1f} par={r.paradigm:12s} al={r.greek_alignment or 0:+d} [{v10_status}]")

    show(v9, "V9-SC")
    show(v10, "V10")

    # What V10 blocks vs V9-SC
    blocked = [r for r in v9 if not passes_v10(r)]
    print(f"\n=== BLOCKED BY V10 (in V9-SC but not V10) ===")
    for r in blocked:
        res = r.outcome_result or "OPEN"
        p = r.outcome_pnl or 0
        print(f"  #{r.id} {r.setup_name:20s} {r.direction:7s} {res:8s} {p:+6.1f} par={r.paradigm}")
