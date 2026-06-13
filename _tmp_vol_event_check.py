"""Check historical vol events in our DB: did SPX revisit the target within the deadline?"""
import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    # 1. ALL distinct vol events ever captured
    rows = c.execute(text("""
        SELECT DISTINCT ev.value->>'triggerDate' AS trig,
               ev.value->>'deadline' AS dl,
               (ev.value->>'targetPrice')::numeric AS tgt
        FROM volland_snapshots,
             jsonb_array_elements(COALESCE(payload->'statistics'->'spot_vol_beta'->'vixEvents','[]'::jsonb)) ev
        ORDER BY 1
    """)).fetchall()
    print(f"distinct vol events captured: {len(rows)}")
    events = []
    for r in rows:
        trig = (r[0] or "")[:10]
        dl = (r[1] or "")[:10]
        tgt = float(r[2])
        events.append((trig, dl, tgt))
        print(f"  trigger={trig}  deadline={dl}  target={tgt:.2f}")

    # 2. for each event: daily spot high after trigger date, did it touch target?
    for trig, dl, tgt in events:
        print(f"\n=== event {trig} -> target {tgt:.2f} by {dl} ===")
        days = c.execute(text("""
            SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
                   MAX(spot) AS hi, MIN(spot) AS lo
            FROM chain_snapshots
            WHERE spot IS NOT NULL
              AND (ts AT TIME ZONE 'America/New_York')::date > :trig
              AND (ts AT TIME ZONE 'America/New_York')::date <= :dl
            GROUP BY 1 ORDER BY 1
        """), {"trig": trig, "dl": dl}).fetchall()
        hit = None
        for d, hi, lo in days:
            mark = ""
            if hit is None and float(hi) >= tgt:
                hit = (d, float(hi))
                mark = "  <-- TARGET HIT"
            print(f"  {d}  hi {float(hi):7.1f}  lo {float(lo):7.1f}{mark}")
        if hit:
            from datetime import date
            t = date.fromisoformat(trig)
            print(f"  RESULT: HIT on {hit[0]} (high {hit[1]:.1f} >= {tgt:.2f}), {(hit[0]-t).days} calendar days after trigger")
        else:
            print(f"  RESULT: NOT hit within window (max high in window: "
                  f"{max((float(h) for _, h, _ in days), default=0):.1f})")

    # 3. Also: trigger-day context (close before vs after) for each event
    for trig, dl, tgt in events:
        row = c.execute(text("""
            SELECT MIN(spot), MAX(spot),
                   (ARRAY_AGG(spot ORDER BY ts DESC))[1]
            FROM chain_snapshots
            WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::date = :d
        """), {"d": trig}).fetchone()
        if row and row[0] is not None:
            print(f"\ntrigger day {trig}: lo {float(row[0]):.1f} hi {float(row[1]):.1f} close {float(row[2]):.1f} (target/prior close {tgt:.2f}, gap at close {tgt - float(row[2]):+.1f})")
