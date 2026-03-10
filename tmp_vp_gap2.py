"""Deeper gap analysis - understand divergence counts + time filter + bar sources."""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
import os

DB_URL = os.environ.get('DATABASE_URL')

def find_swings(bars, pivot_n=2):
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        is_low = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_low'] > bars[i-j]['bar_low'] or bars[i]['bar_low'] > bars[i+j]['bar_low']:
                is_low = False; break
        if is_low:
            swings.append({'type': 'low', 'price': bars[i]['bar_low'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i})
        is_high = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_high'] < bars[i-j]['bar_high'] or bars[i]['bar_high'] < bars[i+j]['bar_high']:
                is_high = False; break
        if is_high:
            swings.append({'type': 'high', 'price': bars[i]['bar_high'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i})
    swings.sort(key=lambda s: s['ts'])
    return swings

def detect_divs(bars, swings):
    divs = []
    lows = [s for s in swings if s['type'] == 'low']
    highs = [s for s in swings if s['type'] == 'high']
    for i in range(1, len(lows)):
        prev, curr = lows[i-1], lows[i]
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'type': 'sell_exhaustion', 'direction': 'long', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'type': 'sell_absorption', 'direction': 'long', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'type': 'buy_exhaustion', 'direction': 'short', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'type': 'buy_absorption', 'direction': 'short', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
    divs.sort(key=lambda d: d['ts'])
    return divs


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── 1. Check timestamp format — are ts_start/ts_end UTC or ET? ──
    print("="*70, flush=True)
    print("  TIMESTAMP CHECK", flush=True)
    print("="*70, flush=True)

    cur.execute("""
        SELECT bar_idx, ts_start, ts_end, trade_date, source
        FROM es_range_bars
        WHERE trade_date = '2026-03-05' AND source = 'rithmic'
        ORDER BY bar_idx LIMIT 3
    """)
    for r in cur.fetchall():
        print(f"  idx={r['bar_idx']} ts_start={r['ts_start']} ts_end={r['ts_end']} "
              f"trade_date={r['trade_date']} source={r['source']}", flush=True)

    cur.execute("""
        SELECT bar_idx, ts_start, ts_end, trade_date, source
        FROM es_range_bars
        WHERE trade_date = '2026-03-05' AND source = 'rithmic'
        ORDER BY bar_idx DESC LIMIT 3
    """)
    for r in cur.fetchall():
        print(f"  idx={r['bar_idx']} ts_start={r['ts_start']} ts_end={r['ts_end']}", flush=True)

    # Same for live
    cur.execute("""
        SELECT bar_idx, ts_start, ts_end, trade_date, source
        FROM es_range_bars
        WHERE trade_date = '2026-03-02' AND source = 'live'
        ORDER BY bar_idx LIMIT 3
    """)
    print("\nLive bars (Mar 2):", flush=True)
    for r in cur.fetchall():
        print(f"  idx={r['bar_idx']} ts_start={r['ts_start']} ts_end={r['ts_end']}", flush=True)

    # ── 2. Divergence details for Mar 5 rithmic ──
    print("\n" + "="*70, flush=True)
    print("  MAR 5 RITHMIC: Swing + Divergence Details", flush=True)
    print("="*70, flush=True)

    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               ts_start, ts_end, trade_date
        FROM es_range_bars
        WHERE source = 'rithmic' AND trade_date = '2026-03-05'
        ORDER BY bar_idx ASC
    """)
    bars = cur.fetchall()
    print(f"  Bars: {len(bars)}", flush=True)

    swings = find_swings(bars)
    divs = detect_divs(bars, swings)
    print(f"  Swings: {len(swings)}, Divergences: {len(divs)}", flush=True)

    # Show first few swings
    print("\n  First 10 swings:", flush=True)
    for s in swings[:10]:
        print(f"    idx={s['bar_idx']} {s['type']:4s} price={s['price']:.1f} cvd={s['cvd']:.0f} ts={s['ts']}", flush=True)

    # Show divergences with timestamps
    print(f"\n  All {len(divs)} divergences:", flush=True)
    for d in divs:
        ts_et = d['ts'] - timedelta(hours=5) if d['ts'].utcoffset() is None else d['ts']
        print(f"    idx={d['bar_idx']} {d['direction']:5s} {d['type']:<20s} "
              f"price={d['price']:.1f} ts_raw={d['ts']} "
              f"ts-5h={ts_et}", flush=True)

    # ── 3. Check vanna levels on Mar 5 ──
    print("\n" + "="*70, flush=True)
    print("  MAR 5 VANNA LEVEL MATCHING", flush=True)
    print("="*70, flush=True)

    cur.execute("""
        WITH latest AS (
            SELECT expiration_option, MAX(ts_utc) AS ts
            FROM volland_exposure_points
            WHERE greek = 'vanna'
              AND expiration_option IN ('THIS_WEEK', 'THIRTY_NEXT_DAYS')
              AND ts_utc::date = '2026-03-05'
            GROUP BY expiration_option
        )
        SELECT vep.strike, vep.value::float AS value, vep.expiration_option AS tf
        FROM volland_exposure_points vep
        JOIN latest l ON vep.expiration_option = l.expiration_option AND vep.ts_utc = l.ts
        WHERE vep.greek = 'vanna'
    """)
    vl_rows = cur.fetchall()

    by_tf = defaultdict(list)
    for r in vl_rows:
        by_tf[r['tf']].append({'strike': float(r['strike']), 'value': float(r['value'])})

    levels = []
    for tf, points in by_tf.items():
        total = sum(abs(p['value']) for p in points)
        if total == 0: continue
        for p in points:
            pct = abs(p['value']) / total * 100
            if pct >= 12:
                levels.append({'strike': p['strike'], 'value': p['value'],
                              'pct': round(pct, 1), 'tf': tf})

    print(f"  Dominant levels: {len(levels)}", flush=True)
    for lv in levels:
        sign = "+" if lv['value'] > 0 else "-"
        print(f"    {lv['strike']:.0f} {sign} ({lv['pct']}%) [{lv['tf']}]", flush=True)

    # Now check which divergences match
    print(f"\n  Matching divergences to vanna levels (proximity=15):", flush=True)
    matches = 0
    for d in divs:
        for lv in levels:
            dist = abs(d['price'] - lv['strike'])
            if dist > 15: continue
            if lv['value'] > 0 and d['direction'] != 'long': continue
            if lv['value'] < 0 and d['direction'] != 'short': continue
            matches += 1
            ts_raw = d['ts']
            print(f"    MATCH: idx={d['bar_idx']} {d['direction']:5s} price={d['price']:.1f} "
                  f"vanna={lv['strike']:.0f} dist={dist:.1f} ts={ts_raw}", flush=True)
            break
    print(f"  Total matches: {matches}", flush=True)

    # ── 4. Compare live vs rithmic bar structure on same date ──
    print("\n" + "="*70, flush=True)
    print("  LIVE vs RITHMIC BAR COMPARISON (Mar 2)", flush=True)
    print("="*70, flush=True)

    for src in ['live', 'rithmic']:
        cur.execute(f"""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                   ts_start, ts_end
            FROM es_range_bars
            WHERE source = '{src}' AND trade_date = '2026-03-02'
            ORDER BY bar_idx ASC
        """)
        b = cur.fetchall()
        sw = find_swings(b)
        dv = detect_divs(b, sw)
        avg_vol = sum(r['volume'] for r in b) / len(b) if b else 0
        avg_delta = sum(abs(r['delta']) for r in b) / len(b) if b else 0
        cvd_range = (max(r['cvd'] for r in b) - min(r['cvd'] for r in b)) if b else 0

        print(f"\n  {src}: {len(b)} bars, {len(sw)} swings, {len(dv)} divs", flush=True)
        print(f"    avg_vol={avg_vol:.0f}, avg_|delta|={avg_delta:.0f}, cvd_range={cvd_range:.0f}", flush=True)
        print(f"    price range: {min(r['bar_low'] for r in b):.1f} - {max(r['bar_high'] for r in b):.1f}", flush=True)
        if b:
            print(f"    first bar ts_start: {b[0]['ts_start']}", flush=True)
            print(f"    last bar ts_end: {b[-1]['ts_end']}", flush=True)

    conn.close()
    print("\nDone.", flush=True)


if __name__ == '__main__':
    main()
