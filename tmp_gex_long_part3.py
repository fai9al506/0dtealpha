"""
Part 3: Forward price analysis for existing GEX Long trades.
For each trade, trace what ES/SPX did in 30/60/120 min after entry.
"""
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import timedelta

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL)

# Get all GEX Long trades
q = """
SELECT id, ts, spot, lis, max_minus_gex, gap_to_lis, upside,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       greek_alignment, grade, direction
FROM setup_log WHERE setup_name = 'GEX Long'
ORDER BY ts
"""
df = pd.read_sql(q, engine)
df['ts_et'] = pd.to_datetime(df['ts']).dt.tz_convert('US/Eastern')
df['trade_date'] = df['ts_et'].dt.date

print("=" * 80)
print("PART 3: FORWARD PRICE ANALYSIS (MFE/MAE from ES data)")
print("=" * 80)

# For each trade, get ES price action from es_delta_bars (1-min bars, has bar_high/bar_low)
print(f"\n{'ID':>4} {'Date':>10} {'Time':>5} {'Entry':>8} | {'MFE30':>7} {'MFE60':>7} {'MFE120':>7} | {'MAE30':>7} {'MAE60':>7} {'MAE120':>7} | {'Result':>7} {'PnL':>6} {'SystemMFE':>9}")

all_results = []

for _, r in df.iterrows():
    trade_ts = pd.Timestamp(r['ts'])
    entry_price = r['spot']  # SPX spot at entry

    # Get ES 1-min bars for the next 2 hours
    # Note: ES trades ~15-20 pts above SPX, but we care about CHANGE not absolute
    eq = f"""
    SELECT ts, bar_open_price, bar_high_price, bar_low_price, bar_close_price
    FROM es_delta_bars
    WHERE ts > '{trade_ts.isoformat()}'
      AND ts <= '{(trade_ts + timedelta(hours=2.5)).isoformat()}'
    ORDER BY ts
    LIMIT 200
    """
    bars = pd.read_sql(eq, engine)

    if len(bars) == 0:
        # Try chain_snapshots for spot
        cq = f"""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts > '{trade_ts.isoformat()}'
          AND ts <= '{(trade_ts + timedelta(hours=2.5)).isoformat()}'
        ORDER BY ts
        LIMIT 100
        """
        chain = pd.read_sql(cq, engine)
        if len(chain) == 0:
            continue

        for mins, label in [(30, 30), (60, 60), (120, 120)]:
            window = chain[chain['ts'] <= trade_ts + timedelta(minutes=mins)]
            if len(window) == 0:
                continue
        continue

    # Calculate MFE/MAE at 30, 60, 120 min windows
    # We use bar_high - entry for MFE (long), entry - bar_low for MAE (long)
    # ES offset: we care about DISTANCE from entry, use first bar to calibrate
    if len(bars) > 0:
        es_entry = bars.iloc[0]['bar_open_price']
        es_offset = es_entry - entry_price  # ES typically ~15-20 above SPX

    results = {}
    for mins in [30, 60, 120]:
        window = bars[bars['ts'] <= trade_ts + timedelta(minutes=mins)]
        if len(window) == 0:
            results[f'mfe_{mins}'] = None
            results[f'mae_{mins}'] = None
            continue

        # MFE = highest high - entry (in SPX terms)
        highest = window['bar_high_price'].max()
        lowest = window['bar_low_price'].min()

        mfe = highest - es_entry  # favorable movement (long = up)
        mae = es_entry - lowest   # adverse movement (long = down, positive = bad)

        results[f'mfe_{mins}'] = round(mfe, 1)
        results[f'mae_{mins}'] = round(mae, 1)

    t = pd.Timestamp(r['ts']).tz_convert('US/Eastern')
    mfe30 = results.get('mfe_30', '-')
    mfe60 = results.get('mfe_60', '-')
    mfe120 = results.get('mfe_120', '-')
    mae30 = results.get('mae_30', '-')
    mae60 = results.get('mae_60', '-')
    mae120 = results.get('mae_120', '-')
    sys_mfe = r['outcome_max_profit'] if pd.notna(r['outcome_max_profit']) else 0

    def fmt(v):
        if v is None or v == '-':
            return '     -'
        return f'{v:>7.1f}'

    print(f"{r['id']:>4} {t.strftime('%m/%d'):>10} {t.strftime('%H:%M'):>5} {entry_price:>8.1f} | {fmt(mfe30)} {fmt(mfe60)} {fmt(mfe120)} | {fmt(mae30)} {fmt(mae60)} {fmt(mae120)} | {r['outcome_result'] or 'N/A':>7} {r['outcome_pnl']:>6.1f} {sys_mfe:>9.1f}")

    all_results.append({
        'id': r['id'],
        'entry': entry_price,
        'result': r['outcome_result'],
        'pnl': r['outcome_pnl'],
        'gap': r['gap_to_lis'],
        'align': r['greek_alignment'],
        'mfe30': results.get('mfe_30'),
        'mfe60': results.get('mfe_60'),
        'mfe120': results.get('mfe_120'),
        'mae30': results.get('mae_30'),
        'mae60': results.get('mae_60'),
        'mae120': results.get('mae_120'),
        'sys_mfe': sys_mfe,
    })

# Summary statistics
print("\n" + "=" * 80)
print("SUMMARY: Forward Price Action")
print("=" * 80)

rdf = pd.DataFrame(all_results)
if len(rdf) == 0:
    print("No data available")
else:
    for col in ['mfe30', 'mfe60', 'mfe120', 'mae30', 'mae60', 'mae120']:
        valid = rdf[col].dropna()
        if len(valid) > 0:
            print(f"  {col}: mean={valid.mean():.1f}, median={valid.median():.1f}, min={valid.min():.1f}, max={valid.max():.1f}")

    # Key question: how many trades had MFE >= 10 (i.e., entry was RIGHT, just stop too tight?)
    print(f"\n--- Was the ENTRY right but STOP too tight? ---")
    for mins, col in [(30, 'mfe30'), (60, 'mfe60'), (120, 'mfe120')]:
        valid = rdf[col].dropna()
        reached_10 = len(valid[valid >= 10])
        reached_15 = len(valid[valid >= 15])
        reached_20 = len(valid[valid >= 20])
        total = len(valid)
        print(f"  {mins}min: {reached_10}/{total} reached +10pts ({100*reached_10/total:.0f}%), {reached_15}/{total} reached +15 ({100*reached_15/total:.0f}%), {reached_20}/{total} reached +20 ({100*reached_20/total:.0f}%)")

    # For LOSSES only: how many would have won with more patience?
    losses = rdf[rdf['result'] == 'LOSS']
    print(f"\n--- LOSSES only ({len(losses)} trades): could any have been saved? ---")
    for mins, col in [(30, 'mfe30'), (60, 'mfe60'), (120, 'mfe120')]:
        valid = losses[col].dropna()
        if len(valid) == 0:
            continue
        reached_10 = len(valid[valid >= 10])
        print(f"  {mins}min: {reached_10}/{len(valid)} losses had MFE >= +10pts")

    # MAE at stop trigger: how many had MAE > 8 (current stop)?
    print(f"\n--- MAE analysis (stop = 8 pts) ---")
    for mins, col in [(30, 'mae30'), (60, 'mae60'), (120, 'mae120')]:
        valid = rdf[col].dropna()
        if len(valid) == 0:
            continue
        hit_stop = len(valid[valid >= 8])
        print(f"  {mins}min: {hit_stop}/{len(valid)} hit 8pt stop ({100*hit_stop/len(valid):.0f}%)")

    # Wins where MFE was much higher than captured profit
    wins = rdf[rdf['result'] == 'WIN']
    if len(wins) > 0 and 'mfe120' in wins.columns:
        wins_valid = wins[wins['mfe120'].notna()]
        if len(wins_valid) > 0:
            print(f"\n--- WINS: captured vs actual MFE120 ---")
            for _, wr in wins_valid.iterrows():
                print(f"  #{int(wr['id']):>4}: captured={wr['pnl']:.1f}, MFE120={wr['mfe120']:.1f}, left_on_table={wr['mfe120'] - wr['pnl']:.1f}")

    # Align >= 1 subset
    print(f"\n--- Align>=1 subset only ---")
    aligned = rdf[rdf['align'] >= 1]
    for mins, col in [(30, 'mfe30'), (60, 'mfe60'), (120, 'mfe120')]:
        valid = aligned[col].dropna()
        if len(valid) > 0:
            reached_10 = len(valid[valid >= 10])
            print(f"  {mins}min: mean MFE={valid.mean():.1f}, {reached_10}/{len(valid)} reached +10 ({100*reached_10/len(valid):.0f}%)")
