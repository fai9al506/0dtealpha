"""Simulate 5pt fixed target vs current system across all setups."""
import os, json, sys
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ['DATABASE_URL'])
conn = engine.connect()
rows = conn.execute(text("""
    SELECT setup_name, direction, spot, outcome_result, outcome_pnl, outcome_max_profit,
           greek_alignment, spot_vol_beta, ts
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    AND outcome_result IN ('WIN','LOSS','EXPIRED','TIMEOUT','BE')
    ORDER BY ts
""")).fetchall()
conn.close()

print(f"Total: {len(rows)} trades with outcomes", flush=True)
print(flush=True)

s5 = defaultdict(lambda: [0, 0, 0.0])   # [trades, wins, pnl]
sc = defaultdict(lambda: [0, 0, 0.0])
d5 = defaultdict(float)
dc = defaultdict(float)
# Greek-filtered versions
gf5 = defaultdict(lambda: [0, 0, 0.0])
gfc = defaultdict(lambda: [0, 0, 0.0])

for r in rows:
    nm, direction, spot, result, orig_pnl = r[0], r[1], r[2], r[3], r[4]
    mp = r[5]  # outcome_max_profit
    align, svb = r[6], r[7]
    if mp is None:
        continue

    dy = str(r[8])[:10]

    # 5pt target: WIN +5 if max_profit >= 5, else original outcome
    p5 = 5.0 if mp >= 5 else (orig_pnl or 0)
    w5 = 1 if mp >= 5 else 0

    # Current system
    pc = orig_pnl or 0
    wc = 1 if result == 'WIN' else 0

    s5[nm][0] += 1; s5[nm][1] += w5; s5[nm][2] += p5
    sc[nm][0] += 1; sc[nm][1] += wc; sc[nm][2] += pc
    d5[dy] += p5; dc[dy] += pc

    # Greek filter check
    skip = False
    if align is not None:
        if nm == 'GEX Long' and align < 1: skip = True
        if nm == 'AG Short' and align == -3: skip = True
        if nm == 'DD Exhaustion' and svb is not None and -0.5 <= svb < 0: skip = True

    if not skip:
        gf5[nm][0] += 1; gf5[nm][1] += w5; gf5[nm][2] += p5
        gfc[nm][0] += 1; gfc[nm][1] += wc; gfc[nm][2] += pc

print(f"{'Setup':<18} {'N':>4} | {'5pt WR':>7} {'5pt PnL':>8} | {'Cur WR':>7} {'Cur PnL':>8} | {'Delta':>7}", flush=True)
print("-" * 80, flush=True)
t5 = [0, 0, 0.0]; tc = [0, 0, 0.0]
for n in sorted(s5.keys()):
    a, b = s5[n], sc[n]
    wr5 = a[1]/a[0]*100 if a[0] else 0
    wrc = b[1]/b[0]*100 if b[0] else 0
    delta = a[2] - b[2]
    print(f"{n:<18} {a[0]:>4} | {wr5:>6.1f}% {a[2]:>+8.1f} | {wrc:>6.1f}% {b[2]:>+8.1f} | {delta:>+7.1f}", flush=True)
    t5[0] += a[0]; t5[1] += a[1]; t5[2] += a[2]
    tc[0] += b[0]; tc[1] += b[1]; tc[2] += b[2]

print("-" * 80, flush=True)
wr5t = t5[1]/t5[0]*100 if t5[0] else 0
wrct = tc[1]/tc[0]*100 if tc[0] else 0
print(f"{'TOTAL':<18} {t5[0]:>4} | {wr5t:>6.1f}% {t5[2]:>+8.1f} | {wrct:>6.1f}% {tc[2]:>+8.1f} | {t5[2]-tc[2]:>+7.1f}", flush=True)

# Greek filtered totals
gt5 = [0, 0, 0.0]; gtc = [0, 0, 0.0]
for n in gf5:
    gt5[0] += gf5[n][0]; gt5[1] += gf5[n][1]; gt5[2] += gf5[n][2]
    gtc[0] += gfc[n][0]; gtc[1] += gfc[n][1]; gtc[2] += gfc[n][2]
wr5g = gt5[1]/gt5[0]*100 if gt5[0] else 0
wrcg = gtc[1]/gtc[0]*100 if gtc[0] else 0
print(f"{'GREEK FILTERED':<18} {gt5[0]:>4} | {wr5g:>6.1f}% {gt5[2]:>+8.1f} | {wrcg:>6.1f}% {gtc[2]:>+8.1f} | {gt5[2]-gtc[2]:>+7.1f}", flush=True)

print(flush=True)
nd = len(d5)
print(f"Days: {nd}", flush=True)
print(f"Avg/day:  5pt = {t5[2]/nd:+.1f} pts | Current = {tc[2]/nd:+.1f} pts", flush=True)

w5d = min(d5.values()); wcd = min(dc.values())
b5d = max(d5.values()); bcd = max(dc.values())
print(f"Worst day: 5pt = {w5d:+.1f} | Current = {wcd:+.1f}", flush=True)
print(f"Best day:  5pt = {b5d:+.1f} | Current = {bcd:+.1f}", flush=True)

print(flush=True)
print("Daily breakdown:", flush=True)
for dy in sorted(d5.keys()):
    marker = " ***" if d5[dy] > dc[dy] else (" ---" if d5[dy] < dc[dy] else "")
    print(f"  {dy}  5pt={d5[dy]:>+7.1f}  cur={dc[dy]:>+7.1f}  diff={d5[dy]-dc[dy]:>+7.1f}{marker}", flush=True)

# Money projection
print(flush=True)
print("=== MONEY PROJECTION (5pt target, Greek filtered) ===", flush=True)
avg5 = gt5[2] / nd
print(f"Avg pts/day (Greek filtered): {avg5:+.1f}", flush=True)
for contracts, label in [(2, "2 ES"), (4, "4 ES"), (8, "8 MES"), (10, "10 MES")]:
    mult = 50 if "ES" in label and "MES" not in label else 5
    daily_dollar = avg5 * contracts * mult
    monthly = daily_dollar * 21
    print(f"  {label}: ${daily_dollar:+,.0f}/day = ${monthly:+,.0f}/month", flush=True)
