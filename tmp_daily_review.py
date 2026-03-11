"""Daily performance review - Mar 10: Options, Futures SIM, Eval"""
import os, sys, json
from collections import defaultdict
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

DATE = '2026-03-10'

# ── ALL PORTAL TRADES ──
print("=" * 80)
print("PORTAL (all setups) - %s" % DATE)
print("=" * 80)
r = c.execute(text("""
    SELECT setup_name, outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE ts::date = :d AND grade != 'LOG' AND outcome_result IS NOT NULL
"""), {"d": DATE}).fetchall()

by_setup = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
total = {"w": 0, "l": 0, "pnl": 0, "n": 0}
for row in r:
    setup, result, pnl, align = row
    p = float(pnl) if pnl else 0
    is_w = result == 'WIN' or (result == 'EXPIRED' and p > 0)
    is_l = result == 'LOSS' or (result == 'EXPIRED' and p < 0)
    by_setup[setup]["n"] += 1
    by_setup[setup]["pnl"] += p
    if is_w: by_setup[setup]["w"] += 1
    elif is_l: by_setup[setup]["l"] += 1
    total["n"] += 1
    total["pnl"] += p
    if is_w: total["w"] += 1
    elif is_l: total["l"] += 1

print("%-20s %5s %5s %5s %6s %8s" % ("Setup", "Total", "Wins", "Loss", "WR%", "PnL"))
print("-" * 55)
for s in sorted(by_setup.keys()):
    d = by_setup[s]
    wr = d["w"]/(d["w"]+d["l"])*100 if d["w"]+d["l"] else 0
    print("%-20s %5d %5d %5d %5.0f%% %+8.1f" % (s, d["n"], d["w"], d["l"], wr, d["pnl"]))
print("-" * 55)
wr = total["w"]/(total["w"]+total["l"])*100 if total["w"]+total["l"] else 0
print("%-20s %5d %5d %5d %5.0f%% %+8.1f" % ("TOTAL", total["n"], total["w"], total["l"], wr, total["pnl"]))

# ── FUTURES SIM (auto_trade_orders) ──
print("\n" + "=" * 80)
print("FUTURES SIM (auto_trader) - %s" % DATE)
print("=" * 80)
sim = c.execute(text("""
    SELECT ato.setup_log_id, ato.state,
           sl.setup_name, sl.direction, sl.outcome_result, sl.outcome_pnl
    FROM auto_trade_orders ato
    JOIN setup_log sl ON sl.id = ato.setup_log_id
    WHERE sl.ts::date = :d
    ORDER BY sl.ts
"""), {"d": DATE}).fetchall()

sim_total = {"w": 0, "l": 0, "pnl": 0, "n": 0, "dollar": 0}
sim_by_setup = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
for row in sim:
    lid, state, setup, direction, result, pnl = row
    s = state if isinstance(state, dict) else json.loads(state)
    p = float(pnl) if pnl else 0
    is_w = result == 'WIN' or (result == 'EXPIRED' and p > 0)
    is_l = result == 'LOSS' or (result == 'EXPIRED' and p < 0)
    qty = s.get("total_qty", 10)
    dollar_pnl = p * qty * 5  # MES = $5/pt
    sim_total["n"] += 1
    sim_total["pnl"] += p
    sim_total["dollar"] += dollar_pnl
    if is_w: sim_total["w"] += 1
    elif is_l: sim_total["l"] += 1
    sim_by_setup[setup]["n"] += 1
    sim_by_setup[setup]["pnl"] += p
    if is_w: sim_by_setup[setup]["w"] += 1
    elif is_l: sim_by_setup[setup]["l"] += 1
    print("  #%-4s %-18s %-6s %-8s %+7.1f pts  ~$%+.0f (%d MES)" % (
        lid, setup, direction, result or "OPEN", p, dollar_pnl, qty))

if sim_total["n"]:
    print("-" * 60)
    wr = sim_total["w"]/(sim_total["w"]+sim_total["l"])*100 if sim_total["w"]+sim_total["l"] else 0
    print("SIM Total: %d trades | %dW/%dL | WR %.0f%% | %+.1f pts | ~$%+.0f" % (
        sim_total["n"], sim_total["w"], sim_total["l"], wr, sim_total["pnl"], sim_total["dollar"]))
else:
    print("  No SIM trades today")

# ── OPTIONS (options_trade_orders) ──
print("\n" + "=" * 80)
print("OPTIONS (options_trader) - %s" % DATE)
print("=" * 80)
try:
    opts = c.execute(text("""
        SELECT oto.setup_log_id, oto.state,
               sl.setup_name, sl.direction, sl.outcome_result, sl.outcome_pnl
        FROM options_trade_orders oto
        JOIN setup_log sl ON sl.id = oto.setup_log_id
        WHERE sl.ts::date = :d
        ORDER BY sl.ts
    """), {"d": DATE}).fetchall()

    opt_total = {"w": 0, "l": 0, "pnl_dollar": 0, "n": 0}
    for row in opts:
        lid, state, setup, direction, result, pnl = row
        s = state if isinstance(state, dict) else json.loads(state)
        status = s.get("status", "?")
        entry = s.get("entry_price", 0)
        exit_p = s.get("exit_price", 0)
        option_pnl = (float(exit_p or 0) - float(entry or 0)) * 100 * int(s.get("qty", 1))
        close_reason = s.get("close_reason", "")
        symbol = s.get("option_symbol", "?")
        print("  #%-4s %-18s %-6s  %s  entry=$%.2f exit=$%.2f  $%+.0f  [%s] %s" % (
            lid, setup, direction, symbol,
            float(entry or 0), float(exit_p or 0),
            option_pnl, status, close_reason))
        is_w = option_pnl > 0
        is_l = option_pnl < 0
        opt_total["n"] += 1
        opt_total["pnl_dollar"] += option_pnl
        if is_w: opt_total["w"] += 1
        elif is_l: opt_total["l"] += 1

    if opt_total["n"]:
        print("-" * 60)
        wr = opt_total["w"]/(opt_total["w"]+opt_total["l"])*100 if opt_total["w"]+opt_total["l"] else 0
        print("Options Total: %d trades | %dW/%dL | WR %.0f%% | $%+.0f" % (
            opt_total["n"], opt_total["w"], opt_total["l"], wr, opt_total["pnl_dollar"]))
    else:
        print("  No options trades today")
except Exception as ex:
    print("  Error: %s" % ex)

# ── EVAL ELIGIBLE (what eval real would take) ──
print("\n" + "=" * 80)
print("EVAL REAL (eligible trades: |align|>=3) - %s" % DATE)
print("=" * 80)
ev = c.execute(text("""
    SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           setup_name, direction, grade, outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE ts::date = :d AND grade != 'LOG' AND outcome_result IS NOT NULL
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'Paradigm Reversal', 'AG Short')
      AND ABS(COALESCE(greek_alignment, 0)) >= 3
    ORDER BY ts
"""), {"d": DATE}).fetchall()

ev_total = {"w": 0, "l": 0, "pnl": 0, "n": 0}
for row in ev:
    lid, t, setup, direction, grade, result, pnl, align = row
    p = float(pnl) if pnl else 0
    is_w = result == 'WIN' or (result == 'EXPIRED' and p > 0)
    is_l = result == 'LOSS' or (result == 'EXPIRED' and p < 0)
    dollar = p * 8 * 5  # 8 MES @ $5/pt
    ev_total["n"] += 1
    ev_total["pnl"] += p
    if is_w: ev_total["w"] += 1
    elif is_l: ev_total["l"] += 1
    print("  %s  #%-4s %-18s %-6s [%s] align=%+d  %-8s %+6.1f pts  ~$%+.0f" % (
        t, lid, setup, direction, grade, align, result, p, dollar))

if ev_total["n"]:
    print("-" * 60)
    wr = ev_total["w"]/(ev_total["w"]+ev_total["l"])*100 if ev_total["w"]+ev_total["l"] else 0
    dollar_total = ev_total["pnl"] * 8 * 5
    print("Eval Total: %d trades | %dW/%dL | WR %.0f%% | %+.1f pts | ~$%+.0f (8 MES)" % (
        ev_total["n"], ev_total["w"], ev_total["l"], wr, ev_total["pnl"], dollar_total))

# ── COMPARISON SUMMARY ──
print("\n" + "=" * 80)
print("DAILY SUMMARY - %s" % DATE)
print("=" * 80)
print("%-15s %5s %5s %5s %6s %8s %10s" % ("System", "Total", "Wins", "Loss", "WR%", "PnL pts", "~$ PnL"))
print("-" * 65)
print("%-15s %5d %5d %5d %5.0f%% %+8.1f %10s" % (
    "Portal (all)", total["n"], total["w"], total["l"],
    total["w"]/(total["w"]+total["l"])*100 if total["w"]+total["l"] else 0,
    total["pnl"], "--"))
if sim_total["n"]:
    print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+10.0f" % (
        "Futures SIM", sim_total["n"], sim_total["w"], sim_total["l"],
        sim_total["w"]/(sim_total["w"]+sim_total["l"])*100 if sim_total["w"]+sim_total["l"] else 0,
        sim_total["pnl"], sim_total["dollar"]))
if ev_total["n"]:
    print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+10.0f" % (
        "Eval Real", ev_total["n"], ev_total["w"], ev_total["l"],
        ev_total["w"]/(ev_total["w"]+ev_total["l"])*100 if ev_total["w"]+ev_total["l"] else 0,
        ev_total["pnl"], ev_total["pnl"] * 8 * 5))

c.close()
