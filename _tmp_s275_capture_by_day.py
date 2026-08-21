"""S275b — measure the sim-to-broker capture ratio on EVERY live day we have,
not just the August week.

Method that removes config drift entirely: do NOT re-run the filter. Take the
lids TSRT ACTUALLY placed (real_trade_orders), score them with the CHAIN model
(setup_log.outcome_pnl) + the standard costs, and compare to broker truth per
DAY (tsrt_daily_stmt.net — the S210 rule: day level, never per-lid broker state).

Whatever the filter/cap/basket config was on a given day is therefore irrelevant:
both sides are looking at the same set of trades.
"""
import os, sys, json, collections
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

ET = ZoneInfo("America/New_York")
S217 = "2026-06-13"
DOLLAR_PER_PT = 5.0
HAIRCUT_PT = 0.6
FEE_PER_RT = 1.92
DEADBAND = 0.15

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    broker = {r[0]: {"gross": float(r[1]), "net": float(r[3]), "n": int(r[4])}
              for r in c.execute(text(
                  "select day, gross, comm, net, n_trades from tsrt_daily_stmt "
                  "order by day")).all()}
    lids = c.execute(text("""
        SELECT o.setup_log_id, o.state, l.outcome_pnl, l.basket_pct, l.direction,
               l.setup_name, l.ts
        FROM real_trade_orders o
        JOIN setup_log l ON l.id = o.setup_log_id
        ORDER BY o.setup_log_id""")).all()

# what keys ever carry a quantity?
qty_keys = collections.Counter()
for _, st, *_ in lids:
    st = st if isinstance(st, dict) else json.loads(st)
    for k in st:
        if "qty" in k.lower() or "quantity" in k.lower() or k in ("size", "contracts"):
            qty_keys[k] += 1
print("quantity-ish keys found in real_trade_orders.state:", dict(qty_keys) or "NONE")


def derived_qty(bp, is_long):
    """Fallback only: the live basket sizing rule, 2x on confirm else 1x."""
    if bp is None:
        return 1
    bp = float(bp)
    if abs(bp) < DEADBAND:
        return 1
    return 2 if ((bp > 0) == is_long) else 1


def lid_qty(st, bp, is_long):
    """Real quantity from the order state; derive only when the key is absent."""
    q = st.get("quantity")
    try:
        q = int(q)
        if q > 0:
            return q, True
    except (TypeError, ValueError):
        pass
    return derived_qty(bp, is_long), False


day_sim = collections.defaultdict(float)
day_qty = collections.defaultdict(int)
day_lids = collections.defaultdict(int)
day_unres = collections.defaultdict(int)
qty_src = collections.Counter()
for lid, st, pnl, bp, direction, setup, ts in lids:
    st = st if isinstance(st, dict) else json.loads(st)
    tsp = st.get("ts_placed")
    if not tsp:
        continue
    d = ts.astimezone(ET).date()
    if d not in broker:
        continue
    il = direction in ("long", "bullish")
    q, real = lid_qty(st, bp, il)
    qty_src["real" if real else "derived"] += 1
    day_qty[d] += q
    day_lids[d] += 1
    if pnl is None:
        day_unres[d] += 1
        continue
    day_sim[d] += (float(pnl) - HAIRCUT_PT) * DOLLAR_PER_PT * q - FEE_PER_RT * q

print("\n{:<12}{:>7}{:>7}{:>8}{:>11}{:>12}{:>10}".format(
    "day", "lids", "qty", "brk_n", "SIM $", "BROKER $", "diff"))
print("-" * 68)
tot_s = tot_b = 0.0
qty_ok = qty_bad = 0
post_s = post_b = 0.0
rows_out = []
for d in sorted(broker):
    b = broker[d]
    s = day_sim.get(d, 0.0)
    match = "" if day_qty.get(d, 0) == b["n"] else "  <-qty mismatch"
    if day_qty.get(d, 0) == b["n"]:
        qty_ok += 1
    else:
        qty_bad += 1
    tot_s += s
    tot_b += b["net"]
    if str(d) >= S217:
        post_s += s
        post_b += b["net"]
    flag = "*" if str(d) >= S217 else " "
    print(f"{str(d):<12}{day_lids.get(d,0):>7}{day_qty.get(d,0):>7}{b['n']:>8}"
          f"{s:>11,.0f}{b['net']:>12,.0f}{s-b['net']:>10,.0f} {flag}{match}")
    rows_out.append((str(d), day_lids.get(d, 0), day_qty.get(d, 0), b["n"], s, b["net"]))

print("-" * 68)
n_all = len(broker)
n_post = sum(1 for d in broker if str(d) >= S217)
print(f"qty source: {dict(qty_src)}")
print(f"qty matches broker contract count on {qty_ok}/{qty_ok+qty_bad} days")
CLEAN = [r for r in rows_out if r[2] == r[3]]
cs = sum(r[4] for r in CLEAN); cb = sum(r[5] for r in CLEAN)
cq = sum(r[2] for r in CLEAN)
print(f"\n>>> CLEAN DAYS ONLY (sim contract count == broker contract count): "
      f"{len(CLEAN)} days, {cq} contract RT")
print(f"    sim ${cs:,.0f}  broker ${cb:,.0f}  ratio {cb/cs if cs else float('nan'):.2f}   "
      f"gap ${(cs-cb)/cq:+.2f}/RT = {(cs-cb)/cq/DOLLAR_PER_PT:+.2f} pt")
CP = [r for r in CLEAN if r[0] >= S217]
if CP:
    ps = sum(r[4] for r in CP); pb = sum(r[5] for r in CP); pq = sum(r[2] for r in CP)
    print(f">>> CLEAN + POST-S217: {len(CP)} days, {pq} contract RT   "
          f"sim ${ps:,.0f}  broker ${pb:,.0f}  ratio {pb/ps if ps else float('nan'):.2f}   "
          f"gap ${(ps-pb)/pq:+.2f}/RT = {(ps-pb)/pq/DOLLAR_PER_PT:+.2f} pt")
    hi2 = sum(1 for r in CP if r[4] > r[5]); lo2 = sum(1 for r in CP if r[4] < r[5])
    print(f"    sign test: sim HIGH {hi2} days / LOW {lo2} days")
print(f"\nALL {n_all} live days   sim ${tot_s:,.0f}  broker ${tot_b:,.0f}  "
      f"ratio {tot_b/tot_s if tot_s else float('nan'):.2f}")
print(f"POST-S217 {n_post} days   sim ${post_s:,.0f}  broker ${post_b:,.0f}  "
      f"ratio {post_b/post_s if post_s else float('nan'):.2f}")

# per-contract gap, the regime-free way to read it
post_q = sum(day_qty[d] for d in broker if str(d) >= S217)
all_q = sum(day_qty[d] for d in broker)
print(f"\nper-contract gap  ALL:      ${(tot_s-tot_b)/all_q:+.2f}/RT "
      f"= {(tot_s-tot_b)/all_q/DOLLAR_PER_PT:+.2f} pt  (n={all_q} contract RT)")
print(f"per-contract gap  POST-S217: ${(post_s-post_b)/post_q:+.2f}/RT "
      f"= {(post_s-post_b)/post_q/DOLLAR_PER_PT:+.2f} pt  (n={post_q} contract RT)")

# how many days is the sim high vs low? (sign test)
hi = sum(1 for d, l, q, bn, s, b in rows_out if str(d) >= S217 and s > b)
lo = sum(1 for d, l, q, bn, s, b in rows_out if str(d) >= S217 and s < b)
print(f"\npost-S217 sign test: sim HIGH on {hi} days, LOW on {lo}")
unres = sum(day_unres.values())
if unres:
    print(f"WARNING: {unres} placed lids have no chain outcome_pnl (excluded from sim side)")
json.dump(rows_out, open("capture_by_day.json", "w"), indent=1)
