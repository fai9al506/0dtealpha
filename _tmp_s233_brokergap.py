# -*- coding: utf-8 -*-
"""S233 part 13 — THE BROKER-VALIDATION GAP (the study's biggest caveat).

The chain sim was validated on trades that actually executed, i.e. trades V16 LET THROUGH.
Every trade this study proposes to re-admit has never been sent to a broker. Question:
is the chain-vs-broker error uniform across trade characteristics? If it is, extrapolating
to the blocked population is defensible. If it varies by grade / setup / direction / time,
it is not.
"""
import os, json, collections, statistics
import psycopg2

c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()
cur.execute("""
  SELECT rto.setup_log_id, sl.setup_name, sl.direction, sl.grade, sl.paradigm, sl.vix,
         sl.greek_alignment,
         to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM-DD') d,
         EXTRACT(hour FROM sl.ts AT TIME ZONE 'America/New_York') hr,
         rto.state, sl.outcome_pnl, sl.outcome_max_profit
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id = rto.setup_log_id
  WHERE sl.ts AT TIME ZONE 'America/New_York' >= '2026-06-13'
  ORDER BY sl.ts""")
rows = cur.fetchall()


def broker_pts(state, direction):
    st = state if isinstance(state, dict) else json.loads(state)
    fp = st.get("fill_price")
    exitp = (st.get("stop_fill_price_pre_fifo_reconcile") or st.get("stop_fill_price")
             or st.get("close_fill_price_pre_fifo_reconcile") or st.get("close_fill_price"))
    if fp is None or exitp is None:
        return None
    d = 1 if str(direction).lower() in ("long", "bullish") else -1
    return (float(exitp) - float(fp)) * d


recs = []
for lid, sn, dirn, gr, para, vix, align, d, hr, state, chain, mfe in rows:
    bp = broker_pts(state, dirn)
    if bp is None or chain is None:
        continue
    recs.append(dict(lid=lid, sn=sn, dir=("LONG" if str(dirn).lower() in ("long", "bullish") else "SHORT"),
                     grade=gr, para=para, vix=float(vix or 0), align=int(align or 0), hr=int(hr),
                     brk=bp, chain=float(chain), err=bp - float(chain), mfe=float(mfe or 0)))

print(f"### executed TSRT trades with broker fills, post-S217 (2026-06-13+): n={len(recs)}")
tb = sum(r["brk"] for r in recs); tc = sum(r["chain"] for r in recs)
print(f"  SUM broker {tb:+.1f} pt   chain {tc:+.1f} pt   bias {(tb-tc)/len(recs):+.2f} pt/trade   "
      f"MAE {statistics.mean(abs(r['err']) for r in recs):.2f} pt")


def bucket(name, keyfn):
    print(f"\n  by {name}:")
    g = collections.defaultdict(list)
    for r in recs:
        g[keyfn(r)].append(r)
    print(f"    {'bucket':<18}{'n':>4}{'broker':>9}{'chain':>9}{'bias/t':>9}{'MAE':>7}")
    for k, rs in sorted(g.items(), key=lambda kv: -len(kv[1])):
        b = sum(x["brk"] for x in rs); ch = sum(x["chain"] for x in rs)
        print(f"    {str(k):<18}{len(rs):>4}{b:>+9.1f}{ch:>+9.1f}{(b-ch)/len(rs):>+9.2f}"
              f"{statistics.mean(abs(x['err']) for x in rs):>7.2f}")


bucket("setup", lambda r: r["sn"])
bucket("direction", lambda r: r["dir"])
bucket("grade", lambda r: r["grade"])
bucket("hour", lambda r: r["hr"])
bucket("VIX band", lambda r: "<17" if r["vix"] < 17 else ("17-19" if r["vix"] < 19 else ">=19"))
bucket("alignment", lambda r: r["align"])
bucket("chain outcome", lambda r: "chain WIN" if r["chain"] > 0 else "chain LOSS")
bucket("trade size", lambda r: "big win >15pt" if r["chain"] > 15 else
       ("win 0-15" if r["chain"] > 0 else "loss"))

print("\n\n### interpretation guide")
print("  If bias/t is roughly the same in every bucket, the chain sim is unbiased across trade")
print("  types and the study's re-admitted population inherits the same ~0.2 pt/trade accuracy.")
print("  A bucket whose bias is much more NEGATIVE = chain overstates that kind of trade.")

# ── how much of the recommended book falls in buckets that HAVE broker validation? ──
print("\n\n### coverage: which buckets of the proposed book have ANY broker validation?")
seen = collections.Counter((r["sn"], r["dir"]) for r in recs)
print("  broker-validated buckets (setup x direction) and their trade counts:")
for k, v in seen.most_common():
    print(f"    {k[0]:<22}{k[1]:<6}{v:>4} trades")
print("\n  buckets the study proposes to ADD that have ZERO broker history are listed in the report.")
