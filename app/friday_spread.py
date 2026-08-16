"""Friday SPX 0DTE call credit spread — LOG-ONLY by default, trade-ready by env.

Study: 2026-08-15 (S267), 189 trading days of real `chain_snapshots` bid/ask,
37 Fridays. Report artifact "189 Days of Selling Premium".
Memory: research_credit_spreads_friday_call.

THE RULE
    Friday only . 12:00 ET . sell the call nearest 0.35 delta (OTM) .
    buy WIDTH points higher . NO stop . hold to the 16:00 cash settlement .
    one spread per Friday.

WHY FRIDAY
    Weekly expiry pins the tape. Across the study no Friday afternoon rallied
    more than +0.61% after 12:00, while every other weekday reached at least
    +0.77% and Tuesday reached +1.63%. A call credit spread does not need price
    to fall, only to not jump — so the pin that starves the MES fade book (see
    the Friday gate, armed 2026-08-15) feeds this one, the same way it feeds v7.

MEASURED, 37 Fridays, 20-pt wide, hold to settlement, real quotes, fees in:
    +$9,704 . $262/Friday . 86.5% WR . worst -$1,457 . maxDD -$1,457
    9/10 green months . never two losing Fridays in a row
    break-even 68.1% on realised losses vs 86.5% actual = +18.4pp margin

DELIBERATE NON-FEATURES — each was tested and rejected, do not "improve" them in:
  * NO STOP LOSS. A 1x-credit stop fires on 8 of 37 Fridays and is WRONG on 4 of
    those 8 (they recover and win). Re-run against 1-minute highs — i.e. a stop
    a broker would actually execute — it earns LESS than doing nothing
    ($6,678 vs $6,874). The 2-minute backtest sampling was the only thing making
    it look good, exactly the S131 trap. Cap risk with WIDTH instead.
  * NO PROFIT TARGET. 50% target = $4,192 vs $7,927 held. 25% (the tastytrade
    rule) lifts the win rate and loses money doing it.
  * NO GEX FILTER. SUPPORT and BREAKOUT_TEST are genuinely the two worst states
    on all days (-$215 and -$187/trade), but filtering them on FRIDAYS removes
    winners: $6,874 -> $5,561. The Friday pin already is the filter. The context
    is captured per trade anyway so the question can be re-asked with live data.
  * GROW BY ADDING SPREADS, NOT WIDTH. Risk:reward degrades with width
    (2.1:1 at 5pt, 3.1:1 at 20pt), so 3 x 5pt beats 1 x 15pt: $1,129/mo vs $876
    on less margin.

SAFETY
    Log-only unless THREE env vars agree. Fails CLOSED on any error, missing
    config, stale chain or bad quote — like v7's gamma gate and unlike
    basket_gate, because this is pure opt-in: refusing to trade is always safe.
    Never raises into the caller.

ENV
    FRIDAY_SPREAD_ENABLED        "true" to run the detector at all   (default true, log-only)
    FRIDAY_SPREAD_TRADE_ENABLED  "true" to place real orders         (default false)
    FRIDAY_SPREAD_LIVE           "true" = live API, else SIM         (default false)
    FRIDAY_SPREAD_ACCOUNT        broker account id                   (default "" -> no orders)
    FRIDAY_SPREAD_WIDTH          spread width in SPX points          (default 5)
    FRIDAY_SPREAD_QTY            number of spreads                   (default 1)
    FRIDAY_SPREAD_DELTA          short-leg target delta              (default 0.35)
    FRIDAY_SPREAD_ENTRY          entry time HH:MM ET                 (default 12:00)
    FRIDAY_SPREAD_MIN_CREDIT     skip if credit below this, points   (default 0.60)
    SPX_OPT_FEE_PER_SIDE         all-in fee per contract per side    (default 1.10)
"""
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, date as _date, time as dtime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

ET = ZoneInfo("America/New_York")

# chain_snapshots row layout (verified against chain_snapshots.columns)
IDX_C_DELTA, IDX_C_BID, IDX_C_ASK = 4, 5, 7
IDX_STRIKE = 10

_engine = None
_get_token = None
_send_telegram = None

# per-day latch; hydrated from the DB on restart
_state = {"date": None, "fired": False, "row_id": None}


# ---------------------------------------------------------------- config ---

def _b(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() == "true"


def _enabled() -> bool:
    return _b("FRIDAY_SPREAD_ENABLED", "true")


def _trade_enabled() -> bool:
    """All three must agree before a single order can be sent."""
    return (_b("FRIDAY_SPREAD_TRADE_ENABLED", "false")
            and bool(os.getenv("FRIDAY_SPREAD_ACCOUNT", "").strip()))


def _api_base() -> str:
    return ("https://api.tradestation.com/v3" if _b("FRIDAY_SPREAD_LIVE", "false")
            else "https://sim-api.tradestation.com/v3")


def _width() -> int:
    try:
        return max(5, int(os.getenv("FRIDAY_SPREAD_WIDTH", "5")))
    except ValueError:
        return 5


def _qty() -> int:
    try:
        return max(1, int(os.getenv("FRIDAY_SPREAD_QTY", "1")))
    except ValueError:
        return 1


def _target_delta() -> float:
    try:
        return float(os.getenv("FRIDAY_SPREAD_DELTA", "0.35"))
    except ValueError:
        return 0.35


def _entry_time() -> dtime:
    raw = os.getenv("FRIDAY_SPREAD_ENTRY", "12:00").strip()
    try:
        hh, mm = raw.split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return dtime(12, 0)


def _min_credit() -> float:
    try:
        return float(os.getenv("FRIDAY_SPREAD_MIN_CREDIT", "0.60"))
    except ValueError:
        return 0.60


def _fee_per_side() -> float:
    try:
        return float(os.getenv("SPX_OPT_FEE_PER_SIDE", "1.10"))
    except ValueError:
        return 1.10


ENTRY_GRACE_MIN = 12       # fire on the first cycle in [entry, entry+grace], then give up
CHAIN_STALE_MIN = 6        # entry chain snapshot must be fresher than this
MULT = 100.0               # SPX index option multiplier


# ------------------------------------------------------------------ init ---

def init(engine, get_token_fn=None, send_telegram_fn=None):
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn
    try:
        _db_init()
    except Exception:
        print(f"[friday-spread] db_init failed: {traceback.format_exc()}", flush=True)
    try:
        _hydrate()
    except Exception:
        print(f"[friday-spread] hydrate failed: {traceback.format_exc()}", flush=True)
    mode = "LIVE ORDERS" if _trade_enabled() else "log-only"
    print(f"[friday-spread] initialized ({mode}) width={_width()} qty={_qty()} "
          f"delta={_target_delta():.2f} entry={_entry_time().strftime('%H:%M')}", flush=True)


def _db_init():
    if not _engine:
        return
    with _engine.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS friday_spread_log (
                id            SERIAL PRIMARY KEY,
                trade_date    DATE NOT NULL UNIQUE,
                entry_et      TIMESTAMP,
                mode          TEXT,
                spot_entry    DOUBLE PRECISION,
                vix           DOUBLE PRECISION,
                short_strike  DOUBLE PRECISION,
                long_strike   DOUBLE PRECISION,
                width         DOUBLE PRECISION,
                short_delta   DOUBLE PRECISION,
                qty           INTEGER,
                credit_pts    DOUBLE PRECISION,
                credit_usd    DOUBLE PRECISION,
                max_profit_usd DOUBLE PRECISION,
                max_loss_usd  DOUBLE PRECISION,
                breakeven     DOUBLE PRECISION,
                order_id      TEXT,
                fill_credit   DOUBLE PRECISION,
                settle_spot   DOUBLE PRECISION,
                settle_cost   DOUBLE PRECISION,
                pnl_pts       DOUBLE PRECISION,
                pnl_usd       DOUBLE PRECISION,
                result        TEXT,
                details       JSONB
            )
        """))


def _hydrate(d: _date | None = None):
    """Restore the latch for date `d` so a restart cannot double-fire.

    Takes the date explicitly rather than re-reading the clock: the caller is
    already working on a specific session, and re-deriving it here once left the
    latch pointing at a different day, which would have allowed a second order.
    """
    if not _engine:
        return
    d = d or datetime.now(ET).date()
    with _engine.begin() as c:
        r = c.execute(text(
            "SELECT id FROM friday_spread_log WHERE trade_date = :d"
        ), {"d": d}).fetchone()
    _state.update(date=d, fired=bool(r), row_id=(r[0] if r else None))
    if r:
        print(f"[friday-spread] already fired today (row {r[0]})", flush=True)


# ------------------------------------------------------------ chain reads ---

def _latest_chain():
    """(et_naive, spot, vix, rows) of the newest 0DTE chain snapshot, or None."""
    if not _engine:
        return None
    with _engine.begin() as c:
        r = c.execute(text("""
            SELECT (ts AT TIME ZONE 'America/New_York'), spot, vix, rows, exp
            FROM chain_snapshots
            WHERE exp IS NOT NULL AND rows IS NOT NULL
            ORDER BY ts DESC LIMIT 1
        """)).fetchone()
    if not r:
        return None
    rows = json.loads(r[3]) if isinstance(r[3], str) else r[3]
    return r[0], float(r[1]), (float(r[2]) if r[2] is not None else None), rows, r[4]


def _call_leg(rows, strike):
    """(bid, ask, delta) for one call strike, or None if unusable."""
    for row in rows:
        try:
            k = float(row[IDX_STRIKE])
        except (TypeError, ValueError, IndexError):
            continue
        if abs(k - strike) > 0.01:
            continue
        try:
            bid = float(row[IDX_C_BID] or 0.0)
            ask = float(row[IDX_C_ASK] or 0.0)
            dlt = float(row[IDX_C_DELTA] or 0.0)
        except (TypeError, ValueError, IndexError):
            return None
        if ask <= 0 or ask < bid:
            return None
        return bid, ask, dlt
    return None


def _pick_short_call(rows, spot, target):
    """OTM call whose delta is nearest `target`. Returns (strike, bid, ask, delta)."""
    best, best_gap = None, float("inf")
    for row in rows:
        try:
            k = float(row[IDX_STRIKE])
            dlt = float(row[IDX_C_DELTA] or 0.0)
            bid = float(row[IDX_C_BID] or 0.0)
            ask = float(row[IDX_C_ASK] or 0.0)
        except (TypeError, ValueError, IndexError):
            continue
        if k <= spot or ask <= 0 or ask < bid:
            continue
        gap = abs(abs(dlt) - target)
        if gap < best_gap:
            best_gap, best = gap, (k, bid, ask, dlt)
    return best


# ------------------------------------------------------------- context -----

def _context(spot):
    """GEX state / v7 condition / morning move — captured, never acted on (S267 §12)."""
    ctx = {}
    try:
        with _engine.begin() as c:
            r = c.execute(text("""
                SELECT state, net_gex, net_dex, zero_gamma, call_wall, et
                FROM gex_state ORDER BY et DESC LIMIT 1
            """)).fetchone()
        if r:
            age = (datetime.now(ET).replace(tzinfo=None) - r[5]).total_seconds() / 60.0
            ctx["gex_state"] = r[0]
            ctx["gex_fresh_min"] = round(age, 1)
            ctx["net_gex"] = float(r[1]) if r[1] is not None else None
            ctx["net_dex"] = float(r[2]) if r[2] is not None else None
            if r[3] is not None:
                ctx["zg_above"] = round(spot - float(r[3]), 2)
            if r[4] is not None:
                ctx["call_wall_dist"] = round(float(r[4]) - spot, 2)
            # v7 fires only in SUPPORT; on all days SUPPORT is the worst state for
            # this trade, but filtering it on Fridays removed winners. Log, do not gate.
            ctx["v7_condition"] = (r[0] == "SUPPORT")
    except Exception as e:
        ctx["gex_error"] = str(e)[:120]
    try:
        with _engine.begin() as c:
            r = c.execute(text("""
                SELECT spot FROM chain_snapshots
                WHERE (ts AT TIME ZONE 'America/New_York')::date = :d
                  AND spot IS NOT NULL ORDER BY ts ASC LIMIT 1
            """), {"d": datetime.now(ET).date()}).fetchone()
        if r and r[0]:
            ctx["am_move_pct"] = round(100.0 * (spot - float(r[0])) / float(r[0]), 3)
    except Exception:
        pass
    return ctx


# --------------------------------------------------------------- detector --

def on_cycle(now_et: datetime, spot: float | None = None, vix: float | None = None):
    """Call once per ~30s market cycle. Never raises."""
    try:
        _on_cycle(now_et, spot, vix)
    except Exception:
        print(f"[friday-spread] cycle error: {traceback.format_exc()}", flush=True)


def _on_cycle(now_et: datetime, spot, vix):
    if not _enabled() or not _engine:
        return
    d = now_et.date()
    if _state["date"] != d:
        _state.update(date=d, fired=False, row_id=None)
        _hydrate(d)
    if _state["fired"] or d.weekday() != 4:      # Friday only
        return

    entry = _entry_time()
    t = now_et.time()
    if t < entry:
        return
    if (now_et - datetime.combine(d, entry, tzinfo=ET)).total_seconds() > ENTRY_GRACE_MIN * 60:
        if not _state["fired"]:
            _state["fired"] = True               # window missed; do not chase
            print(f"[friday-spread] entry window missed (now {t}), skipping today", flush=True)
        return

    snap = _latest_chain()
    if not snap:
        print("[friday-spread] no chain snapshot — skip", flush=True)
        return
    snap_et, snap_spot, snap_vix, rows, exp = snap
    if exp != d:
        print(f"[friday-spread] chain expiry {exp} != today {d} — skip", flush=True)
        return
    age = (now_et.replace(tzinfo=None) - snap_et).total_seconds() / 60.0
    if age > CHAIN_STALE_MIN:
        print(f"[friday-spread] chain is {age:.1f} min stale — skip", flush=True)
        return
    if age < -2:
        # Snapshot newer than the cycle clock. Impossible live, but a replay or a
        # clock skew would otherwise price the entry off end-of-day quotes.
        print(f"[friday-spread] chain is {-age:.1f} min ahead of the clock — skip", flush=True)
        return

    spot = snap_spot
    vix = snap_vix if snap_vix is not None else vix
    width, qty = _width(), _qty()

    short = _pick_short_call(rows, spot, _target_delta())
    if not short:
        print("[friday-spread] no OTM call near target delta — skip", flush=True)
        return
    k_short, s_bid, s_ask, s_delta = short
    k_long = k_short + width
    lng = _call_leg(rows, k_long)
    if not lng:
        print(f"[friday-spread] no quote for the long leg C{k_long:.0f} — skip", flush=True)
        return
    l_bid, l_ask, _ = lng

    # conservative entry pricing: sell the short at bid, buy the long at ask
    credit = round(s_bid - l_ask, 2)
    if credit < _min_credit():
        print(f"[friday-spread] credit {credit:.2f} below minimum "
              f"{_min_credit():.2f} — skip", flush=True)
        return
    if credit >= width:
        print(f"[friday-spread] nonsense credit {credit:.2f} >= width {width} — skip", flush=True)
        return

    fee = _fee_per_side()
    credit_usd = credit * MULT * qty
    max_profit = credit_usd - fee * 2 * qty
    max_loss = -((width - credit) * MULT * qty) - fee * 4 * qty
    ctx = _context(spot)
    ctx.update(short_bid=s_bid, short_ask=s_ask, long_bid=l_bid, long_ask=l_ask,
               chain_age_min=round(age, 2))

    order_id, fill_credit, mode = None, None, "log"
    if _trade_enabled():
        mode = "live"
        order_id, fill_credit, err = _place_order(k_short, k_long, credit, qty, d)
        # Latch BEFORE anything that can raise. If the DB write below fails we must
        # still never send a second order for the same Friday.
        _state["fired"] = True
        if err:
            ctx["order_error"] = err
            mode = "log"          # order refused -> record it as a paper trade, never retry
            print(f"[friday-spread] order failed, logging only: {err}", flush=True)

    row_id = _record(d, now_et, mode, spot, vix, k_short, k_long, width, s_delta,
                     qty, credit, credit_usd, max_profit, max_loss,
                     k_short + credit, order_id, fill_credit, ctx)
    _state.update(fired=True, row_id=row_id)

    print(f"[friday-spread] FIRED {mode} SPXW {d:%y%m%d} "
          f"SELL C{k_short:.0f} / BUY C{k_long:.0f} x{qty}  "
          f"credit ${credit_usd:,.0f}  max win ${max_profit:,.0f}  "
          f"max loss ${max_loss:,.0f}  B/E {k_short + credit:.2f}  "
          f"spot {spot:.2f}  delta {s_delta:.3f}  gex={ctx.get('gex_state')}", flush=True)

    tag = "REAL" if mode == "live" else "PAPER"
    _tg("\n".join([
        f"🗓️ Friday spread opened ({tag})",
        f"Sell C{k_short:.0f} / buy C{k_long:.0f} ×{qty}",
        f"Credit ${credit_usd:,.0f} · risk ${abs(max_loss):,.0f}",
        f"Win if SPX closes under {k_short + credit:,.2f}",
    ]))


# ------------------------------------------------------------- settlement --

def settle():
    """Cron ~16:05 ET. Cash-settles today's spread against the closing print."""
    try:
        _settle()
    except Exception:
        print(f"[friday-spread] settle error: {traceback.format_exc()}", flush=True)


def _settle(for_date: _date | None = None):
    if not _engine:
        return
    d = for_date or datetime.now(ET).date()
    with _engine.begin() as c:
        r = c.execute(text("""
            SELECT id, short_strike, long_strike, width, qty, credit_pts, mode, order_id
            FROM friday_spread_log
            WHERE trade_date = :d AND result IS NULL
        """), {"d": d}).fetchone()
        if not r:
            # Silence is ambiguous — say plainly that nothing was taken, and why.
            if d.weekday() == 4:
                with _engine.begin() as c2:
                    done = c2.execute(text(
                        "SELECT result FROM friday_spread_log WHERE trade_date = :d"
                    ), {"d": d}).fetchone()
                if not done:
                    _tg(f"➖ Friday spread — no trade {d:%d %b} (nothing met the rules)")
            return

    # A live order is a LIMIT order and may not have filled. Confirm before settling,
    # so a day we never got into is recorded as such instead of as a phantom win.
    if r[6] == "live" and r[7]:
        status, filled_credit = _order_outcome(r[7])
        if status == "unfilled":
            with _engine.begin() as c:
                c.execute(text("""
                    UPDATE friday_spread_log
                    SET result = 'NO_FILL', pnl_pts = 0, pnl_usd = 0
                    WHERE id = :i
                """), {"i": r[0]})
            print(f"[friday-spread] {d} limit order never filled — no position", flush=True)
            _tg(f"➖ Friday spread — limit never filled {d:%d %b}, no position")
            return
        if filled_credit is not None:
            with _engine.begin() as c:
                c.execute(text("UPDATE friday_spread_log SET fill_credit = :f WHERE id = :i"),
                          {"f": filled_credit, "i": r[0]})
            r = tuple(r[:5]) + (filled_credit,) + tuple(r[6:])

    with _engine.begin() as c:
        close = c.execute(text("""
            SELECT spot FROM chain_snapshots
            WHERE (ts AT TIME ZONE 'America/New_York')::date = :d AND spot IS NOT NULL
            ORDER BY ts DESC LIMIT 1
        """), {"d": d}).fetchone()
    if not close or close[0] is None:
        print(f"[friday-spread] no closing spot for {d} — settle deferred", flush=True)
        return

    rid, k_s, k_l, width, qty, credit, mode = r[:7]
    sf = float(close[0])
    k_s, k_l, width, credit = float(k_s), float(k_l), float(width), float(credit)
    qty = int(qty or 1)
    itm = max(0.0, sf - k_s)
    cost = min(width, itm)                       # cash settlement, per spread, in points
    fee = _fee_per_side()
    closed_legs = 0 if cost == 0 else 2          # expiring worthless costs nothing to close
    pnl_pts = credit - cost
    pnl_usd = pnl_pts * MULT * qty - fee * qty * (2 + closed_legs)
    result = "WIN" if pnl_usd > 0 else ("FLAT" if abs(pnl_usd) < 1e-9 else
                                        ("MAX_LOSS" if cost >= width - 1e-9 else "LOSS"))

    with _engine.begin() as c:
        c.execute(text("""
            UPDATE friday_spread_log
            SET settle_spot = :sf, settle_cost = :cost, pnl_pts = :pp,
                pnl_usd = :pu, result = :res
            WHERE id = :i
        """), {"sf": sf, "cost": cost, "pp": pnl_pts, "pu": pnl_usd,
               "res": result, "i": rid})

    print(f"[friday-spread] SETTLED {d} {result}  close {sf:.2f}  "
          f"short C{k_s:.0f}  cost {cost:.2f} pt  P&L ${pnl_usd:,.2f}", flush=True)
    _tg(_result_message(d, mode, k_s, k_l, width, qty, credit, sf, cost,
                        pnl_usd, result))


def _result_message(d, mode, k_s, k_l, width, qty, credit, sf, cost, pnl_usd, result):
    """The Friday result. Four lines. Money always in $ and SAR."""
    icon = {"WIN": "✅", "LOSS": "🔻", "MAX_LOSS": "🔻", "FLAT": "➖"}.get(result, "•")
    tag = "REAL" if mode == "live" else "PAPER"
    gap = sf - k_s
    where = (f"{abs(gap):,.1f} under C{k_s:.0f}" if cost <= 0
             else f"{gap:,.1f} over C{k_s:.0f}")
    sgn = "+" if pnl_usd >= 0 else "-"
    lines = [
        f"{icon} Friday spread {result.replace('_', ' ')} ({tag})",
        f"SPX closed {sf:,.2f} — {where}",
        f"{sgn}${abs(pnl_usd):,.2f}  (SAR {sgn}{abs(pnl_usd) * 3.75:,.0f})",
    ]
    tot = _running_totals()
    if tot and tot["n"] > 0:
        ts = "+" if tot["total"] >= 0 else "-"
        more = f" · {20 - tot['n']} to go" if tot["n"] < 20 else ""
        lines.append(f"Run: {tot['n']}F {tot['wins']}W/{tot['losses']}L · "
                     f"{ts}${abs(tot['total']):,.0f}{more}")
    return "\n".join(lines)


def _running_totals():
    """{n, wins, losses, wr, total} across settled rows that actually held a position."""
    try:
        with _engine.begin() as c:
            r = c.execute(text("""
                SELECT count(*), coalesce(sum(pnl_usd), 0),
                       count(*) FILTER (WHERE pnl_usd > 0),
                       count(*) FILTER (WHERE pnl_usd < 0)
                FROM friday_spread_log
                WHERE result IS NOT NULL AND result <> 'NO_FILL'
            """)).fetchone()
        n = int(r[0] or 0)
        return {"n": n, "total": float(r[1] or 0), "wins": int(r[2] or 0),
                "losses": int(r[3] or 0),
                "wr": (100.0 * int(r[2] or 0) / n) if n else 0.0}
    except Exception:
        return None


# ----------------------------------------------------------- order sending --

def _place_order(k_short, k_long, credit, qty, d):
    """Atomic two-leg limit credit spread. Returns (order_id, fill_credit, error)."""
    import requests
    acct = os.getenv("FRIDAY_SPREAD_ACCOUNT", "").strip()
    if not acct:
        return None, None, "no account configured"
    if not _get_token:
        return None, None, "no token function"
    try:
        token = _get_token()
    except Exception as e:
        return None, None, f"token error: {e}"
    if not token:
        return None, None, "no token"

    ds = d.strftime("%y%m%d")
    short_sym = f"SPXW {ds}C{int(k_short)}"
    long_sym = f"SPXW {ds}C{int(k_long)}"
    payload = {
        "AccountID": acct,
        "Symbol": short_sym,
        "Quantity": str(qty),
        "OrderType": "Limit",
        "LimitPrice": str(round(credit, 2)),
        "TradeAction": "SELLTOOPEN",
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
        "Legs": [
            {"Symbol": short_sym, "Quantity": str(qty), "TradeAction": "SELLTOOPEN"},
            {"Symbol": long_sym, "Quantity": str(qty), "TradeAction": "BUYTOOPEN"},
        ],
    }
    try:
        resp = requests.post(f"{_api_base()}/orderexecution/orders",
                             json=payload,
                             headers={"Authorization": f"Bearer {token}"},
                             timeout=15)
        data = resp.json() if resp.content else {}
    except Exception as e:
        return None, None, f"request failed: {e}"

    orders = data.get("Orders") or []
    if not orders:
        return None, None, f"no order in response: {str(data)[:200]}"
    o = orders[0]
    if str(o.get("Error", "")).upper() in ("FAILED", "REJECTED"):
        return None, None, str(o.get("Message", "rejected"))[:200]
    return o.get("OrderID"), None, None


def _order_outcome(order_id: str):
    """('filled'|'unfilled'|'unknown', filled_credit_or_None) for a placed order.

    A limit credit spread can sit unfilled all afternoon. Settling such a day as
    a win would invent money we never made, so an unconfirmed order is treated as
    'unknown' (settle normally on the recorded credit) and only an explicitly
    cancelled/rejected/expired order counts as 'unfilled'.
    """
    import requests
    if not _get_token:
        return "unknown", None
    try:
        token = _get_token()
        acct = os.getenv("FRIDAY_SPREAD_ACCOUNT", "").strip()
        resp = requests.get(f"{_api_base()}/brokerage/accounts/{acct}/historicalorders"
                            f"?since={datetime.now(ET).date().isoformat()}",
                            headers={"Authorization": f"Bearer {token}"}, timeout=15)
        data = resp.json() if resp.content else {}
    except Exception as e:
        print(f"[friday-spread] order status check failed: {e}", flush=True)
        return "unknown", None
    for o in (data.get("Orders") or []):
        if str(o.get("OrderID")) != str(order_id):
            continue
        st = str(o.get("Status", "")).upper()
        if st in ("FLL", "FILLED"):
            px = o.get("FilledPrice") or o.get("LimitPrice")
            try:
                return "filled", float(px)
            except (TypeError, ValueError):
                return "filled", None
        if st in ("CAN", "REJ", "EXP", "OUT", "UROUT", "CANCELED", "CANCELLED",
                  "REJECTED", "EXPIRED"):
            return "unfilled", None
        return "unknown", None
    return "unknown", None


# ------------------------------------------------------------------ misc ---

def _record(d, et, mode, spot, vix, k_s, k_l, width, delta, qty, credit,
            credit_usd, max_profit, max_loss, be, order_id, fill_credit, ctx):
    with _engine.begin() as c:
        r = c.execute(text("""
            INSERT INTO friday_spread_log
                (trade_date, entry_et, mode, spot_entry, vix, short_strike,
                 long_strike, width, short_delta, qty, credit_pts, credit_usd,
                 max_profit_usd, max_loss_usd, breakeven, order_id, fill_credit, details)
            VALUES
                (:d, :et, :m, :sp, :vx, :ks, :kl, :w, :dl, :q, :cp, :cu,
                 :mp, :ml, :be, :oid, :fc, :det)
            ON CONFLICT (trade_date) DO NOTHING
            RETURNING id
        """), {"d": d, "et": et.replace(tzinfo=None), "m": mode, "sp": spot, "vx": vix,
               "ks": k_s, "kl": k_l, "w": width, "dl": delta, "q": qty,
               "cp": credit, "cu": credit_usd, "mp": max_profit, "ml": max_loss,
               "be": be, "oid": order_id, "fc": fill_credit,
               "det": json.dumps(ctx)}).fetchone()
    return r[0] if r else None


def _tg(msg):
    """Telegram, once or twice a Friday. Never raises, never blocks the caller."""
    if not _b("FRIDAY_SPREAD_TELEGRAM", "true"):
        return
    if not _send_telegram:
        print(f"[friday-spread] (no telegram fn) {msg}", flush=True)
        return
    try:
        _send_telegram(msg)
    except Exception as e:
        print(f"[friday-spread] telegram failed: {e}", flush=True)


def status() -> dict:
    """Everything the portal needs. {} on failure — never raises."""
    try:
        out = {
            "enabled": _enabled(),
            "trade_enabled": _trade_enabled(),
            "live_api": _b("FRIDAY_SPREAD_LIVE", "false"),
            "account": (os.getenv("FRIDAY_SPREAD_ACCOUNT", "") or None),
            "width": _width(), "qty": _qty(), "delta": _target_delta(),
            "entry_et": _entry_time().strftime("%H:%M"),
            "min_credit": _min_credit(),
            "fired_today": _state["fired"],
        }
        if _engine:
            with _engine.begin() as c:
                rows = c.execute(text("""
                    SELECT trade_date, mode, short_strike, long_strike, qty,
                           credit_usd, max_loss_usd, settle_spot, pnl_usd, result, details
                    FROM friday_spread_log ORDER BY trade_date DESC LIMIT 30
                """)).fetchall()
                agg = c.execute(text("""
                    SELECT count(*), coalesce(sum(pnl_usd), 0),
                           count(*) FILTER (WHERE pnl_usd > 0)
                    FROM friday_spread_log WHERE result IS NOT NULL
                """)).fetchone()
            out["trades"] = [{
                "date": str(r[0]), "mode": r[1], "short": r[2], "long": r[3],
                "qty": r[4], "credit": r[5], "max_loss": r[6],
                "settle": r[7], "pnl": r[8], "result": r[9],
                "gex_state": (r[10] or {}).get("gex_state") if isinstance(r[10], dict) else None,
            } for r in rows]
            out["n_settled"] = agg[0]
            out["total_pnl"] = float(agg[1] or 0)
            out["win_rate"] = (100.0 * agg[2] / agg[0]) if agg[0] else None
        return out
    except Exception:
        return {}


def backfill_settle(days: int = 90) -> int:
    """Settle any unresolved rows (e.g. after an outage). Returns rows fixed."""
    if not _engine:
        return 0
    with _engine.begin() as c:
        rows = c.execute(text("""
            SELECT trade_date FROM friday_spread_log
            WHERE result IS NULL AND trade_date < :today
              AND trade_date >= :since
            ORDER BY trade_date
        """), {"today": datetime.now(ET).date(),
               "since": datetime.now(ET).date() - timedelta(days=days)}).fetchall()
    n = 0
    for (d,) in rows:
        try:
            _settle(d)
            n += 1
        except Exception:
            print(f"[friday-spread] backfill settle failed for {d}: "
                  f"{traceback.format_exc()}", flush=True)
    return n
