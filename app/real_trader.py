# Real Trader: MES Futures LIVE execution module
# Self-contained -- receives engine, get_token_fn, and send_telegram_fn via init()
# Uses REAL TradeStation API (api.tradestation.com) -- THIS IS REAL MONEY.
#
# Two accounts, direction-routed:
#   210VYX65 -> longs only
#   210VYX91 -> shorts only
#
# 1 MES per trade. No split target -- entry + stop + target.
# Cap: 2 concurrent per direction.
# Trail: SC trail (BE trigger=10, activation=10, gap=5).

import os, json, math, time, calendar, html, requests, zoneinfo
from datetime import datetime, date, timedelta
from threading import Lock

NY = zoneinfo.ZoneInfo("US/Eastern")

# ====== MES CONTRACT AUTO-ROLLOVER ======
_MES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]


def _third_friday(year: int, month: int) -> date:
    c = calendar.Calendar(firstweekday=calendar.MONDAY)
    fridays = [d for d in c.itermonthdates(year, month) if d.month == month and d.weekday() == 4]
    return fridays[2]


def _auto_mes_symbol() -> str:
    """Return front-month MES symbol for TradeStation (e.g. MESH26), rolling ~8 days before expiry."""
    today = date.today()
    for month_num, code in _MES_MONTHS:
        expiry = _third_friday(today.year, month_num)
        if today <= expiry - timedelta(days=8):
            return f"MES{code}{today.year % 100}"
    return f"MESH{(today.year + 1) % 100}"


# ====== CONFIG ======
REAL_BASE = "https://api.tradestation.com/v3"

# Account whitelist -- ONLY these accounts can receive orders
ACCOUNT_WHITELIST = frozenset({"210VYX65", "210VYX91"})

# Direction binding -- each account is locked to one direction
_LONGS_ACCOUNT = os.getenv("REAL_TRADE_LONGS_ACCOUNT", "210VYX65")
_SHORTS_ACCOUNT = os.getenv("REAL_TRADE_SHORTS_ACCOUNT", "210VYX91")
ACCOUNT_DIRECTION_BINDING = {
    _LONGS_ACCOUNT: "long",
    _SHORTS_ACCOUNT: "short",
}

# Master switches -- both default OFF for safety
LONGS_ENABLED = os.getenv("REAL_TRADE_LONGS_ENABLED", "false").lower() == "true"
SHORTS_ENABLED = os.getenv("REAL_TRADE_SHORTS_ENABLED", "false").lower() == "true"

# Symbol
_es_env = os.getenv("REAL_TRADE_MES_SYMBOL", "auto")
MES_SYMBOL = _auto_mes_symbol() if _es_env.lower() == "auto" else _es_env

# Position sizing -- 1 MES per trade, per-direction concurrent caps
# Apr 18 2026: raised SHORTS cap 1→2 after slot study (TSRT-live 1-cap missed
# 12 shorts worth +244 pts / 91% WR over 17 days, ~$1,200 at 1 MES).
# LONGS stay at 1 (slot cap was neutral on longs — 5 skipped = +0.5 pts).
# Margin required per active MES: ~$687 intraday. Accounts funded +$1k each
# (pre-Monday) to support 2 concurrent on shorts.
QTY = 1
MAX_CONCURRENT_LONG = int(os.getenv("REAL_TRADE_MAX_CONCURRENT_LONG", "1"))
MAX_CONCURRENT_SHORT = int(os.getenv("REAL_TRADE_MAX_CONCURRENT_SHORT", "2"))
# Backward-compat alias — any legacy code reading MAX_CONCURRENT_PER_DIR gets
# the conservative LONG value. New code should use the per-direction constants.
MAX_CONCURRENT_PER_DIR = MAX_CONCURRENT_LONG

# Risk management
FIRST_TARGET_PTS = 10.0
MES_TICK_SIZE = 0.25
MES_POINT_VALUE = 5.0
def _margin_per_mes() -> float:
    return float(os.getenv("REAL_TRADE_MARGIN_PER_MES", "700"))  # TS intraday margin $686.75/MES (Jan 2026)
DAILY_LOSS_LIMIT = float(os.getenv("REAL_TRADE_DAILY_LOSS_LIMIT", "300"))  # max daily loss in $

# ── S131 (2026-05-17): SPX-driven exit mode ──
# When enabled: broker SL stays at initial fill (-stop_pts) as SAFETY NET only.
# Bot's internal trail tracks SPX path. When SPX price crosses internal trail
# level, bot fires market exit + cancels broker SL pre-empting MES tick wicks.
# Closes the -$9.65/trade trail-tag-early gap per S132 audit (107% of V14 era).
# Rollback: set env back to false. Broker SL still active = safety net throughout.
def _spx_exit_enabled() -> bool:
    return os.getenv("SPX_EXIT_ENABLED", "false").lower() == "true"


# ── Atomic bracket (2026-05-19): submit entry + SL [+ target] in ONE POST ──
# User-clarified rationale: this is NOT a margin fix. TS displays scary margin
# numbers (InitialMargin=$2,649, negative BP) but doesn't actually block orders
# on them — they're cosmetic, not enforced (verified S101 manual test). Margin
# pre-check removed via the patch above (S156). Atomic ordergroup is still
# worth shipping for THREE other reasons: (1) eliminates the 200-500ms naked
# window between sequential entry-fill and SL-POST — if SL POST fails or
# network blips, no orphan unprotected position; (2) single network round-trip
# vs 3 (latency reduction); (3) TS guarantees SL is registered at entry
# submission. Confirmed accepted on SIM 2026-05-19 (3 OrderIDs returned from
# 1 POST to /orderexecution/ordergroups Type=NORMAL). Feature flag default
# OFF: deploy code, flip true after user confirms first real-money atomic
# placement shows "[real-trader] PLACED ATOMIC:" log line with all 3 OrderIDs.
# Sequential path preserved as fallback.
def _atomic_bracket_enabled() -> bool:
    return os.getenv("ATOMIC_BRACKET_ENABLED", "false").lower() == "true"

# SC Trail parameters
BE_TRIGGER_PTS = 10.0    # move stop to breakeven after 10pts profit
TRAIL_ACTIVATION_PTS = 10.0  # trail activates at 10pts
TRAIL_GAP_PTS = 5.0      # trail gap = max_fav - 5
BE_BUFFER_PTS = 0.25     # breakeven + 1 tick buffer

# Charm S/R limit entry timeout
_LIMIT_ENTRY_TIMEOUT_S = 1800  # 30 min

# DB table name
DB_TABLE = "real_trade_orders"


def _round_mes(price: float) -> float:
    """Round price to nearest MES tick (0.25)."""
    return round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)


def _get_current_mes_price() -> float | None:
    """Fetch current MES Last price via REST for side-of-market validation.
    Used by update_stop() to avoid submitting a stop that market has already
    crossed (TS rejects such modifies and may wipe the original stop).
    ~100-200ms network call. Returns None on any failure."""
    if not _get_token:
        return None
    try:
        token = _get_token()
        sym = MES_SYMBOL.replace("@", "%40")
        r = requests.get(
            f"{REAL_BASE}/marketdata/quotes/{sym}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        if r.status_code != 200:
            print(f"[real-trader] mes price fetch [{r.status_code}]", flush=True)
            return None
        js = r.json()
        for q in js.get("Quotes", []):
            last = q.get("Last")
            if last:
                return float(last)
    except Exception as e:
        print(f"[real-trader] mes price fetch failed: {e}", flush=True)
    return None


def _order_ok(resp: dict | None) -> tuple[bool, str | None]:
    """Check if an order response succeeded. Returns (ok, order_id).
    TS returns HTTP 200 even for FAILED orders -- must check order-level Error."""
    if not resp:
        return False, None
    orders = resp.get("Orders", [])
    if not orders:
        return False, None
    first = orders[0]
    if first.get("Error") == "FAILED":
        msg = first.get("Message", "unknown error")
        print(f"[real-trader] order FAILED: {msg}", flush=True)
        return False, first.get("OrderID")
    oid = first.get("OrderID")
    return bool(oid), oid


def _extract_ts_error(resp: dict | None) -> str:
    """Extract TS rejection text for diagnostic logging/alerts.
    Returns empty string if no error. Checks all known TS error response shapes."""
    if not resp:
        return ""
    # Shape 1: Orders[].Message on FAILED
    orders = resp.get("Orders", [])
    if orders and orders[0].get("Error") == "FAILED":
        msg = orders[0].get("Message") or ""
        if msg:
            return msg[:200]
    # Shape 2: top-level Errors list
    errs = resp.get("Errors", [])
    if isinstance(errs, list) and errs:
        msg = (errs[0].get("Message") if isinstance(errs[0], dict) else str(errs[0])) or ""
        if msg:
            return msg[:200]
    # Shape 3: top-level Error string
    err = resp.get("Error")
    if err:
        return str(err)[:200]
    return ""


# Rate-limit FAILED entry alerts so a margin-cascade burst doesn't spam Telegram.
# Key: f"{setup_name}:{account_id}"  Value: last alert epoch.
_failed_alert_last: dict[str, float] = {}
_FAILED_ALERT_COOLDOWN_S = 60


# ====== STATE ======
_engine = None
_get_token = None       # callable -> str (access token)
_send_telegram = None   # callable(msg) -> bool
_lock = Lock()
_active_orders: dict[int, dict] = {}  # keyed by setup_log_id
# Position reconciliation is now driven by a 30s scheduler job calling
# reconcile_positions() — no throttle variable needed here.


def init(engine, get_token_fn, send_telegram_fn):
    """Initialize real trader. Called once at startup."""
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn
    _load_active_orders()
    n = len(_active_orders)
    print(f"[real-trader] init: longs={LONGS_ENABLED} (acct={_LONGS_ACCOUNT}) "
          f"shorts={SHORTS_ENABLED} (acct={_SHORTS_ACCOUNT}) "
          f"symbol={MES_SYMBOL} qty={QTY} "
          f"max_long={MAX_CONCURRENT_LONG} max_short={MAX_CONCURRENT_SHORT} "
          f"active_orders={n}", flush=True)

    # Validate account configuration
    if _LONGS_ACCOUNT not in ACCOUNT_WHITELIST:
        print(f"[real-trader] FATAL: longs account {_LONGS_ACCOUNT} not in whitelist!", flush=True)
    if _SHORTS_ACCOUNT not in ACCOUNT_WHITELIST:
        print(f"[real-trader] FATAL: shorts account {_SHORTS_ACCOUNT} not in whitelist!", flush=True)
    if _LONGS_ACCOUNT == _SHORTS_ACCOUNT:
        print(f"[real-trader] WARNING: longs and shorts using same account {_LONGS_ACCOUNT}", flush=True)

    # Verify account access on startup (if either direction enabled)
    if (LONGS_ENABLED or SHORTS_ENABLED) and _get_token:
        for acct_id in set(filter(None, [
            _LONGS_ACCOUNT if LONGS_ENABLED else None,
            _SHORTS_ACCOUNT if SHORTS_ENABLED else None,
        ])):
            try:
                # Use /balances endpoint (live TS API returns 404 on bare /accounts/{id})
                bal = _ts_api("GET", f"/brokerage/accounts/{acct_id}/balances", None, acct_id)
                if bal:
                    print(f"[real-trader] account {acct_id} OK: "
                          f"{json.dumps(bal, default=str)[:300]}", flush=True)
                else:
                    print(f"[real-trader] WARNING: cannot access account {acct_id}", flush=True)
                    _alert(f"⚠️ WARNING: Cannot access account {acct_id} on startup")
            except Exception as e:
                print(f"[real-trader] account {acct_id} check error: {e}", flush=True)

    # Pre-market startup cleanup
    if LONGS_ENABLED or SHORTS_ENABLED:
        try:
            from datetime import timezone as _tz
            import zoneinfo
            _et = zoneinfo.ZoneInfo("US/Eastern")
            _now_et = datetime.now(_tz.utc).astimezone(_et)
            _market_open = _now_et.replace(hour=9, minute=20, second=0, microsecond=0)
            _market_close = _now_et.replace(hour=16, minute=10, second=0, microsecond=0)
            if _now_et < _market_open or _now_et > _market_close:
                # Outside market hours -- flatten everything
                for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
                    if acct_id not in ACCOUNT_WHITELIST:
                        continue
                    broker_pos = _get_broker_position(acct_id)
                    if broker_pos:
                        print(f"[real-trader] PRE-MARKET CLEANUP: {acct_id} has "
                              f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}",
                              flush=True)
                        _alert(f"⚠️ PRE-MARKET: Found position on {acct_id}\n"
                               f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}\n"
                               f"Auto-closing...")
                        _flatten_account(acct_id)
                # Mark all tracked orders as closed
                _to_persist = []
                with _lock:
                    for lid, o in _active_orders.items():
                        if o["status"] not in ("closed",):
                            o["status"] = "closed"
                            o["close_reason"] = "pre_market_cleanup"
                            _to_persist.append((lid, o.get('setup_name', '?')))
                # Persist OUTSIDE lock (avoid deadlock — _persist_order also acquires _lock)
                for lid, name in _to_persist:
                    _persist_order(lid)
                    print(f"[real-trader] PRE-MARKET: closed order {name} id={lid}", flush=True)
            else:
                # During market hours -- orphan check
                for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
                    if acct_id in ACCOUNT_WHITELIST:
                        _close_broker_orphans(acct_id, source="STARTUP")
        except Exception as e:
            print(f"[real-trader] startup cleanup error: {e}", flush=True)


# ====== HELPERS ======

def _get_account_for_direction(is_long: bool) -> str | None:
    """Return the account ID for a given direction, or None if disabled."""
    if is_long:
        if not LONGS_ENABLED:
            return None
        return _LONGS_ACCOUNT
    else:
        if not SHORTS_ENABLED:
            return None
        return _SHORTS_ACCOUNT


def _validate_account_direction(account_id: str, is_long: bool) -> bool:
    """Validate that the account is allowed for this direction. CRITICAL SAFETY CHECK."""
    if account_id not in ACCOUNT_WHITELIST:
        print(f"[real-trader] BLOCKED: account {account_id} not in whitelist!", flush=True)
        _alert(f"🚨 SECURITY: Blocked order to non-whitelisted account {account_id}")
        return False
    expected_dir = ACCOUNT_DIRECTION_BINDING.get(account_id)
    if expected_dir is None:
        print(f"[real-trader] BLOCKED: account {account_id} has no direction binding!", flush=True)
        _alert(f"🚨 SECURITY: No direction binding for account {account_id}")
        return False
    actual_dir = "long" if is_long else "short"
    if expected_dir != actual_dir:
        print(f"[real-trader] BLOCKED: account {account_id} bound to {expected_dir}, "
              f"got {actual_dir}!", flush=True)
        _alert(f"🚨 SECURITY: Direction mismatch!\n"
               f"Account {account_id} bound to {expected_dir}, attempted {actual_dir}")
        return False
    return True


def _count_active_for_direction(is_long: bool) -> int:
    """Count currently active orders (pending or filled) for a direction."""
    count = 0
    with _lock:
        for o in _active_orders.values():
            if o["status"] in ("pending_entry", "pending_limit", "pending_stop_entry", "filled"):
                o_is_long = o["direction"].lower() in ("long", "bullish")
                if o_is_long == is_long:
                    count += 1
    return count


# ====== SKIP-REASON TELEMETRY (S114) ======
# When real_trader rejects a signal (cap, dedup, margin, whitelist, etc.) we
# record the reason on setup_log so post-hoc audits can attribute every V14-pass
# signal that did not result in a real trade. Wrapped in try/except so a missing
# column (pre-ALTER) or DB hiccup never breaks the trade flow.

# Rate-limit table for skip-reason Telegram alerts (S114). Most skip reasons
# already alert via _alert() inline; we only need a *dedicated* rate-limited
# Telegram for the "no ES price" class since that one previously had NO alert
# at all and was the silent leak found in the S113 audit.
_skip_alert_last: dict[str, float] = {}
_SKIP_ALERT_COOLDOWN_S = 300  # 5 min per reason key


def _log_skip_reason(setup_log_id, reason: str):
    """Best-effort UPDATE setup_log.real_trade_skip_reason. Never raises.

    Defensive: the column may not yet exist (pre-ALTER) — swallow any error.
    """
    if not setup_log_id or not _engine:
        return
    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            conn.execute(
                text("UPDATE setup_log SET real_trade_skip_reason = :r WHERE id = :id"),
                {"r": reason, "id": setup_log_id},
            )
    except Exception as e:
        # Column missing, DB blip, or shutdown — log but never propagate.
        print(f"[real-trader] skip-reason log failed (non-fatal): {e}", flush=True)


def _alert_no_es_px(setup_name: str, direction: str):
    """Rate-limited Telegram for the 'ES quote stream returned None' skip.

    This is the S114 case: previously silent. Cooldown 5 min per setup_name+dir
    so a stream outage doesn't spam dozens of alerts during the burst window.
    """
    key = f"no_es_px:{setup_name}:{direction.lower()}"
    now = time.time()
    last = _skip_alert_last.get(key, 0)
    if now - last < _SKIP_ALERT_COOLDOWN_S:
        return
    _skip_alert_last[key] = now
    _alert(f"⚠️ SKIPPED {setup_name} {direction}: ES quote unavailable (stream stale)\n"
           f"Account dispatch aborted — no entry price.")


def log_no_es_px_skip(setup_log_id, setup_name: str, direction: str):
    """Public entry point for upstream callers (main.py) that detect a missing
    ES price BEFORE invoking place_trade(). Writes skip_reason + fires alert.

    Defensive: try/except so any failure cannot impact the surrounding code.
    """
    try:
        _log_skip_reason(setup_log_id, "no_es_px")
    except Exception as e:
        print(f"[real-trader] log_no_es_px_skip db error: {e}", flush=True)
    try:
        _alert_no_es_px(setup_name, direction)
    except Exception as e:
        print(f"[real-trader] log_no_es_px_skip alert error: {e}", flush=True)


# ── S149 double-up bucket: SC long BOFA-PURE align=+1 → 2x MES ──
# S20 review hit threshold 2026-05-18: 75t / 73% WR / +$1,833 over Mar-May
# (Apr 47t/79% +$1,550, May 28t/67% +$284). At 2x MES this bucket alone
# projects ~$1,833/mo additional at 1 MES base.
# Risk: cap=3 × 2 MES = 6 MES max long exposure (margin per S141 verified
# intraday ~$265 × 6 = $1,590 ≈ 53% of $3k acct — safe).
# Daily loss cap $300 absorbs ~2 losers at 2x.
# Env flag default ON; flip false to emergency-rollback to 1x without redeploy.
# Kill switch (manual): if 3 consecutive losses on this bucket, flip env false.
def _is_double_up_bucket(setup_name: str, direction: str,
                         paradigm: str | None, align: int | None) -> bool:
    # Default FALSE (2026-05-19, user-directed reassessment): "+$1,833/mo" claim
    # was gross-sim with no capture-rate or regime-decay haircut. Realistic lift
    # ~$650-850/mo at 1 MES. Apr +$1,550 → May +$284 = 81% MTD decay; capture
    # rate on THIS bucket not validated on real money post-fixes (V16.1 + S131 +
    # margin removal + atomic bracket all just shipped — attribution will be
    # messy if 2x stacks on top). Plan: 20-30 SC long BOFA-PURE align=+1 fires
    # at 1x post-deploy, measure capture rate on this bucket specifically, THEN
    # flip env to true if capture confirms expectation. Cost of waiting ~$400-800
    # over 3-4 weeks; cost of being wrong at 2x = real DD acceleration on
    # unvalidated bucket.
    if os.getenv("SC_BOFA_PURE_DOUBLE_UP_ENABLED", "false").lower() != "true":
        return False
    if setup_name != "Skew Charm":
        return False
    if direction.lower() not in ("long", "bullish"):
        return False
    if paradigm != "BOFA-PURE":
        return False
    if align != 1:
        return False
    return True


def _effective_qty(setup_name: str, direction: str,
                   paradigm: str | None, align: int | None) -> int:
    """Returns intended quantity for this trade (1 default, 2 for S149 bucket)."""
    if _is_double_up_bucket(setup_name, direction, paradigm, align):
        return 2
    return QTY


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str,
                es_price: float, target_pts: float | None, stop_pts: float,
                charm_limit_price: float | None = None,
                paradigm: str | None = None,
                greek_alignment: int | None = None):
    """Place 1 MES REAL trade when a setup fires.

    Args:
        setup_log_id: DB id from setup_log table
        setup_name: e.g. "Skew Charm", "AG Short"
        direction: "Long"/"Bullish" or "Short"/"Bearish"
        es_price: current ES/MES price from quote stream
        target_pts: distance in points to target (None for trailing setups)
        stop_pts: distance in points to stop
        charm_limit_price: MES limit entry price (charm S/R shorts). None = market order.
        paradigm: Volland paradigm (passed from main.py dispatch for S149 bucket detection)
        greek_alignment: -3..+3 alignment (passed from main.py dispatch for S149)
    """
    # ── S175 Master kill (2026-05-21 emergency) ──
    # Volland deployed bot detection overnight → headless worker captures 0 widgets,
    # paradigm/LIS/DD stale → real trades graded on cached Greeks. User halted TSRT
    # 11:30 ET until Volland is migrated to pure-HTTP worker (post-market today).
    # DEFAULT: disabled (true) — explicit re-enable required.
    # To re-enable: railway variables --set REAL_TRADE_DISABLED=false --service 0dtealpha
    if os.getenv("REAL_TRADE_DISABLED", "true").lower() != "false":
        print(f"[real-trader] DISABLED (master kill): skip {setup_name} {direction}", flush=True)
        _log_skip_reason(setup_log_id, "master_kill")
        _alert(f"⏭ DISABLED: {setup_name} {direction} blocked (REAL_TRADE_DISABLED=true)")
        return

    is_long = direction.lower() in ("long", "bullish")

    # S149: determine effective quantity (1 default, 2 for SC long BOFA-PURE align=+1).
    # Computed BEFORE all gates so cap/dedup/alerts can reference it consistently.
    qty = _effective_qty(setup_name, direction, paradigm, greek_alignment)

    # Setup filter: only trade Skew Charm + AG Short + VPB-Bull (defense-in-depth, main.py also filters)
    # AG Short added 2026-04-08 — SHORT account only (AG hardcoded direction="short")
    # Vanna Pivot Bounce added 2026-04-22 — LONGS only, bullish regime gated by main.py
    # GEX Long v3 (2026-05-13): split into TWO flags 2026-05-18:
    #   - GEX_LONG_V3_ENABLED        → detector fires v3 signals to setup_log (portal display)
    #   - GEX_LONG_V3_REAL_TRADE_ENABLED → real trader places live trades (default false = PORTAL-ONLY)
    # This lets us monitor v3 signals in portal without committing real money.
    _allowed = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption"}
    # VIX Divergence: disabled 2026-05-18 after 0/4 OOS WR live since May 3 ship
    # (3 BOFA-PURE + 1 AG-LIS — all non-GEX paradigms). Re-enabled 2026-05-27
    # with GEX-paradigm filter in _passes_live_filter() (LONGS only, paradigm
    # LIKE GEX-%). Backtest: 6 GEX-* longs Apr 2 - May 1, 5W + 1 confirm-
    # timeout, 0 LOSSES, +$232 portal at 1 MES. Toggle live via env flag.
    if os.getenv("VIX_DIV_REAL_TRADE_ENABLED", "false").lower() == "true":
        _allowed.add("VIX Divergence")
    if os.getenv("GEX_LONG_V3_REAL_TRADE_ENABLED", "false").lower() == "true":
        _allowed.add("GEX Long")
    # ── V16 (2026-05-17): DD Exhaustion long admit (gated by V14 quality rules in
    # _passes_live_filter: align<3, vix<22, paradigm not in bad-set, grade!=C).
    # Multi-regime validation: Mar +$756 / Apr +$1,028 / May +$774 = +$2,558 over 2.5mo.
    # 146 trades passing V14 DD long quality gates.
    # Defaults ON since the V14 paradigm filter is strong; toggle off via env if needed.
    if os.getenv("DD_EXHAUSTION_REAL_TRADE_ENABLED", "true").lower() == "true":
        _allowed.add("DD Exhaustion")
    if setup_name not in _allowed:
        print(f"[real-trader] skip {setup_name}: not in real-trader whitelist", flush=True)
        _log_skip_reason(setup_log_id, "whitelist_reject")
        _alert(f"⏭ SKIPPED {setup_name} {direction}: setup not in real-trader whitelist")
        return

    # Check master switch for this direction
    account_id = _get_account_for_direction(is_long)
    if not account_id:
        dir_str = "longs" if is_long else "shorts"
        print(f"[real-trader] skip {setup_name}: {dir_str} master switch OFF", flush=True)
        _log_skip_reason(setup_log_id, f"{dir_str}_off")
        _alert(f"⏭ SKIPPED {setup_name} {direction}: {dir_str} master switch OFF")
        return

    # Validate account-direction binding (CRITICAL SAFETY) — already alerts internally
    if not _validate_account_direction(account_id, is_long):
        _log_skip_reason(setup_log_id, "account_direction_mismatch")
        return

    if not setup_log_id:
        # NOTE: can't log skip_reason without an id, but the alert below still fires.
        print(f"[real-trader] skip {setup_name}: no setup_log_id", flush=True)
        _alert(f"⏭ SKIPPED {setup_name} {direction}: missing setup_log_id (race or detector bug)")
        return

    # Dedup: already tracking this setup_log_id
    with _lock:
        if setup_log_id in _active_orders:
            print(f"[real-trader] skip {setup_name} id={setup_log_id}: already active", flush=True)
            _log_skip_reason(setup_log_id, "already_active")
            _alert(f"⏭ SKIPPED {setup_name} {direction}: id={setup_log_id} already active in real-trader")
            return
        # DEDUP: block if same setup_name+direction placed within last 90s (deploy overlap)
        from datetime import timezone as _utc
        _now = datetime.now(_utc.utc)
        _dedup_hit = False
        _dedup_meta = None
        for _lid, _o in _active_orders.items():
            if (_o.get("setup_name") == setup_name and
                _o.get("direction", "").lower() == direction.lower()):
                _placed = _o.get("ts_placed", "")
                if _placed:
                    try:
                        _placed_dt = datetime.fromisoformat(_placed)
                        if _placed_dt.tzinfo is None:
                            _placed_dt = _placed_dt.replace(tzinfo=_utc.utc)
                        _age = (_now - _placed_dt).total_seconds()
                        if _age < 90:
                            print(f"[real-trader] DEDUP {setup_name} id={setup_log_id}: "
                                  f"same setup placed {_age:.0f}s ago (id={_lid})", flush=True)
                            _dedup_hit = True
                            _dedup_meta = (_lid, _age)
                            break
                    except (ValueError, TypeError):
                        pass
        if _dedup_hit:
            _log_skip_reason(setup_log_id, "dedup_window")
            _alert(f"⏭ SKIPPED {setup_name} {direction}: dedup 90s window "
                   f"(prior id={_dedup_meta[0]}, {_dedup_meta[1]:.0f}s ago)")
            return

    # Cap check: max concurrent per direction (asymmetric — longs=1, shorts=2)
    active_count = _count_active_for_direction(is_long)
    cap = MAX_CONCURRENT_LONG if is_long else MAX_CONCURRENT_SHORT
    if active_count >= cap:
        dir_str = "long" if is_long else "short"
        # Log which orders are blocking (for debugging stale-order issues)
        blocking = [(lid, o.get("setup_name"), o.get("ts_placed", "")[:10], o.get("status"))
                    for lid, o in _active_orders.items()
                    if o.get("status") in ("pending_entry", "pending_limit", "pending_stop_entry", "filled")
                    and (o.get("direction", "").lower() in ("long", "bullish")) == is_long]
        print(f"[real-trader] skip {setup_name}: {dir_str} cap reached "
              f"({active_count}/{cap}) blocking={blocking}", flush=True)
        _block_str = ", ".join(f"#{b[0]} {b[1]} ({b[3]})" for b in blocking) or "n/a"
        _log_skip_reason(setup_log_id, f"cap_{dir_str}_full")
        _alert(f"⏭ SKIPPED {setup_name} {direction}: {dir_str} cap reached ({active_count}/{cap})\n"
               f"Blocking: {_block_str}")
        return

    # Margin pre-check REMOVED (2026-05-19, user-directed): user clarified via S101
    # manual testing that the BP/margin numbers TS displays are *cosmetic only* —
    # TS does NOT block orders based on them. Orders go through with negative BP
    # and "insufficient" margin display. Our pre-check was the only thing blocking
    # placement. Today's missed: lid 2987 +$35, 2991 ~$0, 3022 +$73.50 = ~$108.
    # If TS does reject (whatever the real reason), the entry POST returns an
    # error and the rejection handler at the entry call site captures TS's exact
    # reason text in skip_reason + Telegram alert. Diagnostic margin-check still
    # runs via _get_buying_power() for stdout visibility (InitialMargin /
    # DayTradeMargin per attempt) — its return value is no longer consumed for
    # blocking decisions.
    _get_buying_power(account_id)  # logs margin diagnostic to stdout, return ignored

    # Daily loss circuit breaker
    daily_loss = _get_daily_realized_loss()
    if daily_loss >= DAILY_LOSS_LIMIT:
        print(f"[real-trader] CIRCUIT BREAKER: daily loss ${daily_loss:,.0f} >= limit ${DAILY_LOSS_LIMIT:,.0f}", flush=True)
        _log_skip_reason(setup_log_id, "daily_loss_limit")
        _alert(f"🚨 CIRCUIT BREAKER HIT\n"
               f"Daily loss: ${daily_loss:,.0f} >= ${DAILY_LOSS_LIMIT:,.0f}\n"
               f"No more trades today.")
        return

    # Charm S/R limit entry for shorts ONLY (safety: ignore for longs)
    if charm_limit_price is not None and not is_long:
        _place_limit_entry(setup_log_id, setup_name, direction, is_long,
                           account_id, es_price, stop_pts, target_pts,
                           charm_limit_price, qty=qty)
        return

    # BUG 2 FIX (2026-05-18, Option A): VIX Divergence uses STOP-ENTRY confirmation.
    # The detector and backtest both assume entry only fires when price moves +/-1.5
    # in trade direction (proving the setup's directional thesis). Real trader was
    # firing Market orders immediately — caught the reversal half the time. Lid 2911
    # today (2026-05-18) lost -$56 because price went straight DOWN from entry,
    # outcome tracker correctly logged TIMEOUT (entry never confirmed) but broker
    # filled market and stopped out. Switch to StopMarket buy/sell confirmation.
    if setup_name == "VIX Divergence":
        _place_stop_entry(setup_log_id, setup_name, direction, is_long,
                          account_id, es_price, stop_pts, target_pts,
                          confirm_offset_pts=1.5, timeout_minutes=30, qty=qty)
        return

    # Standard market entry
    _place_market_entry(setup_log_id, setup_name, direction, is_long,
                        account_id, es_price, stop_pts, target_pts, qty=qty)


# ====== ORDER PLACEMENT ======

def _place_stop_entry(setup_log_id, setup_name, direction, is_long,
                      account_id, es_price, stop_pts, target_pts,
                      confirm_offset_pts=1.5, timeout_minutes=30, qty: int = 1):
    """Stop-entry confirmation order — only fills if price moves in trade direction.
    Long: BuyStop at es_price + confirm_offset.
    Short: SellStop at es_price - confirm_offset.
    If not filled within timeout_minutes, the order should be cancelled
    (poll_order_status handles timeout cleanup).

    BUG 2 FIX (2026-05-18, Option A): VIX Div backtest assumes stop-entry
    confirmation. Previously fired Market — caught half the reversals.
    """
    if not _validate_account_direction(account_id, is_long):
        return
    side = "Buy" if is_long else "Sell"
    if is_long:
        trigger_price = _round_mes(es_price + confirm_offset_pts)
    else:
        trigger_price = _round_mes(es_price - confirm_offset_pts)

    entry_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(qty),
        "OrderType": "StopMarket",
        "StopPrice": str(trigger_price),
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    print(f"[real-trader] PLACING STOP-ENTRY: {setup_name} {side} {qty} {MES_SYMBOL} "
          f"@ trigger {trigger_price} (signal ~{es_price:.2f}, "
          f"+/-{confirm_offset_pts}pt confirmation) on {account_id}", flush=True)
    resp = _ts_api("POST", "/orderexecution/orders", entry_payload, account_id)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"🚨 FAILED stop-entry for {setup_name}\n"
               f"Account: {account_id}\n"
               f"Side: {side} {qty} {MES_SYMBOL} stop-trigger {trigger_price}")
        return

    # Record as pending_stop_entry — poll_order_status will detect fill OR timeout
    from datetime import datetime as _dt, timezone as _utc, timedelta as _td
    now = _dt.now(_utc.utc)
    timeout_at = (now + _td(minutes=timeout_minutes)).isoformat()
    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction.lower(),
        "account_id": account_id,
        "status": "pending_stop_entry",  # NEW status — distinguishes from pending_limit/pending_entry
        "entry_order_id": entry_oid,
        "signal_es_price": es_price,
        "trigger_price": trigger_price,
        "confirm_offset_pts": confirm_offset_pts,
        "stop_pts": stop_pts,
        "target_pts": target_pts,
        "stop_entry_timeout_at": timeout_at,
        "stop_order_id": None,
        "target_order_id": None,
        "close_order_id": None,
        "fill_price": None,
        "current_stop": None,
        "max_favorable": 0.0,
        "trail_active": False,
        "be_triggered": False,
        "trail_only": target_pts is None,
        "ts_placed": now.isoformat(),
        "close_reason": None,
        "close_fill_price": None,
        "quantity": qty,  # S149
    }
    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)
    _alert(f"🎯 {setup_name} {direction.upper()} STOP-ENTRY placed\n"
           f"Trigger: {trigger_price} (signal {es_price:.2f}, +/-{confirm_offset_pts}pt)\n"
           f"Timeout: {timeout_minutes}min\n"
           f"Account: {account_id}")


def _place_market_entry(setup_log_id, setup_name, direction, is_long,
                        account_id, es_price, stop_pts, target_pts, qty: int = 1):
    """Place market entry + stop (+ optional target).
    When target_pts is None: trail-only mode (Opt2) — no target limit order placed.
    qty: number of MES contracts (default 1; 2 for S149 SC long BOFA-PURE align=+1 bucket).
    """
    # Final safety check before placing order
    if not _validate_account_direction(account_id, is_long):
        return

    side = "Buy" if is_long else "Sell"
    exit_side = "Sell" if is_long else "Buy"
    trail_only = target_pts is None  # Opt2: no target, trail stop only

    # 2026-05-06: Add slippage buffer to initial stop so 5-7pt fill slippage doesn't
    # leave stop on top of fill (lid=2530 today: ES Abs short signal 7347, fill
    # 7354.5, stop placed at signal+8=7355 = 0.5pt above fill = instant stop).
    # The buffer protects pre-fill; on fill confirmation poll_order_status calls
    # update_stop() to realign tight to fill_price ± stop_pts.
    SLIPPAGE_BUFFER = 5.0
    if is_long:
        es_stop = _round_mes(es_price - stop_pts - SLIPPAGE_BUFFER)
        es_target = None if trail_only else _round_mes(es_price + target_pts)
    else:
        es_stop = _round_mes(es_price + stop_pts + SLIPPAGE_BUFFER)
        es_target = None if trail_only else _round_mes(es_price - target_pts)

    # === ATOMIC BRACKET PATH (feature flag) ===
    # When ATOMIC_BRACKET_ENABLED=true, submit entry+SL [+ target] as ONE atomic
    # NORMAL ordergroup. Eliminates the 200-500ms naked window that triggers TS
    # overnight margin charging. Proven accepted on SIM 2026-05-19. See
    # _atomic_bracket_enabled() comment block above for full context.
    if _atomic_bracket_enabled():
        entry_order = {
            "AccountID": account_id, "Symbol": MES_SYMBOL, "Quantity": str(qty),
            "OrderType": "Market", "TradeAction": side,
            "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
        }
        stop_order = {
            "AccountID": account_id, "Symbol": MES_SYMBOL, "Quantity": str(qty),
            "OrderType": "StopMarket", "StopPrice": str(es_stop),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
        }
        group_orders = [entry_order, stop_order]
        if not trail_only and es_target is not None:
            group_orders.append({
                "AccountID": account_id, "Symbol": MES_SYMBOL, "Quantity": str(qty),
                "OrderType": "Limit", "LimitPrice": str(es_target),
                "TradeAction": exit_side,
                "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
            })
        group_payload = {"Type": "NORMAL", "Orders": group_orders}
        print(f"[real-trader] PLACING ATOMIC: {setup_name} {side} {qty} {MES_SYMBOL} "
              f"@ ~{es_price:.2f} stop={es_stop:.2f} target={'trail' if trail_only else f'{es_target:.2f}'} "
              f"on {account_id}", flush=True)
        group_resp = _ts_api("POST", "/orderexecution/ordergroups", group_payload, account_id)
        # Parse 3-order response. TS returns one Orders[] with same order as request.
        entry_oid = stop_oid = t1_oid = None
        atomic_ok = False
        if group_resp and isinstance(group_resp.get("Orders"), list):
            sub_orders = group_resp["Orders"]
            if len(sub_orders) >= 2:
                e_first = sub_orders[0]
                s_first = sub_orders[1]
                if e_first.get("Error") != "FAILED" and e_first.get("OrderID"):
                    entry_oid = e_first["OrderID"]
                    atomic_ok = True
                if s_first.get("Error") != "FAILED" and s_first.get("OrderID"):
                    stop_oid = s_first["OrderID"]
                if len(sub_orders) >= 3 and not trail_only:
                    t_first = sub_orders[2]
                    if t_first.get("Error") != "FAILED" and t_first.get("OrderID"):
                        t1_oid = t_first["OrderID"]
        if not atomic_ok:
            err_text = _extract_ts_error(group_resp)
            reason_short = (err_text[:80] or "atomic_rejection").replace("\n", " ")
            _log_skip_reason(setup_log_id, f"ts_reject:{reason_short}")
            alert_key = f"{setup_name}:{account_id}"
            now_t = time.time()
            if now_t - _failed_alert_last.get(alert_key, 0) >= _FAILED_ALERT_COOLDOWN_S:
                _failed_alert_last[alert_key] = now_t
                _alert(f"🚨 TS REJECTED atomic bracket: {setup_name}\n"
                       f"Account: {account_id}\n"
                       f"Side: {side} {qty} {MES_SYMBOL} @ ~{es_price:.2f}\n"
                       f"TS said: {err_text or '(no error text)'}")
            return
        if stop_oid is None:
            # Entry accepted but SL rejected — naked position, fall back to standalone SL POST
            print(f"[real-trader] atomic: entry OK ({entry_oid}) but SL rejected in group; "
                  f"placing standalone SL", flush=True)
            stop_payload = {
                "AccountID": account_id, "Symbol": MES_SYMBOL, "Quantity": str(qty),
                "OrderType": "StopMarket", "StopPrice": str(es_stop),
                "TradeAction": exit_side,
                "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
            }
            sr = _ts_api("POST", "/orderexecution/orders", stop_payload, account_id)
            so_ok, stop_oid = _order_ok(sr)
            if not so_ok:
                _alert(f"🚨 MANUAL INTERVENTION: {setup_name} atomic-entry placed "
                       f"(id={entry_oid}) but BOTH atomic SL AND fallback SL FAILED!\n"
                       f"Account: {account_id}")
                stop_oid = None
        # Atomic path success — skip the sequential path below
        order = {
            "setup_log_id": setup_log_id, "setup_name": setup_name,
            "direction": direction, "account_id": account_id,
            "entry_order_id": entry_oid, "target_order_id": t1_oid,
            "stop_order_id": stop_oid, "current_stop": es_stop,
            "target_price": es_target, "trail_only": trail_only,
            "status": "pending_entry", "fill_price": None,
            "max_favorable": 0.0, "be_triggered": False, "trail_active": False,
            "ts_placed": datetime.utcnow().isoformat(),
            "stop_pts": stop_pts, "target_pts": target_pts,
            "signal_es_price": es_price, "atomic_bracket": True,
            "quantity": qty,  # S149: stored so downstream poll/update/flatten know the size
        }
        with _lock:
            _active_orders[setup_log_id] = order
        _persist_order(setup_log_id)
        dir_str = "LONG" if is_long else "SHORT"
        tgt_str = "TRAIL-ONLY" if trail_only else f"{es_target:.2f}"
        _q_tag = f" [{qty}x]" if qty != QTY else ""
        print(f"[real-trader] PLACED ATOMIC: {setup_name} {dir_str} {qty} {MES_SYMBOL} "
              f"@ ~{es_price:.2f} target={tgt_str} stop={es_stop:.2f} "
              f"acct={account_id} ids=entry:{entry_oid}/stop:{stop_oid}/tgt:{t1_oid}",
              flush=True)
        dir_label = "Long" if is_long else "Short"
        _alert(f"🟢 {setup_name} PLACED [ATOMIC]{_q_tag}\n"
               f"{dir_label} {qty} MES @ ~{es_price:.2f}\n"
               f"Target: {tgt_str} | Stop: {es_stop:.2f}")
        return
    # === END ATOMIC PATH ===

    # === LEGACY SEQUENTIAL PATH (fallback when ATOMIC_BRACKET_ENABLED=false) ===
    # 1. Market entry
    entry_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(qty),
        "OrderType": "Market",
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    print(f"[real-trader] PLACING: {setup_name} {side} {qty} {MES_SYMBOL} "
          f"@ ~{es_price:.2f} on {account_id}", flush=True)
    resp = _ts_api("POST", "/orderexecution/orders", entry_payload, account_id)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        # TS rejection: capture the actual reason for S101 diagnostic + skip_reason.
        # Now that the bot-side margin pre-check is removed (2026-05-19), TS is the
        # sole authority on whether to fill — its rejection text tells us exactly
        # which constraint blocked us (margin, position limit, route, symbol, etc.).
        err_text = _extract_ts_error(resp)
        reason_short = (err_text[:80] or "unknown_rejection").replace("\n", " ")
        _log_skip_reason(setup_log_id, f"ts_reject:{reason_short}")
        # Rate-limit alerts per setup+account so a margin-cascade burst (5 signals in
        # 30s during a regime) doesn't spam Telegram with 5 near-identical alerts.
        alert_key = f"{setup_name}:{account_id}"
        now_t = time.time()
        last_t = _failed_alert_last.get(alert_key, 0)
        if now_t - last_t >= _FAILED_ALERT_COOLDOWN_S:
            _failed_alert_last[alert_key] = now_t
            _alert(f"🚨 TS REJECTED entry: {setup_name}\n"
                   f"Account: {account_id}\n"
                   f"Side: {side} {qty} {MES_SYMBOL} @ ~{es_price:.2f}\n"
                   f"TS said: {err_text or '(no error text)'}")
        else:
            print(f"[real-trader] TS reject for {setup_name} on {account_id} — "
                  f"alert suppressed (cooldown {_FAILED_ALERT_COOLDOWN_S}s). "
                  f"Reason: {err_text}", flush=True)
        return

    # 2. Stop order
    stop_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(qty),
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    stop_resp = _ts_api("POST", "/orderexecution/orders", stop_payload, account_id)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        stop_oid = None
        _alert(f"🚨 MANUAL INTERVENTION: {setup_name} entry placed "
               f"(id={entry_oid}) but STOP FAILED!\n"
               f"Account: {account_id}\n"
               f"Side: {side} {qty} {MES_SYMBOL} @ ~{es_price:.2f} Stop: {es_stop:.2f}")

    # 3. Target limit (skip for trail-only / Opt2 — saves margin, lets runners run)
    t1_oid = None
    if not trail_only:
        t1_payload = {
            "AccountID": account_id,
            "Symbol": MES_SYMBOL,
            "Quantity": str(qty),
            "OrderType": "Limit",
            "LimitPrice": str(es_target),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        t1_resp = _ts_api("POST", "/orderexecution/orders", t1_payload, account_id)
        t1_ok, t1_oid = _order_ok(t1_resp)
        if not t1_ok:
            t1_oid = None
            print(f"[real-trader] target limit skipped (margin?): {setup_name} "
                  f"target={es_target:.2f}", flush=True)

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "account_id": account_id,
        "entry_order_id": entry_oid,
        "target_order_id": t1_oid,
        "stop_order_id": stop_oid,
        "current_stop": es_stop,
        "target_price": es_target,
        "trail_only": trail_only,
        "status": "pending_entry",
        "fill_price": None,
        "max_favorable": 0.0,
        "be_triggered": False,
        "trail_active": False,
        "ts_placed": datetime.utcnow().isoformat(),
        # Stored for post-fill realign (slippage fix 2026-05-06): on fill detection,
        # poll_order_status() re-anchors stop+target to fill_price ± these distances.
        "stop_pts": stop_pts,
        "target_pts": target_pts,
        "signal_es_price": es_price,
        "quantity": qty,  # S149: stored so downstream poll/update/flatten know the size
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    dir_str = "LONG" if is_long else "SHORT"
    tgt_str = "TRAIL-ONLY" if trail_only else f"{es_target:.2f}"
    _q_tag = f" [{qty}x]" if qty != QTY else ""
    print(f"[real-trader] PLACED: {setup_name} {dir_str} {qty} {MES_SYMBOL} "
          f"@ ~{es_price:.2f} target={tgt_str} stop={es_stop:.2f} "
          f"acct={account_id} ids=entry:{entry_oid}/stop:{stop_oid}/tgt:{t1_oid}",
          flush=True)
    dir_label = "Long" if is_long else "Short"
    _alert(f"🟢 {setup_name} PLACED{_q_tag}\n"
           f"{dir_label} {qty} MES @ ~{es_price:.2f}\n"
           f"Target: {tgt_str} | Stop: {es_stop:.2f}")


def _place_limit_entry(setup_log_id, setup_name, direction, is_long,
                       account_id, es_price, stop_pts, target_pts,
                       limit_entry_price, qty: int = 1):
    """Charm S/R: place LIMIT entry only. Stop/target placed after fill (Phase 2)."""
    # Final safety check
    if not _validate_account_direction(account_id, is_long):
        return

    side = "Buy" if is_long else "Sell"
    limit_price = _round_mes(limit_entry_price)

    entry_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(qty),
        "OrderType": "Limit",
        "LimitPrice": str(limit_price),
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    print(f"[real-trader] PLACING LIMIT: {setup_name} {side} {qty} {MES_SYMBOL} "
          f"LIMIT @ {limit_price:.2f} on {account_id}", flush=True)
    resp = _ts_api("POST", "/orderexecution/orders", entry_payload, account_id)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"🚨 FAILED limit entry for {setup_name}\n"
               f"Account: {account_id}\n"
               f"Side: {side} {qty} {MES_SYMBOL} LIMIT @ {limit_price:.2f}")
        return

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "account_id": account_id,
        "entry_order_id": entry_oid,
        "target_order_id": None,
        "stop_order_id": None,
        "current_stop": None,
        "target_price": None,
        "status": "pending_limit",
        "fill_price": None,
        "max_favorable": 0.0,
        "be_triggered": False,
        "trail_active": False,
        "ts_placed": datetime.utcnow().isoformat(),
        "limit_entry_price": limit_price,
        "limit_placed_at": datetime.utcnow().isoformat(),
        "deferred_stop_pts": stop_pts,
        "deferred_target_pts": target_pts,
        "deferred_es_price": es_price,
        "quantity": qty,  # S149
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    dir_str = "LONG" if is_long else "SHORT"
    _q_tag = f" [{qty}x]" if qty != QTY else ""
    print(f"[real-trader] LIMIT placed: {setup_name} {dir_str} {qty} {MES_SYMBOL} "
          f"LIMIT @ {limit_price:.2f} (market was {es_price:.2f}) "
          f"acct={account_id} id={entry_oid}", flush=True)
    dir_label = "Long" if is_long else "Short"
    _alert(f"🟢 {setup_name} LIMIT entry{_q_tag}\n"
           f"{dir_label} {qty} MES LIMIT @ {limit_price:.2f}\n"
           f"[CHARM S/R] Waiting for fill (market @ {es_price:.2f})")


def _place_deferred_protective_orders(lid, order, fill_price):
    """Phase 2: place stop + target orders after limit entry fills."""
    is_long = order["direction"].lower() in ("long", "bullish")
    account_id = order["account_id"]
    exit_side = "Sell" if is_long else "Buy"
    stop_pts = order["deferred_stop_pts"]
    target_pts = order.get("deferred_target_pts")
    setup_name = order["setup_name"]
    qty = order.get("quantity") or QTY  # S149

    if is_long:
        es_stop = _round_mes(fill_price - stop_pts)
        es_target = _round_mes(fill_price + (target_pts if target_pts else FIRST_TARGET_PTS))
    else:
        es_stop = _round_mes(fill_price + stop_pts)
        es_target = _round_mes(fill_price - (target_pts if target_pts else FIRST_TARGET_PTS))

    # Final safety check
    if not _validate_account_direction(account_id, is_long):
        _alert(f"🚨 MANUAL INTERVENTION: {setup_name} limit FILLED "
               f"@ {fill_price:.2f} but direction validation FAILED!\n"
               f"Account: {account_id}")
        return

    # 1. Stop order
    stop_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(qty),
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    stop_resp = _ts_api("POST", "/orderexecution/orders", stop_payload, account_id)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        stop_oid = None
        _alert(f"🚨 MANUAL INTERVENTION: {setup_name} limit FILLED "
               f"@ {fill_price:.2f} but STOP FAILED!\n"
               f"Account: {account_id}")

    # 2. Target limit
    t1_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(qty),
        "OrderType": "Limit",
        "LimitPrice": str(es_target),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    t1_resp = _ts_api("POST", "/orderexecution/orders", t1_payload, account_id)
    t1_ok, t1_oid = _order_ok(t1_resp)
    if not t1_ok:
        t1_oid = None

    # Update order state
    with _lock:
        order["stop_order_id"] = stop_oid
        order["target_order_id"] = t1_oid
        order["current_stop"] = es_stop
        order["target_price"] = es_target
    _persist_order(lid)

    imp_pts = abs(fill_price - order.get("deferred_es_price", fill_price))
    print(f"[real-trader] DEFERRED orders placed: {setup_name} "
          f"stop={es_stop:.2f} target={es_target:.2f} "
          f"(entry improved {imp_pts:.1f}pts from market) acct={account_id}", flush=True)
    _alert(f"🟢 {setup_name} LIMIT FILLED @ {fill_price:.2f}\n"
           f"[CHARM S/R] Improved {imp_pts:+.1f}pts from market "
           f"({order.get('deferred_es_price', 0):.2f})\n"
           f"Stop: {es_stop:.2f} | Target: {es_target:.2f}")


# ====== TRAIL & CLOSE ======

def update_stop(setup_log_id: int, new_stop_price: float):
    """Update the stop order price (called when trail advances).

    Safety layers:
      L1 Pre-check side-of-market: if market has already crossed the trail level,
         the exit signal has fired — execute immediate market close instead of
         submitting a doomed modify that TS would reject (wiping the stop and
         leaving the position bare).
      L2 Post-check PUT response: if TS returns Error=FAILED on the replace,
         emergency close. TS replace-or-reject semantics mean the original
         stop is gone once the modify is rejected.
    """
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] != "filled":
            return
        old_stop = order["current_stop"]
        if old_stop is None:
            return
        # Skip trivial changes (< 1 MES tick)
        if abs(new_stop_price - old_stop) < MES_TICK_SIZE:
            return
        stop_oid = order["stop_order_id"]
        account_id = order["account_id"]
        setup_name = order["setup_name"]
        direction = order["direction"]

    if not stop_oid:
        return

    new_stop_price = _round_mes(new_stop_price)
    is_long = direction.lower() in ("long", "bullish")

    # Validate account before modifying order
    if not _validate_account_direction(account_id, is_long):
        return

    # LAYER 1: side-of-market validation.
    # For short: stop is a BUY STOP — must live ABOVE current market.
    # For long: stop is a SELL STOP — must live BELOW current market.
    # If market has bounced past the trail's target, the trail's intended exit
    # was already crossed. Execute that exit at current market price instead of
    # trying to modify to a now-invalid price.
    #
    # 2026-05-04: when live MES quote is unavailable (race right after entry,
    # before quote stream catches up), fall back to entry fill_price. This blocks
    # the "instant wrong-side fill" pattern that cost lid=2447/2449/2433 today —
    # portal cycle's pre-existing max_fav was triggering an initial realign that
    # placed the stop on the wrong side of fill_price.
    fill_price = order.get("fill_price")
    current_mes = _get_current_mes_price()
    ref_price = current_mes if current_mes is not None else fill_price
    ref_label = "market" if current_mes is not None else "fill"
    if ref_price is not None:
        SIDE_BUFFER = 0.5  # tolerate 2-tick race noise; real violations are >0.5pt
        wrong_side = False
        if is_long and new_stop_price >= ref_price - SIDE_BUFFER:
            wrong_side = True
            side_reason = (f"long trail {new_stop_price:.2f} >= {ref_label} "
                           f"{ref_price:.2f} (wrong side of entry)")
        elif not is_long and new_stop_price <= ref_price + SIDE_BUFFER:
            wrong_side = True
            side_reason = (f"short trail {new_stop_price:.2f} <= {ref_label} "
                           f"{ref_price:.2f} (wrong side of entry)")
        if wrong_side:
            print(f"[real-trader] WRONG-SIDE TRAIL id={setup_log_id}: "
                  f"{side_reason}", flush=True)
            # If quote was live (current_mes available) market has already
            # crossed the trail level → exit at market. If quote was stale
            # (fallback to fill_price) the trail is just trying to place a
            # bad initial realign → REJECT the modify, keep current stop alive.
            if current_mes is not None:
                _alert(f"⚠️ {setup_name} TRAIL-EXIT via market\n"
                       f"{side_reason}\n"
                       f"Closing at ~{current_mes:.2f}")
                close_trade(setup_log_id, "trail_market_exit")
            else:
                _alert(f"⚠️ {setup_name} stop modify BLOCKED\n"
                       f"{side_reason}\n"
                       f"Keeping current stop {old_stop:.2f}")
            return

    # ── S131 (2026-05-17): SPX-driven exit mode ──
    # Skip broker PUT — track internal trail only. Broker SL stays as safety net.
    # When SPX retraces past internal trail, update_trail() fires market exit
    # via close_trade("spx_trail_exit") pre-empting MES tick wicks.
    #
    # BUG 5 FIX (2026-05-18): post-fill INITIAL REALIGN must still PUT the broker.
    # The initial broker stop is placed at fill ± (stop_pts + SLIPPAGE_BUFFER=5pt)
    # to protect against fill slippage. After fill confirmation, the realign is
    # supposed to tighten the broker stop to fill ± stop_pts (removing the buffer).
    # Pre-fix: S131 skipped this realign → broker SL stayed +5pt wider → every
    # stop-out lost an extra $25 on a -19pt fill instead of -14pt intended.
    # Today (2026-05-18) cost: ~$50 across 2 stop-out trades (2937 + 2929).
    # Fix: detect "first realign after fill" and STILL push to broker; subsequent
    # trail updates skip PUT as designed.
    if _spx_exit_enabled():
        with _lock:
            first_realign = not order.get("initial_realign_done")
            if not first_realign:
                # Trail-mode update — S131 internal only
                order["current_stop"] = new_stop_price
            order["initial_realign_done"] = True
        if first_realign:
            # CRITICAL: remove SLIPPAGE_BUFFER from broker SL. Fall through to PUT.
            print(f"[real-trader] [S131] FIRST realign — pushing to broker to remove "
                  f"slippage buffer (id={setup_log_id}): {old_stop:.2f} -> {new_stop_price:.2f}",
                  flush=True)
            # DO NOT return — fall through to broker PUT below
        else:
            _persist_order(setup_log_id)
            print(f"[real-trader] [S131] internal trail tracked: id={setup_log_id} "
                  f"{old_stop:.2f} -> {new_stop_price:.2f} (broker SL stays as safety net)",
                  flush=True)
            if abs(new_stop_price - old_stop) >= 3.0:
                _alert(f"📍 {setup_name} S131 internal trail\n"
                       f"{old_stop:.2f} → {new_stop_price:.2f}\n"
                       f"Broker SL unchanged (safety net)")
            return

    replace_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(order.get("quantity") or QTY),  # S149
        "OrderType": "StopMarket",
        "StopPrice": str(new_stop_price),
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    resp = _ts_api("PUT", f"/orderexecution/orders/{stop_oid}", replace_payload, account_id)

    # LAYER 2: verify PUT was accepted. TS may return 200 with Error=FAILED at
    # order-level. A rejected replace wipes the original stop → position bare.
    if resp:
        resp_orders = resp.get("Orders", [])
        if resp_orders and resp_orders[0].get("Error") == "FAILED":
            err_msg = resp_orders[0].get("Message", "unknown")[:120]
            print(f"[real-trader] stop modify REJECTED id={setup_log_id} "
                  f"new={new_stop_price:.2f}: {err_msg}", flush=True)
            _alert(f"🚨 {setup_name} STOP MODIFY REJECTED\n"
                   f"{err_msg}\n"
                   f"Closing at market to avoid bare position...")
            close_trade(setup_log_id, "modify_rejected")
            return

        with _lock:
            order["current_stop"] = new_stop_price
            new_orders = resp.get("Orders", [])
            if new_orders and new_orders[0].get("OrderID"):
                order["stop_order_id"] = new_orders[0]["OrderID"]
            # Mark first post-entry realign consumed; all later updates alert normally
            first_realign = not order.get("initial_realign_done")
            order["initial_realign_done"] = True
        _persist_order(setup_log_id)
        print(f"[real-trader] stop updated: id={setup_log_id} "
              f"{old_stop:.2f} -> {new_stop_price:.2f} acct={account_id}"
              f"{' [initial-realign]' if first_realign else ''}", flush=True)
        # Suppress Telegram only for the first update after entry IF delta is small
        # (pure entry-slippage realign). A big first-cycle move (fast trail activation)
        # still alerts. Every subsequent update alerts regardless.
        small_first_realign = first_realign and abs(new_stop_price - old_stop) < 3.0
        if not small_first_realign:
            _alert(f"🔄 {setup_name} stop updated\n"
                   f"{old_stop:.2f} → {new_stop_price:.2f}")
    else:
        # Network/401/timeout. Existing stop may still be live — do NOT
        # emergency close. Just alert for manual review.
        _alert(f"🚨 MANUAL INTERVENTION: stop update FAILED (network)\n"
               f"Account: {account_id}\n"
               f"id={setup_log_id} old={old_stop:.2f} new={new_stop_price:.2f}")


def close_trade(setup_log_id: int, result_type: str):
    """Close a trade on outcome resolution.
    Cancel remaining orders + market close if position still open.
    NOTE: force_release() may have already set status='closed' to free the
    concurrent slot. We still need to do broker cleanup (cancel stop/target),
    so do NOT early-return on status=='closed'."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        already_closed = order["status"] == "closed"
        # 2026-05-14 Bug 2 fix: exclude this lid from reconcile's expected_qty
        # and ghost-stamp candidates during _flatten_position's 2-5sec window.
        # Without this, reconcile races and overwrites close_reason with
        # 'ghost_reconcile' for direct-close paths (trail_market_exit,
        # modify_rejected, stop_rejected_async, deferred close after fill).
        order["closing_in_progress"] = True

    setup_name = order["setup_name"]
    account_id = order["account_id"]

    # Flatten: cancel pending orders + market close (idempotent — safe to re-run)
    try:
        _flatten_position(order)
    finally:
        # Always clear the flag, even on exception, so reconcile can detect
        # genuine orphans/ghosts on next cycle.
        with _lock:
            order.pop("closing_in_progress", None)

    if not already_closed:
        with _lock:
            order["status"] = "closed"
            # 2026-05-06: was `result_type` (corrupts close_reason with WIN/LOSS/EXPIRED outcome).
            # Audit trail must reflect HOW closed (mechanism), not WHAT outcome.
            order["close_reason"] = f"outcome_close_{result_type.lower()}"
        _persist_order(setup_log_id)
        print(f"[real-trader] closed: {setup_name} id={setup_log_id} "
              f"result={result_type} acct={account_id}", flush=True)
        # Build polished alert matching STOP FILLED / EOD FLATTEN format:
        # direction + close fill + P&L + Day line. Falls back to bare format
        # if exit price unavailable (network glitch on _flatten_position).
        is_long = order["direction"].lower() in ("long", "bullish")
        dir_label = "Long" if is_long else "Short"
        close_fp = order.get("close_fill_price")
        entry_fp = order.get("fill_price")
        _qc = order.get("quantity") or QTY  # S149
        pnl = None
        if close_fp is not None and entry_fp is not None:
            try:
                if is_long:
                    pnl = (float(close_fp) - float(entry_fp)) * MES_POINT_VALUE * _qc
                else:
                    pnl = (float(entry_fp) - float(close_fp)) * MES_POINT_VALUE * _qc
            except (TypeError, ValueError):
                pnl = None
        pnl_str = f"${pnl:+.2f}" if pnl is not None else "n/a"
        # S193 (2026-05-29): derive label from broker P&L sign, not outcome tracker.
        # lid 3388: outcome tracker said WIN (SPX hit target +6.28pt) but MES basis
        # drifted -14.5pt during trade life, MES P&L was -$7.50 → "WIN P&L $-7.50"
        # confused the user. Honest label = sign of actual broker P&L.
        if pnl is not None and pnl > 0.5:
            _hdr_label = "WIN"
        elif pnl is not None and pnl < -0.5:
            _hdr_label = "LOSS"
        elif pnl is not None:
            _hdr_label = "SCRATCH"
        else:
            _hdr_label = result_type  # fallback when pnl unavailable
        if close_fp is not None:
            _alert(f"🏁 {setup_name} CLOSED: {_hdr_label}\n"
                   f"{dir_label} {_qc} MES @ {close_fp}\n"
                   f"P&L: {pnl_str}"
                   f"{_day_line(account_id)}")
        else:
            _alert(f"🏁 {setup_name} CLOSED: {_hdr_label}{_day_line(account_id)}")
    else:
        # 2026-05-14 fix: _flatten_position may have set close_fill_price in
        # memory (line ~1100). force_release already persisted with the older
        # state (no close_fill_price). Persist now so daily reconcile can match
        # real broker fills. Without this, every outcome_resolved_* trade lands
        # in DB with close_fill_price=None → NO_EXIT in reconcile.
        _persist_order(setup_log_id)
        print(f"[real-trader] broker cleanup done (slot already released): "
              f"{setup_name} id={setup_log_id} acct={account_id}", flush=True)
        # 2026-05-28 fix: per-trade close Telegram for outcome-resolved trades.
        # Pre-fix: force_release set status='closed' BEFORE close_trade, so this
        # ELSE branch ran silently. Result: every outcome_close_win/loss/expired
        # trade closed mid-day with NO Telegram (only EOD/stop-fill paths alerted).
        # Send same polished alert as the !already_closed branch. Dedup: skip if
        # close_reason indicates the broker-fill path already alerted (stop_filled,
        # target_filled, eod_flatten, stop_filled_race_caught).
        _existing_reason = order.get("close_reason") or ""
        _already_alerted_paths = (
            "stop_filled", "stop_filled_race_caught", "eod_flatten"
        )
        if _existing_reason in _already_alerted_paths or order.get("close_telegram_sent"):
            pass
        else:
            is_long = order["direction"].lower() in ("long", "bullish")
            dir_label = "Long" if is_long else "Short"
            close_fp = order.get("close_fill_price") or order.get("stop_fill_price")
            entry_fp = order.get("fill_price")
            _qc = order.get("quantity") or QTY
            pnl = None
            if close_fp is not None and entry_fp is not None:
                try:
                    if is_long:
                        pnl = (float(close_fp) - float(entry_fp)) * MES_POINT_VALUE * _qc
                    else:
                        pnl = (float(entry_fp) - float(close_fp)) * MES_POINT_VALUE * _qc
                except (TypeError, ValueError):
                    pnl = None
            pnl_str = f"${pnl:+.2f}" if pnl is not None else "n/a"
            # S193 (2026-05-29): derive label from broker P&L sign, not from
            # close_reason (which inherits outcome tracker's SPX-side verdict).
            # See lid 3388 for the case: WIN tag + $-7.50 P&L = user confusion.
            if pnl is not None and pnl > 0.5:
                result_label = "WIN"
            elif pnl is not None and pnl < -0.5:
                result_label = "LOSS"
            elif pnl is not None:
                result_label = "SCRATCH"
            else:
                # No fill prices — fall back to close_reason / result_type
                cr = order.get("close_reason", "") or ""
                if cr.startswith("outcome_resolved_") or cr.startswith("outcome_close_"):
                    result_label = cr.split("_")[-1].upper()
                else:
                    result_label = result_type
            if close_fp is not None:
                _alert(f"🏁 {setup_name} CLOSED: {result_label}\n"
                       f"{dir_label} {_qc} MES @ {close_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(account_id)}")
            else:
                _alert(f"🏁 {setup_name} CLOSED: {result_label}{_day_line(account_id)}")
            with _lock:
                order["close_telegram_sent"] = True
            _persist_order(setup_log_id)


def force_release(setup_log_id: int, result_type: str):
    """Immediately free the concurrent slot for a resolved trade.

    Called DIRECTLY (not via _broker_submit) by outcome tracker to guarantee
    the slot is freed. close_trade() is still called in background for broker
    cleanup, but this ensures MAX_CONCURRENT_PER_DIR is not blocked if
    close_trade fails or is delayed.

    Bug 2026-04-06: #1559 close_trade via _broker_submit silently failed.
    _active_orders kept #1559 as 'filled', blocking 7 subsequent SC shorts
    (+45 pts missed).
    """
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] == "closed":
            return
        # S99 Phase 2B (2026-05-09): if pending_entry, DEFER status change until
        # fill_poll catches the FLL. Today's lid=2646: outcome tracker decided WIN
        # within seconds of placement, set status=closed before bot saw FLL,
        # _flatten_position skipped (status not filled), broker position drifted
        # until orphan reconcile force-closed it (lost close_fill_price).
        # Defer keeps cap slot held (status remains pending_entry, counted in cap)
        # until poll_order_status detects FLL — then triggers proper close.
        if order["status"] == "pending_entry":
            order["pending_close_after_fill"] = result_type
            try:
                _persist_order(setup_log_id)
            except Exception as e:
                print(f"[real-trader] force_release persist (deferred) error: {e}", flush=True)
            print(f"[real-trader] force_release DEFERRED: id={setup_log_id} "
                  f"pending_entry — will close on fill poll (result={result_type})", flush=True)
            return
        old_status = order["status"]
        order["status"] = "closed"
        # 2026-05-06: was `result_type` (e.g., "LOSS"). Use descriptive label so
        # close_reason audit trail reflects HOW (slot release on outcome) not WHAT.
        order["close_reason"] = f"outcome_resolved_{result_type.lower()}"
    try:
        _persist_order(setup_log_id)
    except Exception as e:
        print(f"[real-trader] force_release persist error: {e}", flush=True)
    print(f"[real-trader] force_release: id={setup_log_id} {old_status}->{result_type}", flush=True)


def _flatten_position(order):
    """Market close remaining position + cancel all pending orders."""
    account_id = order["account_id"]
    is_long = order["direction"].lower() in ("long", "bullish")
    close_side = "Sell" if is_long else "Buy"

    # Validate before any order modification
    if not _validate_account_direction(account_id, is_long):
        _alert(f"🚨 CRITICAL: Cannot flatten -- direction validation failed!\n"
               f"Account: {account_id} | {order.get('setup_name')}")
        return

    # Cancel pending stop and target orders FIRST
    for oid_key in ("stop_order_id", "target_order_id"):
        oid = order.get(oid_key)
        if oid:
            _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)

    # Cancel pending limit entry if not yet filled
    if order.get("status") == "pending_limit" and order.get("entry_order_id"):
        _ts_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None, account_id)
        print(f"[real-trader] cancelled pending limit entry: {order['setup_name']} "
              f"acct={account_id}", flush=True)
        _alert(f"🏁 {order['setup_name']} limit entry cancelled")
        return

    # BUG 2 FIX: cancel pending VIX Div stop-entry if outcome resolves before fill
    if order.get("status") == "pending_stop_entry" and order.get("entry_order_id"):
        _ts_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None, account_id)
        print(f"[real-trader] cancelled pending stop-entry: {order['setup_name']} "
              f"acct={account_id}", flush=True)
        _alert(f"🏁 {order['setup_name']} stop-entry cancelled (outcome resolved before confirm)")
        return

    # S99 Phase 2A (2026-05-09): handle pending_entry status. Today's lid=2646:
    # outcome tracker fired before fill polled, force_release set status=closed,
    # _flatten_position skipped because old check `if status == "filled"` was FALSE,
    # but broker had filled the entry. Result: ghost position, orphan reconcile
    # had to clean up at -$13 worse than would-have-been.
    # Fix: cancel entry order if pending_entry, then ALWAYS check broker position
    # (regardless of bot status). If broker has matching position, market-close.
    if order.get("status") == "pending_entry" and order.get("entry_order_id"):
        try:
            _ts_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None, account_id)
            print(f"[real-trader] cancelled pending market entry: {order.get('setup_name')} "
                  f"id={order.get('setup_log_id')} acct={account_id}", flush=True)
        except Exception as e:
            print(f"[real-trader] entry cancel error: {e}", flush=True)
        time.sleep(0.5)  # let cancel settle before broker position check

    # Wait for cancellations to settle
    time.sleep(0.5)

    # Market close if position exists at broker.
    # S99: was `if order["status"] == "filled":` — too restrictive, missed ghosts
    # where bot status was "closed" (force_release-d) but broker had position.
    # Now: always check broker, market-close if position matches direction.
    if order["status"] in ("filled", "pending_entry", "closed"):
        # BUG 3b FIX (2026-05-18): before checking position, verify our stop didn't
        # already fill. Race condition on lid 2935 today: bot fired Market Sell
        # at 12:27:04 even though broker StopMarket already filled at 12:26:54 →
        # opened unintended short by mistake. If stop_order_id shows FLL,
        # broker already closed — backfill the fill price and skip the close.
        stop_oid = order.get("stop_order_id")
        if stop_oid:
            stop_fp = _get_order_fill_price(stop_oid, account_id)
            if stop_fp is not None:
                order["stop_fill_price"] = stop_fp
                order["close_fill_price"] = stop_fp  # for portal/reconcile
                order["close_reason"] = order.get("close_reason") or "stop_filled_race_caught"
                print(f"[real-trader] flatten SKIPPED: stop_order_id={stop_oid} already FLL "
                      f"at {stop_fp} — broker beat us to it (race caught). "
                      f"{order.get('setup_name')} acct={account_id}", flush=True)
                # Cancel any other pending orders (target etc) and return
                for oid_key in ("target_order_id",):
                    oid = order.get(oid_key)
                    if oid:
                        try:
                            _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
                        except Exception:
                            pass
                return

        # Check broker position first -- don't create ghost positions.
        # 2026-05-27 fix: opt into retry path. Bot expected a position here
        # (status was filled/closed/pending_entry to reach this branch). False-
        # flat reads from TS API hiccups were causing silent ORPHAN bugs
        # (lid=3211 today: bot bailed, broker held position 5h29 until
        # reconciler caught it). expect_position=True retries up to 3× / 1s gap.
        broker_pos = _get_broker_position(account_id, expect_position=True)
        if not broker_pos:
            # 2026-05-27: alert on unexpected flat read so we have visibility on
            # how often the retry path saves us vs how often broker truly was
            # already flat (stop/target filled between our DELETEs and this check).
            print(f"[real-trader] flatten SKIPPED: broker reads flat on {account_id} "
                  f"after retries. {order.get('setup_name')} id={order.get('setup_log_id')} "
                  f"status_was={order.get('status')}. Reconciler will catch if false-flat.",
                  flush=True)
            _alert(f"ℹ️ Flatten skipped: {account_id} reads flat\n"
                   f"{order.get('setup_name')} id={order.get('setup_log_id')} "
                   f"(status_was={order.get('status')})\n"
                   f"If position actually exists, reconciler will catch within 30s.")
            return

        # Verify direction matches
        broker_is_long = broker_pos["long_short"] == "Long"
        if broker_is_long != is_long:
            print(f"[real-trader] flatten SKIPPED: direction mismatch on {account_id}! "
                  f"Expected={'Long' if is_long else 'Short'} "
                  f"Actual={broker_pos['long_short']} qty={broker_pos['qty']}", flush=True)
            _alert(f"⚠️ POSITION MISMATCH on {account_id}\n"
                   f"Expected: {'Long' if is_long else 'Short'}\n"
                   f"Broker: {broker_pos['long_short']} {broker_pos['qty']}\n"
                   f"MANUAL REVIEW NEEDED")
            return

        # S149: respect tracked order qty (could be 2 for SC long BOFA-PURE align=+1)
        _intended_qty = order.get("quantity") or QTY
        close_qty = min(_intended_qty, broker_pos["qty"])
        close_payload = {
            "AccountID": account_id,
            "Symbol": MES_SYMBOL,
            "Quantity": str(close_qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _ts_api("POST", "/orderexecution/orders", close_payload, account_id)
        if resp:
            # Check for rejection
            orders_list = resp.get("Orders", [])
            if orders_list and orders_list[0].get("Error") == "FAILED":
                msg = orders_list[0].get("Message", "")
                _alert(f"🚨 FLATTEN REJECTED on {account_id}\n"
                       f"{order['setup_name']} qty={close_qty}\n"
                       f"Error: {msg}\nMANUAL CLOSE REQUIRED")
                return
            # Capture fill price
            close_oid = None
            if orders_list:
                close_oid = orders_list[0].get("OrderID")
            if close_oid:
                # 2026-05-14 Bug 3 fix: store close OID so _backfill_ghost_fill
                # can recover the fill price if reconcile/restart wipes the
                # in-memory close_fill_price before persistence.
                order["close_order_id"] = close_oid
                time.sleep(1)
                close_fp = _get_order_fill_price(close_oid, account_id)
                if close_fp:
                    # S166 (2026-05-21): sanity-check captured close_fill_price.
                    # Bug observed on lid=3033 (2026-05-20): TS API returned 7418.75
                    # as close fill for a "loss" market-close on a LONG entered at
                    # 7378.75 — i.e. broker fill +40 pts ABOVE entry, which is
                    # physically impossible for the LOSS path (stop was 7370.75).
                    # Root cause unverified but suspect TS API stale-read or
                    # wrong-order fill on the close OID.
                    # Guard: reject any close_fill_price whose distance from entry
                    # exceeds MAX_SANE_CLOSE_DIST_PTS (30) — generous enough to
                    # accept all real trail-giveback and target hits, tight enough
                    # to catch +40 anomalies. Reject → leave close_fill_price unset
                    # so _backfill_ghost_fill recovers via historicalorders scan.
                    MAX_SANE_CLOSE_DIST_PTS = 30.0
                    entry_fill = order.get("fill_price")
                    sane = True
                    if entry_fill and entry_fill > 0:
                        diff = abs(close_fp - entry_fill)
                        if diff > MAX_SANE_CLOSE_DIST_PTS:
                            sane = False
                            print(f"[real-trader] WARN close_fill_price SANITY FAIL "
                                  f"lid={order.get('setup_log_id')}: entry={entry_fill} "
                                  f"close_fp={close_fp} |diff|={diff:.2f} > "
                                  f"max={MAX_SANE_CLOSE_DIST_PTS}. NOT storing — "
                                  f"_backfill_ghost_fill will retry via historicalorders.",
                                  flush=True)
                            _alert(f"⚠️ Close fill sanity fail lid={order.get('setup_log_id')}\n"
                                   f"Entry={entry_fill} Close_FP={close_fp} |diff|={diff:.2f}pt\n"
                                   f"Skipping store — backfill will retry.")
                    if sane:
                        order["close_fill_price"] = close_fp
            print(f"[real-trader] flattened: {order['setup_name']} qty={close_qty} "
                  f"fill={order.get('close_fill_price')} acct={account_id}", flush=True)
        else:
            _alert(f"🚨 FLATTEN FAILED on {account_id}\n"
                   f"{order['setup_name']} id={order['setup_log_id']} qty={close_qty}\n"
                   f"MANUAL CLOSE REQUIRED")


# ====== POLL ORDER STATUS ======

def poll_order_status():
    """Check order fills via TS API. Called each ~30s cycle."""
    if not (LONGS_ENABLED or SHORTS_ENABLED):
        return
    with _lock:
        if not _active_orders:
            return
        pending = [(lid, o) for lid, o in _active_orders.items()
                   if o["status"] in ("pending_entry", "pending_limit", "pending_stop_entry", "filled")]
    if not pending:
        return

    # Group by account to minimize API calls
    by_account: dict[str, list] = {}
    for lid, o in pending:
        acct = o.get("account_id", "")
        if acct:
            by_account.setdefault(acct, []).append((lid, o))

    for account_id, order_list in by_account.items():
        if account_id not in ACCOUNT_WHITELIST:
            continue
        try:
            orders_data = _ts_api("GET",
                f"/brokerage/accounts/{account_id}/orders", None, account_id)
        except Exception as e:
            print(f"[real-trader] poll error for {account_id}: {e}", flush=True)
            continue
        if not orders_data:
            continue

        broker_orders = {}
        for o in orders_data.get("Orders", []):
            oid = o.get("OrderID")
            if oid:
                broker_orders[oid] = o

        for lid, order in order_list:
            _check_order_fills(lid, order, broker_orders)

    # Note: position reconciliation was previously throttled inside this
    # function but early-exited above when bot state was empty (leaving a
    # 5-min exposure window covered only by periodic_orphan_check). It is
    # now driven by a dedicated 30s scheduler job calling reconcile_positions()
    # which runs regardless of tracked-order state.


def reconcile_positions():
    """Public entry point for the 30s reconcile scheduler.

    Runs regardless of whether bot state is empty — catches the case where
    force_release has marked a trade closed but broker still holds position
    (e.g., when a stop modify was rejected async and the original stop wiped).
    See #2018, #2031 on 2026-04-21."""
    if not (LONGS_ENABLED or SHORTS_ENABLED) or not _get_token:
        return
    try:
        _reconcile_positions()
    except Exception as e:
        print(f"[real-trader] reconcile_positions error: {e}", flush=True)


def _reconcile_positions():
    """Check broker positions match tracked orders. Alert on mismatch.

    S99 (2026-05-09 fix): expected_qty now includes both pending_entry and filled
    statuses. Previously only counted filled, causing false ORPHAN alerts during
    the 30s window between PLACED (status=pending_entry) and fill-poll catching
    the FLL. Today's lid=2644 + 2646 both triggered false orphan-close attempts
    because reconcile fired before fill_poll updated status. Race surface scales
    linearly with cap (cap=2 = 2 concurrent windows; cap=3 = 3x exposure).
    """
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        # Count expected qty from tracked orders (includes pending_entry to avoid
        # false orphan during fill-poll lag). Aligns with cap-check (line 370).
        with _lock:
            # 2026-05-14 Bug 2 fix: exclude orders currently in close_trade's
            # _flatten_position window (closing_in_progress=True) — they will
            # naturally transition to closed within seconds; counting them as
            # expected leads to false GHOST stamps during the close race.
            expected_qty = sum(
                QTY for o in _active_orders.values()
                if o.get("account_id") == acct_id
                and o["status"] in ("pending_entry", "filled")
                and not o.get("closing_in_progress")
            )
        # Query broker
        broker_pos = _get_broker_position(acct_id)
        broker_qty = broker_pos["qty"] if broker_pos else 0
        if broker_qty != expected_qty:
            print(f"[real-trader] RECONCILE MISMATCH on {acct_id}: "
                  f"expected={expected_qty} broker={broker_qty}", flush=True)
            if broker_qty > 0 and expected_qty == 0:
                # Orphan: broker has position we don't track
                _alert(f"⚠️ POSITION MISMATCH on {acct_id}\n"
                       f"Expected: {expected_qty} MES\n"
                       f"Broker: {broker_qty} MES\n"
                       f"ORPHAN detected -- auto-closing")
                _close_broker_orphans(acct_id, source="RECONCILE")
            elif broker_qty == 0 and expected_qty > 0:
                # Ghost: we think we have position but broker doesn't
                # Before marking closed, try to recover the actual fill price
                # from broker order history so P&L accounting stays accurate.
                _ghost_ids = []
                _backfilled = []
                _lost_price = []
                with _lock:
                    # S99: include pending_entry (entry placed, broker may have rejected/expired
                    # but bot hasn't polled yet — we count this as ghost candidate).
                    # 2026-05-14 Bug 2 fix: skip closing_in_progress lids — the active
                    # close_trade will persist close_fill_price and the proper close_reason
                    # within seconds.
                    _to_process = [
                        (lid, o) for lid, o in _active_orders.items()
                        if o.get("account_id") == acct_id
                        and o["status"] in ("pending_entry", "filled")
                        and not o.get("closing_in_progress")
                    ]
                # Release lock while querying broker history (network call)
                for lid, o in _to_process:
                    backfill = _backfill_ghost_fill(o)
                    with _lock:
                        if backfill:
                            field, price = backfill
                            o[field] = price
                            _backfilled.append((lid, field, price))
                        else:
                            _lost_price.append(lid)
                        o["status"] = "closed"
                        o["close_reason"] = "ghost_reconcile"
                        _ghost_ids.append(o["setup_log_id"])
                for _gid in _ghost_ids:
                    _persist_order(_gid)
                # Alert with backfill detail — tells user if accounting is accurate or not
                _msg = (f"⚠️ GHOST POSITION on {acct_id}\n"
                        f"Expected: {expected_qty} MES · Broker: FLAT\n"
                        f"Marked {len(_ghost_ids)} closed")
                if _backfilled:
                    _msg += f"\n✅ Recovered {len(_backfilled)} fill price(s): "
                    _msg += ", ".join(f"lid={l} {f}={p}" for l, f, p in _backfilled)
                if _lost_price:
                    _msg += f"\n🚨 Could not recover fill for {len(_lost_price)} trade(s): {_lost_price}"
                _alert(_msg)
                print(f"[real-trader] ghost_reconcile {acct_id}: backfilled={_backfilled} lost={_lost_price}", flush=True)
            elif broker_qty != expected_qty:
                # Qty mismatch
                _alert(f"⚠️ QTY MISMATCH on {acct_id}\n"
                       f"Expected: {expected_qty} MES\n"
                       f"Broker: {broker_qty} MES\n"
                       f"Check manually")


def _check_order_fills(lid, order, broker_orders):
    """Check individual order fills and update state."""
    changed = False
    account_id = order.get("account_id", "")

    # Check pending limit entry (Phase 2: deferred stop/target)
    if order["status"] == "pending_limit" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        entry_status = entry.get("Status", "")
        if entry_status == "FLL":
            fill_price = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            _place_deferred_protective_orders(lid, order, fill_price)
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = f"limit_{entry_status}"
            changed = True
            print(f"[real-trader] limit entry {entry_status}: {order['setup_name']} "
                  f"acct={account_id}", flush=True)
            _alert(f"⚠️ {order['setup_name']} LIMIT {entry_status}\n"
                   f"[CHARM S/R] Entry not filled -- trade skipped")
        else:
            # Check timeout (30 min)
            placed_at = order.get("limit_placed_at")
            if placed_at:
                try:
                    placed_dt = datetime.fromisoformat(placed_at)
                    if placed_dt.tzinfo is None:
                        placed_dt = placed_dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
                    elapsed = (datetime.utcnow() - placed_dt.replace(tzinfo=None)).total_seconds()
                    if elapsed > _LIMIT_ENTRY_TIMEOUT_S:
                        _ts_api("DELETE",
                                f"/orderexecution/orders/{order['entry_order_id']}",
                                None, account_id)
                        with _lock:
                            order["status"] = "closed"
                            order["close_reason"] = "limit_timeout"
                        changed = True
                        print(f"[real-trader] LIMIT TIMEOUT: {order['setup_name']} cancelled "
                              f"after {elapsed/60:.0f} min acct={account_id}", flush=True)
                        _alert(f"🏁 {order['setup_name']} LIMIT EXPIRED\n"
                               f"[CHARM S/R] {order.get('limit_entry_price', 0):.2f} not reached "
                               f"in {elapsed/60:.0f} min -- trade skipped")
                except (ValueError, TypeError):
                    pass

    # BUG 2 FIX (2026-05-18): Check VIX Div stop-entry fill or timeout.
    # If filled, transition to "filled" and place SL (trail-only setup — no target).
    if order["status"] == "pending_stop_entry" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        entry_status = entry.get("Status", "")
        if entry_status == "FLL":
            fill_price = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            print(f"[real-trader] STOP-ENTRY CONFIRMED: {order['setup_name']} "
                  f"@ {fill_price} (trigger {order.get('trigger_price')}) acct={account_id}",
                  flush=True)
            qty_se = order.get("quantity") or QTY  # S149
            _alert(f"🟢 {order['setup_name']} STOP-ENTRY CONFIRMED\n"
                   f"{order['direction'].upper()} {qty_se} MES @ {fill_price}\n"
                   f"Now placing protective SL...")
            # Place protective SL only (VIX Div is trail-only — no target limit order).
            # AUDIT FIX (2026-05-18): can't reuse _place_deferred_protective_orders because
            # it reads `deferred_stop_pts` (limit-entry-specific) and always places a target.
            # Inline the SL placement instead, matching trail-only setups.
            is_long_se = order["direction"].lower() in ("long", "bullish")
            exit_side_se = "Sell" if is_long_se else "Buy"
            stop_pts_se = order.get("stop_pts") or 8.0
            es_stop_se = _round_mes((fill_price - stop_pts_se) if is_long_se else (fill_price + stop_pts_se))
            stop_payload = {
                "AccountID": account_id, "Symbol": MES_SYMBOL, "Quantity": str(qty_se),
                "OrderType": "StopMarket", "StopPrice": str(es_stop_se),
                "TradeAction": exit_side_se,
                "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
            }
            stop_resp = _ts_api("POST", "/orderexecution/orders", stop_payload, account_id)
            stop_ok, stop_oid = _order_ok(stop_resp)
            with _lock:
                order["stop_order_id"] = stop_oid if stop_ok else None
                order["current_stop"] = es_stop_se
                order["initial_realign_done"] = True  # placed at fill ± stop_pts, no buffer needed
            if stop_ok:
                print(f"[real-trader] STOP-ENTRY protective SL placed: {order['setup_name']} "
                      f"stop={es_stop_se:.2f} acct={account_id}", flush=True)
            else:
                _alert(f"🚨 MANUAL INTERVENTION: {order['setup_name']} stop-entry FILLED "
                       f"@ {fill_price} but STOP FAILED!\nAccount: {account_id}")
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = f"stop_entry_{entry_status}"
            changed = True
            print(f"[real-trader] stop-entry {entry_status}: {order['setup_name']} "
                  f"acct={account_id}", flush=True)
            _alert(f"⚠️ {order['setup_name']} STOP-ENTRY {entry_status}")
        else:
            # Check 30-min timeout
            timeout_at = order.get("stop_entry_timeout_at")
            if timeout_at:
                try:
                    from datetime import datetime as _dt, timezone as _utc
                    timeout_dt = _dt.fromisoformat(timeout_at)
                    if _dt.now(_utc.utc) > timeout_dt:
                        # Cancel the stop-entry order
                        try:
                            _ts_api("DELETE",
                                    f"/orderexecution/orders/{order['entry_order_id']}",
                                    None, account_id)
                        except Exception as e:
                            print(f"[real-trader] stop-entry cancel error: {e}", flush=True)
                        with _lock:
                            order["status"] = "closed"
                            order["close_reason"] = "stop_entry_timeout"
                        changed = True
                        _alert(f"🏁 {order['setup_name']} STOP-ENTRY EXPIRED\n"
                               f"Trigger {order.get('trigger_price')} not hit in "
                               f"30min — confirmation never arrived, trade skipped.")
                except (ValueError, TypeError):
                    pass
        if changed:
            _persist_order(lid)
        return  # exit early — no other status checks apply

    # Check market entry fill
    if order["status"] == "pending_entry" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        entry_status = entry.get("Status", "")
        if entry_status == "FLL":
            fill_price = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            _qf = order.get("quantity") or QTY  # S149
            print(f"[real-trader] FILLED: {order['setup_name']} "
                  f"{_qf} {MES_SYMBOL} @ {fill_price} acct={account_id}", flush=True)
            dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
            _alert(f"🟢 {order['setup_name']} FILLED\n"
                   f"{dir_label} {_qf} MES @ {fill_price}\n"
                   f"Target: {order.get('target_price', 0):.2f} | "
                   f"Stop: {order['current_stop']:.2f}")

            # Slippage fix (2026-05-06): re-anchor stop+target to fill_price ± distances
            # (initial protective orders were placed at signal_price ± distances + 5pt
            # buffer for the stop). Today's lid=2530: signal 7347, fill 7354.5 (7.5pt
            # against), stop placed at signal+8=7355 = instant stop. Now stop is
            # placed at signal+8+5=7360 then realigned here to fill+8=7362.5.
            #
            # 2026-05-12 verification (lid=2707 VIX Div, fill==signal zero-slippage):
            # Realign block called update_stop() but state never advanced from initial
            # buffered stop. Root cause inferred (logs rotated): silent failure in
            # update_stop (network/timeout or stale broker_orders snapshot returning
            # FAILED). Stop then fired at the un-realigned 7363.75 instead of 7368.75,
            # turning a -$40 loss into -$65. Fix: verify current_stop advanced, retry
            # once on mismatch, alert loudly on persistent failure.
            try:
                stop_pts_local = order.get("stop_pts")
                target_pts_local = order.get("target_pts")
                is_long_local = order["direction"].lower() in ("long", "bullish")
                # Realign stop
                if stop_pts_local is not None and fill_price is not None:
                    desired_stop = (fill_price - stop_pts_local) if is_long_local else (fill_price + stop_pts_local)
                    desired_stop = _round_mes(desired_stop)
                    pre_realign_stop = order["current_stop"]
                    if abs(desired_stop - pre_realign_stop) >= MES_TICK_SIZE:
                        print(f"[real-trader] post-fill realign STOP id={lid}: "
                              f"{pre_realign_stop:.2f} -> {desired_stop:.2f} "
                              f"(fill={fill_price}, stop_pts={stop_pts_local})", flush=True)
                        update_stop(lid, desired_stop)
                        # Verify the realign actually advanced the in-memory stop.
                        # If status flipped to "closed" (wrong-side close path), skip
                        # verification — that's a legitimate exit, not a silent failure.
                        with _lock:
                            post_stop = order.get("current_stop")
                            post_status = order.get("status")
                        if post_status == "filled" and post_stop is not None \
                                and abs(post_stop - desired_stop) >= MES_TICK_SIZE:
                            # Silent failure: update_stop returned without advancing
                            # state (network None, wrong-side-with-no-quote alert path,
                            # or other non-close failure). Retry once before alerting.
                            print(f"[real-trader] post-fill realign VERIFY FAILED id={lid}: "
                                  f"current_stop={post_stop:.2f} desired={desired_stop:.2f} "
                                  f"— retrying", flush=True)
                            update_stop(lid, desired_stop)
                            with _lock:
                                post_stop = order.get("current_stop")
                                post_status = order.get("status")
                            if post_status == "filled" and post_stop is not None \
                                    and abs(post_stop - desired_stop) >= MES_TICK_SIZE:
                                # Persistent failure — alert loudly. Stop still sits
                                # at the buffered level (=initial + SLIPPAGE_BUFFER
                                # extra risk). Trader needs to know so they can
                                # manually flatten or adjust if desired.
                                extra_risk = abs(post_stop - desired_stop)
                                _qrf = order.get("quantity") or QTY  # S149
                                _alert(
                                    f"🚨 {order['setup_name']} REALIGN FAILED id={lid}\n"
                                    f"Stop still at {post_stop:.2f} (wanted {desired_stop:.2f})\n"
                                    f"Extra risk: {extra_risk:.1f} pts (~${extra_risk * MES_POINT_VALUE * _qrf:.2f})\n"
                                    f"Position open — manual review if needed"
                                )
                                print(f"[real-trader] post-fill realign PERSISTENT FAILURE id={lid}: "
                                      f"locked-in extra risk {extra_risk:.2f} pts", flush=True)
                # Realign target (if has target limit, not trail-only)
                if (target_pts_local is not None and fill_price is not None
                        and order.get("target_order_id")
                        and not order.get("trail_only")):
                    desired_target = (fill_price + target_pts_local) if is_long_local else (fill_price - target_pts_local)
                    desired_target = _round_mes(desired_target)
                    cur_target = order.get("target_price")
                    if cur_target is not None and abs(desired_target - cur_target) >= MES_TICK_SIZE:
                        replace_payload = {
                            "AccountID": account_id,
                            "Symbol": MES_SYMBOL,
                            "Quantity": str(order.get("quantity") or QTY),  # S149
                            "OrderType": "Limit",
                            "LimitPrice": str(desired_target),
                            "TimeInForce": {"Duration": "DAY"},
                            "Route": "Intelligent",
                        }
                        tgt_resp = _ts_api("PUT", f"/orderexecution/orders/{order['target_order_id']}",
                                           replace_payload, account_id)
                        if tgt_resp:
                            with _lock:
                                order["target_price"] = desired_target
                                new_orders = tgt_resp.get("Orders", [])
                                if new_orders and new_orders[0].get("OrderID"):
                                    order["target_order_id"] = new_orders[0]["OrderID"]
                            print(f"[real-trader] post-fill realign TARGET id={lid}: "
                                  f"{cur_target:.2f} -> {desired_target:.2f}", flush=True)
            except Exception as exc:
                print(f"[real-trader] post-fill realign error id={lid}: {exc}", flush=True)

            # S99 Phase 2B (2026-05-09): if outcome tracker fired force_release
            # while we were in pending_entry, it left a `pending_close_after_fill`
            # marker. Now that fill is captured (fill_price recorded above),
            # trigger the deferred close. close_trade can now flatten properly
            # because status="filled" and broker has matching position.
            pending_close = order.get("pending_close_after_fill")
            if pending_close:
                print(f"[real-trader] DEFERRED CLOSE firing: id={lid} "
                      f"result={pending_close} (was pending_entry, now filled)", flush=True)
                with _lock:
                    order.pop("pending_close_after_fill", None)
                try:
                    force_release(lid, pending_close)
                    close_trade(lid, pending_close)
                except Exception as exc:
                    print(f"[real-trader] deferred close error id={lid}: {exc}", flush=True)
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = f"entry_{entry_status}"
                # S99: clear deferred-close marker — entry never opened, no need to close
                order.pop("pending_close_after_fill", None)
            changed = True
            rej_reason = (entry.get("RejectReason") or entry.get("StatusDescription")
                          or entry.get("Message", ""))
            print(f"[real-trader] entry {entry_status}: {order['setup_name']} "
                  f"reason={rej_reason} acct={account_id}", flush=True)
            _alert(f"⚠️ {order['setup_name']} entry {entry_status}\n"
                   f"Reason: {rej_reason}")

    # Check target/stop fills for active positions
    if order["status"] == "filled":
        # Check target fill
        if order.get("target_order_id"):
            tgt = broker_orders.get(order["target_order_id"], {})
            if tgt.get("Status") == "FLL":
                tgt_fp = _extract_fill_price(tgt)
                with _lock:
                    order["target_fill_price"] = tgt_fp
                    order["status"] = "closed"
                    order["close_reason"] = "target_filled"
                changed = True
                # Cancel stop since target filled (verify + retry)
                if order.get("stop_order_id"):
                    _cancel_order_verified(order["stop_order_id"], account_id,
                                           f"stop after target fill ({order['setup_name']})")
                pnl = None
                _qf = order.get("quantity") or QTY  # S149
                if tgt_fp and order.get("fill_price"):
                    is_long = order["direction"].lower() in ("long", "bullish")
                    if is_long:
                        pnl = (tgt_fp - order["fill_price"]) * MES_POINT_VALUE * _qf
                    else:
                        pnl = (order["fill_price"] - tgt_fp) * MES_POINT_VALUE * _qf
                pnl_str = f"${pnl:.2f}" if pnl is not None else "n/a"
                dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
                print(f"[real-trader] TARGET filled: {order['setup_name']} "
                      f"@ {tgt_fp} pnl={pnl_str} acct={account_id}", flush=True)
                _alert(f"🏁 {order['setup_name']} TARGET FILLED\n"
                       f"{dir_label} {_qf} MES @ {tgt_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(account_id)}")

        # Check stop REJ — TS can reject a stop-modify asynchronously even after
        # PUT returned 200. Replace-or-reject semantics mean the original stop
        # is wiped. Detect + emergency close before position stays bare.
        if order.get("stop_order_id") and order["status"] == "filled":
            stop_order = broker_orders.get(order["stop_order_id"], {})
            if stop_order.get("Status") == "REJ":
                rej_reason = (stop_order.get("StatusDescription")
                              or stop_order.get("RejectReason")
                              or "rejected async by exchange")
                print(f"[real-trader] stop REJECTED async id={lid}: {rej_reason} "
                      f"acct={account_id}", flush=True)
                _alert(f"🚨 {order['setup_name']} STOP REJECTED BY EXCHANGE\n"
                       f"{rej_reason[:120]}\n"
                       f"Closing at market to avoid bare position...")
                close_trade(lid, "stop_rejected_async")
                return  # stop processing this order; state is now closed

        # Check stop fill
        if order.get("stop_order_id") and order["status"] == "filled":
            stop_order = broker_orders.get(order["stop_order_id"], {})
            if stop_order.get("Status") == "FLL":
                stop_fp = _extract_fill_price(stop_order)
                with _lock:
                    order["stop_fill_price"] = stop_fp
                    order["status"] = "closed"
                    order["close_reason"] = "stop_filled"
                changed = True
                # Cancel target since stop filled (verify + retry)
                if order.get("target_order_id"):
                    _cancel_order_verified(order["target_order_id"], account_id,
                                           f"target after stop fill ({order['setup_name']})")
                pnl = None
                _qs = order.get("quantity") or QTY  # S149
                if stop_fp and order.get("fill_price"):
                    is_long = order["direction"].lower() in ("long", "bullish")
                    if is_long:
                        pnl = (stop_fp - order["fill_price"]) * MES_POINT_VALUE * _qs
                    else:
                        pnl = (order["fill_price"] - stop_fp) * MES_POINT_VALUE * _qs
                pnl_str = f"${pnl:.2f}" if pnl is not None else "n/a"
                dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
                print(f"[real-trader] STOP filled: {order['setup_name']} "
                      f"@ {stop_fp} pnl={pnl_str} acct={account_id}", flush=True)
                _alert(f"🏁 {order['setup_name']} STOP FILLED\n"
                       f"{dir_label} {_qs} MES @ {stop_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(account_id)}")

    if changed:
        _persist_order(lid)


def _cancel_order_verified(order_id: str, account_id: str, label: str, retries: int = 3):
    """Cancel an order and verify it was actually cancelled. Retry + alert on failure."""
    for attempt in range(retries):
        _ts_api("DELETE", f"/orderexecution/orders/{order_id}", None, account_id)
        time.sleep(1)  # give TS time to process
        # Verify: query the order status
        resp = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        if resp:
            orders = resp.get("Orders", [])
            still_active = any(
                o.get("OrderID") == str(order_id) and o.get("Status") in ("OPN", "ACK", "UCN", "DON")
                for o in orders
            )
            if not still_active:
                print(f"[real-trader] cancel verified: {label} order_id={order_id} (attempt {attempt+1})", flush=True)
                return
            print(f"[real-trader] cancel NOT confirmed: {label} order_id={order_id} "
                  f"(attempt {attempt+1}/{retries}), retrying...", flush=True)
        else:
            print(f"[real-trader] cancel verify failed (no response): {label} order_id={order_id}", flush=True)
    # All retries exhausted
    _alert(f"🚨 CRITICAL: Failed to cancel {label}\n"
           f"Order ID: {order_id}\nAccount: {account_id}\n"
           f"MANUAL CANCEL REQUIRED!")


def _extract_fill_price(entry_order: dict) -> float | None:
    """Extract fill price from a broker order response."""
    try:
        fp = float(entry_order.get("FilledPrice", 0))
        if fp > 0:
            return fp
    except (ValueError, TypeError):
        pass
    fills = entry_order.get("Legs", [{}])
    if fills:
        try:
            ep = float(fills[0].get("ExecPrice", 0))
            if ep > 0:
                return ep
        except (ValueError, TypeError):
            pass
    return None


def _get_order_fill_price(order_id: str, account_id: str) -> float | None:
    """Get fill price for a specific order by polling broker.

    BUG 3 FIX (2026-05-18): TS API /historicalorders?since={today} returns 0 rows
    (since is exclusive of today). For INTRADAY recovery we must use /orders
    only — but stop-fill orders may take a few seconds to flip from ACK→FLL.
    Solution: poll /orders up to 3x with 1.5s delay. Also use yesterday as the
    fallback since date so trades that linger past midnight get caught.

    Verified 2026-05-18 with TS API: today's fills LIVE in /orders, never in
    historicalorders. 12 historical ghost trades lost fill price due to old logic.
    """
    if account_id not in ACCOUNT_WHITELIST:
        return None
    import time as _time
    # 1) Poll live /orders up to 3 times — handles ACK→FLL transition lag.
    for attempt in range(3):
        try:
            data = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
            if data:
                for o in data.get("Orders", []):
                    if o.get("OrderID") == order_id and o.get("Status") == "FLL":
                        return _extract_fill_price(o)
        except Exception:
            pass
        if attempt < 2:
            _time.sleep(1.5)
    # 2) Fall back to historical orders with YESTERDAY date (today returns 0 — TS quirk).
    try:
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        # since=yesterday returns orders from yesterday onward; today's order may
        # still be in /orders not /historicalorders, so this is best-effort.
        yesterday = (_dt.now(_tz.utc) - _td(days=1)).strftime("%m-%d-%Y")
        data = _ts_api("GET",
                       f"/brokerage/accounts/{account_id}/historicalorders?since={yesterday}&pageSize=600",
                       None, account_id)
        if data:
            for o in data.get("Orders", []):
                if o.get("OrderID") == order_id and o.get("Status") == "FLL":
                    return _extract_fill_price(o)
    except Exception as e:
        print(f"[real-trader] historicalorders lookup failed for {order_id}: {e}", flush=True)
    # 3) Loud alert if still nothing — this is the leak that 12 historical trades hit.
    print(f"[real-trader] WARN: fill price NOT FOUND for OID={order_id} acct={account_id} "
          f"(both /orders×3 and /historicalorders since=yesterday returned no FLL match)",
          flush=True)
    return None


def _backfill_ghost_fill(order: dict) -> tuple[str, float] | None:
    """Try to recover the real fill price of a position that the bot missed.
    Checks stop_order_id → target_order_id → close_order_id, then falls back to
    scanning broker historicalorders for an opposite-direction market fill that
    matches our qty + entered after our ts_placed.

    Returns (field_name, fill_price) or None if nothing found. Safe to call — all
    exceptions swallowed to keep reconciliation working under any broker hiccup."""
    acct = order.get("account_id", "")
    if acct not in ACCOUNT_WHITELIST:
        return None
    try:
        # Check the closing orders first — if broker says flat, one of these must have filled.
        # 2026-05-14 Bug 3 fix: check close_order_id first (the actual market-close
        # placed by _flatten_position). When that path runs, the original stop is
        # DELETE'd before the market sell, so stop_order_id will have status=OUT
        # not FLL, and backfill via stop OID returns None.
        for field, oid_key in (("close_fill_price", "close_order_id"),
                                ("stop_fill_price", "stop_order_id"),
                                ("target_fill_price", "target_order_id")):
            oid = order.get(oid_key)
            if not oid:
                continue
            fp = _get_order_fill_price(oid, acct)
            if fp is not None and fp > 0:
                return (field, float(fp))

        # 2026-05-18 Bug 3 EXTENSION: heuristic fallback when no tracked OID
        # produced a fill price. Covers cases where the position was closed by:
        #   - User manual TS UI flatten (stop gets cancelled, no close_oid tracked)
        #   - _close_broker_orphans path (no tracked OID lookup)
        #   - Other external close mechanisms we don't capture
        #
        # Strategy: scan historicalorders for opposite-direction market FLL
        # of matching qty, with OpenedDateTime AFTER our entry. If exactly one
        # such candidate exists in the last 60 minutes from now, use it.
        # Ambiguous (multiple candidates) → return None to avoid mis-attribution.
        is_long = (order.get("direction") or "").lower() in ("long", "bullish")
        close_side = "Sell" if is_long else "Buy"
        entry_ts_str = order.get("ts_placed")  # ISO string
        if entry_ts_str:
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
            try:
                # Parse our entry timestamp
                entry_dt = _dt.fromisoformat(entry_ts_str.replace("Z", "+00:00"))
                if entry_dt.tzinfo is None:
                    entry_dt = entry_dt.replace(tzinfo=_tz.utc)
                # BUG 3 FIX (2026-05-18): use yesterday (today returns 0 — TS quirk).
                # ALSO include StopMarket — stop fills are the dominant close path
                # for SL-style setups (was filtering them out before).
                yesterday = (_dt.now(_tz.utc) - _td(days=1)).strftime("%m-%d-%Y")
                data = _ts_api("GET",
                               f"/brokerage/accounts/{acct}/historicalorders?since={yesterday}&pageSize=600",
                               None, acct)
                candidates = []
                for o in (data or {}).get("Orders", []):
                    if o.get("Status") != "FLL":
                        continue
                    if o.get("OrderType") not in ("Market", "StopMarket"):
                        continue
                    legs = o.get("Legs", [{}])[0]
                    if legs.get("BuyOrSell") != close_side:
                        continue
                    # S149: accept either 1 or the order's tracked qty (could be 2)
                    _qexp = str(order.get("quantity") or QTY)
                    if str(legs.get("QuantityOrdered", "")) != _qexp:
                        continue
                    # Parse close timestamp
                    closed_str = o.get("ClosedDateTime") or o.get("OpenedDateTime", "")
                    if not closed_str:
                        continue
                    try:
                        closed_dt = _dt.fromisoformat(closed_str.replace("Z", "+00:00"))
                        if closed_dt.tzinfo is None:
                            closed_dt = closed_dt.replace(tzinfo=_tz.utc)
                    except (ValueError, TypeError):
                        continue
                    # Must be AFTER our entry, within reasonable window
                    if closed_dt < entry_dt:
                        continue
                    if closed_dt - entry_dt > _td(hours=8):  # cap to same session
                        continue
                    candidates.append((closed_dt, o))
                if len(candidates) == 1:
                    fp = _extract_fill_price(candidates[0][1])
                    if fp is not None and fp > 0:
                        print(f"[real-trader] ghost backfill HEURISTIC match for "
                              f"lid={order.get('setup_log_id')}: 1 candidate {close_side} "
                              f"Market FLL at {fp}", flush=True)
                        return ("close_fill_price", float(fp))
                elif len(candidates) > 1:
                    print(f"[real-trader] ghost backfill HEURISTIC ambiguous for "
                          f"lid={order.get('setup_log_id')}: {len(candidates)} candidates "
                          f"— skipping to avoid mis-attribution", flush=True)
            except Exception as e_inner:
                print(f"[real-trader] heuristic backfill inner error: {e_inner}", flush=True)
    except Exception as e:
        print(f"[real-trader] ghost backfill error for lid={order.get('setup_log_id')}: {e}", flush=True)
    return None


# ====== SC TRAIL LOGIC ======

def update_trail(setup_log_id: int, current_es_price: float):
    """Update trailing stop based on current ES price.
    SC Trail: BE trigger=10, activation=10, gap=5.

    Called externally (from main.py's outcome tracking loop) with the current ES price.
    This function tracks max favorable excursion and advances the stop accordingly.
    """
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] != "filled":
            return
        fill_price = order.get("fill_price")
        if not fill_price:
            return

    is_long = order["direction"].lower() in ("long", "bullish")

    # Calculate current profit
    if is_long:
        profit = current_es_price - fill_price
    else:
        profit = fill_price - current_es_price

    # Update max favorable excursion
    with _lock:
        if profit > order.get("max_favorable", 0):
            order["max_favorable"] = profit

        max_fav = order["max_favorable"]
        current_stop = order.get("current_stop")
        if current_stop is None:
            return

        new_stop = current_stop  # default: no change

        # Phase 1: Breakeven trigger
        if not order.get("be_triggered") and max_fav >= BE_TRIGGER_PTS:
            order["be_triggered"] = True
            if is_long:
                be_stop = _round_mes(fill_price + BE_BUFFER_PTS)
                if be_stop > current_stop:
                    new_stop = be_stop
            else:
                be_stop = _round_mes(fill_price - BE_BUFFER_PTS)
                if be_stop < current_stop:
                    new_stop = be_stop

        # Phase 2: Trail activation
        if max_fav >= TRAIL_ACTIVATION_PTS:
            order["trail_active"] = True
            trail_stop = _round_mes(fill_price + (max_fav - TRAIL_GAP_PTS)) if is_long else \
                         _round_mes(fill_price - (max_fav - TRAIL_GAP_PTS))
            # Trail only moves forward (tighter)
            if is_long and trail_stop > new_stop:
                new_stop = trail_stop
            elif not is_long and trail_stop < new_stop:
                new_stop = trail_stop

    # Apply stop update if changed
    if new_stop != current_stop:
        # Trail only moves in protective direction
        if is_long and new_stop > current_stop:
            update_stop(setup_log_id, new_stop)
        elif not is_long and new_stop < current_stop:
            update_stop(setup_log_id, new_stop)

    # ── S131 (2026-05-17): SPX-driven exit trigger ──
    # When SPX exit mode is on, broker SL doesn't trail. We detect "price has
    # crossed internal trail" here and fire immediate market exit + cancel SL.
    # This pre-empts broker SL from firing on MES tick wicks (the trail-tag-early
    # leak, S132: -$9.65/trade = 107% of V14-era execution gap).
    if _spx_exit_enabled():
        with _lock:
            check_stop = order.get("current_stop")
        if check_stop is not None:
            crossed = (is_long and current_es_price <= check_stop) or \
                      (not is_long and current_es_price >= check_stop)
            if crossed:
                print(f"[real-trader] [S131] SPX trail-exit fire: "
                      f"id={setup_log_id} price={current_es_price:.2f} "
                      f"{'<=' if is_long else '>='} stop={check_stop:.2f}",
                      flush=True)
                _alert(f"🎯 {order['setup_name']} S131 SPX TRAIL-EXIT\n"
                       f"ES {current_es_price:.2f} {'<=' if is_long else '>='} "
                       f"internal trail {check_stop:.2f}\n"
                       f"Market-closing + cancelling broker SL...")
                close_trade(setup_log_id, "spx_trail_exit")


# ====== EOD FLATTEN ======

def flatten_all_eod():
    """Force-close all open REAL positions at end of day.
    Called by scheduler at 15:55 ET before market close."""
    # Track the flatten close order ID per account so we can look up actual fill price
    # for per-trade P&L reporting in Phase 1d
    _close_oids: dict[str, str] = {}
    with _lock:
        open_orders = [(lid, o) for lid, o in _active_orders.items()
                       if o["status"] in ("pending_entry", "pending_limit", "filled")]
    if not open_orders:
        print("[real-trader] EOD flatten: no tracked positions", flush=True)
    else:
        print(f"[real-trader] EOD flatten: closing {len(open_orders)} tracked position(s)",
              flush=True)
        _alert(f"⚠️ EOD FLATTEN: closing {len(open_orders)} position(s)")

        # Phase 1a: Cancel ALL orders across ALL tracked trades first
        cancelled = 0
        for lid, order in open_orders:
            account_id = order.get("account_id", "")
            if account_id not in ACCOUNT_WHITELIST:
                continue
            for oid_key in ("entry_order_id", "stop_order_id", "target_order_id"):
                # Cancel entry only for pending_limit
                if oid_key == "entry_order_id" and order.get("status") != "pending_limit":
                    continue
                oid = order.get(oid_key)
                if oid:
                    try:
                        _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
                        cancelled += 1
                    except Exception:
                        pass
        print(f"[real-trader] EOD: cancelled {cancelled} orders", flush=True)

        # Phase 1b: Wait for cancellations to settle
        time.sleep(3)

        # Phase 1c: Close actual broker positions with retry
        for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
            if acct_id not in ACCOUNT_WHITELIST:
                continue
            broker_pos = _get_broker_position(acct_id)
            if not broker_pos:
                print(f"[real-trader] EOD: {acct_id} already flat", flush=True)
                continue

            close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"
            closed = False
            close_oid_for_acct = None
            for attempt, wait in enumerate([0, 3, 5, 10], start=1):
                if attempt > 1:
                    print(f"[real-trader] EOD close retry #{attempt} after {wait}s wait "
                          f"on {acct_id}...", flush=True)
                    time.sleep(wait)
                    broker_pos = _get_broker_position(acct_id)
                    if not broker_pos:
                        print(f"[real-trader] EOD: {acct_id} closed during wait", flush=True)
                        closed = True
                        break
                    close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"

                close_payload = {
                    "AccountID": acct_id,
                    "Symbol": broker_pos["symbol"],
                    "Quantity": str(broker_pos["qty"]),
                    "OrderType": "Market",
                    "TradeAction": close_side,
                    "TimeInForce": {"Duration": "DAY"},
                    "Route": "Intelligent",
                }
                resp = _ts_api("POST", "/orderexecution/orders", close_payload, acct_id)
                if resp:
                    orders_list = resp.get("Orders", [])
                    if orders_list and orders_list[0].get("Error") == "FAILED":
                        msg = orders_list[0].get("Message", "")
                        print(f"[real-trader] EOD close rejected on {acct_id} "
                              f"(attempt {attempt}): {msg}", flush=True)
                        continue
                    # Capture close order ID so we can look up the actual fill price for P&L
                    if orders_list:
                        close_oid_for_acct = orders_list[0].get("OrderID")
                    print(f"[real-trader] EOD: closed {broker_pos['long_short']} "
                          f"{broker_pos['qty']} MES on {acct_id} (attempt {attempt}) "
                          f"close_oid={close_oid_for_acct}", flush=True)
                    closed = True
                    break
                else:
                    print(f"[real-trader] EOD close API error on {acct_id} "
                          f"(attempt {attempt})", flush=True)

            if closed and close_oid_for_acct:
                _close_oids[acct_id] = close_oid_for_acct

            if not closed:
                _alert(f"🚨 EOD CLOSE FAILED after 4 attempts\n"
                       f"Account: {acct_id}\n"
                       f"{broker_pos['long_short']} {broker_pos['qty']} MES\n"
                       f"MANUAL CLOSE REQUIRED IMMEDIATELY")

        # Let close orders settle on broker so fill price is retrievable
        time.sleep(3)

        # Phase 1d: Mark all tracked trades as closed and send per-trade P&L Telegram
        for lid, order in open_orders:
            acct_id = order.get("account_id", "")
            close_fp = None
            close_oid = _close_oids.get(acct_id)
            if close_oid:
                try:
                    close_fp = _get_order_fill_price(close_oid, acct_id)
                except Exception as e:
                    print(f"[real-trader] EOD fill lookup failed for {close_oid}: {e}",
                          flush=True)
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = "eod_flatten"
                if close_fp is not None:
                    order["stop_fill_price"] = close_fp
            _persist_order(lid)

            # Compute P&L and send Telegram per trade
            entry_fp = order.get("fill_price")
            _qe = order.get("quantity") or QTY  # S149
            pnl = None
            if close_fp is not None and entry_fp is not None:
                is_long = order["direction"].lower() in ("long", "bullish")
                if is_long:
                    pnl = (close_fp - entry_fp) * MES_POINT_VALUE * _qe
                else:
                    pnl = (entry_fp - close_fp) * MES_POINT_VALUE * _qe
            pnl_str = f"${pnl:.2f}" if pnl is not None else "n/a"
            dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
            if close_fp is not None:
                _alert(f"🏁 {order['setup_name']} EOD FLATTEN\n"
                       f"{dir_label} {_qe} MES @ {close_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(acct_id)}")
            else:
                # Fallback if we couldn't retrieve fill price
                _alert(f"🏁 {order['setup_name']} EOD FLATTEN\n"
                       f"{dir_label} {_qe} MES\n"
                       f"P&L: n/a (fill price unavailable)"
                       f"{_day_line(acct_id)}")
            print(f"[real-trader] EOD marked closed: {order['setup_name']} id={lid} "
                  f"acct={acct_id} close_fp={close_fp} pnl={pnl_str}", flush=True)

    # Phase 2: Cancel ALL remaining open orders on both accounts
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        try:
            ord_data = _ts_api("GET", f"/brokerage/accounts/{acct_id}/orders", None, acct_id)
            for o in (ord_data or {}).get("Orders", []):
                status = o.get("Status", "")
                if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                    continue
                oid = o.get("OrderID")
                if oid:
                    _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, acct_id)
                    print(f"[real-trader] EOD: cancelled remaining order {oid} "
                          f"on {acct_id}", flush=True)
        except Exception as e:
            print(f"[real-trader] EOD order sweep error on {acct_id}: {e}", flush=True)

    # Phase 3: Close any orphaned positions
    time.sleep(1)
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id in ACCOUNT_WHITELIST:
            _close_broker_orphans(acct_id, source="EOD")

    # Phase 4: Final verification -- confirm we are flat on both accounts
    time.sleep(1)
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        final_pos = _get_broker_position(acct_id)
        if final_pos:
            print(f"[real-trader] EOD CRITICAL: still have position on {acct_id}! "
                  f"{final_pos['long_short']} {final_pos['qty']} {final_pos['symbol']}",
                  flush=True)
            _alert(f"🚨 EOD FLATTEN FAILED on {acct_id}\n"
                   f"STILL OPEN: {final_pos['long_short']} {final_pos['qty']}\n"
                   f"MANUAL INTERVENTION REQUIRED IMMEDIATELY")
            # Last-resort retry
            try:
                _flatten_account(acct_id)
                time.sleep(1)
                still_open = _get_broker_position(acct_id)
                if still_open:
                    print(f"[real-trader] EOD FLATTEN FAILED FINAL on {acct_id}: "
                          f"{still_open}", flush=True)
                else:
                    print(f"[real-trader] EOD retry flatten succeeded on {acct_id}", flush=True)
            except Exception as e:
                print(f"[real-trader] EOD retry flatten error on {acct_id}: {e}", flush=True)
        else:
            print(f"[real-trader] EOD flatten verified flat on {acct_id}", flush=True)


def _flatten_account(account_id: str):
    """Close all positions + cancel all orders on a specific account."""
    if account_id not in ACCOUNT_WHITELIST:
        print(f"[real-trader] BLOCKED: _flatten_account on non-whitelisted {account_id}",
              flush=True)
        return

    # Cancel all open orders
    try:
        ord_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        for o in (ord_data or {}).get("Orders", []):
            status = o.get("Status", "")
            if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                continue
            oid = o.get("OrderID")
            if oid:
                _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
    except Exception as e:
        print(f"[real-trader] flatten-account order cancel error on {account_id}: {e}",
              flush=True)

    time.sleep(1)

    # Close all positions
    try:
        pos_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/positions", None, account_id)
        for pos in (pos_data or {}).get("Positions", []):
            symbol = pos.get("Symbol", "")
            # TS futures positions return signed Quantity (e.g. "-1" for shorts) — use abs
            qty = abs(int(pos.get("Quantity", "0")))
            long_short = pos.get("LongShort", "")
            if qty == 0:
                continue
            close_side = "Sell" if long_short == "Long" else "Buy"
            close_payload = {
                "AccountID": account_id,
                "Symbol": symbol,
                "Quantity": str(qty),
                "OrderType": "Market",
                "TradeAction": close_side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            }
            resp = _ts_api("POST", "/orderexecution/orders", close_payload, account_id)
            if resp:
                orders_list = resp.get("Orders", [])
                if orders_list and orders_list[0].get("Error") == "FAILED":
                    msg = orders_list[0].get("Message", "")
                    print(f"[real-trader] flatten-account REJECTED on {account_id}: {msg}",
                          flush=True)
                    _alert(f"🚨 FLATTEN REJECTED on {account_id}\n"
                           f"{long_short} {qty} {symbol}: {msg}")
                else:
                    print(f"[real-trader] flatten-account: closed {long_short} {qty} {symbol} "
                          f"on {account_id}", flush=True)
    except Exception as e:
        print(f"[real-trader] flatten-account position close error on {account_id}: {e}",
              flush=True)


# ====== ORPHAN DETECTION ======

def _close_broker_orphans(account_id: str, source: str = "EOD"):
    """Check broker for positions not tracked in _active_orders. Close any orphans."""
    if account_id not in ACCOUNT_WHITELIST:
        return

    try:
        pos_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/positions", None, account_id)
        positions = (pos_data or {}).get("Positions", [])
    except Exception as e:
        print(f"[real-trader] {source} orphan check failed on {account_id}: {e}", flush=True)
        return

    if not positions:
        return

    # Check which positions are tracked
    with _lock:
        tracked_acct_directions = set()
        for o in _active_orders.values():
            if o["status"] in ("pending_entry", "filled") and o.get("account_id") == account_id:
                d = o["direction"].lower()
                tracked_acct_directions.add("Long" if d in ("long", "bullish") else "Short")

    for pos in positions:
        symbol = pos.get("Symbol", "")
        # TS futures positions return signed Quantity (e.g. "-1" for shorts) — use abs
        qty = abs(int(pos.get("Quantity", "0")))
        long_short = pos.get("LongShort", "")
        if qty == 0:
            continue
        if long_short in tracked_acct_directions:
            print(f"[real-trader] {source} orphan check on {account_id}: "
                  f"{long_short} {qty} {symbol} -- matches tracked, OK", flush=True)
            continue

        print(f"[real-trader] WARNING: {source} orphan on {account_id} -- "
              f"{long_short} {qty} {symbol}. Closing...", flush=True)
        _alert(f"⚠️ {source} ORPHAN on {account_id}\n"
               f"{long_short} {qty} {symbol}\nAuto-closing...")
        close_side = "Sell" if long_short == "Long" else "Buy"
        close_payload = {
            "AccountID": account_id,
            "Symbol": symbol,
            "Quantity": str(qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        try:
            _ts_api("POST", "/orderexecution/orders", close_payload, account_id)
            print(f"[real-trader] {source} orphan closed on {account_id}: "
                  f"{long_short} {qty} {symbol}", flush=True)
        except Exception as e:
            print(f"[real-trader] {source} orphan close FAILED on {account_id}: {e}",
                  flush=True)

    # Cancel untracked orders
    try:
        ord_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        tracked_oids = set()
        with _lock:
            for o in _active_orders.values():
                if o.get("account_id") == account_id:
                    for k in ("entry_order_id", "stop_order_id", "target_order_id"):
                        oid = o.get(k)
                        if oid:
                            tracked_oids.add(str(oid))
        for o in (ord_data or {}).get("Orders", []):
            status = o.get("Status", "")
            if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                continue
            oid = o.get("OrderID")
            if oid and str(oid) not in tracked_oids:
                _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
                print(f"[real-trader] {source} orphan cleanup: cancelled untracked order "
                      f"{oid} on {account_id}", flush=True)
    except Exception as e:
        print(f"[real-trader] {source} orphan order cancel failed on {account_id}: {e}",
              flush=True)


def periodic_orphan_check():
    """Periodic safety check -- detect orphaned broker positions during market hours.
    Called by scheduler every 5 minutes."""
    if not (LONGS_ENABLED or SHORTS_ENABLED) or not _get_token:
        return

    # Daily cleanup: expire stale orders from previous days
    today_str = date.today().isoformat()
    with _lock:
        stale_ids = []
        for lid, o in _active_orders.items():
            if o["status"] in ("pending_entry", "pending_limit", "filled"):
                ts_placed = o.get("ts_placed", "")
                order_date = ts_placed[:10] if len(ts_placed) >= 10 else ""
                if order_date and order_date < today_str:
                    stale_ids.append(lid)
    # Update + persist OUTSIDE lock (avoid deadlock — _persist_order acquires _lock)
    for lid in stale_ids:
        with _lock:
            o = _active_orders.get(lid)
            if not o:
                continue
            print(f"[real-trader] PERIODIC: expiring stale order {o.get('setup_name', '?')} "
                  f"id={lid} from {o.get('ts_placed', '?')[:10]} "
                  f"acct={o.get('account_id')}", flush=True)
            o["status"] = "closed"
            o["close_reason"] = "stale_overnight_periodic"
        _persist_order(lid)

    # Check each account
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        broker_pos = _get_broker_position(acct_id)
        if not broker_pos:
            continue

        with _lock:
            has_tracked = any(
                o["status"] in ("pending_entry", "filled") and o.get("account_id") == acct_id
                for o in _active_orders.values()
            )

        if not has_tracked:
            print(f"[real-trader] PERIODIC: orphan on {acct_id} -- "
                  f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}. "
                  f"Closing...", flush=True)
            _alert(f"⚠️ PERIODIC ORPHAN on {acct_id}\n"
                   f"{broker_pos['long_short']} {broker_pos['qty']} "
                   f"{broker_pos['symbol']}\nAuto-closing...")
            _close_broker_orphans(acct_id, source="PERIODIC")


# ====== STATUS ======

def get_status() -> dict:
    """Return status dict for health endpoint."""
    with _lock:
        active = {lid: {
            "setup_name": o["setup_name"],
            "direction": o["direction"],
            "account_id": o.get("account_id"),
            "status": o["status"],
            "fill_price": o["fill_price"],
            "current_stop": o["current_stop"],
            "target_price": o.get("target_price"),
            "max_favorable": o.get("max_favorable", 0),
            "be_triggered": o.get("be_triggered", False),
            "trail_active": o.get("trail_active", False),
        } for lid, o in _active_orders.items() if o["status"] != "closed"}

    return {
        "longs_enabled": LONGS_ENABLED,
        "shorts_enabled": SHORTS_ENABLED,
        "longs_account": _LONGS_ACCOUNT,
        "shorts_account": _SHORTS_ACCOUNT,
        "symbol": MES_SYMBOL,
        "qty": QTY,
        "max_concurrent_per_dir": MAX_CONCURRENT_PER_DIR,  # legacy alias = MAX_CONCURRENT_LONG
        "max_concurrent_long": MAX_CONCURRENT_LONG,
        "max_concurrent_short": MAX_CONCURRENT_SHORT,
        "active_count": len(active),
        "active_orders": active,
    }


def get_full_status() -> dict:
    """Full monitoring: balances, positions, orders, daily P&L for both accounts."""
    result = get_status()
    if not _get_token:
        result["error"] = "no token"
        return result

    for acct_id, label in [(_LONGS_ACCOUNT, "longs"), (_SHORTS_ACCOUNT, "shorts")]:
        acct = {"account_id": acct_id}
        # Balances
        try:
            bal_data = _ts_api("GET", f"/brokerage/accounts/{acct_id}/balances", None, acct_id)
            if bal_data:
                b = bal_data.get("Balances", [{}])
                b = b[0] if isinstance(b, list) and b else b
                detail = b.get("BalanceDetail", {})
                acct["cash"] = float(b.get("CashBalance", 0))
                acct["equity"] = float(b.get("Equity", 0))
                acct["buying_power"] = float(b.get("BuyingPower", 0))
                acct["today_pnl"] = float(b.get("TodaysProfitLoss", 0))
                acct["realized_pnl"] = float(detail.get("RealizedProfitLoss", 0))
                acct["unrealized_pnl"] = float(detail.get("UnrealizedProfitLoss", 0))
                acct["day_trade_excess"] = float(detail.get("DayTradeExcess", 0))
                # Diagnosis: which margin rate is TS applying?
                # If DayTradeMargin > 0 and InitialMargin == 0 → intraday rate active (~$265/MES)
                # If InitialMargin > 0 and DayTradeMargin == 0 → overnight rate active (~$2,499/MES)
                acct["initial_margin"] = float(detail.get("InitialMargin", 0))
                acct["day_trade_margin"] = float(detail.get("DayTradeMargin", 0))
                acct["maintenance_margin"] = float(detail.get("MaintenanceMargin", 0))
                acct["required_margin"] = float(detail.get("RequiredMargin", 0))
        except Exception as e:
            acct["balance_error"] = str(e)
        # Positions
        try:
            pos = _get_broker_position(acct_id)
            acct["position"] = pos if pos else "flat"
        except Exception as e:
            acct["position_error"] = str(e)
        # Open orders
        try:
            ord_data = _ts_api("GET", f"/brokerage/accounts/{acct_id}/orders", None, acct_id)
            open_orders = []
            for o in (ord_data or {}).get("Orders", []):
                if o.get("Status") in ("OPN", "ACK", "DON"):
                    open_orders.append({
                        "id": o.get("OrderID"),
                        "type": o.get("Type"),
                        "side": o.get("Legs", [{}])[0].get("BuyOrSell"),
                        "qty": o.get("Quantity"),
                        "price": o.get("LimitPrice") or o.get("StopPrice"),
                        "status": o.get("Status"),
                    })
            acct["open_orders"] = open_orders
        except Exception as e:
            acct["orders_error"] = str(e)
        # Margin warning
        if acct.get("buying_power") is not None:
            margin_needed = _margin_per_mes() * QTY
            acct["can_trade"] = acct["buying_power"] >= margin_needed
            acct["margin_buffer"] = round(acct["buying_power"] - margin_needed, 2)
            losses_until_margin = int(acct.get("margin_buffer", 0) / (14 * MES_POINT_VALUE * QTY)) if acct.get("margin_buffer", 0) > 0 else 0
            acct["losses_until_margin_block"] = losses_until_margin
        result[f"account_{label}"] = acct

    # Daily loss check
    try:
        result["daily_loss"] = _get_daily_loss()
        result["daily_loss_limit"] = DAILY_LOSS_LIMIT
        result["circuit_breaker_triggered"] = result["daily_loss"] >= DAILY_LOSS_LIMIT
    except Exception:
        pass

    return result


# ====== BROKER QUERIES ======

def _get_broker_position(account_id: str, expect_position: bool = False) -> dict | None:
    """Query broker for actual MES position on a specific account.
    Returns {'qty': int, 'long_short': str, 'symbol': str} or None if flat.

    NOTE: TS API returns Quantity as a SIGNED string for futures positions
    (e.g. "-1" for shorts). Use abs() so the filter doesn't drop shorts.

    2026-05-27 fix (lid=3211 ORPHAN root cause): added expect_position retry path.
    When the caller knows there SHOULD be a position (e.g. _flatten_position
    entering with status in (filled, closed)), retry up to 3 times with 1s gap
    on empty/None response. Single-shot was returning false-flat on transient
    TS API hiccups → _flatten_position bailed → broker had position → ORPHAN
    caught hours later by reconciler. Reconciler/other callers keep single-shot
    semantics (expect_position=False) so they can detect genuine flat-broker
    states without latency cost.
    """
    if account_id not in ACCOUNT_WHITELIST:
        return None
    attempts = 3 if expect_position else 1
    for attempt in range(attempts):
        try:
            pos_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/positions", None, account_id)
            for pos in (pos_data or {}).get("Positions", []):
                symbol = pos.get("Symbol", "")
                qty = abs(int(pos.get("Quantity", "0")))
                if qty > 0 and "MES" in symbol.upper():
                    return {
                        "qty": qty,
                        "long_short": pos.get("LongShort", ""),
                        "symbol": symbol,
                    }
            # No position found in this response — may be transient false-flat
            if expect_position and attempt < attempts - 1:
                time.sleep(1.0)
                continue
        except Exception as e:
            if attempt == attempts - 1:
                print(f"[real-trader] broker position query error on {account_id} "
                      f"after {attempts} attempts: {e}", flush=True)
            if attempt < attempts - 1:
                time.sleep(1.0)
                continue
    return None


def _get_daily_realized_loss() -> float:
    """Return today's NET realized loss across both whitelisted accounts, in $.
    Positive return = net down by that much. Zero = flat or net green.

    S161 fix (2026-05-20): Previous implementation summed setup_log.outcome_pnl
    where pnl<0 — a gross-loss calculation using SPX-side outcome labels rather
    than broker reality. That tripped the breaker on a +$388 GREEN day because
    6 SPX-labelled "losses" totalled -60pt × $5/MES = exactly the $300 limit,
    even though 3 of them closed favorably on broker due to SPX↔MES divergence.

    New behavior reads TS BalanceDetail.RealizedProfitLoss directly (same field
    used by `_day_line()` for Telegram alerts), sums across both ACCOUNT_WHITELIST
    accounts, and returns the absolute net loss only (or 0 if net flat/green).
    Breaker now trips only when REAL BROKER MONEY is down >= DAILY_LOSS_LIMIT."""
    try:
        net = 0.0
        got_any = False
        for acct in ACCOUNT_WHITELIST:
            v = _get_daily_realized_pnl(acct)
            if v is not None:
                net += v
                got_any = True
        if not got_any:
            return 0.0
        return max(0.0, -net)  # positive value when net loss; 0 when flat/green
    except Exception as e:
        print(f"[real-trader] daily loss query error: {e}", flush=True)
        return 0.0


def _get_buying_power(account_id: str) -> float | None:
    """Query account buying power from broker."""
    if account_id not in ACCOUNT_WHITELIST:
        return None
    try:
        data = _ts_api("GET", f"/brokerage/accounts/{account_id}/balances", None, account_id)
        if not data:
            return None
        balances = data.get("Balances", [])
        if isinstance(balances, list) and balances:
            b = balances[0]
        elif isinstance(balances, dict):
            b = balances
        else:
            return None
        # Bug fix (2026-05-19): `or` short-circuits on BP=0 (falsy), silently falling
        # back to CashBalance — a *different* number. When TS reports BuyingPower=0
        # (margin fully consumed but position still open), this returned CashBalance
        # instead, misrepresenting actual BP and bypassing the margin gate. Use
        # explicit None check so 0 is treated as authoritative.
        bp = b.get("BuyingPower")
        if bp is None:
            bp = b.get("CashBalance")
        # Diagnostic dump: which margin rate is TS applying right now?
        # MES intraday ~$265, overnight ~$2,499. This tells us the truth on every BP check.
        detail = b.get("BalanceDetail", {}) or {}
        print(f"[real-trader] margin-check {account_id} | BP={bp} cash={b.get('CashBalance')} "
              f"InitialMargin={detail.get('InitialMargin')} DayTradeMargin={detail.get('DayTradeMargin')} "
              f"MaintenanceMargin={detail.get('MaintenanceMargin')} RequiredMargin={detail.get('RequiredMargin')}",
              flush=True)
        return float(bp) if bp else None
    except Exception as e:
        print(f"[real-trader] buying power query error on {account_id}: {e}", flush=True)
        return None


def _get_daily_realized_pnl(account_id: str) -> float | None:
    """Fetch today's realized P&L from broker for Telegram 'Day:' line.
    Uses BalanceDetail.RealizedProfitLoss (authoritative, includes fees)."""
    if account_id not in ACCOUNT_WHITELIST:
        return None
    try:
        data = _ts_api("GET", f"/brokerage/accounts/{account_id}/balances", None, account_id)
        if not data:
            return None
        balances = data.get("Balances", [])
        if isinstance(balances, list) and balances:
            b = balances[0]
        elif isinstance(balances, dict):
            b = balances
        else:
            return None
        detail = b.get("BalanceDetail", {}) or {}
        val = detail.get("RealizedProfitLoss")
        if val is None:
            return None
        return float(val)
    except Exception as e:
        print(f"[real-trader] daily P&L query error on {account_id}: {e}", flush=True)
        return None


def _day_line(account_id: str) -> str:
    """Format 'Day: +$XX.XX' line — sum across BOTH whitelisted accounts so the
    user sees combined daily P&L (longs + shorts), not just the account that
    triggered this alert. account_id arg kept for backward-compat but ignored.
    Returns empty string if both queries fail so alerts still send."""
    total = 0.0
    got_any = False
    for acct in ACCOUNT_WHITELIST:
        v = _get_daily_realized_pnl(acct)
        if v is not None:
            total += v
            got_any = True
    if not got_any:
        return ""
    sign = "+" if total >= 0 else "-"
    return f"\nDay: {sign}${abs(total):.2f}"


# ====== TS API HELPER ======

def _ts_api(method: str, path: str, json_body: dict | None,
            account_id: str) -> dict | None:
    """Authenticated request to TradeStation REAL API.
    Every call validates account_id against whitelist before proceeding."""
    # CRITICAL: validate account on every API call
    if account_id not in ACCOUNT_WHITELIST:
        print(f"[real-trader] BLOCKED API call: account {account_id} not in whitelist! "
              f"method={method} path={path}", flush=True)
        _alert(f"🚨 SECURITY BLOCK: API call to non-whitelisted account {account_id}\n"
               f"{method} {path}")
        return None

    if not _get_token:
        print("[real-trader] no token function", flush=True)
        return None

    for attempt in range(2):
        try:
            token = _get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            url = f"{REAL_BASE}{path}"

            # Log every request (this is real money)
            if json_body:
                print(f"[real-trader] API {method} {path} acct={account_id} "
                      f"payload={json.dumps(json_body, default=str)[:400]}", flush=True)
            else:
                print(f"[real-trader] API {method} {path} acct={account_id}", flush=True)

            if method == "GET":
                r = requests.get(url, headers=headers, timeout=10)
            elif method == "POST":
                r = requests.post(url, headers=headers, json=json_body, timeout=10)
            elif method == "PUT":
                r = requests.put(url, headers=headers, json=json_body, timeout=10)
            elif method == "DELETE":
                r = requests.delete(url, headers=headers, timeout=10)
            else:
                return None

            # Log response
            print(f"[real-trader] API {method} {path} [{r.status_code}] "
                  f"acct={account_id}", flush=True)

            if r.status_code == 401 and attempt == 0:
                print(f"[real-trader] API 401 on {account_id}, retrying with fresh token...",
                      flush=True)
                continue

            if r.status_code >= 400:
                print(f"[real-trader] API ERROR {method} {path} [{r.status_code}]: "
                      f"{r.text[:500]}", flush=True)
                return None

            result = r.json() if r.text else {}

            # Log order responses fully (this is real money)
            if method in ("POST", "PUT") and "order" in path.lower():
                print(f"[real-trader] API RESPONSE {method} {path}: "
                      f"{json.dumps(result, default=str)[:500]}", flush=True)

            return result

        except Exception as e:
            print(f"[real-trader] API error {method} {path} acct={account_id}: {e}", flush=True)
            if attempt == 0:
                continue
            return None

    return None


# ====== PERSISTENCE ======

def _persist_order(setup_log_id: int):
    """Save order state to DB for crash recovery."""
    if not _engine:
        return
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        state = json.dumps(order)

    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {DB_TABLE} (setup_log_id, state, updated_at)
                VALUES (:id, :s, NOW())
                ON CONFLICT (setup_log_id) DO UPDATE SET state = :s, updated_at = NOW()
            """), {"id": setup_log_id, "s": state})
    except Exception as e:
        print(f"[real-trader] persist error: {e}", flush=True)


def _load_active_orders():
    """Load non-closed orders from DB on startup.
    Only loads orders from today -- stale overnight orders are auto-closed."""
    global _active_orders
    if not _engine:
        return
    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            rows = conn.execute(text(f"""
                SELECT setup_log_id, state, updated_at FROM {DB_TABLE}
                WHERE state->>'status' != 'closed'
            """)).mappings().all()

        today_str = date.today().isoformat()
        loaded = 0
        stale = 0
        for row in rows:
            lid = row["setup_log_id"]
            state = row["state"]
            if isinstance(state, str):
                state = json.loads(state)
            ts_placed = state.get("ts_placed", "")
            order_date = ts_placed[:10] if len(ts_placed) >= 10 else ""
            if not order_date and row.get("updated_at"):
                order_date = str(row["updated_at"])[:10]
            is_stale = (order_date < today_str) if order_date else True
            if is_stale:
                stale += 1
                state["status"] = "closed"
                state["close_reason"] = "stale_overnight"
                _active_orders[lid] = state
                _persist_order(lid)
                del _active_orders[lid]
                print(f"[real-trader] STALE order auto-closed: {state.get('setup_name', '?')} "
                      f"id={lid} from {order_date or 'unknown'} "
                      f"acct={state.get('account_id')}", flush=True)
                continue
            _active_orders[lid] = state
            loaded += 1

        if loaded:
            print(f"[real-trader] restored {loaded} active orders", flush=True)
        if stale:
            print(f"[real-trader] auto-closed {stale} stale overnight order(s)", flush=True)
    except Exception as e:
        print(f"[real-trader] load error (non-fatal): {e}", flush=True)


def cleanup_stale_orders():
    """Clear any active orders from previous days. Called daily at market open.

    Defensive layer: if EOD flatten fails to persist closed state, this catches
    stale orders before they block new trades via MAX_CONCURRENT_PER_DIR.
    Bug found 2026-04-06: #1540 from Apr 2 blocked #1544/#1551 shorts all morning.
    """
    today_str = date.today().isoformat()
    cleaned = 0
    # Collect stale IDs first, then update+persist+delete outside iteration
    with _lock:
        stale_ids = [lid for lid, order in _active_orders.items()
                     if order.get("status") != "closed"
                     and (order.get("ts_placed", "")[:10] or "") < today_str
                     and len(order.get("ts_placed", "")) >= 10]
    for lid in stale_ids:
        with _lock:
            order = _active_orders.get(lid)
            if not order or order.get("status") == "closed":
                continue
            order["status"] = "closed"
            order["close_reason"] = "stale_daily_cleanup"
        _persist_order(lid)
        with _lock:
            _active_orders.pop(lid, None)
        cleaned += 1
        print(f"[real-trader] DAILY CLEANUP: stale order closed: "
                  f"{order.get('setup_name', '?')} id={lid} from {order.get('ts_placed', '?')[:10]} "
                  f"acct={order.get('account_id')}", flush=True)
    if cleaned:
        _alert(f"⚠️ Daily cleanup: {cleaned} stale order(s) from previous day(s) removed.\n"
               f"These were blocking new trades via concurrent cap.")
    else:
        print(f"[real-trader] daily cleanup: no stale orders", flush=True)


# ====== TELEGRAM HELPER ======

def _alert(msg: str):
    """Send Telegram alert for EVERY action -- this is real money."""
    if _send_telegram:
        try:
            # send_telegram_setups uses parse_mode=HTML, so any raw '<' or '&'
            # in the message body (e.g. "$327 < $700") breaks the parser and
            # the alert is silently dropped. Escape before sending.
            _send_telegram(html.escape(msg))
        except Exception as e:
            print(f"[real-trader] alert send failed: {e}", flush=True)
    print(f"[real-trader] ALERT: {msg}", flush=True)
