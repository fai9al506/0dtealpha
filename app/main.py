# 0DTE Alpha – live chain + 5-min history (FastAPI + APScheduler + Postgres + Plotly front-end)
from fastapi import FastAPI, Response, Query, Request, Cookie, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from datetime import datetime, time as dtime, timedelta
import os, time, json, re, random, requests, pandas as pd, pytz, secrets
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text
from threading import Lock, Thread
from typing import Any, Optional
import bcrypt as _bcrypt
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

# ====== CONFIG ======
USE_LIVE = True
BASE = "https://api.tradestation.com/v3" if USE_LIVE else "https://sim-api.tradestation.com/v3"
AUTH_DOMAIN = "https://signin.tradestation.com"

CID     = os.getenv("TS_CLIENT_ID", "")
SECRET  = os.getenv("TS_CLIENT_SECRET", "")
RTOKEN  = os.getenv("TS_REFRESH_TOKEN", "")
DB_URL  = os.getenv("DATABASE_URL", "")  # Railway Postgres

# Volland storage (already scraped into Postgres)
VOLLAND_TABLE       = os.getenv("VOLLAND_TABLE", "volland_exposures")
VOLLAND_TS_COL      = os.getenv("VOLLAND_TS_COL", "ts")
VOLLAND_PAYLOAD_COL = os.getenv("VOLLAND_PAYLOAD_COL", "payload")

# Charm exposure points are read directly from volland_exposure_points table

# SQLAlchemy psycopg v3 URI
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

# Cadence
PULL_EVERY     = 30   # seconds
SAVE_EVERY_MIN = 2    # minutes

# Chain window
STREAM_SECONDS = 5.0  # Increased from 2.0 to allow full chain download
TARGET_STRIKES = 40
MIN_REQUIRED_STRIKES = 30  # Minimum rows required; reject data below this threshold

# ====== TELEGRAM ALERTS ======
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_CHAT_ID_SETUPS = os.getenv("TELEGRAM_CHAT_ID_SETUPS", "")
EVAL_API_KEY = os.getenv("EVAL_API_KEY", "")

# Alert state tracking
_alert_state = {
    "last_paradigm": None,
    "last_volume": {},  # {strike: {"call": vol, "put": vol}}
    "last_alert_times": {},  # {"lis": timestamp, "target": timestamp, ...}
    "levels_touched": set(),  # Track which levels have been touched today
    "near_active": set(),  # Track levels currently in "near" zone (for re-entry alerts)
    "sent_10am": False,
    "sent_2pm": False,
    "last_trading_day": None,
}

# Pipeline health state tracking
_pipeline_status = {
    "ts_status": "ok",
    "vol_status": "ok",
    "ts_last_alert": 0,
    "vol_last_alert": 0,
    "ts_error_since": 0,
    "vol_error_since": 0,
    "reminder_minutes": 15,
}

# Default alert settings (loaded from DB on startup)
_alert_settings = {
    "enabled": True,
    "lis_enabled": True,
    "target_enabled": True,
    "max_pos_gamma_enabled": True,
    "max_neg_gamma_enabled": True,
    "paradigm_change_enabled": True,
    "summary_10am_enabled": True,
    "summary_2pm_enabled": True,
    "volume_spike_enabled": True,
    "threshold_points": 5,
    "threshold_volume": 500,
    "cooldown_enabled": True,
    "cooldown_minutes": 10,
}

def send_telegram(message: str) -> bool:
    """Send a message via Telegram bot."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram] missing token or chat_id", flush=True)
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.status_code == 200:
            print(f"[telegram] sent: {message[:50]}...", flush=True)
            return True
        else:
            print(f"[telegram] error: {resp.status_code} {resp.text}", flush=True)
            return False
    except Exception as e:
        print(f"[telegram] exception: {e}", flush=True)
        return False

def is_market_hours() -> bool:
    """Check if current time is within market hours (9:30 AM - 4:00 PM ET)."""
    now = datetime.now(NY)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close

def should_alert(alert_type: str) -> bool:
    """Check if we should send an alert based on cooldown settings."""
    if not _alert_settings.get("enabled"):
        return False
    if not _alert_settings.get("cooldown_enabled"):
        return True

    last_time = _alert_state["last_alert_times"].get(alert_type)
    if last_time is None:
        return True

    cooldown_sec = _alert_settings.get("cooldown_minutes", 10) * 60
    return (time.time() - last_time) >= cooldown_sec

def record_alert(alert_type: str):
    """Record that an alert was sent."""
    _alert_state["last_alert_times"][alert_type] = time.time()

# ====== AUTHENTICATION ======
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_hex(32))
SESSION_MAX_AGE = 60 * 60 * 24 * 7  # 7 days
_serializer = URLSafeTimedSerializer(SECRET_KEY)

def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    try:
        return _bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    except Exception:
        return False

def create_session(user_id: int) -> str:
    return _serializer.dumps({"user_id": user_id})

def verify_session(token: str) -> Optional[int]:
    if not token:
        return None
    try:
        data = _serializer.loads(token, max_age=SESSION_MAX_AGE)
        return data.get("user_id")
    except (BadSignature, SignatureExpired):
        return None

def get_current_user(session: str = None) -> Optional[dict]:
    """Get the current logged-in user from session token."""
    if not session:
        return None
    user_id = verify_session(session)
    if not user_id or not engine:
        return None
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT id, email, is_admin FROM users WHERE id = :id"),
                {"id": user_id}
            ).mappings().first()
            if row:
                return {"id": row["id"], "email": row["email"], "is_admin": row["is_admin"]}
    except Exception:
        pass
    return None

# ====== APP ======
app = FastAPI()
NY = pytz.timezone("US/Eastern")

# Public paths that don't require authentication
PUBLIC_PATHS = {"/", "/login", "/logout", "/request-access", "/api/health", "/favicon.ico", "/favicon.png", "/api/ts/authorize", "/api/ts/callback"}

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Middleware to check authentication for protected routes."""
    path = request.url.path

    # Allow public paths
    if path in PUBLIC_PATHS:
        return await call_next(request)

    # Eval API — authenticate via API key in Authorization header
    if path == "/api/eval/signals":
        if EVAL_API_KEY:
            auth = request.headers.get("Authorization", "")
            if auth == f"Bearer {EVAL_API_KEY}":
                return await call_next(request)
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    # Check session cookie
    session = request.cookies.get("session")
    if not session or not verify_session(session):
        # For API requests, return JSON error
        if path.startswith("/api/"):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        # For page requests, redirect to login
        return RedirectResponse(url="/", status_code=302)

    return await call_next(request)

latest_df: pd.DataFrame | None = None
last_run_status = {"ts": None, "ok": False, "msg": "boot"}
_last_saved_at = 0.0
_df_lock = Lock()
_vix_last: float | None = None  # latest VIX value from TS quotes

# ====== SETUP DETECTOR DEFAULTS ======
_DEFAULT_SETUP_SETTINGS = {
    "gex_long_enabled": True,
    "ag_short_enabled": True,
    "bofa_scalp_enabled": True,
    "absorption_enabled": True,
    "weight_support": 20,
    "weight_upside": 20,
    "weight_floor_cluster": 20,
    "weight_target_cluster": 20,
    "weight_rr": 20,
    "bofa_weight_stability": 20,
    "bofa_weight_width": 20,
    "bofa_weight_charm": 20,
    "bofa_weight_time": 20,
    "bofa_weight_midpoint": 20,
    "bofa_max_proximity": 3,
    "bofa_min_lis_width": 15,
    "bofa_stop_distance": 12,
    "bofa_target_distance": 10,
    "bofa_max_hold_minutes": 30,
    "bofa_cooldown_minutes": 40,
    "abs_pivot_left": 2,
    "abs_pivot_right": 2,
    "abs_vol_window": 10,
    "abs_min_vol_ratio": 1.4,
    "abs_cvd_z_min": 0.5,
    "abs_cvd_std_window": 20,
    "abs_cooldown_bars": 10,
    "abs_weight_divergence": 25,
    "abs_weight_volume": 25,
    "abs_weight_dd": 10,
    "abs_weight_paradigm": 10,
    "abs_weight_lis": 10,
    "abs_weight_lis_side": 10,
    "abs_weight_target_dir": 10,
    "abs_zone_min_away": 5,
    "abs_grade_thresholds": {"A+": 75, "A": 55, "B": 35},
    "brackets": {
        "support": [[5, 100], [10, 75], [15, 50], [20, 25]],
        "upside": [[25, 100], [15, 75], [10, 50]],
        "floor_cluster": [[3, 100], [7, 75], [10, 50]],
        "target_cluster": [[3, 100], [7, 75], [10, 50]],
        "rr": [[3, 100], [2, 75], [1.5, 50], [1, 25]],
    },
    "grade_thresholds": {"A+": 90, "A": 75, "A-Entry": 60},
}

# ====== DB ======
engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else None

def db_init():
    if not engine:
        print("[db] DATABASE_URL missing; history disabled", flush=True)
        return
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS chain_snapshots (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            exp DATE,
            spot DOUBLE PRECISION,
            columns JSONB NOT NULL,
            rows JSONB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_chain_snapshots_ts ON chain_snapshots (ts DESC);
        """))

        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {VOLLAND_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            {VOLLAND_TS_COL} TIMESTAMPTZ NOT NULL DEFAULT now(),
            {VOLLAND_PAYLOAD_COL} JSONB NOT NULL
        );
        """))
        conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS ix_{VOLLAND_TABLE}_{VOLLAND_TS_COL}
        ON {VOLLAND_TABLE} ({VOLLAND_TS_COL} DESC);
        """))

        # Playback snapshots table for historical visualization
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS playback_snapshots (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            spot DOUBLE PRECISION,
            strikes JSONB NOT NULL,
            net_gex JSONB NOT NULL,
            charm JSONB,
            call_vol JSONB NOT NULL,
            put_vol JSONB NOT NULL,
            stats JSONB,
            is_mock BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS ix_playback_snapshots_ts ON playback_snapshots (ts DESC);
        """))
        # Add is_mock column if it doesn't exist (for existing tables)
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE playback_snapshots ADD COLUMN is_mock BOOLEAN DEFAULT FALSE;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE playback_snapshots ADD COLUMN call_gex JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE playback_snapshots ADD COLUMN put_gex JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE playback_snapshots ADD COLUMN call_oi JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE playback_snapshots ADD COLUMN put_oi JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE playback_snapshots ADD COLUMN delta_decay JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))

        # Alert settings table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS alert_settings (
            id INTEGER PRIMARY KEY DEFAULT 1,
            enabled BOOLEAN DEFAULT TRUE,
            lis_enabled BOOLEAN DEFAULT TRUE,
            target_enabled BOOLEAN DEFAULT TRUE,
            max_pos_gamma_enabled BOOLEAN DEFAULT TRUE,
            max_neg_gamma_enabled BOOLEAN DEFAULT TRUE,
            paradigm_change_enabled BOOLEAN DEFAULT TRUE,
            summary_10am_enabled BOOLEAN DEFAULT TRUE,
            summary_2pm_enabled BOOLEAN DEFAULT TRUE,
            volume_spike_enabled BOOLEAN DEFAULT TRUE,
            threshold_points INTEGER DEFAULT 5,
            threshold_volume INTEGER DEFAULT 500,
            cooldown_enabled BOOLEAN DEFAULT TRUE,
            cooldown_minutes INTEGER DEFAULT 10,
            CHECK (id = 1)
        );
        INSERT INTO alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
        """))

        # Setup detector settings table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS setup_settings (
            id INTEGER PRIMARY KEY DEFAULT 1,
            gex_long_enabled BOOLEAN DEFAULT TRUE,
            weight_support INTEGER DEFAULT 20,
            weight_upside INTEGER DEFAULT 20,
            weight_floor_cluster INTEGER DEFAULT 20,
            weight_target_cluster INTEGER DEFAULT 20,
            weight_rr INTEGER DEFAULT 20,
            brackets JSONB,
            grade_thresholds JSONB,
            CHECK (id = 1)
        );
        """))
        conn.execute(text(
            "INSERT INTO setup_settings (id, brackets, grade_thresholds) "
            "VALUES (1, :brackets, :thresholds) ON CONFLICT (id) DO NOTHING"
        ), {
            "brackets": json.dumps(_DEFAULT_SETUP_SETTINGS["brackets"]),
            "thresholds": json.dumps(_DEFAULT_SETUP_SETTINGS["grade_thresholds"]),
        })
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN ag_short_enabled BOOLEAN DEFAULT TRUE;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        # BofA Scalp columns
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN bofa_scalp_enabled BOOLEAN DEFAULT TRUE;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN bofa_settings JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))

        # BofA Scalp extra columns on setup_log
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN bofa_stop_level DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN bofa_target_level DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN bofa_lis_width DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN bofa_max_hold_minutes INTEGER;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN lis_upper DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN comments TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        # Absorption columns on setup_settings
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN absorption_enabled BOOLEAN DEFAULT TRUE;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN absorption_settings JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        # Paradigm Reversal columns on setup_settings
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN paradigm_rev_enabled BOOLEAN DEFAULT TRUE;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_settings ADD COLUMN paradigm_rev_settings JSONB;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        # Absorption extra columns on setup_log
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN abs_vol_ratio DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
        conn.execute(text("""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN abs_es_price DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))

        # Outcome tracking columns on setup_log
        for col, ctype in [
            ("outcome_result", "TEXT"),          # WIN / LOSS / EXPIRED / TIMEOUT
            ("outcome_pnl", "DOUBLE PRECISION"), # P&L in points
            ("outcome_target_level", "DOUBLE PRECISION"),
            ("outcome_stop_level", "DOUBLE PRECISION"),
            ("outcome_max_profit", "DOUBLE PRECISION"),
            ("outcome_max_loss", "DOUBLE PRECISION"),
            ("outcome_first_event", "TEXT"),     # 10pt / target / stop / timeout
            ("outcome_elapsed_min", "INTEGER"),  # minutes from signal to resolution
        ]:
            conn.execute(text(f"""
            DO $$ BEGIN
                ALTER TABLE setup_log ADD COLUMN {col} {ctype};
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$;
            """))

        # VIX column on chain_snapshots, playback_snapshots, setup_log
        for tbl in ("chain_snapshots", "playback_snapshots", "setup_log"):
            conn.execute(text(f"""
            DO $$ BEGIN
                ALTER TABLE {tbl} ADD COLUMN vix DOUBLE PRECISION;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$;
            """))

        # Economic calendar events table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS economic_events (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            title TEXT NOT NULL,
            country TEXT,
            impact TEXT,
            forecast TEXT,
            previous TEXT,
            actual TEXT,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(ts, title, country)
        )
        """))

        # Setup detection log table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS setup_log (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            setup_name TEXT NOT NULL,
            direction TEXT NOT NULL DEFAULT 'long',
            grade TEXT NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            paradigm TEXT,
            spot DOUBLE PRECISION,
            lis DOUBLE PRECISION,
            target DOUBLE PRECISION,
            max_plus_gex DOUBLE PRECISION,
            max_minus_gex DOUBLE PRECISION,
            gap_to_lis DOUBLE PRECISION,
            upside DOUBLE PRECISION,
            rr_ratio DOUBLE PRECISION,
            first_hour BOOLEAN DEFAULT FALSE,
            support_score INTEGER,
            upside_score INTEGER,
            floor_cluster_score INTEGER,
            target_cluster_score INTEGER,
            rr_score INTEGER,
            notified BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS ix_setup_log_ts ON setup_log (ts DESC);
        """))

        # Users table for authentication
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS ix_users_email ON users (email);
        """))

        # Contact messages table for access requests
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS contact_messages (
            id BIGSERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            subject VARCHAR(500),
            message TEXT,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS ix_contact_messages_created ON contact_messages (created_at DESC);
        """))

        # ES cumulative delta snapshots (written by pull_es_delta scheduler job)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS es_delta_snapshots (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            trade_date DATE NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            cumulative_delta BIGINT NOT NULL DEFAULT 0,
            total_volume BIGINT NOT NULL DEFAULT 0,
            buy_volume BIGINT NOT NULL DEFAULT 0,
            sell_volume BIGINT NOT NULL DEFAULT 0,
            last_price DOUBLE PRECISION,
            tick_count BIGINT NOT NULL DEFAULT 0,
            bar_high DOUBLE PRECISION,
            bar_low DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_es_delta_snap_ts ON es_delta_snapshots(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_es_delta_snap_date ON es_delta_snapshots(trade_date DESC);
        """))

        # ES 1-minute delta bars from TradeStation barcharts (UpVolume/DownVolume)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS es_delta_bars (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            trade_date DATE NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            bar_delta BIGINT NOT NULL DEFAULT 0,
            cumulative_delta BIGINT NOT NULL DEFAULT 0,
            bar_volume BIGINT NOT NULL DEFAULT 0,
            bar_buy_volume BIGINT NOT NULL DEFAULT 0,
            bar_sell_volume BIGINT NOT NULL DEFAULT 0,
            bar_open_price DOUBLE PRECISION,
            bar_close_price DOUBLE PRECISION,
            bar_high_price DOUBLE PRECISION,
            bar_low_price DOUBLE PRECISION,
            up_ticks INTEGER NOT NULL DEFAULT 0,
            down_ticks INTEGER NOT NULL DEFAULT 0,
            total_ticks INTEGER NOT NULL DEFAULT 0,
            UNIQUE(ts, symbol)
        );
        CREATE INDEX IF NOT EXISTS idx_es_delta_bars_ts ON es_delta_bars(ts DESC);
        """))

        # ES range bars from streaming quotes (bid/ask delta classification)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS es_range_bars (
            id BIGSERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol VARCHAR(20) NOT NULL DEFAULT '@ES',
            bar_idx INTEGER NOT NULL,
            range_pts DOUBLE PRECISION NOT NULL DEFAULT 5.0,
            bar_open DOUBLE PRECISION NOT NULL,
            bar_high DOUBLE PRECISION NOT NULL,
            bar_low DOUBLE PRECISION NOT NULL,
            bar_close DOUBLE PRECISION NOT NULL,
            bar_volume BIGINT NOT NULL DEFAULT 0,
            bar_buy_volume BIGINT NOT NULL DEFAULT 0,
            bar_sell_volume BIGINT NOT NULL DEFAULT 0,
            bar_delta BIGINT NOT NULL DEFAULT 0,
            cumulative_delta BIGINT NOT NULL DEFAULT 0,
            cvd_open BIGINT NOT NULL DEFAULT 0,
            cvd_high BIGINT NOT NULL DEFAULT 0,
            cvd_low BIGINT NOT NULL DEFAULT 0,
            cvd_close BIGINT NOT NULL DEFAULT 0,
            ts_start TIMESTAMPTZ NOT NULL,
            ts_end TIMESTAMPTZ NOT NULL,
            status VARCHAR(10) NOT NULL DEFAULT 'closed',
            source VARCHAR(10) NOT NULL DEFAULT 'live',
            UNIQUE(trade_date, symbol, bar_idx, range_pts)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS setup_cooldowns (
            trade_date DATE PRIMARY KEY,
            state JSONB NOT NULL DEFAULT '{}'
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS auto_trade_orders (
            setup_log_id BIGINT PRIMARY KEY,
            state JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """))

        # Create default admin user if no users exist
        existing = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        if existing == 0:
            admin_hash = hash_password(os.getenv("ADMIN_PASSWORD", "changeme"))
            conn.execute(text("""
                INSERT INTO users (email, password_hash, is_admin)
                VALUES (:email, :hash, TRUE)
            """), {"email": "faisal.a.d@msn.com", "hash": admin_hash})
            print("[db] created default admin user", flush=True)

    # Load alert settings from database
    load_alert_settings()
    load_setup_settings()
    _load_cooldowns()
    _backfill_outcomes()
    _restore_open_trades()
    print("[db] ready", flush=True)

def load_alert_settings():
    """Load alert settings from database into memory."""
    global _alert_settings
    if not engine:
        return
    try:
        with engine.begin() as conn:
            row = conn.execute(text("SELECT * FROM alert_settings WHERE id = 1")).mappings().first()
            if row:
                _alert_settings = {
                    "enabled": row["enabled"],
                    "lis_enabled": row["lis_enabled"],
                    "target_enabled": row["target_enabled"],
                    "max_pos_gamma_enabled": row["max_pos_gamma_enabled"],
                    "max_neg_gamma_enabled": row["max_neg_gamma_enabled"],
                    "paradigm_change_enabled": row["paradigm_change_enabled"],
                    "summary_10am_enabled": row["summary_10am_enabled"],
                    "summary_2pm_enabled": row["summary_2pm_enabled"],
                    "volume_spike_enabled": row["volume_spike_enabled"],
                    "threshold_points": row["threshold_points"],
                    "threshold_volume": row["threshold_volume"],
                    "cooldown_enabled": row["cooldown_enabled"],
                    "cooldown_minutes": row["cooldown_minutes"],
                }
                print("[alerts] settings loaded from db", flush=True)
    except Exception as e:
        print(f"[alerts] failed to load settings: {e}", flush=True)

def save_alert_settings():
    """Save current alert settings to database."""
    if not engine:
        return False
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE alert_settings SET
                    enabled = :enabled,
                    lis_enabled = :lis_enabled,
                    target_enabled = :target_enabled,
                    max_pos_gamma_enabled = :max_pos_gamma_enabled,
                    max_neg_gamma_enabled = :max_neg_gamma_enabled,
                    paradigm_change_enabled = :paradigm_change_enabled,
                    summary_10am_enabled = :summary_10am_enabled,
                    summary_2pm_enabled = :summary_2pm_enabled,
                    volume_spike_enabled = :volume_spike_enabled,
                    threshold_points = :threshold_points,
                    threshold_volume = :threshold_volume,
                    cooldown_enabled = :cooldown_enabled,
                    cooldown_minutes = :cooldown_minutes
                WHERE id = 1
            """), _alert_settings)
        return True
    except Exception as e:
        print(f"[alerts] failed to save settings: {e}", flush=True)
        return False

# ====== SETUP DETECTOR SETTINGS ======
_setup_settings = dict(_DEFAULT_SETUP_SETTINGS)

def load_setup_settings():
    """Load setup detector settings from database into memory."""
    global _setup_settings
    if not engine:
        return
    try:
        with engine.begin() as conn:
            row = conn.execute(text("SELECT * FROM setup_settings WHERE id = 1")).mappings().first()
            if row:
                rk = row.keys()
                # Load BofA settings from JSONB column or defaults
                bofa_db = {}
                if "bofa_settings" in rk and row["bofa_settings"]:
                    raw = row["bofa_settings"]
                    bofa_db = raw if isinstance(raw, dict) else json.loads(raw)
                # Load Absorption settings from JSONB column or defaults
                abs_db = {}
                if "absorption_settings" in rk and row["absorption_settings"]:
                    raw = row["absorption_settings"]
                    abs_db = raw if isinstance(raw, dict) else json.loads(raw)
                # Load Paradigm Reversal settings from JSONB column or defaults
                pr_db = {}
                if "paradigm_rev_settings" in rk and row["paradigm_rev_settings"]:
                    raw = row["paradigm_rev_settings"]
                    pr_db = raw if isinstance(raw, dict) else json.loads(raw)
                _setup_settings = {
                    "gex_long_enabled": row["gex_long_enabled"],
                    "ag_short_enabled": row["ag_short_enabled"] if "ag_short_enabled" in rk else True,
                    "bofa_scalp_enabled": row["bofa_scalp_enabled"] if "bofa_scalp_enabled" in rk else True,
                    "absorption_enabled": row["absorption_enabled"] if "absorption_enabled" in rk else True,
                    "weight_support": row["weight_support"],
                    "weight_upside": row["weight_upside"],
                    "weight_floor_cluster": row["weight_floor_cluster"],
                    "weight_target_cluster": row["weight_target_cluster"],
                    "weight_rr": row["weight_rr"],
                    "bofa_weight_stability": bofa_db.get("weight_stability", 20),
                    "bofa_weight_width": bofa_db.get("weight_width", 20),
                    "bofa_weight_charm": bofa_db.get("weight_charm", 20),
                    "bofa_weight_time": bofa_db.get("weight_time", 20),
                    "bofa_weight_midpoint": bofa_db.get("weight_midpoint", 20),
                    "bofa_max_proximity": bofa_db.get("max_proximity", 5),
                    "bofa_min_lis_width": bofa_db.get("min_lis_width", 15),
                    "bofa_stop_distance": bofa_db.get("stop_distance", 12),
                    "bofa_target_distance": bofa_db.get("target_distance", 10),
                    "bofa_max_hold_minutes": bofa_db.get("max_hold_minutes", 30),
                    "bofa_cooldown_minutes": bofa_db.get("cooldown_minutes", 40),
                    "abs_pivot_left": abs_db.get("pivot_left", 2),
                    "abs_pivot_right": abs_db.get("pivot_right", 2),
                    "abs_vol_window": abs_db.get("vol_window", 10),
                    "abs_min_vol_ratio": abs_db.get("min_vol_ratio", 1.4),
                    "abs_cvd_z_min": abs_db.get("cvd_z_min", 0.5),
                    "abs_cvd_std_window": abs_db.get("cvd_std_window", 20),
                    "abs_cooldown_bars": abs_db.get("cooldown_bars", 10),
                    "abs_weight_divergence": abs_db.get("weight_divergence", 25),
                    "abs_weight_volume": abs_db.get("weight_volume", 25),
                    "abs_weight_dd": abs_db.get("weight_dd", 10),
                    "abs_weight_paradigm": abs_db.get("weight_paradigm", 10),
                    "abs_weight_lis": abs_db.get("weight_lis", 10),
                    "abs_weight_lis_side": abs_db.get("weight_lis_side", 10),
                    "abs_weight_target_dir": abs_db.get("weight_target_dir", 10),
                    "abs_grade_thresholds": abs_db.get("grade_thresholds", {"A+": 75, "A": 55, "B": 35}),
                    "paradigm_rev_enabled": row["paradigm_rev_enabled"] if "paradigm_rev_enabled" in rk else True,
                    "pr_max_flip_age_s": pr_db.get("max_flip_age_s", 180),
                    "pr_max_lis_distance": pr_db.get("max_lis_distance", 5),
                    "pr_cooldown_minutes": pr_db.get("cooldown_minutes", 30),
                    "pr_weight_proximity": pr_db.get("weight_proximity", 25),
                    "pr_weight_es_volume": pr_db.get("weight_es_volume", 25),
                    "pr_weight_charm": pr_db.get("weight_charm", 20),
                    "pr_weight_dd": pr_db.get("weight_dd", 15),
                    "pr_weight_time": pr_db.get("weight_time", 15),
                    "pr_grade_thresholds": pr_db.get("grade_thresholds", {"A+": 80, "A": 60, "A-Entry": 45}),
                    "brackets": row["brackets"] if isinstance(row["brackets"], dict) else json.loads(row["brackets"]) if row["brackets"] else _DEFAULT_SETUP_SETTINGS["brackets"],
                    "grade_thresholds": row["grade_thresholds"] if isinstance(row["grade_thresholds"], dict) else json.loads(row["grade_thresholds"]) if row["grade_thresholds"] else _DEFAULT_SETUP_SETTINGS["grade_thresholds"],
                }
                print("[setups] settings loaded from db", flush=True)
    except Exception as e:
        print(f"[setups] failed to load settings: {e}", flush=True)

def save_setup_settings():
    """Save current setup detector settings to database."""
    if not engine:
        return False
    try:
        with engine.begin() as conn:
            bofa_json = json.dumps({
                "weight_stability": _setup_settings.get("bofa_weight_stability", 20),
                "weight_width": _setup_settings.get("bofa_weight_width", 20),
                "weight_charm": _setup_settings.get("bofa_weight_charm", 20),
                "weight_time": _setup_settings.get("bofa_weight_time", 20),
                "weight_midpoint": _setup_settings.get("bofa_weight_midpoint", 20),
                "max_proximity": _setup_settings.get("bofa_max_proximity", 5),
                "min_lis_width": _setup_settings.get("bofa_min_lis_width", 15),
                "stop_distance": _setup_settings.get("bofa_stop_distance", 12),
                "target_distance": _setup_settings.get("bofa_target_distance", 10),
                "max_hold_minutes": _setup_settings.get("bofa_max_hold_minutes", 30),
                "cooldown_minutes": _setup_settings.get("bofa_cooldown_minutes", 40),
            })
            abs_json = json.dumps({
                "pivot_left": _setup_settings.get("abs_pivot_left", 2),
                "pivot_right": _setup_settings.get("abs_pivot_right", 2),
                "vol_window": _setup_settings.get("abs_vol_window", 10),
                "min_vol_ratio": _setup_settings.get("abs_min_vol_ratio", 1.4),
                "cvd_z_min": _setup_settings.get("abs_cvd_z_min", 0.5),
                "cvd_std_window": _setup_settings.get("abs_cvd_std_window", 20),
                "cooldown_bars": _setup_settings.get("abs_cooldown_bars", 10),
                "weight_divergence": _setup_settings.get("abs_weight_divergence", 25),
                "weight_volume": _setup_settings.get("abs_weight_volume", 25),
                "weight_dd": _setup_settings.get("abs_weight_dd", 10),
                "weight_paradigm": _setup_settings.get("abs_weight_paradigm", 10),
                "weight_lis": _setup_settings.get("abs_weight_lis", 10),
                "weight_lis_side": _setup_settings.get("abs_weight_lis_side", 10),
                "weight_target_dir": _setup_settings.get("abs_weight_target_dir", 10),
                "grade_thresholds": _setup_settings.get("abs_grade_thresholds", {"A+": 75, "A": 55, "B": 35}),
            })
            pr_json = json.dumps({
                "max_flip_age_s": _setup_settings.get("pr_max_flip_age_s", 180),
                "max_lis_distance": _setup_settings.get("pr_max_lis_distance", 5),
                "cooldown_minutes": _setup_settings.get("pr_cooldown_minutes", 30),
                "weight_proximity": _setup_settings.get("pr_weight_proximity", 25),
                "weight_es_volume": _setup_settings.get("pr_weight_es_volume", 25),
                "weight_charm": _setup_settings.get("pr_weight_charm", 20),
                "weight_dd": _setup_settings.get("pr_weight_dd", 15),
                "weight_time": _setup_settings.get("pr_weight_time", 15),
                "grade_thresholds": _setup_settings.get("pr_grade_thresholds", {"A+": 80, "A": 60, "A-Entry": 45}),
            })
            conn.execute(text("""
                UPDATE setup_settings SET
                    gex_long_enabled = :gex_long_enabled,
                    ag_short_enabled = :ag_short_enabled,
                    bofa_scalp_enabled = :bofa_scalp_enabled,
                    absorption_enabled = :absorption_enabled,
                    paradigm_rev_enabled = :paradigm_rev_enabled,
                    weight_support = :weight_support,
                    weight_upside = :weight_upside,
                    weight_floor_cluster = :weight_floor_cluster,
                    weight_target_cluster = :weight_target_cluster,
                    weight_rr = :weight_rr,
                    brackets = :brackets,
                    grade_thresholds = :grade_thresholds,
                    bofa_settings = :bofa_settings,
                    absorption_settings = :absorption_settings,
                    paradigm_rev_settings = :paradigm_rev_settings
                WHERE id = 1
            """), {
                "gex_long_enabled": _setup_settings["gex_long_enabled"],
                "ag_short_enabled": _setup_settings.get("ag_short_enabled", True),
                "bofa_scalp_enabled": _setup_settings.get("bofa_scalp_enabled", True),
                "absorption_enabled": _setup_settings.get("absorption_enabled", True),
                "paradigm_rev_enabled": _setup_settings.get("paradigm_rev_enabled", True),
                "weight_support": _setup_settings["weight_support"],
                "weight_upside": _setup_settings["weight_upside"],
                "weight_floor_cluster": _setup_settings["weight_floor_cluster"],
                "weight_target_cluster": _setup_settings["weight_target_cluster"],
                "weight_rr": _setup_settings["weight_rr"],
                "brackets": json.dumps(_setup_settings.get("brackets", _DEFAULT_SETUP_SETTINGS["brackets"])),
                "grade_thresholds": json.dumps(_setup_settings.get("grade_thresholds", _DEFAULT_SETUP_SETTINGS["grade_thresholds"])),
                "bofa_settings": bofa_json,
                "absorption_settings": abs_json,
                "paradigm_rev_settings": pr_json,
            })
        return True
    except Exception as e:
        print(f"[setups] failed to save settings: {e}", flush=True)
        return False

def _load_cooldowns():
    """Load today's setup cooldown state from DB."""
    if not engine:
        return
    try:
        from app.setup_detector import import_cooldowns
        today = datetime.now(NY).strftime("%Y-%m-%d")
        with engine.begin() as conn:
            row = conn.execute(text(
                "SELECT state FROM setup_cooldowns WHERE trade_date = :d"
            ), {"d": today}).mappings().first()
        if row and row["state"]:
            import_cooldowns(row["state"])
            print(f"[setups] cooldowns restored for {today}", flush=True)
    except Exception as e:
        print(f"[setups] cooldown load error (non-fatal): {e}", flush=True)

def _backfill_outcomes():
    """Backfill outcome results for setup_log entries that have no outcome yet.

    Runs once on startup. Uses _calculate_setup_outcome() to compute
    WIN/LOSS/EXPIRED for each historical signal from price history.
    """
    if not engine:
        return
    try:
        # NOTE: One-time migration resets removed (2026-02-24).
        # Previously reset EXPIRED and AG Short WIN outcomes on every startup to recalculate.
        # This was destructive — overwrote accurate live tracker values (30s polling + session H/L)
        # with less accurate backfill values (2-min playback snapshots).
        # Backfill now ONLY fills NULL outcomes (never overwrites existing ones).
        # During market hours, skip today's trades — let the live tracker handle them
        # (backfill with incomplete price data persists wrong outcomes)
        _now = now_et()
        _market_open = dtime(9, 30) <= _now.time() <= dtime(16, 5)
        _today_str = _now.strftime("%Y-%m-%d")

        if _market_open:
            _backfill_query = """
                SELECT id, ts, setup_name, direction, grade, score,
                       paradigm, spot, lis, target, max_plus_gex, max_minus_gex,
                       bofa_stop_level, bofa_target_level, bofa_max_hold_minutes,
                       abs_vol_ratio, abs_es_price
                FROM setup_log
                WHERE outcome_result IS NULL
                  AND ts < :today_start
                ORDER BY ts ASC
            """
            _params = {"today_start": f"{_today_str} 00:00:00-05:00"}
        else:
            _backfill_query = """
                SELECT id, ts, setup_name, direction, grade, score,
                       paradigm, spot, lis, target, max_plus_gex, max_minus_gex,
                       bofa_stop_level, bofa_target_level, bofa_max_hold_minutes,
                       abs_vol_ratio, abs_es_price
                FROM setup_log
                WHERE outcome_result IS NULL
                ORDER BY ts ASC
            """
            _params = {}

        with engine.begin() as conn:
            rows = conn.execute(text(_backfill_query), _params).mappings().all()

        if not rows:
            msg = "[backfill] all setup_log entries have outcomes"
            if _market_open:
                msg += " (today's trades deferred to live tracker)"
            print(msg, flush=True)
            return

        print(f"[backfill] computing outcomes for {len(rows)} signals...", flush=True)
        filled = 0
        for row in rows:
            entry = dict(row)
            outcome = _calculate_setup_outcome(entry)
            if not outcome or outcome.get("no_data") or outcome.get("error"):
                continue

            fe = outcome.get("first_event")
            if fe in ("10pt", "target"):
                result_type = "WIN"
            elif fe == "stop":
                result_type = "LOSS"
            elif fe == "timeout":
                result_type = "EXPIRED"
            else:
                # No event hit — expired at market close
                result_type = "EXPIRED"

            # Calculate P&L
            is_long = entry.get("direction", "long").lower() in ("long", "bullish")
            spot = entry.get("spot") or 0
            es_price = entry.get("abs_es_price")
            entry_price = es_price if entry.get("setup_name") == "ES Absorption" and es_price else spot

            is_trailing_setup = entry.get("setup_name") in ("DD Exhaustion", "GEX Long", "AG Short")
            if is_trailing_setup:
                # Trailing stop: P&L = final stop level - entry (or timeout P&L)
                if result_type == "EXPIRED":
                    pnl = outcome.get("timeout_pnl", 0) or 0
                else:
                    sl = outcome.get("trail_final_stop") or outcome.get("dd_final_stop") or outcome.get("stop_level", 0)
                    pnl = (sl - entry_price) if is_long else (entry_price - sl)
            elif result_type == "WIN":
                # Use full target if hit, otherwise use 10pt level
                full_tgt = outcome.get("target_level")
                if outcome.get("hit_target") and full_tgt:
                    pnl = abs(full_tgt - entry_price) if is_long else abs(entry_price - full_tgt)
                else:
                    tgt = outcome.get("ten_pt_level") or outcome.get("bofa_target_level")
                    if tgt:
                        pnl = abs(tgt - entry_price) if is_long else abs(entry_price - tgt)
                    else:
                        pnl = outcome.get("max_profit", 0)
            elif result_type == "LOSS":
                sl = outcome.get("stop_level", 0)
                pnl = -(abs(entry_price - sl)) if is_long else -(abs(sl - entry_price))
            else:
                pnl = outcome.get("max_profit", 0) if outcome.get("max_profit", 0) != 0 else outcome.get("max_loss", 0)

            # Elapsed time
            elapsed = None
            time_key = {"10pt": "time_to_10pt", "target": "time_to_target", "stop": "time_to_stop"}.get(fe)
            if time_key and outcome.get(time_key):
                try:
                    t = datetime.fromisoformat(outcome[time_key])
                    elapsed = int((t - entry["ts"]).total_seconds() / 60)
                except Exception:
                    pass

            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE setup_log SET
                        outcome_result = :res,
                        outcome_pnl = :pnl,
                        outcome_target_level = :tgt,
                        outcome_stop_level = :sl,
                        outcome_max_profit = :mp,
                        outcome_max_loss = :ml,
                        outcome_first_event = :fe,
                        outcome_elapsed_min = :em
                    WHERE id = :id
                """), {
                    "res": result_type,
                    "pnl": round(pnl, 2) if pnl is not None else None,
                    "tgt": outcome.get("target_level") or outcome.get("ten_pt_level") or outcome.get("bofa_target_level"),
                    "sl": outcome.get("stop_level"),
                    "mp": outcome.get("max_profit"),
                    "ml": outcome.get("max_loss"),
                    "fe": fe,
                    "em": elapsed,
                    "id": entry["id"],
                })
            filled += 1

        print(f"[backfill] filled {filled}/{len(rows)} outcomes", flush=True)

        # Second pass: patch legacy rows that have outcome_result but NULL outcome_first_event
        # (live-resolved trades before this fix was deployed)
        with engine.begin() as conn:
            legacy_rows = conn.execute(text("""
                SELECT id, setup_name, outcome_result, outcome_pnl, outcome_max_profit
                FROM setup_log
                WHERE outcome_result IS NOT NULL
                  AND (outcome_first_event IS NULL OR outcome_max_profit IS NULL)
            """)).mappings().all()

        if legacy_rows:
            patched = 0
            trailing_setups = ("DD Exhaustion", "GEX Long", "AG Short")
            for lr in legacy_rows:
                res = lr["outcome_result"]
                sname = lr["setup_name"]
                pnl_val = lr["outcome_pnl"]
                is_trailing = sname in trailing_setups
                if res == "WIN":
                    fe = "target" if is_trailing else "10pt"
                elif res == "LOSS":
                    fe = "stop"
                else:  # EXPIRED
                    fe = "timeout"
                # Approximate max_profit from P&L for legacy rows (actual max >= final P&L for wins)
                mp = lr["outcome_max_profit"]
                if mp is None and pnl_val is not None:
                    mp = max(pnl_val, 0)  # conservative: at least the final P&L if positive
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE setup_log SET
                            outcome_first_event = COALESCE(outcome_first_event, :fe),
                            outcome_max_profit = COALESCE(outcome_max_profit, :mp)
                        WHERE id = :id
                    """), {"fe": fe, "mp": mp, "id": lr["id"]})
                patched += 1
            print(f"[backfill] patched outcome_first_event/max_profit for {patched} legacy rows", flush=True)

    except Exception as e:
        print(f"[backfill] error (non-fatal): {e}", flush=True)
        import traceback
        traceback.print_exc()


def _restore_open_trades():
    """Restore today's unresolved trades to _setup_open_trades on startup.

    After a service restart, in-memory _setup_open_trades is empty.
    This queries setup_log for today's trades with no outcome and re-adds them
    so the live tracker continues monitoring them.

    For each restored trade, queries historical price extremes (min/max spot since
    entry) so the live tracker immediately sees any target/stop hits that occurred
    before the restart.
    """
    global _setup_open_trades
    if not engine:
        return
    try:
        _now = now_et()
        # Only restore during market hours (before EOD summary at 16:05)
        if not (dtime(9, 30) <= _now.time() <= dtime(16, 5)):
            return
        _today_str = _now.strftime("%Y-%m-%d")
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, ts, setup_name, direction, grade, score,
                       spot, lis, target, max_plus_gex, max_minus_gex,
                       bofa_stop_level, bofa_target_level, bofa_max_hold_minutes,
                       abs_vol_ratio, abs_es_price
                FROM setup_log
                WHERE outcome_result IS NULL
                  AND ts >= :today_start
                ORDER BY ts ASC
            """), {"today_start": f"{_today_str} 00:00:00-05:00"}).mappings().all()

        if not rows:
            print("[restore] no unresolved trades to restore", flush=True)
            return

        _trailing_setups = ("DD Exhaustion", "GEX Long", "AG Short")
        restored = 0
        for row in rows:
            entry = dict(row)
            setup_name = entry["setup_name"]
            direction = entry.get("direction", "long")
            spot = entry.get("spot")
            if not spot:
                continue

            # Rebuild the result_data dict needed by _compute_setup_levels
            r = {
                "setup_name": setup_name,
                "direction": direction,
                "spot": spot,
                "lis": entry.get("lis"),
                "target": entry.get("target"),
                "max_plus_gex": entry.get("max_plus_gex"),
                "max_minus_gex": entry.get("max_minus_gex"),
                "bofa_stop_level": entry.get("bofa_stop_level"),
                "bofa_target_level": entry.get("bofa_target_level"),
                "bofa_max_hold_minutes": entry.get("bofa_max_hold_minutes"),
                "abs_es_price": entry.get("abs_es_price"),
            }
            target_lvl, stop_lvl = _compute_setup_levels(r)
            if stop_lvl is None:
                continue
            if target_lvl is None and setup_name not in _trailing_setups:
                continue

            # Reconstruct the trade entry
            ts = entry["ts"]
            if ts.tzinfo is None:
                ts = NY.localize(ts)

            # Query historical price extremes since entry to seed _seen_low/_seen_high
            # Without this, restored trades would miss target/stop hits before restart
            is_long = direction.lower() in ("long", "bullish")
            if setup_name == "ES Absorption":
                # ES Absorption uses ES range bar H/L, not SPX playback
                es_px = entry.get("abs_es_price") or spot
                seen_high = es_px
                seen_low = es_px
                dd_max_fav = 0.0
                try:
                    with engine.begin() as conn:
                        extremes = conn.execute(text("""
                            SELECT MAX(bar_high) as hi, MIN(bar_low) as lo
                            FROM es_range_bars
                            WHERE trade_date = :td AND source = 'rithmic'
                              AND ts_end >= :entry_ts AND ts_end <= NOW()
                        """), {"td": ts.strftime("%Y-%m-%d"), "entry_ts": ts}).mappings().first()
                    if extremes and extremes["hi"] is not None:
                        seen_high = extremes["hi"]
                        seen_low = extremes["lo"]
                except Exception:
                    pass  # fall back to es_px as default
            else:
                seen_high = spot
                seen_low = spot
                dd_max_fav = 0.0
                try:
                    with engine.begin() as conn:
                        extremes = conn.execute(text("""
                            SELECT MAX(spot) as hi, MIN(spot) as lo
                            FROM playback_snapshots
                            WHERE ts >= :entry_ts AND ts <= NOW()
                        """), {"entry_ts": ts}).mappings().first()
                    if extremes and extremes["hi"] is not None:
                        seen_high = extremes["hi"]
                        seen_low = extremes["lo"]
                        fav = (seen_high - spot) if is_long else (spot - seen_low)
                        dd_max_fav = max(0.0, fav)
                except Exception:
                    pass  # fall back to spot as default

            _setup_open_trades.append({
                "setup_name": setup_name,
                "direction": direction,
                "spot": spot,
                "grade": entry.get("grade", ""),
                "target_level": target_lvl,
                "stop_level": stop_lvl,
                "ts": ts,
                "result_data": r,
                "max_hold_minutes": entry.get("bofa_max_hold_minutes"),
                "_trade_date": _now.date(),
                "setup_log_id": entry["id"],
                "_dd_max_fav": dd_max_fav,
                "_seen_high": seen_high,
                "_seen_low": seen_low,
            })
            restored += 1
            print(f"[restore] {setup_name} {direction} id={entry['id']} spot={spot:.1f} "
                  f"seen_lo={seen_low:.1f} seen_hi={seen_high:.1f}", flush=True)

        print(f"[restore] restored {restored} open trades to live tracker", flush=True)
    except Exception as e:
        print(f"[restore] error (non-fatal): {e}", flush=True)
        import traceback
        traceback.print_exc()


def _save_cooldowns():
    """Persist current cooldown state to DB."""
    if not engine:
        return
    try:
        from app.setup_detector import export_cooldowns
        today = datetime.now(NY).strftime("%Y-%m-%d")
        state = export_cooldowns()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO setup_cooldowns (trade_date, state)
                VALUES (:d, :s)
                ON CONFLICT (trade_date) DO UPDATE SET state = :s
            """), {"d": today, "s": json.dumps(state)})
    except Exception as e:
        print(f"[setups] cooldown save error (non-fatal): {e}", flush=True)

# Track current setup log ID per setup type (for UPDATE on improvements)
_current_setup_log = {
    "GEX Long": None,
    "AG Short": None,
    "BofA Scalp": None,
    "ES Absorption": None,
    "DD Exhaustion": None,
    "last_date": None,
}

# Live outcome tracking: open trades awaiting resolution, resolved trades for EOD summary
_setup_open_trades = []
# Each entry: {setup_name, direction, spot, target_level, stop_level, ts, grade, result_data, max_hold_minutes}
_setup_resolved_trades = []
# Each entry: {setup_name, direction, spot, target_level, stop_level, ts, grade, result_type, pnl, elapsed_min, result_data}
# Session H/L tracking: derive intra-cycle extremes from TS API session High/Low
_spx_session = {"high": None, "low": None, "date": None}  # previous poll's session H/L
_spx_cycle_high = None  # derived cycle high (max of spot & any new session high)
_spx_cycle_low = None   # derived cycle low  (min of spot & any new session low)

def log_setup(result_wrapper):
    """
    Insert or update a detection in setup_log table.
    - new/reformed: INSERT new row, store log ID
    - grade_upgrade/gap_improvement: UPDATE existing row
    """
    global _current_setup_log
    if not engine:
        return

    r = result_wrapper["result"]
    reason = result_wrapper.get("notify_reason")
    setup_name = r["setup_name"]

    # Reset tracking on new day
    today = now_et().date()
    if _current_setup_log["last_date"] != today:
        _current_setup_log = {"GEX Long": None, "AG Short": None, "BofA Scalp": None, "ES Absorption": None, "Paradigm Reversal": None, "DD Exhaustion": None, "last_date": today}

    try:
        with engine.begin() as conn:
            if reason in ("new", "reformed") or _current_setup_log.get(setup_name) is None:
                # INSERT new row
                insert_params = dict(r)
                # BofA Scalp extra columns (NULL for GEX/AG/Absorption)
                insert_params.setdefault("bofa_stop_level", r.get("bofa_stop_level"))
                insert_params.setdefault("bofa_target_level", r.get("bofa_target_level"))
                insert_params.setdefault("bofa_lis_width", r.get("bofa_lis_width"))
                insert_params.setdefault("bofa_max_hold_minutes", r.get("bofa_max_hold_minutes"))
                insert_params["lis_upper_val"] = r.get("lis_upper")
                # Absorption extra columns (NULL for other setups)
                insert_params.setdefault("abs_vol_ratio", r.get("abs_vol_ratio"))
                insert_params.setdefault("abs_es_price", r.get("abs_es_price"))
                insert_params["vix"] = _vix_last
                result = conn.execute(text("""
                    INSERT INTO setup_log
                        (setup_name, direction, grade, score, paradigm, spot, lis, target,
                         max_plus_gex, max_minus_gex, gap_to_lis, upside, rr_ratio,
                         first_hour, support_score, upside_score, floor_cluster_score,
                         target_cluster_score, rr_score, notified,
                         bofa_stop_level, bofa_target_level, bofa_lis_width, bofa_max_hold_minutes, lis_upper,
                         abs_vol_ratio, abs_es_price, vix)
                    VALUES
                        (:setup_name, :direction, :grade, :score, :paradigm, :spot, :lis, :target,
                         :max_plus_gex, :max_minus_gex, :gap_to_lis, :upside, :rr_ratio,
                         :first_hour, :support_score, :upside_score, :floor_cluster_score,
                         :target_cluster_score, :rr_score, TRUE,
                         :bofa_stop_level, :bofa_target_level, :bofa_lis_width, :bofa_max_hold_minutes, :lis_upper_val,
                         :abs_vol_ratio, :abs_es_price, :vix)
                    RETURNING id
                """), insert_params)
                log_id = result.fetchone()[0]
                _current_setup_log[setup_name] = log_id
                print(f"[setups] logged new setup id={log_id}", flush=True)
            else:
                # UPDATE existing row (grade_upgrade or gap_improvement)
                log_id = _current_setup_log[setup_name]
                conn.execute(text("""
                    UPDATE setup_log SET
                        grade = :grade, score = :score, spot = :spot,
                        gap_to_lis = :gap_to_lis, upside = :upside, rr_ratio = :rr_ratio,
                        support_score = :support_score, upside_score = :upside_score,
                        floor_cluster_score = :floor_cluster_score, target_cluster_score = :target_cluster_score,
                        rr_score = :rr_score, vix = :vix, ts = NOW()
                    WHERE id = :log_id
                """), {**r, "log_id": log_id, "vix": _vix_last})
                print(f"[setups] updated setup id={log_id} ({reason})", flush=True)
    except Exception as e:
        print(f"[setups] failed to log: {e}", flush=True)

def send_telegram_setups(message: str) -> bool:
    """Send a message to the setups Telegram channel (falls back to main channel)."""
    chat_id = TELEGRAM_CHAT_ID_SETUPS or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        print("[setups-tg] missing token or chat_id", flush=True)
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.status_code == 200:
            print(f"[setups-tg] sent: {message[:50]}...", flush=True)
            return True
        else:
            print(f"[setups-tg] error: {resp.status_code} {resp.text}", flush=True)
            return False
    except Exception as e:
        print(f"[setups-tg] exception: {e}", flush=True)
        return False

def _json_load_maybe(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "ignore")
        except Exception:
            pass
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return v
    return v

def _parse_dd_numeric(dd_str):
    """Parse DD hedging string like '$7,298,110,681' to numeric value."""
    if not dd_str:
        return None
    try:
        return float(str(dd_str).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return None

def db_latest_volland() -> Optional[dict]:
    if not engine:
        return None
    q = text(f"SELECT {VOLLAND_TS_COL} AS ts, {VOLLAND_PAYLOAD_COL} AS payload FROM {VOLLAND_TABLE} ORDER BY {VOLLAND_TS_COL} DESC LIMIT 1")
    with engine.begin() as conn:
        r = conn.execute(q).mappings().first()
    if not r:
        return None
    payload = _json_load_maybe(r["payload"])
    ts = r["ts"]
    return {"ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts), "payload": payload}

def db_volland_history(limit: int = 500) -> list[dict]:
    if not engine:
        return []
    q = text(f"SELECT {VOLLAND_TS_COL} AS ts, {VOLLAND_PAYLOAD_COL} AS payload FROM {VOLLAND_TABLE} ORDER BY {VOLLAND_TS_COL} DESC LIMIT :lim")
    with engine.begin() as conn:
        rows = conn.execute(q, {"lim": int(limit)}).mappings().all()
    out = []
    for r in rows:
        payload = _json_load_maybe(r["payload"])
        ts = r["ts"]
        out.append({"ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts), "payload": payload})
    return out

def db_volland_vanna_window(limit: int = 40) -> dict:
    """
    Returns latest 'limit' strikes centered on the current spot price.
    Reads directly from volland_exposure_points (greek='charm').
    Falls back to max-abs-charm strike if current_price is not available.
    """
    if not engine:
        raise RuntimeError("DATABASE_URL not set")

    lim = int(limit)
    if lim < 5: lim = 5
    if lim > 200: lim = 200

    sql = text("""
    WITH latest AS (
      SELECT max(ts_utc) AS ts_utc
      FROM volland_exposure_points
      WHERE greek = 'charm'
    ),
    center AS (
      SELECT COALESCE(
        (SELECT v.current_price::numeric
         FROM volland_exposure_points v
         JOIN latest l ON v.ts_utc = l.ts_utc
         WHERE v.greek = 'charm' AND v.current_price IS NOT NULL
         LIMIT 1),
        (SELECT v.strike::numeric
         FROM volland_exposure_points v
         JOIN latest l ON v.ts_utc = l.ts_utc
         WHERE v.greek = 'charm'
         ORDER BY abs(v.value::numeric) DESC
         LIMIT 1)
      ) AS mid_strike
    ),
    ranked AS (
      SELECT
        v.ts_utc,
        v.strike::numeric AS strike,
        v.value::numeric  AS vanna,
        c.mid_strike,
        (v.strike::numeric - c.mid_strike) AS rel,
        ROW_NUMBER() OVER (
          ORDER BY abs(v.strike::numeric - c.mid_strike), v.strike::numeric
        ) AS rn
      FROM volland_exposure_points v
      JOIN latest l ON v.ts_utc = l.ts_utc
      CROSS JOIN center c
      WHERE v.greek = 'charm'
    )
    SELECT ts_utc, strike, vanna, mid_strike, rel
    FROM ranked
    WHERE rn <= :lim
    ORDER BY strike;
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"lim": lim}).mappings().all()

    if not rows:
        return {"ts_utc": None, "mid_strike": None, "mid_vanna": None, "points": []}

    ts_utc = rows[0]["ts_utc"]
    mid_strike = rows[0]["mid_strike"]

    pts = []
    for r in rows:
        pts.append({
            "strike": float(r["strike"]) if r["strike"] is not None else None,
            "vanna":  float(r["vanna"])  if r["vanna"]  is not None else None,
            "rel":    float(r["rel"])    if r["rel"]    is not None else None,
        })

    return {
        "ts_utc": ts_utc.isoformat() if hasattr(ts_utc, "isoformat") else str(ts_utc),
        "mid_strike": float(mid_strike) if mid_strike is not None else None,
        "mid_vanna": None,
        "points": pts
    }

def db_volland_delta_decay_window(limit: int = 40) -> dict:
    """
    Returns latest 'limit' strikes centered on the current spot price.
    Reads directly from volland_exposure_points (greek='deltaDecay').
    Falls back to max-abs-value strike if current_price is not available.
    """
    if not engine:
        raise RuntimeError("DATABASE_URL not set")

    lim = int(limit)
    if lim < 5: lim = 5
    if lim > 200: lim = 200

    sql = text("""
    WITH latest AS (
      SELECT max(ts_utc) AS ts_utc
      FROM volland_exposure_points
      WHERE greek = 'deltaDecay'
    ),
    center AS (
      SELECT COALESCE(
        (SELECT v.current_price::numeric
         FROM volland_exposure_points v
         JOIN latest l ON v.ts_utc = l.ts_utc
         WHERE v.greek = 'deltaDecay' AND v.current_price IS NOT NULL
         LIMIT 1),
        (SELECT v.strike::numeric
         FROM volland_exposure_points v
         JOIN latest l ON v.ts_utc = l.ts_utc
         WHERE v.greek = 'deltaDecay'
         ORDER BY abs(v.value::numeric) DESC
         LIMIT 1)
      ) AS mid_strike
    ),
    ranked AS (
      SELECT
        v.ts_utc,
        v.strike::numeric AS strike,
        v.value::numeric  AS delta_decay,
        c.mid_strike,
        (v.strike::numeric - c.mid_strike) AS rel,
        ROW_NUMBER() OVER (
          ORDER BY abs(v.strike::numeric - c.mid_strike), v.strike::numeric
        ) AS rn
      FROM volland_exposure_points v
      JOIN latest l ON v.ts_utc = l.ts_utc
      CROSS JOIN center c
      WHERE v.greek = 'deltaDecay'
    )
    SELECT ts_utc, strike, delta_decay, mid_strike, rel
    FROM ranked
    WHERE rn <= :lim
    ORDER BY strike;
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"lim": lim}).mappings().all()

    if not rows:
        return {"ts_utc": None, "mid_strike": None, "points": []}

    ts_utc = rows[0]["ts_utc"]
    mid_strike = rows[0]["mid_strike"]

    pts = []
    for r in rows:
        pts.append({
            "strike":      float(r["strike"])      if r["strike"]      is not None else None,
            "delta_decay": float(r["delta_decay"])  if r["delta_decay"] is not None else None,
            "rel":         float(r["rel"])          if r["rel"]         is not None else None,
        })

    return {
        "ts_utc": ts_utc.isoformat() if hasattr(ts_utc, "isoformat") else str(ts_utc),
        "mid_strike": float(mid_strike) if mid_strike is not None else None,
        "points": pts
    }

def db_volland_exposure_window(greek: str, expiration_option: str = None, limit: int = 40) -> dict:
    """
    Generic query: returns latest 'limit' strikes centered on spot for any
    greek + expiration_option combo stored in volland_exposure_points.
    When expiration_option is None, does not filter by it (useful for 0DTE greeks).
    """
    if not engine:
        raise RuntimeError("DATABASE_URL not set")

    lim = int(limit)
    if lim < 5: lim = 5
    if lim > 200: lim = 200

    exp_filter = "AND expiration_option = :exp_option" if expiration_option else ""

    sql = text(f"""
    WITH latest AS (
      SELECT max(ts_utc) AS ts_utc
      FROM volland_exposure_points
      WHERE greek = :greek {exp_filter}
    ),
    center AS (
      SELECT COALESCE(
        (SELECT v.current_price::numeric
         FROM volland_exposure_points v
         JOIN latest l ON v.ts_utc = l.ts_utc
         WHERE v.greek = :greek {exp_filter}
               AND v.current_price IS NOT NULL
         LIMIT 1),
        (SELECT v.strike::numeric
         FROM volland_exposure_points v
         JOIN latest l ON v.ts_utc = l.ts_utc
         WHERE v.greek = :greek {exp_filter}
         ORDER BY abs(v.value::numeric) DESC
         LIMIT 1)
      ) AS mid_strike
    ),
    ranked AS (
      SELECT
        v.ts_utc,
        v.strike::numeric AS strike,
        v.value::numeric  AS value,
        c.mid_strike,
        (v.strike::numeric - c.mid_strike) AS rel,
        ROW_NUMBER() OVER (
          ORDER BY abs(v.strike::numeric - c.mid_strike), v.strike::numeric
        ) AS rn
      FROM volland_exposure_points v
      JOIN latest l ON v.ts_utc = l.ts_utc
      CROSS JOIN center c
      WHERE v.greek = :greek {exp_filter}
    )
    SELECT ts_utc, strike, value, mid_strike, rel
    FROM ranked
    WHERE rn <= :lim
    ORDER BY strike;
    """)
    params = {"greek": greek, "lim": lim}
    if expiration_option:
        params["exp_option"] = expiration_option
    with engine.begin() as conn:
        rows = conn.execute(sql, params).mappings().all()

    if not rows:
        return {"ts_utc": None, "mid_strike": None, "points": []}

    ts_utc = rows[0]["ts_utc"]
    mid_strike = rows[0]["mid_strike"]

    pts = []
    for r in rows:
        pts.append({
            "strike": float(r["strike"]) if r["strike"] is not None else None,
            "value":  float(r["value"])  if r["value"]  is not None else None,
            "rel":    float(r["rel"])    if r["rel"]    is not None else None,
        })

    return {
        "ts_utc": ts_utc.isoformat() if hasattr(ts_utc, "isoformat") else str(ts_utc),
        "mid_strike": float(mid_strike) if mid_strike is not None else None,
        "points": pts
    }

def db_volland_stats() -> Optional[dict]:
    """
    Get Volland statistics from the latest snapshot.
    Reads the 'statistics' field that volland_worker saves.
    Statistics persist even when market is closed.
    """
    if not engine:
        return None
    
    # Get the most recent snapshot that has statistics (no time limit - persist after hours)
    q = text("""
        SELECT ts, payload 
        FROM volland_snapshots 
        WHERE payload->>'error_event' IS NULL
          AND payload->'statistics' IS NOT NULL
        ORDER BY ts DESC 
        LIMIT 10
    """)
    
    with engine.begin() as conn:
        rows = conn.execute(q).mappings().all()
    
    if not rows:
        return {"ts": None, "stats": None, "error": "No statistics found"}
    
    stats = {
        "paradigm": None,
        "target": None,
        "lines_in_sand": None,
        "delta_decay_hedging": None,
        "opt_volume": None,
        "page_url": None,
        "has_statistics": False,
    }
    
    ts = None
    
    # Search through recent snapshots for statistics data
    for row in rows:
        payload = _json_load_maybe(row["payload"])
        if not payload or not isinstance(payload, dict):
            continue
        
        # Check for statistics field
        statistics = payload.get("statistics", {})
        if statistics and isinstance(statistics, dict):
            # Check if we have any actual data
            has_data = any(v for k, v in statistics.items() if v)
            if has_data:
                stats["has_statistics"] = True
                stats["paradigm"] = statistics.get("paradigm")
                stats["target"] = statistics.get("target")
                stats["lines_in_sand"] = statistics.get("lines_in_sand")
                stats["delta_decay_hedging"] = statistics.get("delta_decay_hedging")
                stats["opt_volume"] = statistics.get("opt_volume")
                svb = statistics.get("spot_vol_beta")
                if svb and isinstance(svb, dict):
                    stats["svb_correlation"] = svb.get("correlation")
                ts = row["ts"]
                break
    
    if not ts and rows:
        ts = rows[0]["ts"]
    
    return {
        "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else None,
        "stats": stats
    }

# ====== Auth ======
REFRESH_EARLY_SEC = 300
_access_token = None
_access_exp_at = 0.0
_refresh_token = RTOKEN or ""
_last_401_alert = 0.0  # timestamp of last 401 Telegram alert (cooldown)

def _alert_401(source: str):
    """Send Telegram alert on persistent 401 (dead refresh token). Max once per 5 min."""
    global _last_401_alert
    now = time.time()
    if now - _last_401_alert < 300:
        return
    _last_401_alert = now
    msg = (
        "🚨 <b>TS API 401 — Token Dead</b>\n\n"
        f"Source: <code>{source}</code>\n"
        "Refresh token may be expired. Manual re-auth required."
    )
    print(f"[auth] ALERT: persistent 401 from {source}", flush=True)
    send_telegram(msg)

def _stamp_token(exp_in: int):
    global _access_exp_at
    _access_exp_at = time.time() + int(exp_in or 900) - REFRESH_EARLY_SEC

def ts_access_token() -> str:
    global _access_token, _refresh_token
    now = time.time()
    if _access_token and now < _access_exp_at - 60:
        return _access_token
    if not (CID and SECRET and _refresh_token):
        raise RuntimeError("Missing env: TS_CLIENT_ID / TS_CLIENT_SECRET / TS_REFRESH_TOKEN")
    r = requests.post(
        f"{AUTH_DOMAIN}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": _refresh_token,
            "client_id": CID,
            "client_secret": SECRET,
            "scope": "openid profile MarketData ReadAccount Trade OptionSpreads offline_access",
        },
        timeout=15,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"token refresh [{r.status_code}] {r.text[:300]}")
    tok = r.json()
    _access_token = tok["access_token"]
    if tok.get("refresh_token"):
        _refresh_token = tok["refresh_token"]
    _stamp_token(tok.get("expires_in", 900))
    print("[auth] token refreshed; expires_in:", tok.get("expires_in"), flush=True)
    return _access_token

def api_get(path, params=None, stream=False, timeout=10):
    def do_req(h):
        return requests.get(f"{BASE}{path}", headers=h, params=params or {}, timeout=timeout, stream=stream)
    token = ts_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    r = do_req(headers)
    if r.status_code == 401:
        try:
            _ = ts_access_token()
            headers["Authorization"] = f"Bearer {_access_token}"
            r = do_req(headers)
        except Exception:
            pass
        if r.status_code == 401:
            _alert_401(f"api_get({path})")
    if stream:
        if r.status_code != 200:
            raise RuntimeError(f"STREAM {path} [{r.status_code}] {r.text[:300]}")
        return r
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} [{r.status_code}] {r.text[:300]}")
    return r

# ====== Time helpers ======
def now_et():
    return datetime.now(NY)

def fmt_et(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M %Z")

def market_open_now() -> bool:
    t = now_et()
    if t.weekday() >= 5:
        return False
    return dtime(9, 30) <= t.time() <= dtime(16, 0)

# ====== TS helpers ======
def get_spx_quote() -> dict:
    """Return {last, high, low, vix} from TS API quote. Fetches SPX + VIX in one call."""
    js = api_get("/marketdata/quotes/%24SPX.X,%24VIX.X", timeout=8).json()
    result = {"last": 0.0, "high": None, "low": None, "vix": None}
    for q in js.get("Quotes", []):
        sym = q.get("Symbol", "")
        if sym == "$SPX.X":
            v = q.get("Last") or q.get("Close")
            try:
                result["last"] = float(v)
            except Exception:
                pass
            try:
                h = q.get("High")
                if h is not None:
                    result["high"] = float(h)
            except Exception:
                pass
            try:
                lo = q.get("Low")
                if lo is not None:
                    result["low"] = float(lo)
            except Exception:
                pass
        elif sym == "$VIX.X":
            try:
                vv = q.get("Last") or q.get("Close")
                if vv is not None:
                    result["vix"] = float(vv)
            except Exception:
                pass
    return result

def get_spx_last() -> float:
    return get_spx_quote()["last"]

def get_0dte_exp() -> str:
    ymd = now_et().date().isoformat()
    try:
        js = api_get("/marketdata/options/expirations/%24SPXW.X", timeout=10).json()
        for e in js.get("Expirations", []):
            d = str(e.get("Date") or e.get("Expiration") or "")[:10]
            if d == ymd:
                return d
    except Exception as e:
        print("[exp] lookup failed; using today", ymd, "|", e, flush=True)
    return ymd

def _expiration_variants(ymd: str):
    yield ymd
    try:
        yield datetime.strptime(ymd, "%Y-%m-%d").strftime("%m-%d-%Y")
    except Exception:
        pass
    yield ymd + "T00:00:00Z"

def _fnum(x):
    if x in (None, "", "-", "NaN", "nan"):
        return None
    try:
        return float(str(x).replace(",", ""))
    except:
        return None

def _consume_chain_stream(r, max_seconds: float) -> list[dict]:
    out, start = [], time.time()
    completed_normally = False
    try:
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                if time.time() - start > max_seconds:
                    print(f"[stream] TIMEOUT after {time.time()-start:.1f}s with {len(out)} items (empty line)", flush=True)
                    break
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict) and obj.get("StreamStatus") == "EndSnapshot":
                completed_normally = True
                break
            if isinstance(obj, dict):
                out.append(obj)
            if time.time() - start > max_seconds:
                print(f"[stream] TIMEOUT after {time.time()-start:.1f}s with {len(out)} items", flush=True)
                break
    finally:
        try:
            r.close()
        except Exception:
            pass
    if completed_normally:
        print(f"[stream] completed normally with {len(out)} items in {time.time()-start:.1f}s", flush=True)
    return out

def get_chain_rows(exp_ymd: str, spot: float) -> list[dict]:
    params_stream = {
        "spreadType": "Single",
        "enableGreeks": "true",
        "priceCenter": f"{spot:.2f}" if spot else "",
        "strikeProximity": 125,  # Increased: 125/5 = 25 strikes each direction = 50 total
        "optionType": "All",
        "strikeInterval": 5  # SPX uses $5 strike intervals
    }
    last_err = None
    for exp in _expiration_variants(exp_ymd):
        try:
            p = dict(params_stream); p["expiration"] = exp
            r = api_get("/marketdata/stream/options/chains/%24SPXW.X", params=p, stream=True, timeout=8)
            objs = _consume_chain_stream(r, max_seconds=STREAM_SECONDS)
            if objs:
                rows = []
                for it in objs:
                    legs = it.get("Legs") or []
                    leg0 = legs[0] if legs else {}
                    side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
                    side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                    rows.append({
                        "Type": side,
                        "Strike": _fnum(leg0.get("StrikePrice")),
                        "Bid": _fnum(it.get("Bid")), "Ask": _fnum(it.get("Ask")), "Last": _fnum(it.get("Last")),
                        "BidSize": it.get("BidSize"), "AskSize": it.get("AskSize"),
                        "Delta": _fnum(it.get("Delta") or it.get("TheoDelta")),
                        "Gamma": _fnum(it.get("Gamma") or it.get("TheoGamma")),
                        "Theta": _fnum(it.get("Theta") or it.get("TheoTheta")),
                        "IV": _fnum(it.get("ImpliedVolatility") or it.get("TheoIV")),
                        "Vega": _fnum(it.get("Vega")),
                        "Volume": _fnum(it.get("TotalVolume") or it.get("Volume")),
                        "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
                    })
                if rows:
                    return rows
        except Exception as e:
            last_err = e
            continue

    params_snap = {
        "symbol": "$SPXW.X",
        "enableGreeks": "true",
        "optionType": "All",
        "priceCenter": f"{spot:.2f}" if spot else "",
        "strikeProximity": 125,  # Increased: 125/5 = 25 strikes each direction = 50 total
        "strikeInterval": 5,  # SPX uses $5 strike intervals
        "spreadType": "Single",
    }
    for exp in _expiration_variants(exp_ymd):
        try:
            p = dict(params_snap); p["expiration"] = exp
            js = api_get("/marketdata/options/chains", params=p, timeout=12).json()
            rows = []
            for it in js.get("Options", []):
                legs = it.get("Legs") or []
                leg0 = legs[0] if legs else {}
                side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
                side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                rows.append({
                    "Type": side,
                    "Strike": _fnum(leg0.get("StrikePrice")),
                    "Bid": _fnum(it.get("Bid")), "Ask": _fnum(it.get("Ask")), "Last": _fnum(it.get("Last")),
                    "BidSize": it.get("BidSize"), "AskSize": it.get("AskSize"),
                    "Delta": _fnum(it.get("Delta") or it.get("TheoDelta")),
                    "Gamma": _fnum(it.get("Gamma") or it.get("TheoGamma")),
                    "Theta": _fnum(it.get("Theta") or it.get("TheoTheta")),
                    "IV": _fnum(it.get("ImpliedVolatility") or it.get("TheoIV")),
                    "Vega": _fnum(it.get("Vega")),
                    "Volume": _fnum(it.get("TotalVolume") or it.get("Volume")),
                    "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
                })
            if rows:
                return rows
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"SPXW chain fetch failed; last_err={last_err}")

# ====== shaping ======
CANONICAL_COLS = [
    "C_Volume","C_OpenInterest","C_IV","C_Gamma","C_Delta","C_Bid","C_BidSize","C_Ask","C_AskSize","C_Last",
    "Strike",
    "P_Last","P_Ask","P_AskSize","P_Bid","P_BidSize","P_Delta","P_Gamma","P_IV","P_OpenInterest","P_Volume"
]
DISPLAY_COLS = [
    "Volume","Open Int","IV","Gamma","Delta","BID","BID QTY","ASK","ASK QTY","LAST",
    "Strike",
    "LAST","ASK","ASK QTY","BID","BID QTY","Delta","Gamma","IV","Open Int","Volume"
]

def to_side_by_side(rows: list[dict]) -> pd.DataFrame:
    calls, puts = {}, {}
    for r in rows:
        if r.get("Strike") is None:
            continue
        (calls if r["Type"] == "C" else puts)[r["Strike"]] = r
    strikes = sorted(set(calls) | set(puts))
    recs = []
    for k in strikes:
        c, p = calls.get(k, {}), puts.get(k, {})
        recs.append({
            "C_Volume": c.get("Volume"), "C_OpenInterest": c.get("OpenInterest"), "C_IV": c.get("IV"),
            "C_Gamma": c.get("Gamma"), "C_Delta": c.get("Delta"), "C_Bid": c.get("Bid"),
            "C_BidSize": c.get("BidSize"), "C_Ask": c.get("Ask"), "C_AskSize": c.get("AskSize"),
            "C_Last": c.get("Last"),
            "Strike": k,
            "P_Last": p.get("Last"), "P_Ask": p.get("Ask"), "P_AskSize": p.get("AskSize"),
            "P_Bid": p.get("Bid"), "P_BidSize": p.get("BidSize"),
            "P_Delta": p.get("Delta"), "P_Gamma": p.get("Gamma"), "P_IV": p.get("IV"),
            "P_OpenInterest": p.get("OpenInterest"), "P_Volume": p.get("Volume"),
        })
    df = pd.DataFrame.from_records(recs, columns=CANONICAL_COLS)
    if not df.empty:
        df = df.sort_values("Strike").reset_index(drop=True)
    return df

def pick_centered(df: pd.DataFrame, spot: float, n: int) -> pd.DataFrame:
    """
    Select n strikes centered around spot: n/2 above and n/2 below.
    Ensures balanced distribution above and below spot price.
    """
    if df is None or df.empty or not spot:
        return df

    half = n // 2  # 20 above, 20 below for n=40

    # Split into above and below spot
    above = df[df["Strike"] >= spot].sort_values("Strike").head(half)
    below = df[df["Strike"] < spot].sort_values("Strike", ascending=False).head(half)

    # Combine and sort by strike
    result = pd.concat([below, above]).sort_values("Strike").reset_index(drop=True)

    # If we don't have enough strikes on one side, take more from the other
    if len(result) < n:
        # Get strikes we already selected
        selected_strikes = set(result["Strike"].tolist())
        # Get remaining strikes sorted by distance from spot
        remaining = df[~df["Strike"].isin(selected_strikes)]
        remaining = remaining.iloc[(remaining["Strike"] - spot).abs().argsort()]
        needed = n - len(result)
        extra = remaining.head(needed)
        result = pd.concat([result, extra]).sort_values("Strike").reset_index(drop=True)

    return result

# ====== jobs ======
def run_market_job():
    global latest_df, last_run_status, _spx_session, _spx_cycle_high, _spx_cycle_low, _vix_last
    try:
        if not market_open_now():
            last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": "outside market hours"}
            print("[pull] skipped (closed)", last_run_status["ts"], flush=True)
            return
        quote = get_spx_quote()
        spot = quote["last"]
        sess_high = quote["high"]
        sess_low = quote["low"]
        if quote["vix"] is not None:
            _vix_last = quote["vix"]

        # Derive intra-cycle extremes from session H/L changes
        today = now_et().date()
        if _spx_session["date"] != today:
            # Daily reset
            _spx_session = {"high": None, "low": None, "date": today}

        cycle_hi = spot
        cycle_lo = spot
        prev_h = _spx_session["high"]
        prev_l = _spx_session["low"]
        if sess_high is not None and prev_h is not None and sess_high > prev_h:
            cycle_hi = max(spot, sess_high)
        if sess_low is not None and prev_l is not None and sess_low < prev_l:
            cycle_lo = min(spot, sess_low)

        _spx_cycle_high = cycle_hi
        _spx_cycle_low = cycle_lo

        # Update session state for next cycle
        if sess_high is not None:
            _spx_session["high"] = sess_high
        if sess_low is not None:
            _spx_session["low"] = sess_low

        if cycle_hi != spot or cycle_lo != spot:
            print(f"[pull] cycle extremes: spot={spot:.2f} hi={cycle_hi:.2f} lo={cycle_lo:.2f} (sess H={sess_high} L={sess_low})", flush=True)

        exp  = get_0dte_exp()
        rows = get_chain_rows(exp, spot)
        raw_count = len(rows)
        df   = pick_centered(to_side_by_side(rows), spot, TARGET_STRIKES)
        final_count = len(df)

        # Validate: reject incomplete data
        if final_count < MIN_REQUIRED_STRIKES:
            # Keep previous data if current fetch is incomplete
            last_run_status = {
                "ts": fmt_et(now_et()),
                "ok": False,
                "msg": f"INCOMPLETE: exp={exp} spot={round(spot or 0,2)} raw={raw_count} final={final_count} (min={MIN_REQUIRED_STRIKES})"
            }
            print("[pull] REJECTED - insufficient rows:", last_run_status["msg"], flush=True)
            return  # Don't update latest_df with bad data

        with _df_lock:
            latest_df = df.copy()
        last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": f"exp={exp} spot={round(spot or 0,2)} rows={final_count}"}
        print("[pull] OK", last_run_status["msg"], flush=True)

        # Check alerts after successful data pull
        try:
            check_alerts()
            send_scheduled_summary()
        except Exception as alert_err:
            print(f"[alerts] error in check: {alert_err}", flush=True)

        # Check trading setups
        try:
            _run_setup_check()
        except Exception as setup_err:
            print(f"[setups] error in check: {setup_err}", flush=True)
    except Exception as e:
        last_run_status = {"ts": fmt_et(now_et()), "ok": False, "msg": f"error: {e}"}
        print("[pull] ERROR", e, flush=True)
    finally:
        # Check pipeline health every cycle during market hours
        if market_open_now():
            try:
                check_pipeline_health()
            except Exception as health_err:
                print(f"[pipeline] health check error: {health_err}", flush=True)

def save_history_job():
    global _last_saved_at
    if not engine:
        return
    with _df_lock:
        if latest_df is None or latest_df.empty:
            return
        df_copy = latest_df.copy()
    if time.time() - _last_saved_at < 60:
        return
    try:
        df = df_copy
        df.columns = DISPLAY_COLS
        payload = {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}
        msg = (last_run_status.get("msg") or "")
        spot = None; exp = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot", ""))
            exp  = parts.get("exp")
        except:
            pass
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO chain_snapshots (ts, exp, spot, vix, columns, rows) VALUES (:ts, :exp, :spot, :vix, :columns, :rows)"),
                {"ts": now_et(), "exp": exp, "spot": spot, "vix": _vix_last,
                 "columns": json.dumps(payload["columns"]),
                 "rows": json.dumps(payload["rows"])}
            )
        _last_saved_at = time.time()
        print("[save] snapshot inserted", flush=True)
    except Exception as e:
        print("[save] failed:", e, flush=True)

_last_playback_saved_at = 0.0

def save_playback_snapshot():
    """Save combined GEX/Charm/Volume/Stats snapshot for historical playback."""
    global _last_playback_saved_at
    if not engine:
        return
    if not market_open_now():
        return
    if time.time() - _last_playback_saved_at < 60:
        return

    try:
        # Get current series data (GEX, Volume)
        with _df_lock:
            if latest_df is None or latest_df.empty:
                return
            df = latest_df.copy()

        # Extract spot price
        msg = last_run_status.get("msg") or ""
        spot = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot", ""))
        except:
            pass

        if not spot:
            return

        # Calculate series data
        sdf = df.sort_values("Strike")
        strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float).tolist()
        call_vol = pd.to_numeric(sdf["C_Volume"], errors="coerce").fillna(0.0).astype(float).tolist()
        put_vol = pd.to_numeric(sdf["P_Volume"], errors="coerce").fillna(0.0).astype(float).tolist()
        call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
        put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
        c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
        p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
        call_gex = (c_gamma * call_oi * 100.0).astype(float)
        put_gex = (-p_gamma * put_oi * 100.0).astype(float)
        net_gex = (call_gex + put_gex).astype(float).tolist()

        # Get Charm data from Volland
        charm_data = None
        try:
            vanna_window = db_volland_vanna_window(limit=40)
            if vanna_window and vanna_window.get("points"):
                # Create charm dict keyed by strike for alignment
                charm_by_strike = {p["strike"]: p["vanna"] for p in vanna_window["points"]}
                charm_data = [charm_by_strike.get(s, 0) for s in strikes]
        except Exception as e:
            print(f"[playback] charm fetch error: {e}", flush=True)

        # Get Delta Decay data from Volland (same approach as charm)
        dd_data = None
        try:
            dd_window = db_volland_delta_decay_window(limit=200)
            if dd_window and dd_window.get("points"):
                dd_by_strike = {p["strike"]: p["delta_decay"] for p in dd_window["points"]}
                dd_data = [dd_by_strike.get(s, 0) for s in strikes]
        except Exception as e:
            print(f"[playback] delta_decay fetch error: {e}", flush=True)

        # Get Stats from Volland
        stats_data = None
        try:
            stats_result = db_volland_stats()
            if stats_result and stats_result.get("stats"):
                s = stats_result["stats"]
                stats_data = {
                    "paradigm": s.get("paradigm"),
                    "target": s.get("target"),
                    "lis": s.get("lines_in_sand"),
                    "dd_hedging": s.get("delta_decay_hedging"),
                    "opt_volume": s.get("opt_volume"),
                }
        except Exception as e:
            print(f"[playback] stats fetch error: {e}", flush=True)

        # Save to database
        with engine.begin() as conn:
            conn.execute(
                text("""INSERT INTO playback_snapshots
                        (ts, spot, vix, strikes, net_gex, charm, delta_decay, call_vol, put_vol, stats, call_gex, put_gex, call_oi, put_oi)
                        VALUES (:ts, :spot, :vix, :strikes, :net_gex, :charm, :delta_decay, :call_vol, :put_vol, :stats, :call_gex, :put_gex, :call_oi, :put_oi)"""),
                {
                    "ts": now_et(),
                    "spot": spot,
                    "vix": _vix_last,
                    "strikes": json.dumps(strikes),
                    "net_gex": json.dumps(net_gex),
                    "charm": json.dumps(charm_data) if charm_data else None,
                    "delta_decay": json.dumps(dd_data) if dd_data else None,
                    "call_vol": json.dumps(call_vol),
                    "put_vol": json.dumps(put_vol),
                    "stats": json.dumps(stats_data) if stats_data else None,
                    "call_gex": json.dumps(call_gex.tolist()),
                    "put_gex": json.dumps(put_gex.tolist()),
                    "call_oi": json.dumps(call_oi.tolist()),
                    "put_oi": json.dumps(put_oi.tolist()),
                }
            )
        _last_playback_saved_at = time.time()
        print("[playback] snapshot saved", flush=True)
    except Exception as e:
        print(f"[playback] save failed: {e}", flush=True)

# ====== ALERT CHECKING ======
def check_alerts():
    """Check all alert conditions and send Telegram notifications."""
    global _alert_state

    if not _alert_settings.get("enabled"):
        return

    if not is_market_hours():
        return

    # Reset daily state at market open
    today = datetime.now(NY).date()
    if _alert_state["last_trading_day"] != today:
        _alert_state["last_trading_day"] = today
        _alert_state["levels_touched"] = set()
        _alert_state["near_active"] = set()
        _alert_state["sent_10am"] = False
        _alert_state["sent_2pm"] = False
        _alert_state["last_paradigm"] = None
        print("[alerts] reset daily state", flush=True)

    try:
        # Get current data
        with _df_lock:
            if latest_df is None or latest_df.empty:
                return
            df = latest_df.copy()

        # Get spot price
        msg = last_run_status.get("msg") or ""
        spot = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot", ""))
        except:
            pass

        if not spot:
            return

        # Get stats for levels
        stats_result = db_volland_stats()
        stats = stats_result.get("stats", {}) if stats_result else {}

        # Calculate GEX for max gamma levels
        sdf = df.sort_values("Strike")
        strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float)
        call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
        put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
        c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
        p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
        call_gex = (c_gamma * call_oi * 100.0)
        put_gex = (-p_gamma * put_oi * 100.0)
        net_gex = (call_gex + put_gex)

        # Find max +GEX and -GEX strikes
        max_pos_idx = net_gex.idxmax() if not net_gex.empty else None
        max_neg_idx = net_gex.idxmin() if not net_gex.empty else None
        max_pos_gamma = strikes.loc[max_pos_idx] if max_pos_idx is not None else None
        max_neg_gamma = strikes.loc[max_neg_idx] if max_neg_idx is not None else None

        # Parse LIS and Target
        threshold = _alert_settings.get("threshold_points", 5)

        lis_low, lis_high = None, None
        if stats.get("lines_in_sand"):
            lis_str = str(stats["lines_in_sand"]).replace("$", "").replace(",", "")
            import re
            lis_match = re.findall(r"[\d.]+", lis_str)
            if len(lis_match) >= 2:
                lis_low, lis_high = float(lis_match[0]), float(lis_match[1])
            elif len(lis_match) == 1:
                lis_low = float(lis_match[0])

        target = None
        if stats.get("target"):
            target_str = str(stats["target"]).replace("$", "").replace(",", "")
            target_match = re.search(r"[\d.]+", target_str)
            if target_match:
                target = float(target_match.group())

        # Check price alerts — state-based: fires on zone entry, resets when price leaves
        def check_level(level, name, setting_key):
            if not level or not _alert_settings.get(setting_key):
                return
            distance = abs(spot - level)
            near_key = f"{name}_{int(level)}_near"
            touch_key = f"{name}_{int(level)}"

            # Near alert: fire once on entry into zone, reset when price moves away
            if distance <= threshold:
                if near_key not in _alert_state["near_active"]:
                    send_telegram(f"🎯 <b>SPX near {name}</b>\nPrice: {spot:.2f}\n{name}: {level:.0f}\nDistance: {distance:.1f} pts")
                    _alert_state["near_active"].add(near_key)
            elif distance > threshold + 3:
                # Price moved away — reset both so next approach triggers again
                _alert_state["near_active"].discard(near_key)
                _alert_state["levels_touched"].discard(touch_key)

            # Touch/Cross alert
            if distance <= 1 and touch_key not in _alert_state["levels_touched"]:
                send_telegram(f"✅ <b>SPX touched {name}</b>\nPrice: {spot:.2f}\n{name}: {level:.0f}")
                _alert_state["levels_touched"].add(touch_key)

        if _alert_settings.get("lis_enabled"):
            if lis_low:
                check_level(lis_low, "LIS", "lis_enabled")
            if lis_high:
                check_level(lis_high, "LIS", "lis_enabled")

        if _alert_settings.get("target_enabled") and target:
            check_level(target, "Target", "target_enabled")

        if _alert_settings.get("max_pos_gamma_enabled") and max_pos_gamma:
            check_level(max_pos_gamma, "+Gamma", "max_pos_gamma_enabled")

        if _alert_settings.get("max_neg_gamma_enabled") and max_neg_gamma:
            check_level(max_neg_gamma, "-Gamma", "max_neg_gamma_enabled")

        # Check paradigm change
        if _alert_settings.get("paradigm_change_enabled"):
            current_paradigm = stats.get("paradigm")
            if current_paradigm and _alert_state["last_paradigm"] and current_paradigm != _alert_state["last_paradigm"]:
                msg = f"🔄 <b>Paradigm Changed</b>\n"
                msg += f"From: {_alert_state['last_paradigm']}\n"
                msg += f"To: {current_paradigm}\n"
                if target:
                    msg += f"Target: {target:.0f}\n"
                if lis_low:
                    msg += f"LIS: {lis_low:.0f}"
                    if lis_high:
                        msg += f" - {lis_high:.0f}"
                send_telegram(msg)
            _alert_state["last_paradigm"] = current_paradigm

        # Check volume spikes
        if _alert_settings.get("volume_spike_enabled"):
            call_vol = pd.to_numeric(sdf["C_Volume"], errors="coerce").fillna(0.0).astype(float)
            put_vol = pd.to_numeric(sdf["P_Volume"], errors="coerce").fillna(0.0).astype(float)
            vol_threshold = _alert_settings.get("threshold_volume", 500)

            current_volume = {}
            for i, strike in enumerate(strikes):
                current_volume[strike] = {"call": call_vol.iloc[i], "put": put_vol.iloc[i]}

            if _alert_state["last_volume"]:
                for strike, vols in current_volume.items():
                    if strike in _alert_state["last_volume"]:
                        prev = _alert_state["last_volume"][strike]
                        call_change = vols["call"] - prev["call"]
                        put_change = vols["put"] - prev["put"]

                        # OTM calls only (strike > spot)
                        if strike > spot and call_change >= vol_threshold and should_alert(f"vol_call_{int(strike)}"):
                            send_telegram(f"📈 <b>OTM Call Volume Spike</b>\nStrike: {strike:.0f}\nChange: +{call_change:.0f} contracts\nSPX: {spot:.2f}")
                            record_alert(f"vol_call_{int(strike)}")

                        # OTM puts only (strike < spot)
                        if strike < spot and put_change >= vol_threshold and should_alert(f"vol_put_{int(strike)}"):
                            send_telegram(f"📉 <b>OTM Put Volume Spike</b>\nStrike: {strike:.0f}\nChange: +{put_change:.0f} contracts\nSPX: {spot:.2f}")
                            record_alert(f"vol_put_{int(strike)}")

            _alert_state["last_volume"] = current_volume

    except Exception as e:
        print(f"[alerts] check error: {e}", flush=True)

def send_scheduled_summary():
    """Send scheduled summary at 10 AM and 2 PM."""
    if not _alert_settings.get("enabled"):
        return

    now = datetime.now(NY)
    hour = now.hour
    minute = now.minute

    # 10 AM summary (10:00-10:01)
    if hour == 10 and minute == 0 and not _alert_state["sent_10am"] and _alert_settings.get("summary_10am_enabled"):
        send_summary_alert("10:00 AM")
        _alert_state["sent_10am"] = True

    # 2 PM summary (14:00-14:01)
    if hour == 14 and minute == 0 and not _alert_state["sent_2pm"] and _alert_settings.get("summary_2pm_enabled"):
        send_summary_alert("2:00 PM")
        _alert_state["sent_2pm"] = True

def send_summary_alert(time_label: str):
    """Send a full stats summary."""
    try:
        # Get spot
        msg = last_run_status.get("msg") or ""
        spot = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot", ""))
        except:
            pass

        # Get stats
        stats_result = db_volland_stats()
        stats = stats_result.get("stats", {}) if stats_result else {}

        # Get max gamma
        with _df_lock:
            if latest_df is not None and not latest_df.empty:
                df = latest_df.copy()
                sdf = df.sort_values("Strike")
                strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float)
                call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
                put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
                c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
                p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
                net_gex = (c_gamma * call_oi * 100.0) + (-p_gamma * put_oi * 100.0)
                max_pos_idx = net_gex.idxmax() if not net_gex.empty else None
                max_neg_idx = net_gex.idxmin() if not net_gex.empty else None
                max_pos_gamma = strikes.loc[max_pos_idx] if max_pos_idx is not None else None
                max_neg_gamma = strikes.loc[max_neg_idx] if max_neg_idx is not None else None
            else:
                max_pos_gamma, max_neg_gamma = None, None

        summary = f"📊 <b>{time_label} Summary</b>\n\n"
        summary += f"SPX: {spot:.2f}\n" if spot else "SPX: N/A\n"
        summary += f"Paradigm: {stats.get('paradigm', 'N/A')}\n"
        summary += f"Target: {stats.get('target', 'N/A')}\n"
        summary += f"LIS: {stats.get('lines_in_sand', 'N/A')}\n"
        summary += f"DD Hedging: {stats.get('delta_decay_hedging', 'N/A')}\n"
        summary += f"Max +Gamma: {max_pos_gamma:.0f}\n" if max_pos_gamma else "Max +Gamma: N/A\n"
        summary += f"Max -Gamma: {max_neg_gamma:.0f}\n" if max_neg_gamma else "Max -Gamma: N/A\n"

        send_telegram(summary)
    except Exception as e:
        print(f"[alerts] summary error: {e}", flush=True)

def _compute_setup_levels(r: dict):
    """Compute (target_level, stop_level) from a setup result dict.

    Mirrors the level logic in _calculate_setup_outcome() but works from
    the live result dict directly (no DB query needed).
    Returns (target_level, stop_level) or (None, None) if levels can't be determined.
    """
    setup_name = r.get("setup_name", "")
    direction = r.get("direction", "long")
    spot = r.get("spot")
    if not spot:
        return None, None

    is_long = direction.lower() in ("long", "bullish")

    if setup_name == "BofA Scalp":
        target_lvl = r.get("bofa_target_level")
        stop_lvl = r.get("bofa_stop_level")
        return target_lvl, stop_lvl

    if setup_name == "ES Absorption":
        es_price = r.get("abs_es_price")
        if not es_price:
            return None, None
        target_lvl = es_price + 10 if is_long else es_price - 10
        stop_lvl = es_price - 12 if is_long else es_price + 12
        return round(target_lvl, 2), round(stop_lvl, 2)

    if setup_name == "DD Exhaustion":
        # Trailing stop — no fixed target; initial SL = 12 pts
        # target_level=None signals trailing mode in _check_setup_outcomes
        stop_lvl = spot - 12 if is_long else spot + 12
        return None, round(stop_lvl, 2)

    if setup_name == "GEX Long":
        # Trailing stop — no fixed target; initial SL = 8 pts
        # Hybrid trail: BE at +10, continuous trail activation=15, gap=5
        stop_lvl = spot - 8 if is_long else spot + 8
        return None, round(stop_lvl, 2)

    if setup_name == "Paradigm Reversal":
        target_lvl = spot + 10 if is_long else spot - 10
        stop_lvl = spot - 15 if is_long else spot + 15
        return round(target_lvl, 2), round(stop_lvl, 2)

    # AG Short — trailing mode (hybrid: BE at +10, trail at +15 gap=5)
    lis = r.get("lis")
    target = r.get("target")
    if not lis or not target:
        return None, None
    max_minus_gex = r.get("max_minus_gex")
    max_plus_gex = r.get("max_plus_gex")
    max_stop_dist = 20
    if is_long:
        stop_lvl = lis - 5
        if max_minus_gex is not None and max_minus_gex < stop_lvl:
            stop_lvl = max_minus_gex
        stop_lvl = max(stop_lvl, spot - max_stop_dist)
        return None, round(stop_lvl, 2)
    else:
        stop_lvl = lis + 5
        if max_plus_gex is not None and max_plus_gex > stop_lvl:
            stop_lvl = max_plus_gex
        stop_lvl = min(stop_lvl, spot + max_stop_dist)
        return None, round(stop_lvl, 2)


def _check_setup_outcomes(spot: float, cycle_high=None, cycle_low=None):
    """Check open trades for target/stop hits. Called each cycle (~30s).

    Uses session-derived cycle high/low to catch SL/TP breaches between checks.
    For ES Absorption, uses ES price (abs_es_price) instead of SPX spot.
    Sends Telegram outcome for each resolved trade and moves to resolved list.
    """
    global _setup_open_trades, _setup_resolved_trades
    if not spot:
        return

    # Auto-trade: poll broker for order fills
    try:
        from app import auto_trader
        auto_trader.poll_order_status()
    except Exception:
        pass

    from app.setup_detector import format_setup_outcome

    # Use session-derived cycle extremes (or fall back to spot)
    spx_cycle_high = cycle_high if cycle_high is not None else spot
    spx_cycle_low = cycle_low if cycle_low is not None else spot

    now = now_et()
    today = now.date()

    # Daily reset
    if _setup_open_trades and _setup_open_trades[0].get("_trade_date") != today:
        _setup_open_trades = []
        _setup_resolved_trades = []

    market_closed = now.time() >= dtime(16, 0)
    still_open = []

    # Get current ES price + bar H/L extremes for absorption outcome checks
    # Primary: Rithmic bars (absorption signals fire from Rithmic, same bar_idx space)
    # Fallback: TS quote stream bars (if Rithmic not available)
    es_price = None
    es_bars_snapshot = []
    try:
        from rithmic_es_stream import get_rithmic_bars
        rithmic_bars = get_rithmic_bars()
        if rithmic_bars:
            last_bar = rithmic_bars[-1]
            es_price = last_bar.get("close")
            es_bars_snapshot = rithmic_bars
    except (ImportError, Exception):
        pass
    if not es_bars_snapshot:
        with _es_quote_lock:
            if _es_quote["_completed_bars"]:
                last_bar = _es_quote["_completed_bars"][-1]
                es_price = last_bar.get("close")
                es_bars_snapshot = list(_es_quote["_completed_bars"])

    for trade in _setup_open_trades:
        setup_name = trade["setup_name"]
        direction = trade["direction"]
        entry_spot = trade["spot"]
        target_lvl = trade["target_level"]
        stop_lvl = trade["stop_level"]
        ts_entry = trade["ts"]
        is_long = direction.lower() in ("long", "bullish")

        # Use ES price for absorption, SPX spot for everything else
        if setup_name == "ES Absorption":
            if not es_price:
                still_open.append(trade)
                continue  # Skip — no ES data; never fall back to SPX
            check_price = es_price
        else:
            check_price = spot

        # Determine entry price for P&L calc (ES price for absorption, SPX for others)
        if setup_name == "ES Absorption":
            entry_price = trade.get("result_data", {}).get("abs_es_price", entry_spot)
        else:
            entry_price = entry_spot

        elapsed = (now - ts_entry).total_seconds() / 60.0

        # BofA max hold expiry
        max_hold = trade.get("max_hold_minutes")
        bofa_expired = max_hold and elapsed >= max_hold

        result_type = None
        pnl = None

        # Update per-trade price tracking with cycle extremes
        if setup_name == "ES Absorption":
            # ES Absorption: scan completed ES range bar H/L since entry bar
            # This catches intra-bar target/stop hits that bar-close checks miss
            entry_bar_idx = trade.get("result_data", {}).get("bar_idx", 0)
            last_scanned = trade.get("_es_last_bar_idx", entry_bar_idx)
            for bar in es_bars_snapshot:
                bidx = bar.get("idx", 0)
                if bidx <= last_scanned:
                    continue
                bh = bar.get("high")
                bl = bar.get("low")
                if bh is not None:
                    trade["_seen_high"] = max(trade.get("_seen_high", bh), bh)
                if bl is not None:
                    trade["_seen_low"] = min(trade.get("_seen_low", bl), bl)
                trade["_es_last_bar_idx"] = bidx
        else:
            # SPX setups: use session-derived cycle extremes
            trade["_seen_low"] = min(trade.get("_seen_low", spx_cycle_low), spx_cycle_low)
            trade["_seen_high"] = max(trade.get("_seen_high", spx_cycle_high), spx_cycle_high)

        # Trailing stop setups: DD Exhaustion, GEX Long, AG Short
        # DD: continuous trail (activation=20, gap=5) — waits for confirmed move before trailing
        # GEX/AG: hybrid trail (BE at +10, continuous trail activation=15, gap=5)
        # Uses cycle low/high (not all-time) since trail level changes each cycle
        _trail_params = {
            "DD Exhaustion": {"mode": "continuous", "activation": 20, "gap": 5},
            "GEX Long": {"mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5},
            "AG Short": {"mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5},
        }
        _tp = _trail_params.get(setup_name)
        if _tp is not None:
            # Advance trail using cycle high (long) or cycle low (short)
            fav_price = spx_cycle_high if is_long else spx_cycle_low
            fav = (fav_price - entry_price) if is_long else (entry_price - fav_price)
            max_fav = trade.get("_dd_max_fav", 0.0)
            if fav > max_fav:
                max_fav = fav
                trade["_dd_max_fav"] = max_fav
            trail_lock = None
            if _tp["mode"] == "continuous":
                # Continuous trail: after activation, lock at max_fav - gap
                if max_fav >= _tp["activation"]:
                    trail_lock = max_fav - _tp["gap"]
            elif _tp["mode"] == "hybrid":
                # Hybrid trail: breakeven at be_trigger, then continuous trail
                if max_fav >= _tp["activation"]:
                    trail_lock = max_fav - _tp["gap"]
                elif max_fav >= _tp["be_trigger"]:
                    trail_lock = 0  # breakeven
            else:
                # Rung-based trail: step every N pts with lock offset
                rung = _tp["rung_start"]
                while rung <= max_fav:
                    trail_lock = rung - _tp["lock_offset"]
                    rung += _tp["step"]
            if trail_lock is not None:
                # Move stop to lock-in level
                if is_long:
                    new_stop = entry_price + trail_lock
                    if new_stop > stop_lvl:
                        stop_lvl = new_stop
                        trade["stop_level"] = stop_lvl
                else:
                    new_stop = entry_price - trail_lock
                    if new_stop < stop_lvl:
                        stop_lvl = new_stop
                        trade["stop_level"] = stop_lvl
                # Auto-trade: update ES stop to match trail
                try:
                    from app import auto_trader
                    log_id = trade.get("setup_log_id")
                    if log_id:
                        at_order = auto_trader._active_orders.get(log_id)
                        if at_order and at_order.get("fill_price"):
                            es_stop = at_order["fill_price"] + (stop_lvl - entry_price)
                            auto_trader.update_stop(log_id, round(es_stop, 2))
                except Exception:
                    pass
            # Check trailing stop hit using cycle extreme (not just current price)
            stop_check = spx_cycle_low if is_long else spx_cycle_high
            if is_long and stop_check <= stop_lvl:
                result_type = "WIN" if stop_lvl >= entry_price else "LOSS"
                pnl = stop_lvl - entry_price
            elif not is_long and stop_check >= stop_lvl:
                result_type = "WIN" if stop_lvl <= entry_price else "LOSS"
                pnl = entry_price - stop_lvl
            elif market_closed:
                result_type = "EXPIRED"
                pnl = (check_price - entry_price) if is_long else (entry_price - check_price)
        elif is_long:
            # Use all-time seen_high/seen_low for fixed SL/TP (never miss a breach)
            # Fallback to entry_price (not check_price) to avoid false WIN on first cycle
            _fallback = entry_price if setup_name == "ES Absorption" else check_price
            price_high = trade.get("_seen_high", _fallback)
            price_low = trade.get("_seen_low", _fallback)
            if price_high >= target_lvl:
                result_type = "WIN"
                pnl = target_lvl - entry_price
            elif price_low <= stop_lvl:
                result_type = "LOSS"
                pnl = stop_lvl - entry_price
            elif market_closed or bofa_expired:
                result_type = "EXPIRED"
                pnl = check_price - entry_price
        else:
            _fallback = entry_price if setup_name == "ES Absorption" else check_price
            price_high = trade.get("_seen_high", _fallback)
            price_low = trade.get("_seen_low", _fallback)
            if price_low <= target_lvl:
                result_type = "WIN"
                pnl = entry_price - target_lvl
            elif price_high >= stop_lvl:
                result_type = "LOSS"
                pnl = entry_price - stop_lvl
            elif market_closed or bofa_expired:
                result_type = "EXPIRED"
                pnl = entry_price - check_price

        if result_type:
            pnl = round(pnl, 1)
            elapsed_min = int(elapsed)
            trade["close_price"] = check_price

            # Send individual outcome Telegram
            try:
                outcome_msg = format_setup_outcome(trade, result_type, pnl, elapsed_min)
                send_telegram_setups(outcome_msg)
            except Exception as e:
                print(f"[outcome] telegram error: {e}", flush=True)

            # Persist outcome to setup_log DB
            log_id = trade.get("setup_log_id")
            if log_id and engine:
                try:
                    # Compute first_event
                    is_trailing = setup_name in ("DD Exhaustion", "GEX Long", "AG Short")
                    if result_type == "WIN":
                        first_event = "target" if is_trailing else "10pt"
                    elif result_type == "LOSS":
                        first_event = "stop"
                    else:  # EXPIRED
                        first_event = "timeout"

                    # Compute max_profit / max_loss from seen extremes
                    seen_high = trade.get("_seen_high", entry_price)
                    seen_low = trade.get("_seen_low", entry_price)
                    if is_long:
                        max_profit = round(seen_high - entry_price, 2)
                        max_loss = round(seen_low - entry_price, 2)
                    else:
                        max_profit = round(entry_price - seen_low, 2)
                        max_loss = round(entry_price - seen_high, 2)

                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE setup_log SET
                                outcome_result = :res,
                                outcome_pnl = :pnl,
                                outcome_target_level = :tgt,
                                outcome_stop_level = :sl,
                                outcome_elapsed_min = :em,
                                outcome_first_event = :fe,
                                outcome_max_profit = :mp,
                                outcome_max_loss = :ml
                            WHERE id = :id
                        """), {
                            "res": result_type,
                            "pnl": pnl,
                            "tgt": target_lvl,
                            "sl": stop_lvl,
                            "em": elapsed_min,
                            "fe": first_event,
                            "mp": max_profit,
                            "ml": max_loss,
                            "id": log_id,
                        })
                except Exception as db_err:
                    print(f"[outcome] DB persist error: {db_err}", flush=True)

            # Auto-trade: close ES position on outcome
            try:
                from app import auto_trader
                if log_id:
                    auto_trader.close_trade(log_id, result_type)
            except Exception:
                pass

            # Move to resolved list
            resolved = {**trade, "result_type": result_type, "pnl": pnl, "elapsed_min": elapsed_min,
                        "ts_str": ts_entry.strftime("%H:%M") if hasattr(ts_entry, "strftime") else ""}
            _setup_resolved_trades.append(resolved)
            print(f"[outcome] {setup_name} {direction} -> {result_type} {pnl:+.1f}pts ({elapsed_min}min)", flush=True)
        else:
            still_open.append(trade)

    _setup_open_trades = still_open


def _run_setup_check():
    """Run setup detectors (GEX Long + AG Short + BofA Scalp) after each data pull."""
    # Get spot price (same pattern as check_alerts)
    msg = last_run_status.get("msg") or ""
    spot = None
    try:
        parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
        spot = float(parts.get("spot", ""))
    except Exception:
        pass
    if not spot:
        return

    # Check open trades for outcome resolution each cycle
    _check_setup_outcomes(spot, _spx_cycle_high, _spx_cycle_low)

    # Get Volland stats
    stats_result = db_volland_stats()
    stats = stats_result.get("stats", {}) if stats_result else {}
    paradigm = stats.get("paradigm")

    # Parse LIS (single value for GEX/AG, both values for BofA)
    lis = None
    lis_lower = None
    lis_upper = None
    if stats.get("lines_in_sand"):
        lis_str = str(stats["lines_in_sand"]).replace("$", "").replace(",", "")
        lis_match = re.findall(r"[\d.]+", lis_str)
        if lis_match:
            lis = float(lis_match[0])
            lis_lower = float(lis_match[0])
        if len(lis_match) >= 2:
            lis_upper = float(lis_match[1])

    # Parse target
    target = None
    if stats.get("target"):
        target_str = str(stats["target"]).replace("$", "").replace(",", "")
        target_match = re.search(r"[\d.]+", target_str)
        if target_match:
            target = float(target_match.group())

    # Parse aggregated charm for BofA Scalp
    aggregated_charm = None
    statistics_raw = None
    # Get raw statistics from volland snapshot payload
    if engine:
        try:
            with engine.begin() as conn:
                snap_row = conn.execute(text("""
                    SELECT payload FROM volland_snapshots
                    WHERE payload->>'error_event' IS NULL
                      AND payload->'statistics' IS NOT NULL
                    ORDER BY ts DESC LIMIT 1
                """)).mappings().first()
            if snap_row:
                payload = _json_load_maybe(snap_row["payload"])
                if payload and isinstance(payload, dict):
                    statistics_raw = payload.get("statistics", {})
                    if statistics_raw and isinstance(statistics_raw, dict):
                        charm_val = statistics_raw.get("aggregatedCharm")
                        if charm_val is not None:
                            try:
                                aggregated_charm = float(charm_val)
                            except (ValueError, TypeError):
                                pass
        except Exception:
            pass

    # Extract DD hedging from Volland stats (for Paradigm Reversal)
    dd_hedging = None
    if statistics_raw and isinstance(statistics_raw, dict):
        dd_hedging = statistics_raw.get("deltadecayHedging") or statistics_raw.get("delta_decay_hedging")

    # Parse DD hedging to numeric value for DD Exhaustion
    from app.setup_detector import update_dd_tracker
    dd_numeric = _parse_dd_numeric(dd_hedging)
    dd_shift = update_dd_tracker(dd_numeric) if dd_numeric is not None else None

    # Query recent ES 1-min bars for Paradigm Reversal volume check
    es_bars = []
    if engine:
        try:
            with engine.begin() as conn:
                rows = conn.execute(text("""
                    SELECT bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                           cumulative_delta, bar_close_price, ts
                    FROM es_delta_bars
                    WHERE trade_date = :td AND symbol = :sym
                    ORDER BY ts DESC LIMIT 15
                """), {"td": now_et().strftime("%Y-%m-%d"), "sym": ES_DELTA_SYMBOL}).mappings().all()
                es_bars = list(reversed(rows))  # oldest first
        except Exception:
            pass

    # Calculate max +GEX / -GEX strikes from latest_df
    max_plus_gex, max_minus_gex = None, None
    with _df_lock:
        if latest_df is not None and not latest_df.empty:
            df = latest_df.copy()
            sdf = df.sort_values("Strike")
            strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float)
            call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
            put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
            c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
            p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
            net_gex = (c_gamma * call_oi * 100.0) + (-p_gamma * put_oi * 100.0)
            max_pos_idx = net_gex.idxmax() if not net_gex.empty else None
            max_neg_idx = net_gex.idxmin() if not net_gex.empty else None
            max_plus_gex = float(strikes.loc[max_pos_idx]) if max_pos_idx is not None else None
            max_minus_gex = float(strikes.loc[max_neg_idx]) if max_neg_idx is not None else None

    from app.setup_detector import check_setups as _check_setups_fn
    result_wrappers = _check_setups_fn(
        spot, paradigm, lis, target, max_plus_gex, max_minus_gex, _setup_settings,
        lis_lower=lis_lower, lis_upper=lis_upper, aggregated_charm=aggregated_charm,
        dd_hedging=dd_hedging, es_bars=es_bars,
        dd_value=dd_numeric, dd_shift=dd_shift,
    )
    for rw in result_wrappers:
        setup_name = rw["result"]["setup_name"]
        grade = rw["result"]["grade"]
        score = rw["result"]["score"]
        reason = rw.get("notify_reason")
        r = rw["result"]

        # Only log and notify on meaningful events
        if rw["notify"]:
            log_setup(rw)

            # Different telegram messages based on reason
            if reason in ("new", "reformed"):
                # Full setup alert for new/reformed setups
                send_telegram_setups(rw["message"])
                print(f"[setups] {setup_name} NEW: {grade} ({score})", flush=True)
                # Record open trade for live outcome tracking
                target_lvl, stop_lvl = _compute_setup_levels(r)
                # Trailing setups use trailing stop (target_lvl=None is OK)
                _trailing_setups = ("DD Exhaustion", "GEX Long", "AG Short")
                if stop_lvl is not None and (target_lvl is not None or setup_name in _trailing_setups):
                    _setup_open_trades.append({
                        "setup_name": setup_name, "direction": r["direction"],
                        "spot": r["spot"], "grade": grade,
                        "target_level": target_lvl, "stop_level": stop_lvl,
                        "ts": now_et(), "result_data": r,
                        "max_hold_minutes": r.get("bofa_max_hold_minutes"),
                        "_trade_date": now_et().date(),
                        "setup_log_id": _current_setup_log.get(setup_name),
                        "_dd_max_fav": 0.0,  # track max favorable excursion for trailing
                    })
                    tgt_str = "trail" if target_lvl is None else f"{target_lvl:.1f}"
                    print(f"[outcome] tracking {setup_name}: target={tgt_str} stop={stop_lvl:.1f}", flush=True)
                    # Auto-trade: place MES SIM order
                    try:
                        from app import auto_trader
                        es_px = None
                        with _es_quote_lock:
                            es_px = _es_quote.get("last_price")
                        if not es_px:
                            # Fallback: use ES delta stream price (1-min bars)
                            with _es_delta_lock:
                                es_px = _es_delta.get("last_price")
                            if es_px:
                                print(f"[auto-trader] using ES delta price fallback: {es_px}", flush=True)
                        if es_px and stop_lvl is not None:
                            stop_dist = abs(r["spot"] - stop_lvl)
                            target_dist = abs(target_lvl - r["spot"]) if target_lvl else None
                            # Compute Volland full target distance for split-target setups
                            # AG Short and DD Exhaustion: trail-only T2 (no limit order)
                            if setup_name in ("DD Exhaustion", "AG Short"):
                                full_target_dist = None
                            else:
                                full_tgt = r.get("target") or r.get("bofa_target_level")
                                full_target_dist = abs(full_tgt - r["spot"]) if full_tgt else target_dist
                            auto_trader.place_trade(
                                setup_log_id=_current_setup_log.get(setup_name),
                                setup_name=setup_name, direction=r["direction"],
                                es_price=es_px, target_pts=target_dist, stop_pts=stop_dist,
                                full_target_pts=full_target_dist,
                            )
                        elif not es_px:
                            print(f"[auto-trader] SKIPPED {setup_name}: no ES price available (quote stream and delta both None)", flush=True)
                        elif stop_lvl is None:
                            print(f"[auto-trader] SKIPPED {setup_name}: stop_lvl is None", flush=True)
                    except Exception as e:
                        print(f"[auto-trader] place error: {e}", flush=True)
            elif reason == "grade_upgrade":
                # Short upgrade notice
                emoji = "⬆️"
                msg = f"{emoji} <b>{setup_name} upgraded to {grade}</b>\n"
                msg += f"Score: {score} | SPX: {r['spot']:.0f} | Gap: {r['gap_to_lis']:.1f} | R:R: {r['rr_ratio']:.1f}x"
                send_telegram_setups(msg)
                print(f"[setups] {setup_name} UPGRADED: {grade} ({score})", flush=True)
            elif reason == "gap_improvement":
                # Short improvement notice
                emoji = "📈"
                msg = f"{emoji} <b>{setup_name} improved</b>\n"
                msg += f"{grade} | SPX: {r['spot']:.0f} | Gap: {r['gap_to_lis']:.1f} | R:R: {r['rr_ratio']:.1f}x"
                send_telegram_setups(msg)
                print(f"[setups] {setup_name} GAP IMPROVED: {grade} ({score})", flush=True)
        else:
            print(f"[setups] {setup_name} active: {grade} ({score}) - no change", flush=True)

    # Persist cooldown state after each evaluation cycle
    if result_wrappers:
        _save_cooldowns()

# ====== ES CUMULATIVE DELTA (TradeStation streaming barcharts — real-time) ======
ES_DELTA_SYMBOL = "@ES"
_es_delta_lock = Lock()
_es_delta = {
    "cumulative_delta": 0,
    "total_volume": 0,
    "buy_volume": 0,       # UpVolume (uptick trades)
    "sell_volume": 0,      # DownVolume (downtick trades)
    "tick_count": 0,
    "last_price": None,
    "session_high": None,
    "session_low": None,
    "trade_date": None,
    "stream_ok": False,
    # Internal: separate completed vs open bar tracking
    "_completed_delta": 0,
    "_completed_volume": 0,
    "_completed_buy_vol": 0,
    "_completed_sell_vol": 0,
    "_completed_ticks": 0,
    "_open_epoch": 0,
    "_open_delta": 0,
    "_open_volume": 0,
    "_open_buy_vol": 0,
    "_open_sell_vol": 0,
    "_open_ticks": 0,
    "_bars_buffer": [],    # completed bars queued for DB flush
}

def _es_delta_reset(today: str):
    """Reset ES delta state for a new trading day."""
    with _es_delta_lock:
        _es_delta.update({
            "cumulative_delta": 0, "total_volume": 0, "buy_volume": 0, "sell_volume": 0,
            "tick_count": 0, "last_price": None, "session_high": None, "session_low": None,
            "trade_date": today, "stream_ok": False,
            "_completed_delta": 0, "_completed_volume": 0, "_completed_buy_vol": 0,
            "_completed_sell_vol": 0, "_completed_ticks": 0,
            "_open_epoch": 0, "_open_delta": 0, "_open_volume": 0,
            "_open_buy_vol": 0, "_open_sell_vol": 0, "_open_ticks": 0,
            "_bars_buffer": [],
        })
    print(f"[es-delta] daily reset for {today}", flush=True)

def _es_delta_process_bar(bar: dict):
    """Process a single bar from the streaming barchart feed."""
    epoch = bar.get("Epoch", 0)
    up_vol = int(bar.get("UpVolume") or 0)
    down_vol = int(bar.get("DownVolume") or 0)
    total_vol = int(bar.get("TotalVolume") or 0)
    up_ticks = int(bar.get("UpTicks") or 0)
    down_ticks = int(bar.get("DownTicks") or 0)
    total_ticks = int(bar.get("TotalTicks") or 0)
    bar_delta = up_vol - down_vol
    bar_status = bar.get("BarStatus", "Closed")

    close_p = float(bar.get("Close") or 0)
    high_p = float(bar.get("High") or 0)
    low_p = float(bar.get("Low") or 0)
    open_p = float(bar.get("Open") or 0)

    with _es_delta_lock:
        # Update price
        if close_p:
            _es_delta["last_price"] = close_p
        if high_p and (_es_delta["session_high"] is None or high_p > _es_delta["session_high"]):
            _es_delta["session_high"] = high_p
        if low_p and (_es_delta["session_low"] is None or low_p < _es_delta["session_low"]):
            _es_delta["session_low"] = low_p

        if bar_status == "Open":
            # Current bar being formed — update in-place (replaces previous open bar state)
            _es_delta["_open_epoch"] = epoch
            _es_delta["_open_delta"] = bar_delta
            _es_delta["_open_volume"] = total_vol
            _es_delta["_open_buy_vol"] = up_vol
            _es_delta["_open_sell_vol"] = down_vol
            _es_delta["_open_ticks"] = total_ticks
        else:
            # Closed bar (historical backfill or the open bar just completed)
            if epoch == _es_delta["_open_epoch"]:
                # Open bar just closed — clear open state
                _es_delta["_open_epoch"] = 0
                _es_delta["_open_delta"] = 0
                _es_delta["_open_volume"] = 0
                _es_delta["_open_buy_vol"] = 0
                _es_delta["_open_sell_vol"] = 0
                _es_delta["_open_ticks"] = 0

            _es_delta["_completed_delta"] += bar_delta
            _es_delta["_completed_volume"] += total_vol
            _es_delta["_completed_buy_vol"] += up_vol
            _es_delta["_completed_sell_vol"] += down_vol
            _es_delta["_completed_ticks"] += total_ticks

            # Buffer completed bar for DB flush
            _es_delta["_bars_buffer"].append({
                "ts": bar.get("TimeStamp"), "epoch": epoch,
                "bar_delta": bar_delta,
                "cumulative_delta": _es_delta["_completed_delta"],
                "bar_volume": total_vol, "bar_buy_volume": up_vol, "bar_sell_volume": down_vol,
                "bar_open_price": open_p, "bar_close_price": close_p,
                "bar_high_price": high_p, "bar_low_price": low_p,
                "up_ticks": up_ticks, "down_ticks": down_ticks, "total_ticks": total_ticks,
            })

        # Update combined totals (completed + current open bar)
        _es_delta["cumulative_delta"] = _es_delta["_completed_delta"] + _es_delta["_open_delta"]
        _es_delta["total_volume"] = _es_delta["_completed_volume"] + _es_delta["_open_volume"]
        _es_delta["buy_volume"] = _es_delta["_completed_buy_vol"] + _es_delta["_open_buy_vol"]
        _es_delta["sell_volume"] = _es_delta["_completed_sell_vol"] + _es_delta["_open_sell_vol"]
        _es_delta["tick_count"] = _es_delta["_completed_ticks"] + _es_delta["_open_ticks"]

# ====== ES QUOTE STREAM (bid/ask delta classification — ATAS-style range bars) ======
_es_quote_lock = Lock()
_es_quote = {
    "stream_ok": False,
    "trade_date": None,
    "last_price": None,
    "last_bid": None,
    "last_ask": None,
    "cumulative_delta": 0,
    "total_volume": 0,
    "buy_volume": 0,
    "sell_volume": 0,
    "trade_count": 0,
    "_range_pts": 5.0,
    "_forming_bar": None,
    "_completed_bars": [],
    "_completed_bars_flushed": 0,
    "_cvd": 0,
    "_bar_idx": 0,
    "_flush_buffer": [],
    "_last_trade_time": None,
}

def _classify_trade_delta(last: float, bid: float, ask: float, volume: int):
    """ATAS-style bid/ask classification.
    Last >= Ask → buy (hit the ask) | Last <= Bid → sell (hit the bid)
    Between → classify by proximity to mid
    Returns (buy_vol, sell_vol, delta)
    """
    if last >= ask:
        return volume, 0, volume
    if last <= bid:
        return 0, volume, -volume
    mid = (bid + ask) / 2.0
    if last >= mid:
        return volume, 0, volume
    return 0, volume, -volume

def _new_quote_range_bar(price: float, ts: str):
    """Create a new forming range bar starting at the given price."""
    return {
        "open": price, "high": price, "low": price, "close": price,
        "volume": 0, "buy": 0, "sell": 0, "delta": 0,
        "ts_start": ts, "ts_end": ts,
        "cvd_open": _es_quote["_cvd"],
        "cvd_high": _es_quote["_cvd"],
        "cvd_low": _es_quote["_cvd"],
    }

def _es_quote_process_trade(last: float, bid: float, ask: float, volume: int, ts: str):
    """Process a single trade tick into the forming range bar.
    Must be called under _es_quote_lock.

    Returns list snapshot of completed bars if a bar just closed (for fallback
    absorption callback when Rithmic is unavailable), or None.
    """
    q = _es_quote
    buy_vol, sell_vol, delta = _classify_trade_delta(last, bid, ask, volume)

    q["last_price"] = last
    q["last_bid"] = bid
    q["last_ask"] = ask
    q["total_volume"] += volume
    q["buy_volume"] += buy_vol
    q["sell_volume"] += sell_vol
    q["cumulative_delta"] += delta
    q["trade_count"] += 1
    q["_last_trade_time"] = ts

    # Ensure we have a forming bar
    if q["_forming_bar"] is None:
        q["_forming_bar"] = _new_quote_range_bar(last, ts)

    bar = q["_forming_bar"]
    bar["close"] = last
    bar["high"] = max(bar["high"], last)
    bar["low"] = min(bar["low"], last)
    bar["volume"] += volume
    bar["buy"] += buy_vol
    bar["sell"] += sell_vol
    bar["delta"] += delta
    bar["ts_end"] = ts

    # Track CVD within bar
    q["_cvd"] += delta
    bar["cvd_high"] = max(bar["cvd_high"], q["_cvd"])
    bar["cvd_low"] = min(bar["cvd_low"], q["_cvd"])

    # Check if range bar is complete
    range_pts = q["_range_pts"]
    if bar["high"] - bar["low"] >= range_pts - 0.001:
        # Close this bar
        completed = {
            "idx": q["_bar_idx"],
            "open": bar["open"], "high": bar["high"],
            "low": bar["low"], "close": bar["close"],
            "volume": bar["volume"], "delta": bar["delta"],
            "buy_volume": bar["buy"], "sell_volume": bar["sell"],
            "cvd": q["_cvd"],
            "cvd_open": bar["cvd_open"],
            "cvd_high": bar["cvd_high"],
            "cvd_low": bar["cvd_low"],
            "cvd_close": q["_cvd"],
            "ts_start": bar["ts_start"], "ts_end": bar["ts_end"],
            "status": "closed",
        }
        q["_completed_bars"].append(completed)
        q["_flush_buffer"].append(completed)
        q["_bar_idx"] += 1
        print(f"[es-quote] bar #{completed['idx']} closed: "
              f"O={completed['open']:.2f} H={completed['high']:.2f} "
              f"L={completed['low']:.2f} C={completed['close']:.2f} "
              f"vol={completed['volume']} delta={completed['delta']:+d} "
              f"cvd={completed['cvd']:+d}", flush=True)
        # Start new bar at the close price of the completed bar
        q["_forming_bar"] = _new_quote_range_bar(last, ts)
        # Return snapshot for fallback absorption when Rithmic unavailable
        return list(q["_completed_bars"])

    return None

# ====== ES ABSORPTION DETECTOR (via setup_detector) ======
_absorption_signals = []  # detected signals for chart markers

def _es_quote_reset():
    """Reset quote stream state for new session or process restart.
    Reloads previously-flushed bars from DB to survive restarts.
    Must NOT be called under lock (acquires lock internally).
    """
    session_date = _es_session_date()
    # Load existing bars from DB
    db_bars = []
    if engine:
        try:
            with engine.begin() as conn:
                rows = conn.execute(text("""
                    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                           bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                           cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                           ts_start, ts_end, status
                    FROM es_range_bars
                    WHERE trade_date = :td AND symbol = :sym AND range_pts = :rp
                    ORDER BY bar_idx ASC
                """), {"td": session_date, "sym": ES_DELTA_SYMBOL, "rp": 5.0}).mappings().all()
                for r in rows:
                    db_bars.append({
                        "idx": r["bar_idx"],
                        "open": r["bar_open"], "high": r["bar_high"],
                        "low": r["bar_low"], "close": r["bar_close"],
                        "volume": r["bar_volume"], "delta": r["bar_delta"],
                        "buy_volume": r["bar_buy_volume"], "sell_volume": r["bar_sell_volume"],
                        "cvd": r["cvd_close"],
                        "cvd_open": r["cvd_open"], "cvd_high": r["cvd_high"],
                        "cvd_low": r["cvd_low"], "cvd_close": r["cvd_close"],
                        "ts_start": r["ts_start"].isoformat() if r["ts_start"] else "",
                        "ts_end": r["ts_end"].isoformat() if r["ts_end"] else "",
                        "status": r["status"],
                    })
        except Exception as e:
            print(f"[es-quote] DB reload error: {e}", flush=True)

    with _es_quote_lock:
        _es_quote.update({
            "stream_ok": False,
            "trade_date": session_date,
            "last_price": None,
            "last_bid": None,
            "last_ask": None,
            "cumulative_delta": 0,
            "total_volume": 0,
            "buy_volume": 0,
            "sell_volume": 0,
            "trade_count": 0,
            "_forming_bar": None,
            "_completed_bars": db_bars,
            "_completed_bars_flushed": len(db_bars),
            "_cvd": db_bars[-1]["cvd_close"] if db_bars else 0,
            "_bar_idx": (db_bars[-1]["idx"] + 1) if db_bars else 0,
            "_flush_buffer": [],
            "_last_trade_time": None,
        })
    if db_bars:
        print(f"[es-quote] restored {len(db_bars)} bars from DB (session {session_date}, "
              f"cvd={_es_quote['_cvd']:+d})", flush=True)
    else:
        print(f"[es-quote] fresh session {session_date} (no prior bars)", flush=True)

    # Reset absorption detector for new session
    global _last_absorption_bar_idx
    _last_absorption_bar_idx = -1
    from app.setup_detector import reset_absorption_session
    reset_absorption_session()
    _absorption_signals.clear()

def _run_absorption_detection(bars: list) -> dict | None:
    """Thin wrapper: evaluates absorption via setup_detector, logs, and notifies.

    Returns signal dict (for chart markers) or None.
    """
    from app.setup_detector import (
        evaluate_absorption, should_notify_absorption, format_absorption_message,
    )

    # Build volland stats dict for setup_detector
    volland_stats = None
    try:
        vstat = db_volland_stats()
        if vstat and vstat.get("stats") and vstat["stats"].get("has_statistics"):
            volland_stats = vstat["stats"]
    except Exception as e:
        print(f"[absorption] volland lookup error: {e}", flush=True)

    # SPX spot from latest chain pull (needed for LIS distance calc)
    spx_spot = None
    try:
        msg = last_run_status.get("msg") or ""
        parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
        spx_spot = float(parts.get("spot", ""))
    except Exception:
        pass

    result = evaluate_absorption(bars, volland_stats, _setup_settings, spx_spot=spx_spot)
    if result is None:
        return None

    # Freshness check: compare trigger bar price to CURRENT Rithmic price.
    # If the market moved significantly while the callback was queued/processing,
    # the signal is stale and would enter at the wrong price.
    MAX_STALE_DISTANCE = 10.0  # max pts between trigger bar and current ES
    trigger_price = result.get("abs_es_price")
    if trigger_price:
        try:
            from rithmic_es_stream import get_rithmic_bars
            current_bars = get_rithmic_bars()
            if current_bars:
                current_price = current_bars[-1].get("close", trigger_price)
                stale_dist = abs(current_price - trigger_price)
                if stale_dist > MAX_STALE_DISTANCE:
                    print(f"[absorption] SKIPPED stale signal: trigger={trigger_price:.2f} "
                          f"current={current_price:.2f} dist={stale_dist:.1f} > {MAX_STALE_DISTANCE}",
                          flush=True)
                    return None
        except (ImportError, Exception) as e:
            print(f"[absorption] freshness check error: {e}", flush=True)

    # Parse SPX target from Volland stats
    target_spx = None
    if volland_stats and volland_stats.get("target"):
        target_str = str(volland_stats["target"]).replace("$", "").replace(",", "")
        target_match = re.search(r"[\d.]+", target_str)
        if target_match:
            target_spx = float(target_match.group())

    # Get +GEX/-GEX from latest options chain
    gex_plus, gex_minus = None, None
    with _df_lock:
        if latest_df is not None and not latest_df.empty:
            try:
                df = latest_df.copy()
                sdf = df.sort_values("Strike")
                strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float)
                call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
                put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
                c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
                p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
                net_gex = (c_gamma * call_oi * 100.0) + (-p_gamma * put_oi * 100.0)
                max_pos_idx = net_gex.idxmax() if not net_gex.empty else None
                max_neg_idx = net_gex.idxmin() if not net_gex.empty else None
                gex_plus = float(strikes.loc[max_pos_idx]) if max_pos_idx is not None else None
                gex_minus = float(strikes.loc[max_neg_idx]) if max_neg_idx is not None else None
            except Exception:
                pass

    # Override result fields with SPX context for setup_log
    # spot = SPX spot (for conversion offset), abs_es_price = ES entry price
    if spx_spot:
        result["spot"] = round(spx_spot, 2)
    result["target"] = target_spx
    result["max_plus_gex"] = gex_plus
    result["max_minus_gex"] = gex_minus

    # Build signal dict for chart markers
    signal = {
        "bar_idx": result["bar_idx"],
        "direction": result["direction"],
        "grade": result["grade"],
        "score": result["score"],
        "max_score": 100,
        "price": result["abs_es_price"],
        "cvd": result["cvd"],
        "high": result["high"],
        "low": result["low"],
        "vol_ratio": result["abs_vol_ratio"],
        "vol_trigger": result["vol_trigger"],
        "div_score": result["div_raw"],
        "vol_score": result["vol_raw"],
        "dd_score": result["dd_raw"],
        "para_score": result["para_raw"],
        "lis_score": result["lis_raw"],
        "lis_side_score": result.get("lis_side_raw", 0),
        "target_dir_score": result.get("target_dir_raw", 0),
        "paradigm": result["paradigm"],
        "dd_hedging": result["dd_hedging"],
        "lis_val": result["lis_val"],
        "lis_dist": result.get("lis_dist"),
        "target_val": result.get("target_val"),
        "ts": result.get("ts", ""),
        "pattern": result.get("pattern", "unknown"),
        "best_swing": result.get("best_swing"),
        "all_divergences": result.get("all_divergences"),
        "swing_count": result.get("swing_count", 0),
    }

    _absorption_signals.append(signal)

    # Console log — primary swing
    best = result.get("best_swing", {})
    best_sw = best.get("swing", {}) if best else {}
    all_divs = result.get("all_divergences", [])
    pattern = result.get("pattern", "unknown")
    ref_sw = best.get("ref_swing", {}) if best else {}
    print(f"[absorption] {result['direction'].upper()} {result['grade']} ({result['score']:.0f}/100) "
          f"pattern={pattern} price={result['abs_es_price']:.2f} cvd={result['cvd']:+d} "
          f"vol={result['vol_trigger']}({result['abs_vol_ratio']:.1f}x) "
          f"swing_pair: {ref_sw.get('type','?')}@{ref_sw.get('price',0):.2f}"
          f" -> {best_sw.get('type','?')}@{best_sw.get('price',0):.2f} "
          f"cvd_z={best.get('cvd_z',0):.2f} price_atr={best.get('price_atr',0):.1f}x "
          f"swings={result.get('swing_count',0)}",
          flush=True)
    # Log all confirming divergences
    for i, div in enumerate(all_divs):
        sw = div["swing"]
        ref = div.get("ref_swing", {})
        print(f"  div#{i+1}: {div.get('pattern','?')} "
              f"{ref.get('type','?')}@{ref.get('price',0):.2f} -> "
              f"{sw['type']}@{sw['price']:.2f} idx={sw['bar_idx']} "
              f"cvd={sw['cvd']:+d} -> z={div['cvd_z']:.2f} atr={div['price_atr']:.1f}x "
              f"score={div['score']:.0f}",
              flush=True)

    # Notification gate
    fire, reason = should_notify_absorption(result)

    # Always log signal to setup_log for history (regardless of cooldown)
    rw = {
        "result": result,
        "notify": fire,
        "notify_reason": reason or "cooldown",
        "message": format_absorption_message(result),
    }
    log_setup(rw)

    # Send Telegram only when notification gate passes
    if fire:
        try:
            send_telegram_setups(rw["message"])
        except Exception as e:
            print(f"[absorption] telegram error: {e}", flush=True)

    # Always track outcome for results validation (regardless of cooldown)
    # Uses reason="new" from gate, or "cooldown" — either way we track
    target_lvl, stop_lvl = _compute_setup_levels(result)
    if target_lvl is not None and stop_lvl is not None:
        es_entry = result.get("abs_es_price", result["spot"])
        _setup_open_trades.append({
            "setup_name": "ES Absorption", "direction": result["direction"],
            "spot": result["spot"], "grade": result["grade"],
            "target_level": target_lvl, "stop_level": stop_lvl,
            "ts": now_et(), "result_data": result,
            # Initialize seen_high/low to ES entry price so fallback never
            # triggers false WIN/LOSS (bars will update these as they complete)
            "_seen_high": es_entry, "_seen_low": es_entry,
            "_es_last_bar_idx": result.get("bar_idx", 0),
            "max_hold_minutes": None,
            "_trade_date": now_et().date(),
            "setup_log_id": _current_setup_log.get("ES Absorption"),
        })
        print(f"[outcome] tracking ES Absorption: target={target_lvl:.1f} stop={stop_lvl:.1f}", flush=True)
        # Auto-trade: ES Absorption uses ES price directly
        try:
            from app import auto_trader
            es_px = result.get("abs_es_price")
            if not es_px:
                # Fallback: use quote stream or delta stream price
                with _es_quote_lock:
                    es_px = _es_quote.get("last_price")
                if not es_px:
                    with _es_delta_lock:
                        es_px = _es_delta.get("last_price")
                if es_px:
                    print(f"[auto-trader] ES Absorption using fallback price: {es_px}", flush=True)
            if es_px and stop_lvl is not None:
                stop_dist = abs(es_px - stop_lvl)
                target_dist = abs(target_lvl - es_px) if target_lvl else None
                auto_trader.place_trade(
                    setup_log_id=_current_setup_log.get("ES Absorption"),
                    setup_name="ES Absorption", direction=result["direction"],
                    es_price=es_px, target_pts=target_dist, stop_pts=stop_dist,
                    full_target_pts=target_dist,
                )
            elif not es_px:
                print(f"[auto-trader] SKIPPED ES Absorption: no ES price available", flush=True)
        except Exception as e:
            print(f"[auto-trader] absorption place error: {e}", flush=True)

    return signal


# Track last bar idx to avoid re-evaluating same bar
_last_absorption_bar_idx = -1
_absorption_thread_lock = Lock()


def _on_rithmic_bar_complete(bars: list):
    """Callback from Rithmic stream when a range bar completes.

    Runs absorption detection in a background thread so the Rithmic event
    loop isn't blocked by DB/HTTP calls (which caused stale-signal bugs
    where the trigger bar was minutes old by the time it was logged).
    """
    global _last_absorption_bar_idx
    if not bars:
        return
    # Only run during RTH market hours (10:00-16:00 ET, matching absorption time gate)
    t = now_et()
    if not (dtime(10, 0) <= t.time() <= dtime(16, 0)):
        return
    # Avoid re-evaluating the same bar
    last_bar = bars[-1]
    bar_idx = last_bar.get("idx", -1)
    if bar_idx <= _last_absorption_bar_idx:
        return
    _last_absorption_bar_idx = bar_idx
    # Skip stale bars restored from DB after a deploy/restart.
    # Only fire absorption when trigger bar was built from live ticks.
    try:
        from rithmic_es_stream import get_live_since_idx
        live_idx = get_live_since_idx()
        if bar_idx < live_idx:
            return
    except (ImportError, Exception):
        pass
    # Offload to thread so Rithmic tick processing isn't blocked.
    # The bars snapshot is already a copy (list of dicts) so thread-safe.
    bars_copy = list(bars)
    Thread(
        target=_run_absorption_in_thread,
        args=(bars_copy,),
        daemon=True,
    ).start()


def _run_absorption_in_thread(bars: list):
    """Thread wrapper: runs absorption detection with freshness check."""
    # Serialize — only one absorption detection at a time
    with _absorption_thread_lock:
        try:
            _run_absorption_detection(bars)
        except Exception as e:
            print(f"[absorption] proactive eval error: {e}", flush=True)


def _es_session_date() -> str:
    """Return the ES futures session date.

    ES sessions run 6 PM ET → 5 PM ET next day. The session date is the
    NEXT calendar date once the clock passes 6 PM (matching pro platforms
    like Sierra Chart, NinjaTrader, ATAS). Before 6 PM = today's date.
    """
    t = now_et()
    if t.hour >= 18:  # 6 PM or later → next day's session
        return (t + timedelta(days=1)).strftime("%Y-%m-%d")
    return t.strftime("%Y-%m-%d")

def _es_futures_open() -> bool:
    """Check if ES futures are currently trading.

    ES futures: Sunday 6 PM ET → Friday 5 PM ET
    Daily maintenance break: 5 PM – 6 PM ET (Mon–Thu)
    Closed: Friday 5 PM → Sunday 6 PM
    """
    t = now_et()
    wd = t.weekday()  # Mon=0 … Sun=6
    hour = t.hour

    # Saturday: always closed
    if wd == 5:
        return False
    # Sunday: open only from 6 PM onward
    if wd == 6:
        return hour >= 18
    # Friday: open until 5 PM only
    if wd == 4:
        return hour < 17
    # Mon-Thu: closed during 5 PM – 6 PM maintenance window
    return not (hour == 17)

def _es_delta_stream_loop():
    """Background thread: streams @ES 1-min barcharts for real-time delta updates.

    Covers the full futures session (6 PM ET → 5 PM ET next day) with
    1380 bars backfill (~23 hours) to match pro platforms.
    """
    while True:
        try:
            # Wait for ES futures session
            if not _es_futures_open():
                time.sleep(30)
                continue

            session_date = _es_session_date()
            if _es_delta["trade_date"] != session_date:
                _es_delta_reset(session_date)

            # Open streaming connection with full session backfill
            token = ts_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            params = {"interval": "1", "unit": "Minute", "barsback": "1380"}
            r = requests.get(
                f"{BASE}/marketdata/stream/barcharts/%40ES",
                headers=headers, params=params, stream=True, timeout=30,
            )
            # Retry on 401
            if r.status_code == 401:
                token = ts_access_token()
                headers["Authorization"] = f"Bearer {token}"
                r = requests.get(
                    f"{BASE}/marketdata/stream/barcharts/%40ES",
                    headers=headers, params=params, stream=True, timeout=30,
                )
                if r.status_code == 401:
                    _alert_401("es-delta stream")
            if r.status_code != 200:
                print(f"[es-delta] stream error [{r.status_code}] {r.text[:200]}", flush=True)
                time.sleep(10)
                continue

            with _es_delta_lock:
                _es_delta["stream_ok"] = True
            print(f"[es-delta] stream connected (session {session_date}, backfilling 1380 bars)", flush=True)

            for line in r.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except Exception:
                    continue

                # Stream control messages
                if "Heartbeat" in data:
                    continue
                if "Error" in data:
                    print(f"[es-delta] stream error msg: {data}", flush=True)
                    break
                if data.get("StreamStatus") == "GoAway":
                    print("[es-delta] GoAway received, reconnecting", flush=True)
                    break
                if data.get("StreamStatus") == "EndSnapshot":
                    print(f"[es-delta] backfill done: delta={_es_delta['cumulative_delta']:+d} "
                          f"vol={_es_delta['total_volume']} price={_es_delta['last_price']}", flush=True)
                    continue

                # Session date rollover check (6 PM ET = new session)
                new_session = _es_session_date()
                if _es_delta["trade_date"] != new_session:
                    print(f"[es-delta] session rollover → {new_session}", flush=True)
                    _es_delta_reset(new_session)

                # Process bar update
                if "Epoch" in data:
                    _es_delta_process_bar(data)

            try:
                r.close()
            except Exception:
                pass

        except Exception as e:
            print(f"[es-delta] stream error: {e}", flush=True)

        with _es_delta_lock:
            _es_delta["stream_ok"] = False
        time.sleep(5)  # brief delay before reconnect

def _es_quote_stream_loop():
    """Background thread: streams @ES quotes for bid/ask delta classification.

    Tracks DailyVolume to detect trades. Each trade is classified as buy/sell
    based on whether Last >= Ask (buy) or Last <= Bid (sell), then fed into
    tick-perfect range bar construction.
    """
    backoff = 1.0
    prev_daily_vol = None
    last_vals = {}  # persistent NBBO: Last, Bid, Ask

    while True:
        try:
            # Wait for ES futures session
            if not _es_futures_open():
                backoff = 1.0
                time.sleep(30)
                continue

            session_date = _es_session_date()
            if _es_quote["trade_date"] != session_date:
                _es_quote_reset()
                prev_daily_vol = None
                last_vals = {}

            token = ts_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            r = requests.get(
                f"{BASE}/marketdata/stream/quotes/%40ES",
                headers=headers, stream=True, timeout=30,
            )
            if r.status_code == 401:
                token = ts_access_token()
                headers["Authorization"] = f"Bearer {token}"
                r = requests.get(
                    f"{BASE}/marketdata/stream/quotes/%40ES",
                    headers=headers, stream=True, timeout=30,
                )
                if r.status_code == 401:
                    _alert_401("es-quote stream")
            if r.status_code != 200:
                print(f"[es-quote] stream error [{r.status_code}] {r.text[:200]}", flush=True)
                time.sleep(min(backoff, 60))
                backoff *= 2
                continue

            # Connected successfully — reset backoff
            backoff = 1.0
            with _es_quote_lock:
                _es_quote["stream_ok"] = True
            print(f"[es-quote] stream connected (session {session_date})", flush=True)

            for line in r.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except Exception:
                    continue

                # Stream control messages
                if "Heartbeat" in data:
                    continue
                if "Error" in data:
                    err_msg = data.get("Error", "")
                    print(f"[es-quote] stream error msg: {data}", flush=True)
                    if err_msg == "DualLogon":
                        # Another session is connected — wait longer before retry
                        backoff = max(backoff, 15.0)
                    break
                if data.get("StreamStatus") == "GoAway":
                    print("[es-quote] GoAway received, reconnecting", flush=True)
                    break

                # Session date rollover
                new_session = _es_session_date()
                if _es_quote["trade_date"] != new_session:
                    print(f"[es-quote] session rollover → {new_session}", flush=True)
                    _es_quote_reset()
                    prev_daily_vol = None
                    last_vals = {}

                # Merge partial updates into persistent NBBO
                # TradeStation uses "Volume" (not "DailyVolume") for cumulative daily volume
                for key in ("Last", "Bid", "Ask", "Volume"):
                    if key in data:
                        last_vals[key] = data[key]

                # Detect trade: Volume increased
                daily_vol_str = last_vals.get("Volume")
                if daily_vol_str is None:
                    continue
                try:
                    daily_vol = int(daily_vol_str)
                except (ValueError, TypeError):
                    continue

                if prev_daily_vol is None:
                    # First snapshot — set baseline, no trade to process
                    prev_daily_vol = daily_vol
                    continue

                if daily_vol <= prev_daily_vol:
                    continue  # No new trade

                trade_vol = daily_vol - prev_daily_vol
                prev_daily_vol = daily_vol

                # Need Last, Bid, Ask to classify
                last_p = last_vals.get("Last")
                bid_p = last_vals.get("Bid")
                ask_p = last_vals.get("Ask")
                if last_p is None or bid_p is None or ask_p is None:
                    continue
                try:
                    last_f = float(last_p)
                    bid_f = float(bid_p)
                    ask_f = float(ask_p)
                except (ValueError, TypeError):
                    continue

                ts_now = now_et().isoformat()
                ts_bar_snapshot = None
                with _es_quote_lock:
                    ts_bar_snapshot = _es_quote_process_trade(last_f, bid_f, ask_f, trade_vol, ts_now)
                    tc = _es_quote["trade_count"]
                    # Log first 5 trades, then every 1000th for diagnostics
                    if tc <= 5 or tc % 1000 == 0:
                        fb = _es_quote.get("_forming_bar")
                        fb_range = f"{fb['high'] - fb['low']:.2f}" if fb else "?"
                        print(f"[es-quote] trade #{tc}: last={last_f} vol={trade_vol} "
                              f"completed={len(_es_quote['_completed_bars'])} "
                              f"forming_range={fb_range}/{_es_quote['_range_pts']}",
                              flush=True)

                # Fallback absorption: run on TS bars only if Rithmic is not connected
                if ts_bar_snapshot:
                    try:
                        from rithmic_es_stream import get_rithmic_state
                        rithmic_ok = get_rithmic_state().get("connected", False)
                    except Exception:
                        rithmic_ok = False
                    if not rithmic_ok:
                        _on_rithmic_bar_complete(ts_bar_snapshot)

            try:
                r.close()
            except Exception:
                pass

        except Exception as e:
            print(f"[es-quote] stream error: {e}", flush=True)

        with _es_quote_lock:
            _es_quote["stream_ok"] = False
        reconnect_wait = min(backoff, 60)
        print(f"[es-quote] reconnecting in {reconnect_wait:.0f}s", flush=True)
        time.sleep(reconnect_wait)
        backoff *= 2

def save_es_delta():
    """Scheduler job: flush buffered bars + write snapshot to DB (every 2 min)."""
    try:
        if not _es_futures_open():
            return
        if not engine:
            return
        with _es_delta_lock:
            if _es_delta["total_volume"] == 0:
                return
            today = _es_delta["trade_date"] or now_et().strftime("%Y-%m-%d")
            # Snapshot buffered bars and current state under lock
            bars = _es_delta["_bars_buffer"]
            _es_delta["_bars_buffer"] = []
            snap = {
                "cd": _es_delta["cumulative_delta"],
                "tv": _es_delta["total_volume"],
                "bv": _es_delta["buy_volume"],
                "sv": _es_delta["sell_volume"],
                "lp": _es_delta["last_price"],
                "tc": _es_delta["tick_count"],
                "bh": _es_delta["session_high"],
                "bl": _es_delta["session_low"],
            }
        if bars:
            with engine.begin() as conn:
                for b in bars:
                    conn.execute(text("""
                        INSERT INTO es_delta_bars
                            (ts, trade_date, symbol, bar_delta, cumulative_delta,
                             bar_volume, bar_buy_volume, bar_sell_volume,
                             bar_open_price, bar_close_price, bar_high_price, bar_low_price,
                             up_ticks, down_ticks, total_ticks)
                        VALUES (:ts, :td, :sym, :bd, :cd, :v, :bv, :sv, :op, :cp, :hp, :lp, :ut, :dt, :tt)
                        ON CONFLICT (ts, symbol) DO UPDATE SET
                            bar_delta = EXCLUDED.bar_delta,
                            cumulative_delta = EXCLUDED.cumulative_delta,
                            bar_volume = EXCLUDED.bar_volume,
                            bar_buy_volume = EXCLUDED.bar_buy_volume,
                            bar_sell_volume = EXCLUDED.bar_sell_volume,
                            bar_close_price = EXCLUDED.bar_close_price,
                            bar_high_price = EXCLUDED.bar_high_price,
                            bar_low_price = EXCLUDED.bar_low_price,
                            up_ticks = EXCLUDED.up_ticks,
                            down_ticks = EXCLUDED.down_ticks,
                            total_ticks = EXCLUDED.total_ticks
                    """), {
                        "ts": b["ts"], "td": today, "sym": ES_DELTA_SYMBOL,
                        "bd": b["bar_delta"], "cd": b["cumulative_delta"],
                        "v": b["bar_volume"], "bv": b["bar_buy_volume"], "sv": b["bar_sell_volume"],
                        "op": b["bar_open_price"], "cp": b["bar_close_price"],
                        "hp": b["bar_high_price"], "lp": b["bar_low_price"],
                        "ut": b["up_ticks"], "dt": b["down_ticks"], "tt": b["total_ticks"],
                    })
            print(f"[es-delta] flushed {len(bars)} bars to DB", flush=True)

        # Write snapshot from snapshotted state (lock already released)
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO es_delta_snapshots
                    (trade_date, symbol, cumulative_delta, total_volume,
                     buy_volume, sell_volume, last_price, tick_count,
                     bar_high, bar_low)
                VALUES (:td, :sym, :cd, :tv, :bv, :sv, :lp, :tc, :bh, :bl)
            """), {
                "td": today, "sym": ES_DELTA_SYMBOL,
                **snap,
            })
    except Exception as e:
        print(f"[es-delta] save error: {e}", flush=True)

def save_es_range_bars():
    """Scheduler job: flush quote-stream range bars to DB (every 2 min)."""
    try:
        if not _es_futures_open():
            return
        if not engine:
            print("[es-quote-save] no DB engine, skipping", flush=True)
            return

        with _es_quote_lock:
            bars = _es_quote["_flush_buffer"]
            _es_quote["_flush_buffer"] = []
            # Snapshot diagnostics under lock
            _diag_stream = _es_quote.get("stream_ok", False)
            _diag_trades = _es_quote.get("trade_count", 0)
            _diag_completed = len(_es_quote.get("_completed_bars", []))
            _diag_forming = _es_quote.get("_forming_bar")
            _diag_range_pts = _es_quote.get("_range_pts", 5.0)

        if not bars:
            forming_info = "none"
            if _diag_forming:
                fr = _diag_forming["high"] - _diag_forming["low"]
                forming_info = f"{fr:.2f}/{_diag_range_pts}pt vol={_diag_forming['volume']}"
            print(f"[es-quote-save] empty buffer: stream={_diag_stream} trades={_diag_trades} "
                  f"completed={_diag_completed} forming={forming_info}", flush=True)
            return

        today = _es_quote["trade_date"] or _es_session_date()
        with engine.begin() as conn:
            for b in bars:
                conn.execute(text("""
                    INSERT INTO es_range_bars
                        (trade_date, symbol, bar_idx, range_pts,
                         bar_open, bar_high, bar_low, bar_close,
                         bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                         cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                         ts_start, ts_end, status, source)
                    VALUES (:td, :sym, :idx, :rp,
                            :bo, :bh, :bl, :bc,
                            :bv, :bbv, :bsv, :bd,
                            :cd, :co, :ch, :cl, :cc,
                            :ts0, :ts1, :st, 'live')
                    ON CONFLICT (trade_date, symbol, bar_idx, range_pts) DO UPDATE SET
                        bar_open = EXCLUDED.bar_open, bar_high = EXCLUDED.bar_high,
                        bar_low = EXCLUDED.bar_low, bar_close = EXCLUDED.bar_close,
                        bar_volume = EXCLUDED.bar_volume, bar_buy_volume = EXCLUDED.bar_buy_volume,
                        bar_sell_volume = EXCLUDED.bar_sell_volume, bar_delta = EXCLUDED.bar_delta,
                        cumulative_delta = EXCLUDED.cumulative_delta,
                        cvd_open = EXCLUDED.cvd_open, cvd_high = EXCLUDED.cvd_high,
                        cvd_low = EXCLUDED.cvd_low, cvd_close = EXCLUDED.cvd_close,
                        ts_start = EXCLUDED.ts_start, ts_end = EXCLUDED.ts_end,
                        status = EXCLUDED.status
                """), {
                    "td": today, "sym": ES_DELTA_SYMBOL, "idx": b["idx"], "rp": 5.0,
                    "bo": b["open"], "bh": b["high"], "bl": b["low"], "bc": b["close"],
                    "bv": b["volume"], "bbv": b["buy_volume"], "bsv": b["sell_volume"], "bd": b["delta"],
                    "cd": b["cvd"], "co": b["cvd_open"], "ch": b["cvd_high"],
                    "cl": b["cvd_low"], "cc": b["cvd_close"],
                    "ts0": b["ts_start"], "ts1": b["ts_end"], "st": b["status"],
                })
        print(f"[es-quote] flushed {len(bars)} range bars to DB", flush=True)
    except Exception as e:
        print(f"[es-quote] save error: {e}", flush=True)

def _save_rithmic_bars():
    """Scheduler job: flush Rithmic range bars to DB (every 2 min)."""
    try:
        if not _es_futures_open() or not engine:
            return
        from rithmic_es_stream import flush_rithmic_bars
        flush_rithmic_bars(engine)
    except Exception as e:
        print(f"[rithmic] save error: {e}", flush=True)

def _auto_trade_eod_flatten():
    """Flatten all open SIM auto-trade positions before market close. Runs at 15:55 ET."""
    try:
        from app import auto_trader
        auto_trader.flatten_all_eod()
    except Exception as e:
        print(f"[auto-trade-eod] flatten error: {e}", flush=True)


def _send_setup_eod_summary():
    """Send end-of-day summary of all setup outcomes via Telegram. Runs at 16:05 ET."""
    global _setup_open_trades, _setup_resolved_trades
    from app.setup_detector import format_setup_outcome, format_setup_daily_summary

    now = now_et()
    print(f"[eod-summary] running at {now.strftime('%H:%M:%S')}", flush=True)

    # First, expire any still-open trades (market closed)
    es_price = None
    spot = None
    try:
        msg = last_run_status.get("msg") or ""
        parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
        spot = float(parts.get("spot", ""))
    except Exception:
        pass
    with _es_quote_lock:
        if _es_quote["_completed_bars"]:
            es_price = _es_quote["_completed_bars"][-1].get("close")

    for trade in _setup_open_trades:
        setup_name = trade["setup_name"]
        direction = trade["direction"]
        is_long = direction.lower() in ("long", "bullish")
        ts_entry = trade["ts"]

        if setup_name == "ES Absorption":
            check_price = es_price if es_price else spot
            entry_price = trade.get("result_data", {}).get("abs_es_price", trade["spot"])
        else:
            check_price = spot
            entry_price = trade["spot"]

        if check_price and entry_price:
            pnl = round((check_price - entry_price) if is_long else (entry_price - check_price), 1)
        else:
            pnl = 0.0

        elapsed_min = int((now - ts_entry).total_seconds() / 60.0) if ts_entry else 0
        trade["close_price"] = check_price

        try:
            outcome_msg = format_setup_outcome(trade, "EXPIRED", pnl, elapsed_min)
            send_telegram_setups(outcome_msg)
        except Exception as e:
            print(f"[eod-summary] expire telegram error: {e}", flush=True)

        resolved = {**trade, "result_type": "EXPIRED", "pnl": pnl, "elapsed_min": elapsed_min,
                    "ts_str": ts_entry.strftime("%H:%M") if hasattr(ts_entry, "strftime") else ""}
        _setup_resolved_trades.append(resolved)

        # Persist expired outcome to DB (was missing — caused Telegram vs portal P&L mismatch)
        log_id = trade.get("setup_log_id")
        if log_id and engine:
            try:
                seen_high = trade.get("_seen_high", entry_price)
                seen_low = trade.get("_seen_low", entry_price)
                if is_long:
                    max_profit = round(seen_high - entry_price, 2)
                    max_loss = round(seen_low - entry_price, 2)
                else:
                    max_profit = round(entry_price - seen_low, 2)
                    max_loss = round(entry_price - seen_high, 2)
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE setup_log SET
                            outcome_result = 'EXPIRED',
                            outcome_pnl = :pnl,
                            outcome_target_level = :tgt,
                            outcome_stop_level = :sl,
                            outcome_elapsed_min = :em,
                            outcome_first_event = 'timeout',
                            outcome_max_profit = :mp,
                            outcome_max_loss = :ml
                        WHERE id = :id AND outcome_result IS NULL
                    """), {
                        "pnl": pnl,
                        "tgt": trade.get("target_level"),
                        "sl": trade.get("stop_level"),
                        "em": elapsed_min,
                        "mp": max_profit,
                        "ml": max_loss,
                        "id": log_id,
                    })
            except Exception as db_err:
                print(f"[eod-summary] DB persist error: {db_err}", flush=True)

        print(f"[eod-summary] expired {setup_name} {direction} {pnl:+.1f}pts", flush=True)

    _setup_open_trades = []

    # Build daily summary from DB (not in-memory) so mid-day restarts don't lose trades
    trades_for_summary = []
    if engine:
        try:
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT setup_name, direction, grade, ts,
                           outcome_result, outcome_pnl, outcome_elapsed_min
                    FROM setup_log
                    WHERE ts >= :today_start
                      AND outcome_result IS NOT NULL
                    ORDER BY ts ASC
                """), {"today_start": today_start}).fetchall()
            for row in rows:
                ts_val = row[3]
                ts_str = ts_val.strftime("%H:%M") if hasattr(ts_val, "strftime") else ""
                trades_for_summary.append({
                    "setup_name": row[0],
                    "direction": row[1],
                    "grade": row[2],
                    "ts_str": ts_str,
                    "result_type": row[4],
                    "pnl": float(row[5]) if row[5] is not None else 0.0,
                    "elapsed_min": int(row[6]) if row[6] is not None else 0,
                })
            print(f"[eod-summary] loaded {len(trades_for_summary)} trades from DB", flush=True)
        except Exception as db_err:
            print(f"[eod-summary] DB query error, falling back to in-memory: {db_err}", flush=True)
            trades_for_summary = _setup_resolved_trades

    if not trades_for_summary:
        trades_for_summary = _setup_resolved_trades

    if trades_for_summary:
        summary_msg = format_setup_daily_summary(trades_for_summary)
        if summary_msg:
            send_telegram_setups(summary_msg)
            print(f"[eod-summary] sent daily summary ({len(trades_for_summary)} trades)", flush=True)
    else:
        print("[eod-summary] no trades today, skipping summary", flush=True)


ECON_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

def fetch_economic_calendar():
    """Fetch this week's economic events and upsert into DB. Runs Monday 8 AM ET + on startup."""
    if not engine:
        return
    try:
        resp = requests.get(ECON_CALENDAR_URL, timeout=15)
        resp.raise_for_status()
        events = resp.json()
        if not isinstance(events, list):
            print(f"[econ-cal] unexpected response type: {type(events)}", flush=True)
            return

        count = 0
        with engine.begin() as conn:
            for ev in events:
                title = ev.get("title", "").strip()
                country = ev.get("country", "").strip()
                date_str = ev.get("date", "")
                impact = ev.get("impact", "").strip()
                forecast = ev.get("forecast", "").strip() or None
                previous = ev.get("previous", "").strip() or None
                actual = ev.get("actual", "").strip() or None

                if not title or not date_str:
                    continue

                conn.execute(text("""
                    INSERT INTO economic_events (ts, title, country, impact, forecast, previous, actual, fetched_at)
                    VALUES (:ts, :title, :country, :impact, :forecast, :previous, :actual, NOW())
                    ON CONFLICT (ts, title, country) DO UPDATE SET
                        impact = EXCLUDED.impact,
                        forecast = EXCLUDED.forecast,
                        previous = EXCLUDED.previous,
                        actual = COALESCE(EXCLUDED.actual, economic_events.actual),
                        fetched_at = NOW()
                """), {
                    "ts": date_str, "title": title, "country": country,
                    "impact": impact, "forecast": forecast,
                    "previous": previous, "actual": actual,
                })
                count += 1

        print(f"[econ-cal] upserted {count} events for this week", flush=True)
    except Exception as e:
        print(f"[econ-cal] fetch error: {e}", flush=True)


def start_scheduler():
    sch = BackgroundScheduler(timezone="US/Eastern")
    sch.add_job(run_market_job, "interval", seconds=PULL_EVERY, id="pull", coalesce=True, max_instances=1)
    sch.add_job(save_history_job, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="save", coalesce=True, max_instances=1)
    sch.add_job(save_playback_snapshot, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="playback", coalesce=True, max_instances=1)
    sch.add_job(save_es_delta, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="es_delta_save", coalesce=True, max_instances=1)
    sch.add_job(save_es_range_bars, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="es_range_save", coalesce=True, max_instances=1)
    sch.add_job(_save_rithmic_bars, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="rithmic_range_save", coalesce=True, max_instances=1)
    sch.add_job(_auto_trade_eod_flatten, "cron", hour=15, minute=55,
                id="auto_trade_eod", coalesce=True, max_instances=1)
    sch.add_job(_send_setup_eod_summary, "cron", hour=16, minute=5,
                id="setup_eod", coalesce=True, max_instances=1)
    sch.add_job(fetch_economic_calendar, "cron", day_of_week="mon", hour=8, minute=0,
                id="econ_cal", coalesce=True, max_instances=1)
    sch.start()
    print("[sched] started; pull every", PULL_EVERY, "s; save every", SAVE_EVERY_MIN, "min; ES delta save every", SAVE_EVERY_MIN, "min", flush=True)
    return sch

REQUIRED_ENVS = ["TS_CLIENT_ID", "TS_CLIENT_SECRET", "TS_REFRESH_TOKEN", "DATABASE_URL"]
def missing_envs():
    return [k for k in REQUIRED_ENVS if not os.getenv(k)]

scheduler: BackgroundScheduler | None = None

@app.on_event("startup")
def on_startup():
    miss = missing_envs()
    if miss:
        print("[env] missing:", miss, flush=True)
    if engine:
        db_init()
    else:
        print("[db] engine not created (no DATABASE_URL)", flush=True)
    global scheduler
    scheduler = start_scheduler()
    # Fetch economic calendar on startup (don't wait for Monday cron)
    Thread(target=fetch_economic_calendar, daemon=True).start()
    # Start ES delta streaming thread (real-time barchart feed)
    Thread(target=_es_delta_stream_loop, daemon=True).start()
    print("[es-delta] streaming thread started", flush=True)
    # Start ES quote streaming thread (bid/ask delta classification)
    Thread(target=_es_quote_stream_loop, daemon=True).start()
    print("[es-quote] streaming thread started", flush=True)
    # Start Rithmic ES stream (parallel pipeline — skips if RITHMIC_USER not set)
    from rithmic_es_stream import start_rithmic_stream, set_on_bar_complete
    start_rithmic_stream(engine, send_telegram)
    # Register absorption detection callback on Rithmic bar completion
    set_on_bar_complete(_on_rithmic_bar_complete)
    print("[absorption] registered proactive callback on Rithmic bar completion", flush=True)
    # Initialize auto-trader (SIM ES execution — disabled by default)
    try:
        from app.auto_trader import init as auto_trader_init
        auto_trader_init(engine, ts_access_token, send_telegram_setups)
    except Exception as e:
        print(f"[auto-trader] init error (non-fatal): {e}", flush=True)

@app.on_event("shutdown")
def on_shutdown():
    global scheduler
    if scheduler:
        scheduler.shutdown()
        print("[sched] stopped", flush=True)

# ====== API ======
@app.get("/api/series")
def api_series():
    with _df_lock:
        df = None if (latest_df is None or latest_df.empty) else latest_df.copy()
    if df is None or df.empty:
        return {
            "strikes": [], "callVol": [], "putVol": [], "callOI": [], "putOI": [],
            "callGEX": [], "putGEX": [], "netGEX": [], "spot": None
        }
    sdf = df.sort_values("Strike")
    s  = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float)
    call_vol = pd.to_numeric(sdf["C_Volume"],       errors="coerce").fillna(0.0).astype(float)
    put_vol  = pd.to_numeric(sdf["P_Volume"],       errors="coerce").fillna(0.0).astype(float)
    call_oi  = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
    put_oi   = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
    c_gamma  = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
    p_gamma  = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
    call_gex = ( c_gamma * call_oi * 100.0).astype(float)
    put_gex  = (-p_gamma * put_oi  * 100.0).astype(float)
    net_gex  = (call_gex + put_gex).astype(float)
    spot = None
    try:
        parts = dict(splt.split("=", 1) for splt in (last_run_status.get("msg") or "").split() if "=" in splt)
        spot = float(parts.get("spot", ""))
    except:
        spot = None
    return {
        "strikes": s.tolist(),
        "callVol": call_vol.tolist(), "putVol": put_vol.tolist(),
        "callOI":  call_oi.tolist(),  "putOI":  put_oi.tolist(),
        "callGEX": call_gex.tolist(), "putGEX": put_gex.tolist(), "netGEX": net_gex.tolist(),
        "spot": spot
    }

@app.get("/api/economic-calendar")
def api_economic_calendar(country: str = "USD", impact: str = None):
    """Return economic events. Filter by country (default USD) and optionally impact level."""
    if not engine:
        return JSONResponse({"error": "no db"}, 500)
    with engine.connect() as conn:
        q = "SELECT ts, title, country, impact, forecast, previous, actual FROM economic_events WHERE country = :country"
        params = {"country": country}
        if impact:
            q += " AND LOWER(impact) = LOWER(:impact)"
            params["impact"] = impact
        q += " ORDER BY ts"
        rows = conn.execute(text(q), params).fetchall()
    return [{"ts": r[0].isoformat(), "title": r[1], "country": r[2],
             "impact": r[3], "forecast": r[4], "previous": r[5], "actual": r[6]} for r in rows]

@app.get("/api/health")
def api_health():
    """Component-level health with freshness, stale flags, and overall status."""
    freshness = api_data_freshness()
    is_open = market_open_now()

    # Chain (TS API) freshness
    chain_age = freshness["ts_api"].get("age_seconds")
    chain_status = freshness["ts_api"]["status"]
    chain_stale = is_open and chain_age is not None and chain_age > 300  # >5min

    # Volland freshness
    vol_age = freshness["volland"].get("age_seconds")
    vol_status = freshness["volland"]["status"]
    vol_stale = is_open and vol_age is not None and vol_age > 600  # >10min

    # ES delta stream
    with _es_delta_lock:
        es_delta_ok = _es_delta.get("stream_ok", False)
    # ES quote stream
    with _es_quote_lock:
        es_quote_ok = _es_quote.get("stream_ok", False)

    # Rithmic stream (optional — graceful if module not loaded)
    rithmic_info = None
    try:
        from rithmic_es_stream import get_rithmic_state
        rithmic_info = get_rithmic_state()
    except ImportError:
        pass

    # Overall status
    if not is_open:
        overall = "closed"
    elif chain_status == "error" or vol_status == "error":
        overall = "down"
    elif chain_stale or vol_stale or (not es_delta_ok and is_open) or (not es_quote_ok and is_open):
        overall = "degraded"
    else:
        overall = "healthy"

    return {
        "status": overall,
        "market_open": is_open,
        "components": {
            "chain": {
                "age_seconds": chain_age,
                "status": chain_status,
                "stale": chain_stale,
            },
            "volland": {
                "age_seconds": vol_age,
                "status": vol_status,
                "stale": vol_stale,
            },
            "es_delta_stream": {"connected": es_delta_ok},
            "es_quote_stream": {"connected": es_quote_ok},
            **({"rithmic_stream": rithmic_info} if rithmic_info is not None else {}),
            **_auto_trader_health(),
        },
        "last": last_run_status,
    }

def _auto_trader_health() -> dict:
    """Get auto-trader status for health endpoint (graceful if not loaded)."""
    try:
        from app import auto_trader
        return {"auto_trader": auto_trader.get_status()}
    except Exception:
        return {}

@app.get("/status")
def status():
    return last_run_status

@app.get("/api/auto-trade/status")
def api_auto_trade_status():
    """Get auto-trader status and toggles."""
    try:
        from app import auto_trader
        return auto_trader.get_status()
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/auto-trade/toggle")
def api_auto_trade_toggle(setup_name: str = Query(...), enabled: bool = Query(...)):
    """Toggle auto-trading for a specific setup."""
    try:
        from app import auto_trader
        ok = auto_trader.set_toggle(setup_name, enabled)
        if not ok:
            return {"error": f"Unknown setup: {setup_name}"}
        return {"ok": True, "toggles": auto_trader.get_toggles()}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/auto-trade/test")
def api_auto_trade_test():
    """Place a test 1 MES order to diagnose SIM API issues."""
    try:
        from app import auto_trader
        return auto_trader.test_order()
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/auto-trade/log")
def api_auto_trade_log(limit: int = Query(200)):
    """Return TS SIM auto-trade orders joined with setup_log for the dashboard."""
    if not engine:
        return []
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT ato.setup_log_id, ato.state, ato.created_at, ato.updated_at,
                       sl.setup_name, sl.direction, sl.grade, sl.ts, sl.spot,
                       sl.outcome_result, sl.outcome_pnl, sl.outcome_elapsed_min
                FROM auto_trade_orders ato
                JOIN setup_log sl ON ato.setup_log_id = sl.id
                ORDER BY sl.ts DESC
                LIMIT :lim
            """), {"lim": min(int(limit), 500)}).mappings().all()

        MES_PV = 5.0
        results = []
        for r in rows:
            st = r["state"]
            if isinstance(st, str):
                import json as _json
                st = _json.loads(st)
            fill = st.get("fill_price")
            stop = st.get("current_stop")
            t1_price = st.get("first_target_price")
            t2_price = st.get("full_target_price")
            is_long = (st.get("direction", "").lower() in ("long", "bullish"))

            # Original entry qty: t1_qty + t2_qty = TOTAL_QTY at trade time
            t1q = st.get("t1_qty", 0) or 0
            t2q = st.get("t2_qty", 0) or 0
            orig_qty = t1q + t2q
            if orig_qty == 0:
                orig_qty = max(st.get("stop_qty", 0) or 0, 1)

            # Compute real MES $ P&L from actual exit fill prices
            # Only available for trades with fill tracking (new trades after this deploy)
            mes_pnl = None
            if fill and st.get("status") == "closed":
                has_exits = any(st.get(k) for k in
                    ("t1_fill_price", "t2_fill_price", "stop_fill_price", "close_fill_price"))
                if has_exits:
                    sign = 1 if is_long else -1
                    pnl = 0.0
                    if st.get("t1_filled") and st.get("t1_fill_price"):
                        pnl += (st["t1_fill_price"] - fill) * sign * t1q * MES_PV
                    if st.get("t2_filled") and st.get("t2_fill_price"):
                        pnl += (st["t2_fill_price"] - fill) * sign * t2q * MES_PV
                    sfp = st.get("stop_fill_price")
                    sfq = st.get("stop_filled_qty", 0)
                    if sfp and sfq > 0:
                        pnl += (sfp - fill) * sign * sfq * MES_PV
                    cfp = st.get("close_fill_price")
                    cq = st.get("close_qty", 0)
                    if cfp and cq > 0:
                        pnl += (cfp - fill) * sign * cq * MES_PV
                    mes_pnl = round(pnl, 2)

            results.append({
                "setup_log_id": r["setup_log_id"],
                "setup_name": r["setup_name"],
                "direction": r["direction"],
                "grade": r["grade"],
                "ts": r["ts"].isoformat() if hasattr(r["ts"], "isoformat") else r["ts"],
                "spot": r["spot"],
                "fill_price": fill,
                "current_stop": stop,
                "first_target_price": t1_price,
                "full_target_price": t2_price,
                "t1_filled": st.get("t1_filled", False),
                "t2_filled": st.get("t2_filled", False),
                "status": st.get("status", "unknown"),
                "stop_qty": st.get("stop_qty", 0),
                "outcome_result": r["outcome_result"],
                "outcome_pnl": r["outcome_pnl"],
                "outcome_elapsed_min": r["outcome_elapsed_min"],
                "mes_qty": orig_qty,
                "mes_pnl": mes_pnl,
            })
        return results
    except Exception as e:
        print(f"[auto-trade] log query error: {e}", flush=True)
        return []

@app.get("/api/eval/log")
def api_eval_log(limit: int = Query(200)):
    """Return eval-eligible setup_log entries with computed MES qty and $ P&L."""
    if not engine:
        return []
    _EVAL_SETUPS = ("AG Short", "DD Exhaustion", "ES Absorption", "Paradigm Reversal")
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, ts, setup_name, direction, grade, score, spot,
                       abs_es_price, outcome_result, outcome_pnl,
                       outcome_max_profit, outcome_max_loss, outcome_elapsed_min
                FROM setup_log
                WHERE setup_name = ANY(:setups)
                ORDER BY ts DESC
                LIMIT :lim
            """), {"setups": list(_EVAL_SETUPS), "lim": min(int(limit), 500)}).mappings().all()

        results = []
        for r in rows:
            entry_price = r["abs_es_price"] or r["spot"] or 0
            # Compute qty based on $200 max risk / (stop_pts * $5/pt)
            stop_map = {"AG Short": 25, "DD Exhaustion": 20, "ES Absorption": 15, "Paradigm Reversal": 25}
            stop_pts = stop_map.get(r["setup_name"], 20)
            qty = max(1, int(200 / (stop_pts * 5)))  # $200 risk / ($5 * stop)

            results.append({
                "id": r["id"],
                "ts": r["ts"].isoformat() if hasattr(r["ts"], "isoformat") else r["ts"],
                "setup_name": r["setup_name"],
                "direction": r["direction"],
                "grade": r["grade"],
                "score": r["score"],
                "spot": r["spot"],
                "entry_price": entry_price,
                "stop_pts": stop_pts,
                "qty": qty,
                "outcome_result": r["outcome_result"],
                "outcome_pnl": r["outcome_pnl"],
                "outcome_elapsed_min": r["outcome_elapsed_min"],
            })
        return results
    except Exception as e:
        print(f"[eval] log query error: {e}", flush=True)
        return []

@app.get("/api/eval/signals")
def api_eval_signals(since_id: int = Query(0, ge=0)):
    """Return today's setup signals and outcomes for the eval trader.

    Auth: Bearer token via EVAL_API_KEY (checked in middleware).
    Query: since_id=N returns entries with id > N.
    """
    if not engine:
        return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
    now_et = datetime.now(pytz.timezone("US/Eastern"))
    today_str = now_et.strftime("%Y-%m-%d")
    try:
        with engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT id, ts, setup_name, direction, grade, score, spot, target, lis, "
                "paradigm, bofa_stop_level, bofa_target_level, abs_es_price, "
                "max_plus_gex, max_minus_gex, "
                "outcome_result, outcome_pnl "
                "FROM setup_log "
                "WHERE id > :since AND ts::date = :today "
                "ORDER BY id ASC"
            ), {"since": since_id, "today": today_str}).mappings().all()

        signals, outcomes = [], []
        for r in rows:
            row = dict(r)
            # Compute target/stop levels
            tgt_lvl, stop_lvl = _compute_setup_levels(row)
            entry = {
                "id": row["id"],
                "ts": row["ts"].isoformat() if row["ts"] else None,
                "setup_name": row["setup_name"],
                "direction": row["direction"],
                "grade": row["grade"],
                "score": row["score"],
                "spot": row["spot"],
                "target": row["target"],
                "lis": row["lis"],
                "paradigm": row["paradigm"],
                "bofa_stop_level": row["bofa_stop_level"],
                "bofa_target_level": row["bofa_target_level"],
                "abs_es_price": row["abs_es_price"],
                "stop_level": stop_lvl,
                "target_level": tgt_lvl,
                "outcome_result": row["outcome_result"],
            }
            signals.append(entry)
            if row["outcome_result"]:
                outcomes.append({
                    "id": row["id"],
                    "setup_name": row["setup_name"],
                    "outcome_result": row["outcome_result"],
                    "outcome_pnl": row["outcome_pnl"],
                })
        # Include current ES price so eval_trader can compute stop/target
        # relative to MES (SPX and MES differ by ~15-20 pts spread)
        with _es_quote_lock:
            es_price = _es_quote.get("last_price")
        return {"signals": signals, "outcomes": outcomes, "es_price": es_price}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/snapshot")
def snapshot():
    with _df_lock:
        df = None if (latest_df is None or latest_df.empty) else latest_df.copy()
    if df is None or df.empty:
        return {"columns": DISPLAY_COLS, "rows": []}
    df.columns = DISPLAY_COLS
    return {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}

@app.get("/api/history")
def api_history(limit: int = Query(288, ge=1, le=5000)):
    if not engine:
        return {"error": "DATABASE_URL not set"}
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT ts, exp, spot, columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT :lim"
        ), {"lim": limit}).mappings().all()
    for r in rows:
        r["columns"] = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        r["rows"]    = json.loads(r["rows"])    if isinstance(r["rows"], str) else r["rows"]
        r["ts"]      = r["ts"].isoformat()
    return rows

@app.get("/download/history.csv")
def download_history_csv(limit: int = Query(288, ge=1, le=5000)):
    if not engine:
        return Response("DATABASE_URL not set", media_type="text/plain", status_code=500)
    with engine.begin() as conn:
        recs = conn.execute(text(
            "SELECT ts, exp, spot, columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT :lim"
        ), {"lim": limit}).mappings().all()
    out = []
    for r in recs:
        cols = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        rows = json.loads(r["rows"])    if isinstance(r["rows"], str) else r["rows"]
        for arr in rows:
            obj = {"ts": r["ts"].isoformat(), "exp": r["exp"], "spot": r["spot"]}
            obj.update({cols[i]: arr[i] for i in range(len(cols))})
            out.append(obj)
    df = pd.DataFrame(out)
    csv = df.to_csv(index=False)
    return Response(csv, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=history.csv"})

# ===== Volland API (from Postgres) =====
@app.get("/api/volland/latest")
def api_volland_latest():
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        r = db_latest_volland()
        if not r:
            return {"ts": None, "payload": None}
        return r
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/history")
def api_volland_history(limit: int = Query(500, ge=1, le=5000)):
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_volland_history(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/vanna_window")
def api_volland_vanna_window(limit: int = Query(40, ge=5, le=200)):
    """
    Latest strikes around mid_strike (mid_strike = strike where abs(vanna) is max).
    UI draws the vertical line at SPOT (from /api/series).
    """
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_volland_vanna_window(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/delta_decay_window")
def api_volland_delta_decay_window(limit: int = Query(40, ge=5, le=200)):
    """
    Latest deltaDecay strikes around mid_strike (TODAY expiration only).
    """
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_volland_delta_decay_window(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/exposure_window")
def api_volland_exposure_window(
    greek: str = Query(..., description="Greek name: vanna or gamma"),
    expiration: str = Query(None, description="Expiration option: THIS_WEEK, THIRTY_NEXT_DAYS, ALL (omit for 0DTE)"),
    limit: int = Query(40, ge=5, le=200),
):
    """
    Generic exposure window for any greek + expiration_option combo.
    Omit expiration for 0DTE data (no expiration filter).
    """
    ALLOWED_GREEKS = {"vanna", "gamma"}
    ALLOWED_EXPIRATIONS = {"TODAY", "THIS_WEEK", "THIRTY_NEXT_DAYS", "ALL"}
    if greek not in ALLOWED_GREEKS:
        return JSONResponse({"error": f"greek must be one of {ALLOWED_GREEKS}"}, status_code=400)
    if expiration is not None and expiration not in ALLOWED_EXPIRATIONS:
        return JSONResponse({"error": f"expiration must be one of {ALLOWED_EXPIRATIONS}"}, status_code=400)
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_volland_exposure_window(greek=greek, expiration_option=expiration, limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/stats")
def api_volland_stats():
    """Get SPX statistics from Volland snapshots. Persists after market close."""
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        result = db_volland_stats()
        if not result:
            return {"ts": None, "stats": None, "error": "No stats available"}
        return result
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()[-500:]}, status_code=500)

# ====== ES DELTA ENDPOINTS ======

def db_es_delta_latest():
    """Get the most recent ES cumulative delta snapshot."""
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT ts, trade_date, symbol,
                   cumulative_delta, total_volume, buy_volume, sell_volume,
                   last_price, tick_count,
                   bar_high AS session_high, bar_low AS session_low
            FROM es_delta_snapshots
            ORDER BY ts DESC LIMIT 1
        """)).mappings().first()
        if not row:
            return None
        r = dict(row)
        r["ts"] = r["ts"].isoformat() if r["ts"] else None
        r["trade_date"] = str(r["trade_date"]) if r["trade_date"] else None
        return r

def db_es_delta_history(limit: int = 500):
    """Get today's ES cumulative delta snapshots as a time-series."""
    today = datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d")
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT ts, trade_date, symbol,
                   cumulative_delta, total_volume, buy_volume, sell_volume,
                   last_price, tick_count,
                   bar_high AS session_high, bar_low AS session_low
            FROM es_delta_snapshots
            WHERE trade_date = :today
            ORDER BY ts ASC
            LIMIT :lim
        """), {"today": today, "lim": limit}).mappings().all()
        result = []
        for row in rows:
            r = dict(row)
            r["ts"] = r["ts"].isoformat() if r["ts"] else None
            r["trade_date"] = str(r["trade_date"]) if r["trade_date"] else None
            result.append(r)
        return result

def _es_session_start_utc() -> str:
    """Return the UTC timestamp of when the current ES session opened (6 PM ET prior day)."""
    t = now_et()
    if t.hour >= 18:
        # After 6 PM — session started today at 6 PM ET
        session_open_et = t.replace(hour=18, minute=0, second=0, microsecond=0)
    else:
        # Before 6 PM — session started yesterday at 6 PM ET
        session_open_et = (t - timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
    return session_open_et.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

def db_es_delta_bars(limit: int = 1400):
    """Get current ES session's 1-minute delta bars (from 6 PM ET session open)."""
    session_start = _es_session_start_utc()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT ts, trade_date, symbol,
                   bar_delta, cumulative_delta,
                   bar_volume, bar_buy_volume, bar_sell_volume,
                   bar_open_price, bar_close_price, bar_high_price, bar_low_price,
                   up_ticks, down_ticks, total_ticks
            FROM es_delta_bars
            WHERE ts >= :session_start
            ORDER BY ts ASC
            LIMIT :lim
        """), {"session_start": session_start, "lim": limit}).mappings().all()
        result = []
        for row in rows:
            r = dict(row)
            r["ts"] = r["ts"].isoformat() if r["ts"] else None
            r["trade_date"] = str(r["trade_date"]) if r["trade_date"] else None
            result.append(r)
        return result

@app.get("/api/es/delta/latest")
def api_es_delta_latest():
    """Get latest ES cumulative delta — reads from live in-memory state (real-time)."""
    with _es_delta_lock:
        if _es_delta["total_volume"] == 0:
            return {"error": "No delta data available", "ts": None}
        return {
            "ts": fmt_et(now_et()),
            "trade_date": _es_delta["trade_date"],
            "symbol": ES_DELTA_SYMBOL,
            "cumulative_delta": _es_delta["cumulative_delta"],
            "total_volume": _es_delta["total_volume"],
            "buy_volume": _es_delta["buy_volume"],
            "sell_volume": _es_delta["sell_volume"],
            "last_price": _es_delta["last_price"],
            "tick_count": _es_delta["tick_count"],
            "session_high": _es_delta["session_high"],
            "session_low": _es_delta["session_low"],
            "stream_ok": _es_delta["stream_ok"],
        }

@app.get("/api/es/delta/history")
def api_es_delta_history(limit: int = Query(500, ge=1, le=2000)):
    """Get today's ES cumulative delta snapshots (time-series)."""
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_es_delta_history(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/es/delta/bars")
def api_es_delta_bars(limit: int = Query(1400, ge=1, le=2000)):
    """Get current ES session's 1-minute delta bars."""
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_es_delta_bars(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/es/delta/rangebars")
def api_es_delta_rangebars(range_pts: float = Query(5.0, alias="range", ge=1.0, le=50.0)):
    """Build range bars for ES delta chart.

    Priority: Rithmic exchange aggressor → TS quote-stream fallback.
    """
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)

        # Primary: Rithmic exchange aggressor data (100% accurate)
        result = None
        try:
            from rithmic_es_stream import get_rithmic_bars
            rithmic_bars = get_rithmic_bars()
            if rithmic_bars:
                result = rithmic_bars
        except ImportError:
            pass

        # Fallback: TS quote-stream bars (bid/ask inference)
        if result is None:
            with _es_quote_lock:
                completed = list(_es_quote["_completed_bars"])
                forming = _es_quote["_forming_bar"]
                cvd_now = _es_quote["_cvd"]

            result = list(completed)
            if forming and (forming["volume"] > 0 or abs(forming["open"] - forming["close"]) > 0.001):
                result.append({
                    "idx": len(completed),
                    "open": forming["open"], "high": forming["high"],
                    "low": forming["low"], "close": forming["close"],
                    "volume": forming["volume"], "delta": forming["delta"],
                    "buy_volume": forming["buy"], "sell_volume": forming["sell"],
                    "cvd": cvd_now,
                    "cvd_open": forming["cvd_open"],
                    "cvd_high": forming["cvd_high"],
                    "cvd_low": forming["cvd_low"],
                    "cvd_close": cvd_now,
                    "ts_start": forming["ts_start"], "ts_end": forming["ts_end"],
                    "status": "open",
                })

        # Absorption detection now runs proactively via Rithmic/TS bar callbacks
        # (no longer depends on dashboard polling)
        return {"bars": result, "signals": _absorption_signals}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/es/rithmic/rangebars")
def api_es_rithmic_rangebars():
    """Rithmic parallel pipeline range bars (symbol @ES-R)."""
    try:
        from rithmic_es_stream import get_rithmic_bars, get_rithmic_state
        return {"bars": get_rithmic_bars(), "state": get_rithmic_state()}
    except ImportError:
        return JSONResponse({"error": "Rithmic module not available"}, status_code=501)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

def _es_price_path(o: float, h: float, l: float, c: float) -> list:
    """Simulate tick-by-tick ES price path from OHLC at 0.25 increments.

    Estimates intra-bar price movement:
      Bullish (close >= open): O → L → H → C
      Bearish (close < open):  O → H → L → C

    Returns list of prices where consecutive pairs are tick transitions.
    """
    tick = 0.25
    o, h, l, c = (round(v * 4) / 4 for v in (o, h, l, c))
    path = [o]

    def _walk_to(target):
        start = path[-1]
        if abs(target - start) < 0.001:
            return
        step = tick if target > start else -tick
        p = start + step
        limit = 10000
        while abs(p - target) > 0.001 and limit > 0:
            path.append(round(p * 4) / 4)
            p += step
            limit -= 1
        path.append(round(target * 4) / 4)

    if c >= o:
        _walk_to(l); _walk_to(h); _walk_to(c)
    else:
        _walk_to(h); _walk_to(l); _walk_to(c)

    return path if path else [o]


def _build_range_bars(bars_1m: list, range_pts: float) -> list:
    """Convert 1-minute bars into range bars using tick-by-tick price simulation.

    Instead of proportional volume splitting (inaccurate when 1-min bars span
    multiple range bar boundaries), this simulates the price path within each
    1-min bar at 0.25 ES tick increments and distributes volume evenly across
    tick transitions.  Range bars close precisely when high-low reaches
    range_pts, giving much more accurate volume/delta attribution per bar.
    """
    result = []
    cvd = 0.0
    cur = None  # current forming range bar

    def _emit(status="closed"):
        nonlocal cvd, cur
        if cur is None:
            return
        d = int(round(cur["delta"]))
        cvd += d
        result.append({
            "idx": len(result),
            "open": cur["open"], "high": cur["high"],
            "low": cur["low"], "close": cur["close"],
            "volume": max(int(round(cur["vol"])), 0),
            "delta": d,
            "buy_volume": max(int(round(cur["buy"])), 0),
            "sell_volume": max(int(round(cur["sell"])), 0),
            "cvd": int(cvd),
            "cvd_open": int(round(cur["cvd0"])),
            "cvd_high": int(round(max(cur["cvd_hi"], cvd))),
            "cvd_low": int(round(min(cur["cvd_lo"], cvd))),
            "cvd_close": int(cvd),
            "ts_start": cur["ts0"], "ts_end": cur["ts1"],
            "status": status,
        })
        cur = None

    def _new_bar(price, ts):
        return {
            "open": price, "high": price, "low": price, "close": price,
            "vol": 0.0, "delta": 0.0, "buy": 0.0, "sell": 0.0,
            "ts0": ts, "ts1": ts,
            "cvd0": cvd, "cvd_run": cvd, "cvd_hi": cvd, "cvd_lo": cvd,
        }

    for b in bars_1m:
        o = float(b.get("bar_open_price") or 0)
        h = float(b.get("bar_high_price") or 0)
        l = float(b.get("bar_low_price") or 0)
        c = float(b.get("bar_close_price") or 0)
        vol = int(b.get("bar_volume") or 0)
        buy = int(b.get("bar_buy_volume") or 0)
        sell = int(b.get("bar_sell_volume") or 0)
        delta = int(b.get("bar_delta") or 0)
        ts = b.get("ts", "")
        if o == 0 and c == 0:
            continue

        path = _es_price_path(o, h, l, c)
        n_trans = len(path) - 1

        if n_trans <= 0:
            # No price movement — add all volume at single price
            if cur is None:
                cur = _new_bar(path[0], ts)
            cur["close"] = path[0]
            cur["high"] = max(cur["high"], path[0])
            cur["low"] = min(cur["low"], path[0])
            cur["vol"] += vol; cur["buy"] += buy
            cur["sell"] += sell; cur["delta"] += delta
            cur["ts1"] = ts
            cur["cvd_run"] += delta
            cur["cvd_hi"] = max(cur["cvd_hi"], cur["cvd_run"])
            cur["cvd_lo"] = min(cur["cvd_lo"], cur["cvd_run"])
            if cur["high"] - cur["low"] >= range_pts - 0.001:
                _emit()
            continue

        v_s = vol / n_trans
        b_s = buy / n_trans
        s_s = sell / n_trans
        d_s = delta / n_trans

        # Opening price: position update only, no volume
        p0 = path[0]
        if cur is None:
            cur = _new_bar(p0, ts)
        else:
            cur["close"] = p0
            cur["high"] = max(cur["high"], p0)
            cur["low"] = min(cur["low"], p0)
            cur["ts1"] = ts
            if cur["high"] - cur["low"] >= range_pts - 0.001:
                _emit()
                cur = _new_bar(p0, ts)

        # Tick transitions: add volume first, then check for range completion
        for i in range(1, len(path)):
            price = path[i]
            if cur is None:
                cur = _new_bar(price, ts)
                continue
            cur["close"] = price
            cur["high"] = max(cur["high"], price)
            cur["low"] = min(cur["low"], price)
            cur["vol"] += v_s; cur["buy"] += b_s
            cur["sell"] += s_s; cur["delta"] += d_s
            cur["ts1"] = ts
            cur["cvd_run"] += d_s
            cur["cvd_hi"] = max(cur["cvd_hi"], cur["cvd_run"])
            cur["cvd_lo"] = min(cur["cvd_lo"], cur["cvd_run"])
            if cur["high"] - cur["low"] >= range_pts - 0.001:
                _emit()
                cur = _new_bar(price, ts)

    # Emit last forming bar
    if cur is not None and (cur["vol"] > 0.5 or abs(cur["open"] - cur["close"]) > 0.001):
        _emit(status="open")

    return result

@app.get("/api/spx_candles")
def api_spx_candles(bars: int = Query(60, ge=10, le=200)):
    """
    Fetch SPX 3-minute candlestick data from TradeStation API.
    Returns OHLC data for building a Plotly candlestick chart.
    """
    try:
        # TradeStation barcharts endpoint for $SPX.X
        # interval=3 for 3-minute bars, unit=Minute
        params = {
            "interval": "3",
            "unit": "Minute",
            "barsback": str(bars),
        }
        r = api_get("/marketdata/barcharts/$SPX.X", params=params, timeout=15)
        data = r.json()

        bars_list = data.get("Bars", [])
        if not bars_list:
            return {"error": "No bars returned", "candles": []}

        candles = []
        for bar in bars_list:
            candles.append({
                "time": bar.get("TimeStamp"),
                "open": bar.get("Open"),
                "high": bar.get("High"),
                "low": bar.get("Low"),
                "close": bar.get("Close"),
                "volume": bar.get("TotalVolume"),
            })

        return {"candles": candles, "count": len(candles)}
    except Exception as e:
        print(f"[spx_candles] error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/spx_candles_1m")
def api_spx_candles_1m(bars: int = Query(120, ge=10, le=400)):
    """
    Fetch SPX 1-minute candlestick data from TradeStation API.
    Returns OHLC data with timestamps in NY Eastern time.
    """
    try:
        params = {
            "interval": "1",
            "unit": "Minute",
            "barsback": str(bars),
        }
        r = api_get("/marketdata/barcharts/$SPX.X", params=params, timeout=15)
        data = r.json()

        bars_list = data.get("Bars", [])
        if not bars_list:
            return {"error": "No bars returned", "candles": []}

        candles = []
        for bar in bars_list:
            # Convert timestamp to NY Eastern time
            ts_raw = bar.get("TimeStamp", "")
            try:
                # Parse the timestamp and convert to Eastern
                from datetime import datetime
                dt = pd.to_datetime(ts_raw)
                if dt.tzinfo is None:
                    dt = dt.tz_localize('UTC')
                dt_et = dt.tz_convert('US/Eastern')
                time_str = dt_et.strftime('%Y-%m-%dT%H:%M:%S')
            except:
                time_str = ts_raw

            candles.append({
                "time": time_str,
                "open": bar.get("Open"),
                "high": bar.get("High"),
                "low": bar.get("Low"),
                "close": bar.get("Close"),
                "volume": bar.get("TotalVolume"),
            })

        return {"candles": candles, "count": len(candles)}
    except Exception as e:
        print(f"[spx_candles_1m] error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/spx_candles_date")
def api_spx_candles_date(date: str = Query(..., description="YYYY-MM-DD"), interval: int = Query(5, ge=1, le=5)):
    """
    Fetch SPX OHLC candles for a specific historical date from TradeStation API.
    Uses lastdate param to query any past trading day (up to ~1 year back).
    interval: 1 for 1-min bars, 5 for 5-min bars.
    """
    if interval not in (1, 5):
        return JSONResponse({"error": "interval must be 1 or 5"}, status_code=400)
    try:
        parts = date.split("-")
        today_str = now_et().strftime("%Y-%m-%d")
        is_today = (date == today_str)

        # For today: 390/78 bars is enough (recent data, mostly market hours)
        # For historical: need ~800/160 bars because TS includes pre-market in count
        if is_today:
            barsback = 390 if interval == 1 else 78
        else:
            barsback = 800 if interval == 1 else 160

        params = {
            "interval": str(interval),
            "unit": "Minute",
            "barsback": str(barsback),
        }

        # For historical dates: add lastdate (barsback + lastdate works; + firstdate does NOT)
        # Convert 16:05 ET to UTC (handles DST: EST=21:05Z, EDT=20:05Z)
        if not is_today:
            dt_et = NY.localize(datetime(int(parts[0]), int(parts[1]), int(parts[2]), 16, 5))
            dt_utc = dt_et.astimezone(pytz.utc)
            params["lastdate"] = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"[spx_candles_date] date={date} interval={interval} params={params}", flush=True)
        r = api_get("/marketdata/barcharts/$SPX.X", params=params, timeout=15)
        data = r.json()

        bars_list = data.get("Bars", [])
        print(f"[spx_candles_date] got {len(bars_list)} bars from TS API", flush=True)
        if not bars_list:
            return {"error": "No bars returned", "candles": []}

        candles = []
        for bar in bars_list:
            ts_raw = bar.get("TimeStamp", "")
            try:
                dt = pd.to_datetime(ts_raw)
                if dt.tzinfo is None:
                    dt = dt.tz_localize('UTC')
                # Convert to naive ET string for consistent NY market time display
                dt_et = dt.tz_convert('US/Eastern')
                # Filter to selected date + market hours only (9:30-16:00 ET)
                if dt_et.strftime('%Y-%m-%d') != date:
                    continue
                if dt_et.hour < 9 or (dt_et.hour == 9 and dt_et.minute < 30) or dt_et.hour >= 16:
                    continue
                time_str = dt_et.strftime('%Y-%m-%dT%H:%M:%S')
            except:
                time_str = ts_raw

            candles.append({
                "time": time_str,
                "open": bar.get("Open"),
                "high": bar.get("High"),
                "low": bar.get("Low"),
                "close": bar.get("Close"),
                "volume": bar.get("TotalVolume"),
            })

        if candles:
            print(f"[spx_candles_date] first={candles[0]['time']} last={candles[-1]['time']}", flush=True)
        return {"candles": candles, "count": len(candles)}
    except Exception as e:
        print(f"[spx_candles_date] error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/statistics_levels")
def api_statistics_levels():
    """
    Get key price levels for Statistics chart:
    - Target (from stats)
    - LIS low/high (from stats)
    - Max positive gamma strike
    - Max negative gamma strike
    """
    try:
        result = {
            "target": None,
            "lis_low": None,
            "lis_high": None,
            "max_pos_gamma": None,
            "max_neg_gamma": None,
            "spot": None,
        }

        # Get stats (target, LIS)
        stats = db_volland_stats()
        if stats and stats.get("stats"):
            s = stats["stats"]
            # Target - parse number from various formats like "$6,050" or "6050" or "N/A"
            try:
                target_str = str(s.get("target", "")).replace("$", "").replace(",", "").strip()
                if target_str and target_str.lower() not in ["n/a", "na", "-", ""]:
                    result["target"] = float(target_str)
            except:
                pass
            # LIS - parse from formats like "$6,926 - $6,966" or "6000/6100" or single "6050"
            lis = s.get("lines_in_sand")
            if lis:
                try:
                    lis_str = str(lis).replace("$", "").replace(",", "")
                    # Extract all numbers from the string
                    lis_numbers = re.findall(r"[\d.]+", lis_str)
                    if len(lis_numbers) >= 2:
                        result["lis_low"] = float(lis_numbers[0])
                        result["lis_high"] = float(lis_numbers[1])
                    elif len(lis_numbers) == 1:
                        result["lis_low"] = float(lis_numbers[0])
                except:
                    pass

        # Get GEX data for max gamma strikes
        with _df_lock:
            if latest_df is not None and not latest_df.empty:
                df = latest_df.copy()
                sdf = df.sort_values("Strike")
                strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float).tolist()
                call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
                put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
                c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
                p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
                net_gex = (c_gamma * call_oi * 100.0) + (-p_gamma * put_oi * 100.0)

                # Find max positive and max negative gamma strikes
                if len(strikes) > 0 and len(net_gex) > 0:
                    max_pos_idx = net_gex.idxmax() if net_gex.max() > 0 else None
                    max_neg_idx = net_gex.idxmin() if net_gex.min() < 0 else None

                    if max_pos_idx is not None:
                        result["max_pos_gamma"] = strikes[max_pos_idx]
                    if max_neg_idx is not None:
                        result["max_neg_gamma"] = strikes[max_neg_idx]

        # Get spot price
        try:
            parts = dict(s.split("=", 1) for s in (last_run_status.get("msg") or "").split() if "=" in s)
            result["spot"] = float(parts.get("spot", ""))
        except:
            pass

        return result
    except Exception as e:
        print(f"[statistics_levels] error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

# ===== Playback API =====

@app.post("/api/playback/save_now")
def api_playback_save_now():
    """Manually trigger a playback snapshot save (for testing)."""
    try:
        global _last_playback_saved_at
        if not engine:
            return {"error": "DATABASE_URL not set"}

        with _df_lock:
            if latest_df is None or latest_df.empty:
                return {"error": "No chain data available yet. Wait for market data to load."}
            df = latest_df.copy()

        msg = last_run_status.get("msg") or ""
        spot = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot", ""))
        except:
            pass

        if not spot:
            return {"error": "No spot price available"}

        sdf = df.sort_values("Strike")
        strikes = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float).tolist()
        call_vol = pd.to_numeric(sdf["C_Volume"], errors="coerce").fillna(0.0).astype(float).tolist()
        put_vol = pd.to_numeric(sdf["P_Volume"], errors="coerce").fillna(0.0).astype(float).tolist()
        call_oi = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
        put_oi = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
        c_gamma = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
        p_gamma = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
        net_gex = ((c_gamma * call_oi * 100.0) + (-p_gamma * put_oi * 100.0)).astype(float).tolist()

        charm_data = None
        try:
            vanna_window = db_volland_vanna_window(limit=40)
            if vanna_window and vanna_window.get("points"):
                charm_by_strike = {p["strike"]: p["vanna"] for p in vanna_window["points"]}
                charm_data = [charm_by_strike.get(s, 0) for s in strikes]
        except:
            pass

        stats_data = None
        try:
            stats_result = db_volland_stats()
            if stats_result and stats_result.get("stats"):
                s = stats_result["stats"]
                stats_data = {"paradigm": s.get("paradigm"), "target": s.get("target"),
                              "lis": s.get("lines_in_sand"), "dd_hedging": s.get("delta_decay_hedging"),
                              "opt_volume": s.get("opt_volume")}
        except:
            pass

        with engine.begin() as conn:
            conn.execute(
                text("""INSERT INTO playback_snapshots (ts, spot, strikes, net_gex, charm, call_vol, put_vol, stats)
                        VALUES (:ts, :spot, :strikes, :net_gex, :charm, :call_vol, :put_vol, :stats)"""),
                {"ts": now_et(), "spot": spot, "strikes": json.dumps(strikes), "net_gex": json.dumps(net_gex),
                 "charm": json.dumps(charm_data) if charm_data else None, "call_vol": json.dumps(call_vol),
                 "put_vol": json.dumps(put_vol), "stats": json.dumps(stats_data) if stats_data else None}
            )

        _last_playback_saved_at = time.time()
        return {"success": True, "message": "Snapshot saved", "spot": spot, "strikes_count": len(strikes)}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/playback/generate_mock")
def api_playback_generate_mock(force: bool = Query(False, description="Set to true to delete existing data")):
    """Generate mock playback data for testing (3 days, ~100 snapshots). Requires force=true if data exists."""
    import random
    if not engine:
        return {"error": "DATABASE_URL not set"}

    try:
        # Check if existing mock data exists (only delete mock data, never real data)
        with engine.connect() as conn:
            mock_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock = TRUE")).scalar()
            real_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock = FALSE OR is_mock IS NULL")).scalar()

        if mock_count > 0 and not force:
            return {"error": f"Refusing to delete {mock_count} existing mock snapshots. Use force=true to confirm.", "mock_count": mock_count, "real_count": real_count}

        # Delete only mock data (real data is always preserved)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM playback_snapshots WHERE is_mock = TRUE"))

        # Generate 3 days of mock data, every 5 minutes during "market hours" (9:30-16:00)
        base_spot = 6050.0
        strikes = [base_spot + (i - 20) * 5 for i in range(41)]  # 41 strikes centered around spot

        snapshots = []
        start_time = now_et().replace(hour=9, minute=30, second=0, microsecond=0) - pd.Timedelta(days=3)

        for day in range(3):
            day_start = start_time + pd.Timedelta(days=day)
            # Simulate price movement for the day
            day_spot = base_spot + random.uniform(-30, 30)

            for minute in range(0, 390, 5):  # 9:30 to 16:00 = 390 minutes
                ts = day_start + pd.Timedelta(minutes=minute)

                # Random walk for spot price
                day_spot += random.uniform(-2, 2)
                spot = round(day_spot, 2)

                # Generate mock GEX (higher near spot, random sign)
                net_gex = []
                for s in strikes:
                    dist = abs(s - spot)
                    magnitude = max(0, 5000 - dist * 100) * random.uniform(0.5, 1.5)
                    sign = 1 if random.random() > 0.4 else -1
                    net_gex.append(round(magnitude * sign, 2))

                # Generate mock Charm
                charm = []
                for s in strikes:
                    dist = abs(s - spot)
                    magnitude = max(0, 3000 - dist * 80) * random.uniform(0.3, 1.2)
                    sign = 1 if s > spot else -1
                    charm.append(round(magnitude * sign, 2))

                # Generate mock Volume
                call_vol = [int(random.uniform(100, 5000) * max(0.1, 1 - abs(s - spot) / 100)) for s in strikes]
                put_vol = [int(random.uniform(100, 5000) * max(0.1, 1 - abs(s - spot) / 100)) for s in strikes]

                # Mock stats
                paradigms = ["Positive Charm", "Negative Charm", "Neutral"]
                stats = {
                    "paradigm": random.choice(paradigms),
                    "target": str(int(spot + random.uniform(-20, 20))),
                    "lis": f"{int(spot - 30)}/{int(spot + 30)}",
                    "dd_hedging": f"{random.choice(['+', '-'])}{random.randint(1, 50)}M",
                    "opt_volume": f"{random.randint(500, 2000)}K"
                }

                snapshots.append({
                    "ts": ts,
                    "spot": spot,
                    "strikes": strikes,
                    "net_gex": net_gex,
                    "charm": charm,
                    "call_vol": call_vol,
                    "put_vol": put_vol,
                    "stats": stats
                })

        # Insert all snapshots with is_mock=true
        with engine.begin() as conn:
            for snap in snapshots:
                conn.execute(
                    text("""INSERT INTO playback_snapshots (ts, spot, strikes, net_gex, charm, call_vol, put_vol, stats, is_mock)
                            VALUES (:ts, :spot, :strikes, :net_gex, :charm, :call_vol, :put_vol, :stats, TRUE)"""),
                    {"ts": snap["ts"], "spot": snap["spot"], "strikes": json.dumps(snap["strikes"]),
                     "net_gex": json.dumps(snap["net_gex"]), "charm": json.dumps(snap["charm"]),
                     "call_vol": json.dumps(snap["call_vol"]), "put_vol": json.dumps(snap["put_vol"]),
                     "stats": json.dumps(snap["stats"])}
                )

        return {"success": True, "message": f"Generated {len(snapshots)} mock snapshots", "count": len(snapshots)}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/playback/delete_all")
def api_playback_delete_all(force: bool = Query(False, description="Must be true to confirm deletion")):
    """Delete ALL playback snapshots. Requires force=true to confirm."""
    if not engine:
        return {"error": "DATABASE_URL not set"}

    try:
        # Check count first
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots")).scalar()

        if count > 0 and not force:
            return {"error": f"Refusing to delete {count} snapshots. Use force=true to confirm deletion.", "existing_count": count}

        with engine.begin() as conn:
            result = conn.execute(text("DELETE FROM playback_snapshots"))
            deleted = result.rowcount

        return {"success": True, "message": f"Deleted {deleted} snapshots", "deleted_count": deleted}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/playback/delete_mock")
def api_playback_delete_mock():
    """Delete only MOCK playback snapshots (is_mock=true). Real data is preserved."""
    if not engine:
        return {"error": "DATABASE_URL not set"}

    try:
        with engine.connect() as conn:
            mock_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock = TRUE")).scalar()
            real_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock = FALSE OR is_mock IS NULL")).scalar()

        if mock_count == 0:
            return {"message": "No mock data to delete", "mock_count": 0, "real_count": real_count}

        with engine.begin() as conn:
            result = conn.execute(text("DELETE FROM playback_snapshots WHERE is_mock = TRUE"))
            deleted = result.rowcount

        return {"success": True, "message": f"Deleted {deleted} mock snapshots. {real_count} real snapshots preserved.", "deleted_count": deleted, "real_count": real_count}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/playback/mark_as_mock_before")
def api_playback_mark_as_mock_before(before_date: str = Query(..., description="Mark data before this date as mock (YYYY-MM-DD)")):
    """Mark all data before a certain date as mock. Use to separate old mock data from new real data."""
    if not engine:
        return {"error": "DATABASE_URL not set"}

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("UPDATE playback_snapshots SET is_mock = TRUE WHERE ts < :before_date"),
                {"before_date": before_date}
            )
            updated = result.rowcount

        return {"success": True, "message": f"Marked {updated} snapshots before {before_date} as mock data", "updated_count": updated}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/playback/status")
def api_playback_status():
    """Get playback data status: counts of mock vs real snapshots."""
    if not engine:
        return {"error": "DATABASE_URL not set"}

    try:
        with engine.connect() as conn:
            mock_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock = TRUE")).scalar()
            real_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock = FALSE")).scalar()
            null_count = conn.execute(text("SELECT COUNT(*) FROM playback_snapshots WHERE is_mock IS NULL")).scalar()
            total = mock_count + real_count + null_count

            # Get timestamp range
            result = conn.execute(text(
                "SELECT MIN(ts) as first_ts, MAX(ts) as last_ts FROM playback_snapshots WHERE is_mock = FALSE OR is_mock IS NULL"
            )).mappings().first()
            first_ts = result["first_ts"].isoformat() if result and result["first_ts"] else None
            last_ts = result["last_ts"].isoformat() if result and result["last_ts"] else None

        return {
            "total": total,
            "mock_count": mock_count,
            "real_count": real_count,
            "unmarked_count": null_count,
            "first_real": first_ts,
            "last_real": last_ts,
            "note": "Unmarked data was created before is_mock column. Use /api/playback/mark_existing_as_mock to mark as mock." if null_count > 0 else None
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/playback/range")
def api_playback_range(start_date: str = Query(None, description="Start date YYYY-MM-DD, default 7 days ago"), load_all: bool = Query(False, description="Load all data ignoring date filter")):
    """
    Get 7 days of playback snapshots starting from start_date.
    Returns timestamps, spot prices, and per-snapshot data for visualization.
    Use load_all=true to get all data regardless of date (for debugging).
    """
    if not engine:
        return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)

    try:
        with engine.begin() as conn:
            if load_all:
                # Load all data for debugging
                rows = conn.execute(text("""
                    SELECT ts, spot, strikes, net_gex, charm, delta_decay, call_vol, put_vol, stats
                    FROM playback_snapshots
                    ORDER BY ts ASC
                    LIMIT 1000
                """)).mappings().all()
                start_dt = None
                end_dt = None
            else:
                # Load from start_date (or 7 days ago) until now
                if start_date:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    start_dt = NY.localize(start_dt.replace(hour=0, minute=0, second=0))
                else:
                    start_dt = now_et().replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=7)

                # Always end at current time + buffer to include latest data
                end_dt = now_et() + pd.Timedelta(hours=1)

                rows = conn.execute(text("""
                    SELECT ts, spot, strikes, net_gex, charm, delta_decay, call_vol, put_vol, stats
                    FROM playback_snapshots
                    WHERE ts >= :start_ts AND ts < :end_ts
                    ORDER BY ts ASC
                """), {"start_ts": start_dt, "end_ts": end_dt}).mappings().all()

            # Fallback: fetch DD from volland_exposure_points for old rows without delta_decay column
            dd_grouped = {}
            dd_timestamps = []
            needs_dd_fallback = rows and any(r.get("delta_decay") is None for r in rows)
            if needs_dd_fallback:
                ts_min = rows[0]["ts"]
                ts_max = rows[-1]["ts"]
                dd_rows = conn.execute(text("""
                    SELECT ts_utc, strike::numeric AS strike, value::numeric AS dd_val
                    FROM volland_exposure_points
                    WHERE greek = 'deltaDecay'
                      AND ts_utc >= :ts_min - interval '3 minutes'
                      AND ts_utc <= :ts_max + interval '3 minutes'
                    ORDER BY ts_utc, strike
                """), {"ts_min": ts_min, "ts_max": ts_max}).mappings().all()

                for dr in dd_rows:
                    ts_key = dr["ts_utc"]
                    if ts_key not in dd_grouped:
                        dd_grouped[ts_key] = {}
                    dd_grouped[ts_key][float(dr["strike"])] = float(dr["dd_val"]) if dr["dd_val"] is not None else 0
                dd_timestamps = sorted(dd_grouped.keys())

        snapshots = []
        for r in rows:
            snap_ts = r["ts"]
            strikes = _json_load_maybe(r["strikes"]) or []

            # Use stored delta_decay if available, otherwise fallback to Volland lookup
            dd_data = _json_load_maybe(r.get("delta_decay"))
            if dd_data is None and dd_timestamps:
                best_ts = None
                best_diff = 999
                for dt in dd_timestamps:
                    diff = abs((dt - snap_ts).total_seconds())
                    if diff < best_diff:
                        best_diff = diff
                        best_ts = dt
                    elif diff > best_diff:
                        break
                if best_ts and best_diff <= 180 and dd_grouped.get(best_ts):
                    dd_map = dd_grouped[best_ts]
                    dd_data = [dd_map.get(s, 0) for s in strikes]

            snapshots.append({
                "ts": snap_ts.isoformat() if hasattr(snap_ts, "isoformat") else str(snap_ts),
                "spot": r["spot"],
                "strikes": strikes,
                "net_gex": _json_load_maybe(r["net_gex"]),
                "charm": _json_load_maybe(r["charm"]),
                "delta_decay": dd_data,
                "call_vol": _json_load_maybe(r["call_vol"]),
                "put_vol": _json_load_maybe(r["put_vol"]),
                "stats": _json_load_maybe(r["stats"]),
            })

        return {
            "start_date": start_dt.strftime("%Y-%m-%d") if start_dt else "all",
            "end_date": end_dt.strftime("%Y-%m-%d") if end_dt else "all",
            "count": len(snapshots),
            "snapshots": snapshots
        }
    except Exception as e:
        print(f"[playback/range] error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/export/playback")
def api_export_playback(start_date: str = Query(None, description="Start date YYYY-MM-DD"), load_all: bool = Query(False, description="Export all data")):
    """
    Export playback data as CSV.
    Each row is a snapshot with flattened strike-level data.
    """
    if not engine:
        return Response("DATABASE_URL not set", media_type="text/plain", status_code=500)

    try:
        with engine.begin() as conn:
            if load_all:
                rows = conn.execute(text("""
                    SELECT ts, spot, strikes, net_gex, charm, call_vol, put_vol, stats, call_gex, put_gex, call_oi, put_oi
                    FROM playback_snapshots
                    ORDER BY ts ASC
                    LIMIT 5000
                """)).mappings().all()
                start_dt = None
                end_dt = None
            else:
                if start_date:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    start_dt = NY.localize(start_dt.replace(hour=0, minute=0, second=0))
                else:
                    start_dt = now_et().replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=7)

                end_dt = now_et() + pd.Timedelta(hours=1)

                rows = conn.execute(text("""
                    SELECT ts, spot, strikes, net_gex, charm, call_vol, put_vol, stats, call_gex, put_gex, call_oi, put_oi
                    FROM playback_snapshots
                    WHERE ts >= :start_ts AND ts < :end_ts
                    ORDER BY ts ASC
                """), {"start_ts": start_dt, "end_ts": end_dt}).mappings().all()

        # Build CSV with one row per snapshot, strikes as columns
        csv_rows = []
        for r in rows:
            # Convert timestamp to ET for readability
            ts_raw = r["ts"]
            if hasattr(ts_raw, "astimezone"):
                ts_et = ts_raw.astimezone(NY)
                ts = ts_et.strftime("%Y-%m-%d %H:%M:%S ET")
            else:
                ts = str(ts_raw)
            spot = r["spot"]
            strikes = _json_load_maybe(r["strikes"]) or []
            net_gex = _json_load_maybe(r["net_gex"]) or []
            charm = _json_load_maybe(r["charm"]) or []
            call_vol = _json_load_maybe(r["call_vol"]) or []
            put_vol = _json_load_maybe(r["put_vol"]) or []
            stats = _json_load_maybe(r["stats"]) or {}
            call_gex = _json_load_maybe(r["call_gex"]) or []
            put_gex = _json_load_maybe(r["put_gex"]) or []
            call_oi = _json_load_maybe(r["call_oi"]) or []
            put_oi = _json_load_maybe(r["put_oi"]) or []

            # Create a row for each strike
            for i, strike in enumerate(strikes):
                csv_rows.append({
                    "timestamp": ts,
                    "spot": spot,
                    "strike": strike,
                    "call_gex": call_gex[i] if i < len(call_gex) else None,
                    "put_gex": put_gex[i] if i < len(put_gex) else None,
                    "net_gex": net_gex[i] if i < len(net_gex) else None,
                    "call_oi": call_oi[i] if i < len(call_oi) else None,
                    "put_oi": put_oi[i] if i < len(put_oi) else None,
                    "charm": charm[i] if i < len(charm) else None,
                    "call_vol": call_vol[i] if i < len(call_vol) else None,
                    "put_vol": put_vol[i] if i < len(put_vol) else None,
                    "paradigm": stats.get("paradigm"),
                    "target": stats.get("target"),
                    "lis": stats.get("lis"),
                    "dd_hedging": stats.get("dd_hedging"),
                    "opt_volume": stats.get("opt_volume"),
                })

        df = pd.DataFrame(csv_rows)
        csv_content = df.to_csv(index=False)

        if start_dt and end_dt:
            filename = f"playback_{start_dt.strftime('%Y%m%d')}_to_{end_dt.strftime('%Y%m%d')}.csv"
        else:
            filename = f"playback_all_{now_et().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"[export/playback] error: {e}", flush=True)
        return Response(f"Error: {e}", media_type="text/plain", status_code=500)

@app.get("/api/export/playback_summary")
def api_export_playback_summary(start_date: str = Query(None, description="Start date YYYY-MM-DD"), load_all: bool = Query(False, description="Export all data")):
    """
    Export playback data as summary CSV.
    One row per timestamp with aggregated statistics - easy to review.
    """
    if not engine:
        return Response("DATABASE_URL not set", media_type="text/plain", status_code=500)

    try:
        with engine.begin() as conn:
            if load_all:
                rows = conn.execute(text("""
                    SELECT ts, spot, strikes, net_gex, charm, call_vol, put_vol, stats, call_gex, put_gex, call_oi, put_oi
                    FROM playback_snapshots
                    ORDER BY ts ASC
                    LIMIT 5000
                """)).mappings().all()
                start_dt = None
                end_dt = None
            else:
                if start_date:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    start_dt = NY.localize(start_dt.replace(hour=0, minute=0, second=0))
                else:
                    start_dt = now_et().replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=7)

                end_dt = now_et() + pd.Timedelta(hours=1)

                rows = conn.execute(text("""
                    SELECT ts, spot, strikes, net_gex, charm, call_vol, put_vol, stats, call_gex, put_gex, call_oi, put_oi
                    FROM playback_snapshots
                    WHERE ts >= :start_ts AND ts < :end_ts
                    ORDER BY ts ASC
                """), {"start_ts": start_dt, "end_ts": end_dt}).mappings().all()

        # Build summary CSV - one row per timestamp
        csv_rows = []
        for r in rows:
            ts = r["ts"]
            # Convert timestamp to ET for readability
            if hasattr(ts, "astimezone"):
                ts_et = ts.astimezone(NY)
                ts_str = ts_et.strftime("%Y-%m-%d %H:%M:%S ET")
            else:
                ts_str = str(ts)
            spot = r["spot"]
            strikes = _json_load_maybe(r["strikes"]) or []
            net_gex = _json_load_maybe(r["net_gex"]) or []
            charm = _json_load_maybe(r["charm"]) or []
            call_vol = _json_load_maybe(r["call_vol"]) or []
            put_vol = _json_load_maybe(r["put_vol"]) or []
            stats = _json_load_maybe(r["stats"]) or {}
            call_gex = _json_load_maybe(r["call_gex"]) or []
            put_gex = _json_load_maybe(r["put_gex"]) or []
            call_oi = _json_load_maybe(r["call_oi"]) or []
            put_oi = _json_load_maybe(r["put_oi"]) or []

            # Find max +GEX and -GEX strikes
            max_pos_gex_strike, max_neg_gex_strike = None, None
            max_pos_val, max_neg_val = 0, 0
            for i, gex in enumerate(net_gex):
                if i < len(strikes):
                    if gex > max_pos_val:
                        max_pos_val = gex
                        max_pos_gex_strike = strikes[i]
                    if gex < max_neg_val:
                        max_neg_val = gex
                        max_neg_gex_strike = strikes[i]

            # Calculate totals
            total_call_vol = sum(call_vol) if call_vol else 0
            total_put_vol = sum(put_vol) if put_vol else 0
            total_vol = total_call_vol + total_put_vol
            net_gex_total = sum(net_gex) if net_gex else 0
            net_charm_total = sum(charm) if charm else 0
            call_gex_total = sum(call_gex) if call_gex else 0
            put_gex_total = sum(put_gex) if put_gex else 0
            total_call_oi = sum(call_oi) if call_oi else 0
            total_put_oi = sum(put_oi) if put_oi else 0

            csv_rows.append({
                "timestamp": ts_str,
                "spot": spot,
                "paradigm": stats.get("paradigm"),
                "target": stats.get("target"),
                "lis": stats.get("lis"),
                "max_pos_gex_strike": max_pos_gex_strike,
                "max_neg_gex_strike": max_neg_gex_strike,
                "dd_hedging": stats.get("dd_hedging"),
                "total_volume": total_vol,
                "call_volume": total_call_vol,
                "put_volume": total_put_vol,
                "net_gex_total": round(net_gex_total, 2) if net_gex_total else None,
                "call_gex_total": round(call_gex_total, 2) if call_gex_total else None,
                "put_gex_total": round(put_gex_total, 2) if put_gex_total else None,
                "total_call_oi": round(total_call_oi, 2) if total_call_oi else None,
                "total_put_oi": round(total_put_oi, 2) if total_put_oi else None,
                "net_charm_total": round(net_charm_total, 2) if net_charm_total else None,
            })

        df = pd.DataFrame(csv_rows)
        csv_content = df.to_csv(index=False)

        if start_dt and end_dt:
            filename = f"playback_summary_{start_dt.strftime('%Y%m%d')}_to_{end_dt.strftime('%Y%m%d')}.csv"
        else:
            filename = f"playback_summary_{now_et().strftime('%Y%m%d_%H%M')}.csv"
        return Response(
            csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"[export/playback_summary] error: {e}", flush=True)
        return Response(f"Error: {e}", media_type="text/plain", status_code=500)

# ===== Alert Settings API =====
@app.get("/api/alerts/settings")
def api_alerts_settings_get():
    """Get current alert settings."""
    return _alert_settings

@app.post("/api/alerts/settings")
def api_alerts_settings_post(
    enabled: bool = Query(None),
    lis_enabled: bool = Query(None),
    target_enabled: bool = Query(None),
    max_pos_gamma_enabled: bool = Query(None),
    max_neg_gamma_enabled: bool = Query(None),
    paradigm_change_enabled: bool = Query(None),
    summary_10am_enabled: bool = Query(None),
    summary_2pm_enabled: bool = Query(None),
    volume_spike_enabled: bool = Query(None),
    threshold_points: int = Query(None),
    threshold_volume: int = Query(None),
    cooldown_enabled: bool = Query(None),
    cooldown_minutes: int = Query(None),
):
    """Update alert settings."""
    global _alert_settings

    if enabled is not None:
        _alert_settings["enabled"] = enabled
    if lis_enabled is not None:
        _alert_settings["lis_enabled"] = lis_enabled
    if target_enabled is not None:
        _alert_settings["target_enabled"] = target_enabled
    if max_pos_gamma_enabled is not None:
        _alert_settings["max_pos_gamma_enabled"] = max_pos_gamma_enabled
    if max_neg_gamma_enabled is not None:
        _alert_settings["max_neg_gamma_enabled"] = max_neg_gamma_enabled
    if paradigm_change_enabled is not None:
        _alert_settings["paradigm_change_enabled"] = paradigm_change_enabled
    if summary_10am_enabled is not None:
        _alert_settings["summary_10am_enabled"] = summary_10am_enabled
    if summary_2pm_enabled is not None:
        _alert_settings["summary_2pm_enabled"] = summary_2pm_enabled
    if volume_spike_enabled is not None:
        _alert_settings["volume_spike_enabled"] = volume_spike_enabled
    if threshold_points is not None:
        _alert_settings["threshold_points"] = threshold_points
    if threshold_volume is not None:
        _alert_settings["threshold_volume"] = threshold_volume
    if cooldown_enabled is not None:
        _alert_settings["cooldown_enabled"] = cooldown_enabled
    if cooldown_minutes is not None:
        _alert_settings["cooldown_minutes"] = cooldown_minutes

    # Save to database
    save_alert_settings()
    return {"status": "ok", "settings": _alert_settings}

@app.post("/api/alerts/test")
def api_alerts_test():
    """Send a test alert to verify Telegram configuration."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {
            "status": "error",
            "message": "Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.",
            "token_set": bool(TELEGRAM_BOT_TOKEN),
            "chat_id_set": bool(TELEGRAM_CHAT_ID)
        }

    success = send_telegram("🧪 <b>Test Alert</b>\n\nYour 0DTE Alpha alerts are working!")
    if success:
        return {"status": "ok", "message": "Test alert sent successfully!"}
    else:
        return {"status": "error", "message": "Failed to send test alert. Check your token and chat ID."}

@app.get("/api/alerts/status")
def api_alerts_status():
    """Check Telegram configuration status (for debugging)."""
    return {
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "token_set": bool(TELEGRAM_BOT_TOKEN),
        "token_preview": TELEGRAM_BOT_TOKEN[:10] + "..." if TELEGRAM_BOT_TOKEN else None,
        "chat_id_set": bool(TELEGRAM_CHAT_ID),
        "chat_id_preview": TELEGRAM_CHAT_ID[:5] + "..." if TELEGRAM_CHAT_ID else None,
        "settings": _alert_settings
    }

# ====== TRADESTATION OAUTH RE-AUTHORIZATION ======
# Used to upgrade scopes (e.g. add Trade scope for SIM auto-trading)
TS_REDIRECT_URI = "https://0dtealpha.com/api/ts/callback"
TS_SCOPES = "openid profile MarketData ReadAccount Trade offline_access"

@app.get("/api/ts/authorize")
def ts_authorize():
    """Redirect to TradeStation OAuth to re-authorize with Trade scope."""
    if not CID:
        return JSONResponse({"error": "TS_CLIENT_ID not set"}, status_code=500)
    import urllib.parse
    params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": CID,
        "redirect_uri": TS_REDIRECT_URI,
        "audience": "https://api.tradestation.com",
        "scope": TS_SCOPES,
    })
    url = f"{AUTH_DOMAIN}/authorize?{params}"
    return RedirectResponse(url=url)

@app.get("/api/ts/callback")
def ts_callback(code: str = Query(None), error: str = Query(None)):
    """OAuth callback — exchange code for tokens with Trade scope."""
    global _access_token, _refresh_token, _access_exp_at
    if error:
        return HTMLResponse(f"<h2>OAuth Error</h2><p>{error}</p>", status_code=400)
    if not code:
        return HTMLResponse("<h2>Missing code parameter</h2>", status_code=400)
    try:
        r = requests.post(
            f"{AUTH_DOMAIN}/oauth/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": CID,
                "client_secret": SECRET,
                "redirect_uri": TS_REDIRECT_URI,
            },
            timeout=15,
        )
        if r.status_code >= 400:
            return HTMLResponse(f"<h2>Token exchange failed</h2><pre>{r.text[:500]}</pre>", status_code=400)
        tok = r.json()
        _access_token = tok["access_token"]
        new_refresh = tok.get("refresh_token", "")
        if new_refresh:
            _refresh_token = new_refresh
        _stamp_token(tok.get("expires_in", 900))
        # Show the new refresh token so the user can update Railway env var
        masked = new_refresh[:8] + "..." + new_refresh[-8:] if len(new_refresh) > 20 else new_refresh
        scopes = tok.get("scope", "unknown")
        html = f"""<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;padding:40px">
        <h2 style="color:#22c55e">OAuth Success</h2>
        <p><b>Scopes:</b> {scopes}</p>
        <p><b>Token active in memory</b> — trading will work until next restart.</p>
        <hr>
        <p><b>To persist across restarts</b>, update the Railway env var:</p>
        <pre style="background:#161b22;padding:12px;border-radius:6px;overflow-x:auto;user-select:all">{new_refresh}</pre>
        <p style="color:#f59e0b">Copy the full token above and set it as <code>TS_REFRESH_TOKEN</code> on Railway.</p>
        <p><a href="/dashboard" style="color:#3b82f6">Back to Dashboard</a></p>
        </body></html>"""
        print(f"[auth] OAuth re-auth success: scopes={scopes} refresh={masked}", flush=True)
        send_telegram(f"🔑 <b>TS OAuth re-authorized</b>\nScopes: {scopes}")
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<h2>Error</h2><pre>{e}</pre>", status_code=500)

# ====== SETUP DETECTOR ENDPOINTS ======

@app.get("/api/setup/settings")
def api_setup_settings_get():
    """Get current setup detector settings."""
    return _setup_settings

@app.post("/api/setup/settings")
def api_setup_settings_post(
    gex_long_enabled: bool = Query(None),
    ag_short_enabled: bool = Query(None),
    bofa_scalp_enabled: bool = Query(None),
    absorption_enabled: bool = Query(None),
    weight_support: int = Query(None),
    weight_upside: int = Query(None),
    weight_floor_cluster: int = Query(None),
    weight_target_cluster: int = Query(None),
    weight_rr: int = Query(None),
    grade_a_plus: int = Query(None),
    grade_a: int = Query(None),
    grade_a_entry: int = Query(None),
    bofa_weight_stability: int = Query(None),
    bofa_weight_width: int = Query(None),
    bofa_weight_charm: int = Query(None),
    bofa_weight_time: int = Query(None),
    bofa_weight_midpoint: int = Query(None),
    bofa_stop_distance: int = Query(None),
    bofa_target_distance: int = Query(None),
    bofa_max_hold_minutes: int = Query(None),
    bofa_cooldown_minutes: int = Query(None),
    abs_pivot_left: int = Query(None),
    abs_pivot_right: int = Query(None),
    abs_vol_window: int = Query(None),
    abs_min_vol_ratio: float = Query(None),
    abs_cvd_z_min: float = Query(None),
    abs_cvd_std_window: int = Query(None),
    abs_cooldown_bars: int = Query(None),
    abs_weight_divergence: int = Query(None),
    abs_weight_volume: int = Query(None),
    abs_weight_dd: int = Query(None),
    abs_weight_paradigm: int = Query(None),
    abs_weight_lis: int = Query(None),
    abs_weight_lis_side: int = Query(None),
    abs_weight_target_dir: int = Query(None),
):
    """Update setup detector settings."""
    global _setup_settings

    if gex_long_enabled is not None:
        _setup_settings["gex_long_enabled"] = gex_long_enabled
    if ag_short_enabled is not None:
        _setup_settings["ag_short_enabled"] = ag_short_enabled
    if bofa_scalp_enabled is not None:
        _setup_settings["bofa_scalp_enabled"] = bofa_scalp_enabled
    if absorption_enabled is not None:
        _setup_settings["absorption_enabled"] = absorption_enabled
    if weight_support is not None:
        _setup_settings["weight_support"] = weight_support
    if weight_upside is not None:
        _setup_settings["weight_upside"] = weight_upside
    if weight_floor_cluster is not None:
        _setup_settings["weight_floor_cluster"] = weight_floor_cluster
    if weight_target_cluster is not None:
        _setup_settings["weight_target_cluster"] = weight_target_cluster
    if weight_rr is not None:
        _setup_settings["weight_rr"] = weight_rr
    if grade_a_plus is not None or grade_a is not None or grade_a_entry is not None:
        thresholds = dict(_setup_settings.get("grade_thresholds", _DEFAULT_SETUP_SETTINGS["grade_thresholds"]))
        if grade_a_plus is not None:
            thresholds["A+"] = grade_a_plus
        if grade_a is not None:
            thresholds["A"] = grade_a
        if grade_a_entry is not None:
            thresholds["A-Entry"] = grade_a_entry
        _setup_settings["grade_thresholds"] = thresholds
    # BofA Scalp weights
    if bofa_weight_stability is not None:
        _setup_settings["bofa_weight_stability"] = bofa_weight_stability
    if bofa_weight_width is not None:
        _setup_settings["bofa_weight_width"] = bofa_weight_width
    if bofa_weight_charm is not None:
        _setup_settings["bofa_weight_charm"] = bofa_weight_charm
    if bofa_weight_time is not None:
        _setup_settings["bofa_weight_time"] = bofa_weight_time
    if bofa_weight_midpoint is not None:
        _setup_settings["bofa_weight_midpoint"] = bofa_weight_midpoint
    if bofa_stop_distance is not None:
        _setup_settings["bofa_stop_distance"] = bofa_stop_distance
    if bofa_target_distance is not None:
        _setup_settings["bofa_target_distance"] = bofa_target_distance
    if bofa_max_hold_minutes is not None:
        _setup_settings["bofa_max_hold_minutes"] = bofa_max_hold_minutes
    if bofa_cooldown_minutes is not None:
        _setup_settings["bofa_cooldown_minutes"] = bofa_cooldown_minutes
    # Absorption weights/params
    if abs_pivot_left is not None:
        _setup_settings["abs_pivot_left"] = abs_pivot_left
    if abs_pivot_right is not None:
        _setup_settings["abs_pivot_right"] = abs_pivot_right
    if abs_vol_window is not None:
        _setup_settings["abs_vol_window"] = abs_vol_window
    if abs_min_vol_ratio is not None:
        _setup_settings["abs_min_vol_ratio"] = abs_min_vol_ratio
    if abs_cvd_z_min is not None:
        _setup_settings["abs_cvd_z_min"] = abs_cvd_z_min
    if abs_cvd_std_window is not None:
        _setup_settings["abs_cvd_std_window"] = abs_cvd_std_window
    if abs_cooldown_bars is not None:
        _setup_settings["abs_cooldown_bars"] = abs_cooldown_bars
    if abs_weight_divergence is not None:
        _setup_settings["abs_weight_divergence"] = abs_weight_divergence
    if abs_weight_volume is not None:
        _setup_settings["abs_weight_volume"] = abs_weight_volume
    if abs_weight_dd is not None:
        _setup_settings["abs_weight_dd"] = abs_weight_dd
    if abs_weight_paradigm is not None:
        _setup_settings["abs_weight_paradigm"] = abs_weight_paradigm
    if abs_weight_lis is not None:
        _setup_settings["abs_weight_lis"] = abs_weight_lis
    if abs_weight_lis_side is not None:
        _setup_settings["abs_weight_lis_side"] = abs_weight_lis_side
    if abs_weight_target_dir is not None:
        _setup_settings["abs_weight_target_dir"] = abs_weight_target_dir

    save_setup_settings()
    return {"status": "ok", "settings": _setup_settings}

@app.get("/api/setup/log")
def api_setup_log(limit: int = Query(50)):
    """Get recent setup detection log entries."""
    if not engine:
        return []
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, ts, setup_name, direction, grade, score,
                       paradigm, spot, lis, target, max_plus_gex, max_minus_gex,
                       gap_to_lis, upside, rr_ratio, first_hour,
                       support_score, upside_score, floor_cluster_score,
                       target_cluster_score, rr_score, notified,
                       bofa_stop_level, bofa_target_level, bofa_lis_width,
                       bofa_max_hold_minutes, lis_upper,
                       abs_vol_ratio, abs_es_price
                FROM setup_log
                ORDER BY ts DESC
                LIMIT :lim
            """), {"lim": min(int(limit), 200)}).mappings().all()
            return [
                {
                    **{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}
                }
                for r in rows
            ]
    except Exception as e:
        print(f"[setups] log query error: {e}", flush=True)
        return []


def _calculate_absorption_outcome(entry: dict) -> dict:
    """Calculate outcome for ES Absorption using ES range bars.

    10pt first target, Volland target (converted SPX→ES) as second target
    (or 10pt only for BofA paradigm), 12pt fixed stop.
    """
    try:
        ts = entry.get("ts")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if not ts:
            return {"no_data": True}

        es_entry = entry.get("abs_es_price")
        if not es_entry:
            return {"no_data": True}

        direction = entry.get("direction", "bullish")
        is_long = direction.lower() in ("long", "bullish")
        paradigm = (entry.get("paradigm") or "").upper()
        is_bofa_paradigm = "BOFA" in paradigm

        # Determine levels
        ten_pt_level = es_entry + 10 if is_long else es_entry - 10
        stop_level = es_entry - 12 if is_long else es_entry + 12

        # Volland target converted to ES
        target_level = None
        spx_spot = entry.get("spot")
        spx_target = entry.get("target")
        if not is_bofa_paradigm and spx_target and spx_spot and spx_spot != es_entry:
            offset = es_entry - spx_spot
            target_level = spx_target + offset
        if target_level is None:
            target_level = ten_pt_level  # fallback: same as 10pt

        # Get ES range bars for that session date
        alert_date = ts.astimezone(NY).date() if ts.tzinfo else NY.localize(ts).date()
        with engine.begin() as conn:
            bar_rows = conn.execute(text("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       ts_start, ts_end, status
                FROM es_range_bars
                WHERE trade_date = :td AND symbol = '@ES' AND status = 'closed'
                ORDER BY bar_idx ASC
            """), {"td": alert_date.isoformat()}).mappings().all()

        if not bar_rows:
            return {"no_data": True}

        # Find the signal bar: last completed bar that started before the trade timestamp
        signal_bar_idx = None
        for r in bar_rows:
            bar_start = r["ts_start"]
            if hasattr(bar_start, "tzinfo") and bar_start.tzinfo is None:
                bar_start = NY.localize(bar_start)
            if bar_start <= ts:
                signal_bar_idx = r["bar_idx"]  # keep updating — last bar before ts
            else:
                break  # bars are ordered by idx, so stop once we pass ts
        if signal_bar_idx is None:
            return {"no_data": True}

        # Walk bars after the signal bar
        hit_10pt = False
        hit_target = False
        hit_stop = False
        time_to_10pt = None
        time_to_target = None
        time_to_stop = None
        max_profit = 0.0
        max_profit_ts = None
        max_loss = 0.0
        max_loss_ts = None
        first_event = None
        bars_after = 0

        for r in bar_rows:
            if r["bar_idx"] <= signal_bar_idx:
                continue
            bars_after += 1
            bar_high = r["bar_high"]
            bar_low = r["bar_low"]
            bar_ts = r["ts_end"]

            if is_long:
                profit_high = bar_high - es_entry
                profit_low = bar_low - es_entry
                if not hit_10pt and bar_high >= ten_pt_level:
                    hit_10pt = True
                    time_to_10pt = bar_ts
                    if first_event is None:
                        first_event = "10pt"
                if not hit_target and bar_high >= target_level:
                    hit_target = True
                    time_to_target = bar_ts
                    if first_event is None:
                        first_event = "target"
                if not hit_stop and bar_low <= stop_level:
                    hit_stop = True
                    time_to_stop = bar_ts
                    if first_event is None:
                        first_event = "stop"
                if profit_high > max_profit:
                    max_profit = profit_high
                    max_profit_ts = bar_ts
                if profit_low < max_loss:
                    max_loss = profit_low
                    max_loss_ts = bar_ts
            else:
                profit_high = es_entry - bar_low
                profit_low = es_entry - bar_high
                if not hit_10pt and bar_low <= ten_pt_level:
                    hit_10pt = True
                    time_to_10pt = bar_ts
                    if first_event is None:
                        first_event = "10pt"
                if not hit_target and bar_low <= target_level:
                    hit_target = True
                    time_to_target = bar_ts
                    if first_event is None:
                        first_event = "target"
                if not hit_stop and bar_high >= stop_level:
                    hit_stop = True
                    time_to_stop = bar_ts
                    if first_event is None:
                        first_event = "stop"
                if profit_high > max_profit:
                    max_profit = profit_high
                    max_profit_ts = bar_ts
                if profit_low < max_loss:
                    max_loss = profit_low
                    max_loss_ts = bar_ts

        if first_event is None:
            first_event = "pending" if bars_after < 20 else "miss"

        def _iso(v):
            return v.isoformat() if v and hasattr(v, "isoformat") else v

        return {
            "is_absorption": True,
            "hit_10pt": hit_10pt,
            "hit_target": hit_target,
            "hit_stop": hit_stop,
            "time_to_10pt": _iso(time_to_10pt),
            "time_to_target": _iso(time_to_target),
            "time_to_stop": _iso(time_to_stop),
            "max_profit": round(max_profit, 2),
            "max_profit_ts": _iso(max_profit_ts),
            "max_loss": round(max_loss, 2),
            "max_loss_ts": _iso(max_loss_ts),
            "first_event": first_event,
            "ten_pt_level": round(ten_pt_level, 2),
            "target_level": round(target_level, 2),
            "stop_level": round(stop_level, 2),
            "bars_after": bars_after,
            "is_bofa_paradigm": is_bofa_paradigm,
        }
    except Exception as e:
        print(f"[setups] absorption outcome error: {e}", flush=True)
        return {"error": str(e)}


def _calculate_setup_outcome(entry: dict) -> dict:
    """
    Calculate outcome for a setup alert by querying price history.
    Returns dict with hit_10pt, hit_target, hit_stop, max_profit, max_loss, etc.
    BofA Scalp uses different parameters: 10pt target, 12pt stop, 30-min max hold.
    ES Absorption uses ES range bars: 10pt first target, converted Volland target, 12pt stop.
    """
    if not engine:
        return {}

    # ES Absorption: outcome tracking using ES range bars
    if entry.get("setup_name") == "ES Absorption":
        return _calculate_absorption_outcome(entry)

    try:
        ts = entry.get("ts")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        spot = entry.get("spot")
        direction = entry.get("direction", "long")
        lis = entry.get("lis")
        target = entry.get("target")
        setup_name = entry.get("setup_name", "")
        is_bofa = setup_name == "BofA Scalp"
        is_dd = setup_name == "DD Exhaustion"
        is_paradigm = setup_name == "Paradigm Reversal"
        is_gex = setup_name == "GEX Long"
        is_ag = setup_name == "AG Short"
        is_trailing = is_dd or is_gex or is_ag  # setups with trailing stop, no fixed target

        if not all([ts, spot]):
            return {}
        # Trailing, BofA, Paradigm don't need lis/target — they use fixed pts from spot
        if not is_bofa and not is_trailing and not is_paradigm and not lis:
            return {}
        if not is_bofa and not is_trailing and not is_paradigm and target is None:
            return {}

        # Get market close time for that day (4:00 PM ET)
        alert_date = ts.astimezone(NY).date() if ts.tzinfo else NY.localize(ts).date()
        market_close = NY.localize(datetime.combine(alert_date, dtime(16, 0)))

        # BofA Scalp: limit window to max hold time (default 30 min)
        if is_bofa:
            max_hold = entry.get("bofa_max_hold_minutes") or 30
            end_ts = ts + timedelta(minutes=max_hold)
            if end_ts > market_close:
                end_ts = market_close
        else:
            end_ts = market_close

        # Query playback_snapshots
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT ts, spot FROM playback_snapshots
                WHERE ts >= :start_ts AND ts <= :end_ts
                ORDER BY ts ASC
            """), {"start_ts": ts, "end_ts": end_ts}).mappings().all()

        if not rows:
            return {"no_data": True}

        prices = [(r["ts"], r["spot"]) for r in rows if r["spot"] is not None]
        if not prices:
            return {"no_data": True}

        # Calculate levels
        is_long = direction.lower() == "long"

        # Trailing stop parameters
        # DD Exhaustion: continuous trail (activation=20, gap=5, initial_sl=12)
        # GEX Long: hybrid trail (BE at +10, continuous trail activation=15 gap=5, initial_sl=8)
        # AG Short: hybrid trail (BE at +10, continuous trail activation=15 gap=5)
        _trail_params = {
            "DD Exhaustion": {"mode": "continuous", "activation": 20, "gap": 5, "initial_sl": 12},
            "GEX Long": {"mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5, "initial_sl": 8},
            "AG Short": {"mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5},
        }

        if is_trailing:
            tp = _trail_params[setup_name]
            if "initial_sl" in tp:
                initial_sl = tp["initial_sl"]
                stop_level = spot - initial_sl if is_long else spot + initial_sl
            else:
                # AG Short: use LIS-based stop (same as non-trailing AG logic)
                max_minus_gex = entry.get("max_minus_gex")
                max_plus_gex = entry.get("max_plus_gex")
                max_stop_dist = 20
                if is_long:
                    stop_level = lis - 5 if lis else spot - 20
                    if max_minus_gex is not None and max_minus_gex < stop_level:
                        stop_level = max_minus_gex
                    stop_level = max(stop_level, spot - max_stop_dist)
                else:
                    stop_level = lis + 5 if lis else spot + 20
                    if max_plus_gex is not None and max_plus_gex > stop_level:
                        stop_level = max_plus_gex
                    stop_level = min(stop_level, spot + max_stop_dist)
            rung_start = tp.get("rung_start") or tp.get("activation") or tp.get("be_trigger", 10)
            ten_pt_level = spot + rung_start if is_long else spot - rung_start  # first trail activation
            target_level = None  # trailing — no fixed target
        elif is_paradigm:
            # Paradigm Reversal: fixed 10pt target, 15pt stop from spot
            ten_pt_level = spot + 10 if is_long else spot - 10
            target_level = ten_pt_level
            stop_level = spot - 15 if is_long else spot + 15
        elif is_bofa:
            # BofA Scalp: fixed 10pt target, 12pt stop beyond LIS
            bofa_target_dist = entry.get("bofa_target_level") or (spot + 10 if is_long else spot - 10)
            bofa_stop = entry.get("bofa_stop_level")
            if bofa_stop is None:
                bofa_stop = lis - 12 if is_long else lis + 12
            ten_pt_level = bofa_target_dist  # For BofA, this is the 10pt target
            stop_level = bofa_stop
            target_level = bofa_target_dist
        else:
            # GEX Long / AG Short: original logic
            ten_pt_level = spot + 10 if is_long else spot - 10
            target_level = target
            max_minus_gex = entry.get("max_minus_gex")
            max_plus_gex = entry.get("max_plus_gex")
            max_stop_dist = 20  # Cap stop at 20 pts from entry
            if is_long:
                stop_level = lis - 5
                if max_minus_gex is not None and max_minus_gex < stop_level:
                    stop_level = max_minus_gex
                stop_level = max(stop_level, spot - max_stop_dist)
            else:
                stop_level = lis + 5
                if max_plus_gex is not None and max_plus_gex > stop_level:
                    stop_level = max_plus_gex
                stop_level = min(stop_level, spot + max_stop_dist)

        # Track outcomes
        hit_10pt = False
        hit_target = False
        hit_stop = False
        time_to_10pt = None
        time_to_target = None
        time_to_stop = None

        max_profit = 0.0
        max_profit_ts = None
        max_loss = 0.0
        max_loss_ts = None

        first_event = None  # "10pt", "target", "stop", or "timeout" (BofA only)

        trail_max_fav = 0.0  # Trailing setups: track max favorable excursion
        trail_stopped = False

        for price_ts, price in prices:
            if is_long:
                profit = price - spot

                # Trailing stop logic (DD Exhaustion, GEX Long, AG Short)
                if is_trailing and not trail_stopped:
                    if profit > trail_max_fav:
                        trail_max_fav = profit
                    trail_lock = None
                    if tp["mode"] == "continuous":
                        if trail_max_fav >= tp["activation"]:
                            trail_lock = trail_max_fav - tp["gap"]
                    elif tp["mode"] == "hybrid":
                        if trail_max_fav >= tp["activation"]:
                            trail_lock = trail_max_fav - tp["gap"]
                        elif trail_max_fav >= tp["be_trigger"]:
                            trail_lock = 0  # breakeven
                    else:
                        rung = tp["rung_start"]
                        while rung <= trail_max_fav:
                            trail_lock = rung - tp["lock_offset"]
                            rung += tp["step"]
                    if trail_lock is not None:
                        new_stop = spot + trail_lock
                        if new_stop > stop_level:
                            stop_level = new_stop
                    if price <= stop_level:
                        trail_stopped = True
                        hit_stop = True
                        time_to_stop = price_ts
                        pnl_at_stop = stop_level - spot
                        first_event = "target" if pnl_at_stop > 0 else "stop"
                        continue

                if not hit_10pt and price >= ten_pt_level:
                    hit_10pt = True
                    time_to_10pt = price_ts
                    if first_event is None:
                        first_event = "10pt"
                if target_level is not None and not hit_target and price >= target_level:
                    hit_target = True
                    time_to_target = price_ts
                    if first_event is None:
                        first_event = "target"
                if not is_trailing and not hit_stop and price <= stop_level:
                    hit_stop = True
                    time_to_stop = price_ts
                    if first_event is None:
                        first_event = "stop"
                if profit > max_profit:
                    max_profit = profit
                    max_profit_ts = price_ts
                if profit < max_loss:
                    max_loss = profit
                    max_loss_ts = price_ts
            else:  # SHORT
                profit = spot - price

                # Trailing stop logic (DD Exhaustion, GEX Long, AG Short)
                if is_trailing and not trail_stopped:
                    if profit > trail_max_fav:
                        trail_max_fav = profit
                    trail_lock = None
                    if tp["mode"] == "continuous":
                        if trail_max_fav >= tp["activation"]:
                            trail_lock = trail_max_fav - tp["gap"]
                    elif tp["mode"] == "hybrid":
                        if trail_max_fav >= tp["activation"]:
                            trail_lock = trail_max_fav - tp["gap"]
                        elif trail_max_fav >= tp["be_trigger"]:
                            trail_lock = 0  # breakeven
                    else:
                        rung = tp["rung_start"]
                        while rung <= trail_max_fav:
                            trail_lock = rung - tp["lock_offset"]
                            rung += tp["step"]
                    if trail_lock is not None:
                        new_stop = spot - trail_lock
                        if new_stop < stop_level:
                            stop_level = new_stop
                    if price >= stop_level:
                        trail_stopped = True
                        hit_stop = True
                        time_to_stop = price_ts
                        pnl_at_stop = spot - stop_level
                        first_event = "target" if pnl_at_stop > 0 else "stop"
                        continue

                if not hit_10pt and price <= ten_pt_level:
                    hit_10pt = True
                    time_to_10pt = price_ts
                    if first_event is None:
                        first_event = "10pt"
                if target_level is not None and not hit_target and price <= target_level:
                    hit_target = True
                    time_to_target = price_ts
                    if first_event is None:
                        first_event = "target"
                if not is_trailing and not hit_stop and price >= stop_level:
                    hit_stop = True
                    time_to_stop = price_ts
                    if first_event is None:
                        first_event = "stop"
                if profit > max_profit:
                    max_profit = profit
                    max_profit_ts = price_ts
                if profit < max_loss:
                    max_loss = profit
                    max_loss_ts = price_ts

        # Trailing setups: if trail stopped with profit, mark as target hit
        if is_trailing and trail_stopped:
            pnl_at_stop = stop_level - spot if is_long else spot - stop_level
            if pnl_at_stop > 0:
                hit_target = True
                time_to_target = time_to_stop

        # BofA Scalp: if no event by end of window, it's a timeout
        timeout_pnl = None
        if is_bofa and first_event is None:
            first_event = "timeout"
            if prices:
                last_price = prices[-1][1]
                timeout_pnl = round((last_price - spot) if is_long else (spot - last_price), 2)

        # Trailing setups: if trailing stop never hit, EOD mark-to-market
        if is_trailing and first_event is None and prices:
            last_price = prices[-1][1]
            timeout_pnl = round((last_price - spot) if is_long else (spot - last_price), 2)
            first_event = "timeout"

        result = {
            "hit_10pt": hit_10pt,
            "hit_target": hit_target,
            "hit_stop": hit_stop,
            "time_to_10pt": time_to_10pt.isoformat() if time_to_10pt and hasattr(time_to_10pt, "isoformat") else time_to_10pt,
            "time_to_target": time_to_target.isoformat() if time_to_target and hasattr(time_to_target, "isoformat") else time_to_target,
            "time_to_stop": time_to_stop.isoformat() if time_to_stop and hasattr(time_to_stop, "isoformat") else time_to_stop,
            "max_profit": round(max_profit, 2),
            "max_profit_ts": max_profit_ts.isoformat() if max_profit_ts and hasattr(max_profit_ts, "isoformat") else max_profit_ts,
            "max_loss": round(max_loss, 2),
            "max_loss_ts": max_loss_ts.isoformat() if max_loss_ts and hasattr(max_loss_ts, "isoformat") else max_loss_ts,
            "first_event": first_event,
            "ten_pt_level": round(ten_pt_level, 2),
            "target_level": round(target_level, 2) if target_level is not None else None,
            "stop_level": round(stop_level, 2),
            "price_count": len(prices),
        }
        if is_bofa:
            result["is_bofa"] = True
            result["timeout_pnl"] = timeout_pnl
            result["bofa_target_level"] = round(ten_pt_level, 2)
            result["bofa_max_hold_minutes"] = entry.get("bofa_max_hold_minutes") or 30
        if is_trailing:
            result["is_trailing"] = True
            result["timeout_pnl"] = timeout_pnl
            result["trail_max_fav"] = round(trail_max_fav, 2)
            result["trail_final_stop"] = round(stop_level, 2)
            # Keep legacy keys for backward compat
            result["dd_max_fav"] = result["trail_max_fav"]
            result["dd_final_stop"] = result["trail_final_stop"]
        return result
    except Exception as e:
        print(f"[setups] outcome calculation error: {e}", flush=True)
        return {"error": str(e)}


@app.get("/api/setup/log/{log_id}/outcome")
def api_setup_log_outcome(log_id: int):
    """Get detailed outcome data for a single setup alert, including price history for charting."""
    if not engine:
        return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)

    try:
        # Get the setup entry
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT id, ts, setup_name, direction, grade, score,
                       paradigm, spot, lis, target, max_plus_gex, max_minus_gex,
                       gap_to_lis, upside, rr_ratio, first_hour, notified,
                       bofa_stop_level, bofa_target_level, bofa_lis_width,
                       bofa_max_hold_minutes, lis_upper, comments,
                       abs_vol_ratio, abs_es_price
                FROM setup_log WHERE id = :log_id
            """), {"log_id": log_id}).mappings().first()

        if not row:
            return JSONResponse({"error": "Setup not found"}, status_code=404)

        entry = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}

        # Get price history
        ts = row["ts"]
        is_bofa = row["setup_name"] == "BofA Scalp"
        is_abs = row["setup_name"] == "ES Absorption"
        alert_date = ts.astimezone(NY).date() if ts.tzinfo else NY.localize(ts).date()
        market_open = NY.localize(datetime.combine(alert_date, dtime(9, 30)))
        market_close = NY.localize(datetime.combine(alert_date, dtime(16, 0)))

        if is_abs:
            # ES Absorption: fetch ES range bars from es_range_bars table
            with engine.begin() as conn:
                es_rows = conn.execute(text("""
                    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                           bar_volume, bar_delta, cumulative_delta,
                           ts_start, ts_end, status
                    FROM es_range_bars
                    WHERE trade_date = :td AND symbol = '@ES'
                    ORDER BY bar_idx ASC
                """), {"td": alert_date.isoformat()}).mappings().all()

            es_bars = [
                {
                    "idx": r["bar_idx"],
                    "open": r["bar_open"], "high": r["bar_high"],
                    "low": r["bar_low"], "close": r["bar_close"],
                    "volume": r["bar_volume"], "delta": r["bar_delta"],
                    "cvd": r["cumulative_delta"],
                    "ts_start": r["ts_start"].isoformat() if hasattr(r["ts_start"], "isoformat") else r["ts_start"],
                    "ts_end": r["ts_end"].isoformat() if hasattr(r["ts_end"], "isoformat") else r["ts_end"],
                    "status": r["status"],
                }
                for r in es_rows
            ]

            outcome = _calculate_setup_outcome(dict(row))
            # ES entry price for chart, SPX spot for conversion offset
            es_entry = row.get("abs_es_price") or row["spot"]
            spx_spot = row["spot"]
            offset = (es_entry - spx_spot) if (es_entry and spx_spot and es_entry != spx_spot) else 0
            levels = {
                "entry": es_entry,
                "spot_spx": spx_spot,
                "offset": round(offset, 2),
                "lis": row["lis"],
                "target": row["target"],
                "max_plus_gex": row["max_plus_gex"],
                "max_minus_gex": row["max_minus_gex"],
                "abs_es_price": row.get("abs_es_price"),
                "abs_vol_ratio": row.get("abs_vol_ratio"),
                # Outcome levels (ES prices)
                "ten_pt": outcome.get("ten_pt_level"),
                "target_es": outcome.get("target_level"),
                "stop": outcome.get("stop_level"),
            }
            return {
                "entry": entry,
                "outcome": outcome,
                "prices": [],
                "es_bars": es_bars,
                "levels": levels,
            }

        # BofA Scalp: show entry ± 1hr for context, GEX/AG: full day
        if is_bofa:
            chart_start = ts - timedelta(hours=1)
            if chart_start < market_open:
                chart_start = market_open
            chart_end = ts + timedelta(hours=1)
            if chart_end > market_close:
                chart_end = market_close
        else:
            chart_start = market_open
            chart_end = market_close

        with engine.begin() as conn:
            price_rows = conn.execute(text("""
                SELECT ts, spot FROM playback_snapshots
                WHERE ts >= :start_ts AND ts <= :end_ts
                ORDER BY ts ASC
            """), {"start_ts": chart_start, "end_ts": chart_end}).mappings().all()

        prices = [
            {"ts": r["ts"].isoformat() if hasattr(r["ts"], "isoformat") else r["ts"], "spot": r["spot"]}
            for r in price_rows if r["spot"] is not None
        ]

        # Calculate outcome
        outcome = _calculate_setup_outcome(dict(row))

        levels = {
            "entry": row["spot"],
            "lis": row["lis"],
            "target": row["target"],
            "ten_pt": outcome.get("ten_pt_level"),
            "stop": outcome.get("stop_level"),
            "max_plus_gex": row["max_plus_gex"],
            "max_minus_gex": row["max_minus_gex"],
        }
        if is_bofa:
            levels["lis_upper"] = row.get("lis_upper")
            levels["bofa_target_level"] = outcome.get("bofa_target_level")
            levels["bofa_max_hold_minutes"] = row.get("bofa_max_hold_minutes") or 30

        return {
            "entry": entry,
            "outcome": outcome,
            "prices": prices,
            "levels": levels,
        }
    except Exception as e:
        print(f"[setups] outcome query error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/setup/log/{log_id}/comment")
async def api_setup_log_comment(log_id: int, request: Request):
    """Save a comment/remark on a setup log entry."""
    if not engine:
        return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
    try:
        body = await request.json()
        comments = body.get("comments", "")
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE setup_log SET comments = :comments WHERE id = :log_id"
            ), {"comments": comments, "log_id": log_id})
        return {"ok": True}
    except Exception as e:
        print(f"[setups] comment save error: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/setup/log_with_outcomes")
def api_setup_log_with_outcomes(limit: int = Query(50)):
    """Get recent setup detection log entries with basic outcome indicators."""
    if not engine:
        return []
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, ts, setup_name, direction, grade, score,
                       paradigm, spot, lis, target, max_plus_gex, max_minus_gex,
                       gap_to_lis, upside, rr_ratio, first_hour,
                       support_score, upside_score, floor_cluster_score,
                       target_cluster_score, rr_score, notified,
                       bofa_stop_level, bofa_target_level, bofa_lis_width,
                       bofa_max_hold_minutes, lis_upper,
                       abs_vol_ratio, abs_es_price,
                       comments, outcome_result, outcome_pnl,
                       outcome_max_profit, outcome_max_loss,
                       outcome_first_event, outcome_elapsed_min
                FROM setup_log
                ORDER BY ts DESC
                LIMIT :lim
            """), {"lim": min(int(limit), 500)}).mappings().all()

        results = []
        for r in rows:
            entry = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}
            # Use stored outcome if already resolved (avoids expensive per-row DB query)
            if entry.get("outcome_result"):
                fe = entry.get("outcome_first_event")
                pnl = entry.get("outcome_pnl")
                mp = entry.get("outcome_max_profit") or 0
                # hit_10pt = price ever reached +10 from entry (independent of final outcome)
                # Derive from max_profit (most reliable), fall back to P&L for legacy rows
                hit_10 = mp >= 10 if mp else (pnl is not None and pnl >= 10)
                entry["outcome"] = {
                    "result": entry["outcome_result"],
                    "pnl": pnl,
                    "first_event": fe,
                    "max_profit": mp,
                    "max_loss": entry.get("outcome_max_loss") or 0,
                    "elapsed_min": entry.get("outcome_elapsed_min"),
                    "hit_10pt": hit_10,
                    "hit_target": entry["outcome_result"] == "WIN",
                    "hit_stop": entry["outcome_result"] == "LOSS",
                }
            else:
                # Only compute in real-time for OPEN/unresolved trades
                entry["outcome"] = _calculate_setup_outcome(dict(r))
            results.append(entry)

        return results
    except Exception as e:
        print(f"[setups] log with outcomes query error: {e}", flush=True)
        return []


@app.get("/api/setup/export")
def api_setup_export(
    start_date: str = Query(None, description="Start date YYYY-MM-DD"),
    end_date: str = Query(None, description="End date YYYY-MM-DD"),
):
    """
    Export setup log with outcomes to CSV for Excel analysis.
    Includes: date, time, direction, grade, score, SPX, LIS, target, gap, R:R,
    hit_10pt, hit_target, hit_stop, max_profit, max_loss, result (WIN/LOSS/BE).
    """
    if not engine:
        return Response("DATABASE_URL not set", status_code=500)

    try:
        # Build date filter
        where_clause = ""
        params = {}
        if start_date:
            where_clause += " AND ts >= :start_date"
            params["start_date"] = start_date
        if end_date:
            where_clause += " AND ts <= :end_date::date + interval '1 day'"
            params["end_date"] = end_date

        with engine.begin() as conn:
            rows = conn.execute(text(f"""
                SELECT id, ts, setup_name, direction, grade, score,
                       paradigm, spot, lis, target, max_plus_gex, max_minus_gex,
                       gap_to_lis, upside, rr_ratio, first_hour, notified,
                       bofa_stop_level, bofa_target_level, bofa_lis_width,
                       bofa_max_hold_minutes, lis_upper, comments,
                       abs_vol_ratio, abs_es_price
                FROM setup_log
                WHERE 1=1 {where_clause}
                ORDER BY ts DESC
                LIMIT 500
            """), params).mappings().all()

        if not rows:
            return Response("No data found", status_code=404)

        # Build CSV with outcomes
        import io
        output = io.StringIO()

        # Header (PGEX/NGEX instead of +GEX/-GEX to avoid Excel formula issues)
        headers = [
            "Date", "Time", "Direction", "Grade", "Score", "SPX", "LIS", "Target",
            "PGEX", "NGEX", "Gap", "Upside", "R:R", "First Hour", "Notified",
            "10pt Hit", "Target Hit", "Stop Hit", "Max Profit", "Max Loss",
            "10pt Level", "Stop Level", "Result", "Points P/L", "Comments"
        ]
        output.write(",".join(headers) + "\n")

        for row in rows:
            r = dict(row)
            outcome = _calculate_setup_outcome(r)

            # Determine result
            is_bofa_row = r.get("setup_name") == "BofA Scalp"
            result = ""
            points_pl = 0
            if outcome.get("first_event") == "stop":
                result = "LOSS"
                points_pl = outcome.get("max_loss", 0)
            elif outcome.get("first_event") in ("10pt", "target", "15pt"):
                result = "WIN"
                points_pl = 10
            elif outcome.get("first_event") == "timeout":
                # BofA timeout: result based on P&L at expiry
                tp = outcome.get("timeout_pnl", 0) or 0
                result = "WIN" if tp > 0 else "LOSS"
                points_pl = tp
            elif outcome.get("hit_10pt"):
                result = "WIN"
                points_pl = 15 if is_bofa_row else 10
            elif outcome.get("first_event") == "pending":
                result = "PENDING"
                points_pl = outcome.get("max_profit", 0)
            elif outcome.get("first_event") == "miss":
                result = "MISS"
                points_pl = outcome.get("max_profit", 0)
            elif outcome.get("no_data"):
                result = "NO DATA"
            else:
                result = "OPEN"
                points_pl = outcome.get("max_profit", 0)

            ts = r["ts"]
            # Convert to ET for readability
            if hasattr(ts, "astimezone"):
                ts = ts.astimezone(NY)
            date_str = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
            time_str = ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)[11:19]

            row_data = [
                date_str,
                time_str,
                r.get("direction", "").upper(),
                r.get("grade", ""),
                str(r.get("score", "")),
                f"{r.get('spot', 0):.2f}" if r.get("spot") else "",
                f"{r.get('lis', 0):.0f}" if r.get("lis") else "",
                f"{r.get('target', 0):.0f}" if r.get("target") else "",
                f"{r.get('max_plus_gex', 0):.0f}" if r.get("max_plus_gex") else "",
                f"{r.get('max_minus_gex', 0):.0f}" if r.get("max_minus_gex") else "",
                f"{r.get('gap_to_lis', 0):.1f}" if r.get("gap_to_lis") else "",
                f"{r.get('upside', 0):.1f}" if r.get("upside") else "",
                f"{r.get('rr_ratio', 0):.1f}" if r.get("rr_ratio") else "",
                "Yes" if r.get("first_hour") else "No",
                "Yes" if r.get("notified") else "No",
                "Yes" if outcome.get("hit_10pt") else "No",
                "Yes" if outcome.get("hit_target") else "No",
                "Yes" if outcome.get("hit_stop") else "No",
                f"{outcome.get('max_profit', 0):.1f}",
                f"{outcome.get('max_loss', 0):.1f}",
                f"{outcome.get('ten_pt_level', 0):.0f}" if outcome.get("ten_pt_level") else "",
                f"{outcome.get('stop_level', 0):.0f}" if outcome.get("stop_level") else "",
                result,
                f"{points_pl:.1f}",
                '"' + (r.get("comments") or "").replace('"', '""') + '"',
            ]
            output.write(",".join(row_data) + "\n")

        csv_content = output.getvalue()
        output.close()

        # Return as downloadable CSV
        filename = f"setup_alerts_{now_et().strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"[setups] export error: {e}", flush=True)
        return Response(f"Export error: {e}", status_code=500)


@app.post("/api/setup/test")
def api_setup_test():
    """Send a test alert to the setups Telegram channel."""
    chat_id = TELEGRAM_CHAT_ID_SETUPS or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return {
            "status": "error",
            "message": "Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID_SETUPS (or TELEGRAM_CHAT_ID).",
            "token_set": bool(TELEGRAM_BOT_TOKEN),
            "chat_id_set": bool(chat_id)
        }

    success = send_telegram_setups("🧪 <b>Test Setup Alert</b>\n\nYour 0DTE Alpha setup detector alerts are working!")
    if success:
        return {"status": "ok", "message": "Test setup alert sent successfully!"}
    else:
        return {"status": "error", "message": "Failed to send test alert. Check your token and chat ID."}

@app.get("/api/data_freshness")
def api_data_freshness():
    """
    Returns the last update timestamps for each data source.
    Used to verify data is flowing correctly before making trading decisions.
    """
    now_et = datetime.now(NY)
    is_open = market_open_now()

    # Extract spot from last_run_status msg (e.g. "spot=6045.5 ...")
    spot = None
    try:
        msg = last_run_status.get("msg") or ""
        parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
        spot = float(parts["spot"])
    except Exception:
        pass

    result = {
        "spot": spot,
        "vix": _vix_last,
        "ts_api": {"last_update": None, "age_seconds": None, "status": "closed"},
        "volland": {"last_update": None, "age_seconds": None, "status": "closed"},
    }

    # TS API: use in-memory last_run_status (pulled every 30s, not DB which saves every 5min)
    ts_str = last_run_status.get("ts")
    if ts_str and last_run_status.get("ok"):
        try:
            # Parse "2026-01-26 11:16 EST" format
            ts_time = datetime.strptime(ts_str.replace(" EST", "").replace(" EDT", ""), "%Y-%m-%d %H:%M")
            ts_time = NY.localize(ts_time)
            age = (now_et - ts_time).total_seconds()

            if not is_open:
                status = "closed"
            elif age < 120:
                status = "ok"
            elif age < 300:
                status = "stale"
            else:
                status = "error"

            result["ts_api"] = {
                "last_update": ts_time.isoformat(),
                "age_seconds": int(age),
                "status": status
            }
        except Exception as e:
            print(f"[data_freshness] ts parse error: {e}", flush=True)

    # Volland: latest REAL snapshot (exclude errors, require actual data)
    if engine:
        try:
            with engine.begin() as conn:
                volland_row = conn.execute(text("""
                    SELECT ts FROM volland_snapshots
                    WHERE payload->>'error_event' IS NULL
                      AND CASE WHEN payload->>'exposure_points_saved' ~ '^\d+$'
                               THEN (payload->>'exposure_points_saved')::int > 0
                               ELSE false END
                    ORDER BY ts DESC LIMIT 1
                """)).mappings().first()

                if volland_row and volland_row["ts"]:
                    v_time = volland_row["ts"]
                    if v_time.tzinfo is not None:
                        age = (datetime.now(v_time.tzinfo) - v_time).total_seconds()
                    else:
                        age = (now_et.replace(tzinfo=None) - v_time).total_seconds()

                    if not is_open:
                        status = "closed"
                    elif age < 180:
                        status = "ok"
                    elif age < 600:
                        status = "stale"
                    else:
                        status = "error"

                    result["volland"] = {
                        "last_update": v_time.isoformat() if hasattr(v_time, "isoformat") else str(v_time),
                        "age_seconds": int(age),
                        "status": status
                    }
        except Exception as e:
            print(f"[data_freshness] volland error: {e}", flush=True)

    return result

def check_pipeline_health():
    """Check data pipeline freshness and send Telegram alerts on error/recovery."""
    freshness = api_data_freshness()
    now = time.time()
    reminder_sec = _pipeline_status["reminder_minutes"] * 60
    is_open = market_open_now()

    for source, key_prefix, label in [
        ("ts_api", "ts", "TS API"),
        ("volland", "vol", "Volland"),
    ]:
        current = freshness[source]["status"]
        prev = _pipeline_status[f"{key_prefix}_status"]
        age_sec = freshness[source].get("age_seconds")
        age_min = round(age_sec / 60) if age_sec else 0

        # If market is open but freshness returned "closed" (query failed/no data),
        # treat as error — don't silently skip
        if current == "closed" and is_open:
            print(f"[pipeline] {label}: status='closed' during market hours (query issue or no data), treating as error", flush=True)
            current = "error"
            age_min = 0

        # Skip if market is actually closed
        if current == "closed":
            _pipeline_status[f"{key_prefix}_status"] = current
            continue

        # Transition to error
        if current == "error" and prev != "error":
            print(f"[pipeline] {label}: {prev} → error (age={age_min}m), sending alert", flush=True)
            _pipeline_status[f"{key_prefix}_error_since"] = now
            _pipeline_status[f"{key_prefix}_last_alert"] = now
            _pipeline_status[f"{key_prefix}_status"] = current
            send_telegram(f"\U0001f534 DATA PIPELINE ERROR: {label} data is {age_min} minutes old — not updating")
            continue

        # Still in error — send reminder
        if current == "error" and prev == "error":
            if (now - _pipeline_status[f"{key_prefix}_last_alert"]) >= reminder_sec:
                down_min = round((now - _pipeline_status[f"{key_prefix}_error_since"]) / 60)
                _pipeline_status[f"{key_prefix}_last_alert"] = now
                send_telegram(f"\U0001f534 STILL DOWN: {label} data has been stale for {down_min} minutes")
            continue

        # Recovery from error
        if prev == "error" and current in ("ok", "stale"):
            down_min = round((now - _pipeline_status[f"{key_prefix}_error_since"]) / 60)
            _pipeline_status[f"{key_prefix}_status"] = current
            send_telegram(f"\U0001f7e2 DATA RECOVERED: {label} is updating again (was down {down_min} minutes)")
            continue

        _pipeline_status[f"{key_prefix}_status"] = current

# ====== TABLE & DASHBOARD HTML TEMPLATES ======

TABLE_HTML_TEMPLATE = """
<html><head><meta charset="utf-8"><title>0DTE Alpha</title>
<style>
  body { font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
         background:#0a0a0a; color:#e5e5e5; padding:20px; }
  .last { color:#9ca3af; font-size:12px; line-height:1.25; margin:0 0 10px 0; }
  table.table { border-collapse:collapse; width:100%; font-size:12px; }
  .table th,.table td { border:1px solid #333; padding:6px 8px; text-align:right; }
  .table th { background:#111; position:sticky; top:0; z-index:1; }
  .table td:nth-child(7), .table th:nth-child(7) { background:#111; text-align:center; }
  .table td:first-child, .table th:first-child { text-align:center; }
  .table tr.atm td { background:#1a2634; }
</style>
</head><body>
  <h2>SPXW 0DTE - Live Table</h2>
  <div class="last">
    Last run: __TS__<br>exp=__EXP__<br>spot=__SPOT__<br>rows=__ROWS__
  </div>
  __BODY__
  <script>
  // Auto-refresh: update data in-place without page reload.
  // Each tab already has its own polling timer (startCharts, startEsDelta, etc.)
  // We just need to refresh the Table iframe and the footer status line.
  (function(){
    const REFRESH_MS = __PULL_MS__;
    // Refresh table iframe when Table tab is visible
    setInterval(()=>{
      const tf = document.getElementById('tableFrame');
      if(tf && tf.offsetParent !== null) tf.src = tf.src;
    }, REFRESH_MS);
    // Update footer status line
    setInterval(async ()=>{
      try {
        const r = await fetch('/api/health', {cache:'no-store'});
        const h = await r.json();
        const el = document.querySelector('.last');
        if(el && h.last){
          const l = h.last;
          el.innerHTML = 'Last run: '+(l.ts||'')+'<br>exp='+(l.msg?l.msg.match(/exp=([^ ]*)/)?.[1]||'':'')+'<br>spot='+(l.msg?l.msg.match(/spot=([^ ]*)/)?.[1]||'':'');
        }
      } catch(e){}
    }, REFRESH_MS);
  })();
  </script>
</body></html>
"""

LOGIN_HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>0DTE Alpha - Login</title>
  <link rel="icon" type="image/png" href="/favicon.png">
  <style>
    :root {
      --bg:#0b0c10; --panel:#121417; --muted:#8a8f98; --text:#e6e7e9; --border:#23262b;
      --green:#22c55e; --red:#ef4444; --blue:#60a5fa;
    }
    * { box-sizing: border-box; }
    body {
      margin:0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      font-size: 14px;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .login-box {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 32px;
      width: 100%;
      max-width: 380px;
    }
    .brand {
      font-size: 24px;
      font-weight: 700;
      text-align: center;
      margin-bottom: 8px;
    }
    .subtitle {
      color: var(--muted);
      text-align: center;
      margin-bottom: 24px;
      font-size: 13px;
    }
    .form-group {
      margin-bottom: 16px;
    }
    .form-group label {
      display: block;
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 12px;
    }
    .form-group input {
      width: 100%;
      padding: 10px 12px;
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      color: var(--text);
      font-size: 14px;
    }
    .form-group input:focus {
      outline: none;
      border-color: var(--blue);
    }
    .btn-login {
      width: 100%;
      padding: 12px;
      background: var(--green);
      border: none;
      border-radius: 8px;
      color: white;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      margin-top: 8px;
    }
    .btn-login:hover {
      opacity: 0.9;
    }
    .error {
      background: rgba(239,68,68,0.1);
      border: 1px solid var(--red);
      color: var(--red);
      padding: 10px;
      border-radius: 8px;
      margin-bottom: 16px;
      font-size: 13px;
      text-align: center;
    }
    .request-link {
      text-align: center;
      margin-top: 20px;
      padding-top: 20px;
      border-top: 1px solid var(--border);
    }
    .request-link a {
      color: var(--blue);
      text-decoration: none;
      font-size: 13px;
    }
    .request-link a:hover {
      text-decoration: underline;
    }
  </style>
</head>
<body>
  <div class="login-box">
    <div class="brand">0DTE Alpha</div>
    <div class="subtitle">Real-time SPX Options Dashboard</div>
    __ERROR__
    <form method="POST" action="/login">
      <div class="form-group">
        <label>Email</label>
        <input type="email" name="email" required autofocus placeholder="your@email.com">
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" name="password" required placeholder="Enter password">
      </div>
      <button type="submit" class="btn-login">Sign In</button>
    </form>
    <div class="request-link">
      <a href="/request-access">Request Access</a>
    </div>
  </div>
</body>
</html>
"""

REQUEST_ACCESS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>0DTE Alpha - Request Access</title>
  <link rel="icon" type="image/png" href="/favicon.png">
  <style>
    :root {
      --bg:#0b0c10; --panel:#121417; --muted:#8a8f98; --text:#e6e7e9; --border:#23262b;
      --green:#22c55e; --red:#ef4444; --blue:#60a5fa;
    }
    * { box-sizing: border-box; }
    body {
      margin:0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      font-size: 14px;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .request-box {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 32px;
      width: 100%;
      max-width: 420px;
    }
    .brand {
      font-size: 24px;
      font-weight: 700;
      text-align: center;
      margin-bottom: 8px;
    }
    .subtitle {
      color: var(--muted);
      text-align: center;
      margin-bottom: 24px;
      font-size: 13px;
    }
    .form-group {
      margin-bottom: 16px;
    }
    .form-group label {
      display: block;
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 12px;
    }
    .form-group input, .form-group textarea {
      width: 100%;
      padding: 10px 12px;
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      color: var(--text);
      font-size: 14px;
      font-family: inherit;
    }
    .form-group textarea {
      min-height: 100px;
      resize: vertical;
    }
    .form-group input:focus, .form-group textarea:focus {
      outline: none;
      border-color: var(--blue);
    }
    .btn-submit {
      width: 100%;
      padding: 12px;
      background: var(--blue);
      border: none;
      border-radius: 8px;
      color: white;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      margin-top: 8px;
    }
    .btn-submit:hover {
      opacity: 0.9;
    }
    .success {
      background: rgba(34,197,94,0.1);
      border: 1px solid var(--green);
      color: var(--green);
      padding: 14px;
      border-radius: 8px;
      margin-bottom: 16px;
      font-size: 13px;
      text-align: center;
    }
    .back-link {
      text-align: center;
      margin-top: 20px;
      padding-top: 20px;
      border-top: 1px solid var(--border);
    }
    .back-link a {
      color: var(--blue);
      text-decoration: none;
      font-size: 13px;
    }
    .back-link a:hover {
      text-decoration: underline;
    }
  </style>
</head>
<body>
  <div class="request-box">
    <div class="brand">Request Access</div>
    <div class="subtitle">Send a message to request access to 0DTE Alpha</div>
    __MESSAGE__
    <form method="POST" action="/request-access">
      <div class="form-group">
        <label>Your Email</label>
        <input type="email" name="email" required placeholder="your@email.com">
      </div>
      <div class="form-group">
        <label>Subject</label>
        <input type="text" name="subject" required placeholder="Access request">
      </div>
      <div class="form-group">
        <label>Message</label>
        <textarea name="message" required placeholder="Tell us why you'd like access..."></textarea>
      </div>
      <button type="submit" class="btn-submit">Send Request</button>
    </form>
    <div class="back-link">
      <a href="/">Back to Login</a>
    </div>
  </div>
</body>
</html>
"""

DASH_HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>SPXW 0DTE - Dashboard</title>
  <link rel="icon" type="image/png" href="/favicon.png">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg:#0b0c10; --panel:#121417; --muted:#8a8f98; --text:#e6e7e9; --border:#23262b;
      --green:#22c55e; --red:#ef4444; --blue:#60a5fa;
    }
    * { box-sizing: border-box; }
    body {
      margin:0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      font-size: 13px;
    }

    .layout {
      display: grid;
      grid-template-columns: 240px 1fr;
      min-height: 100vh;
    }
    .sidebar {
      background: var(--panel);
      border-right: 1px solid var(--border);
      padding: 18px 14px;
      position: sticky;
      top:0;
      height:100vh;
      display: flex;
      flex-direction: column;
    }
    .brand { font-weight: 700; margin-bottom: 4px; font-size:14px; }
    .small { color: var(--muted); font-size: 11px; margin-bottom: 12px; }
    .status {
      display:flex;
      gap:8px;
      align-items:center;
      padding:8px;
      border:1px solid var(--border);
      border-radius:10px;
      background:#0f1216;
      margin-bottom:12px;
    }
    .dot { width:9px; height:9px; border-radius:999px; background:__STATUS_COLOR__; }
    .nav { display: grid; gap: 6px; margin-top: 6px; }
    .btn {
      display:block;
      width:100%;
      text-align:left;
      padding:8px 10px;
      border-radius:9px;
      border:1px solid var(--border);
      background:transparent;
      color:var(--text);
      cursor:pointer;
      font-size:12px;
    }
    .btn.active { background:#121a2e; border-color:#2a3a57; }

    .content { padding: 14px 16px; }
    .panel {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      overflow:hidden;
    }
    .header {
      display:flex;
      align-items:center;
      justify-content:space-between;
      padding:4px 6px 8px;
      border-bottom:1px solid var(--border);
      margin-bottom:8px;
      font-size:13px;
    }
    .pill {
      font-size:11px;
      padding:3px 7px;
      border:1px solid var(--border);
      border-radius:999px;
      color:var(--muted);
    }

    .charts { display:flex; flex-direction:column; gap:18px; }
    .charts-grid {
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:8px;
    }
    .charts-grid > div { width:100%; height:380px; }
    iframe {
      width:100%;
      height: calc(100vh - 190px);
      border:0;
      background:#0f1115;
    }
    #volChart, #oiChart, #gexChart, #vannaChart, #deltaDecayChart { width:100%; height:420px; }
    #gexNetChart, #gexCallPutChart, #vannaOdteChart, #gammaOdteChart { width:100%; height:380px; }

    .ht-grid {
      display:grid;
      grid-template-columns:1fr 1fr 1fr;
      gap:8px;
    }
    .ht-grid > div { width:100%; height:380px; }

    .unified-grid {
      display:grid;
      grid-template-columns: 2fr 1fr 1fr 1fr 1fr;
      gap:8px;
      align-items:stretch;
      height: calc(100vh - 140px);
      min-height:500px;
    }
    .unified-card {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:10px;
      padding:8px;
      display:flex;
      flex-direction:column;
      overflow:hidden;
    }
    .unified-card h3 {
      margin:0 0 4px;
      font-size:11px;
      color:var(--muted);
      font-weight:600;
      text-transform:uppercase;
      letter-spacing:0.5px;
    }
    .unified-plot {
      flex:1;
      width:100%;
      min-height:0;
    }
    .spot-grid {
      display:grid;
      grid-template-columns: 2fr 1fr 1fr;
      gap:10px;
      align-items:stretch;
    }
    .card {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      min-height:360px;
      display:flex;
      flex-direction:column;
    }
    .card h3 {
      margin:0 0 6px;
      font-size:13px;
      color:var(--muted);
      font-weight:600;
    }
    .plot { width:100%; height:100% }

    @media (max-width: 900px) {
      .layout { display:block; min-height:0; }
      .sidebar {
        position:static;
        height:auto;
        border-right:none;
        border-bottom:1px solid var(--border);
        padding:10px 10px 6px;
      }
      .status { margin-bottom:8px; }
      .nav {
        grid-auto-flow:column;
        grid-auto-columns:1fr;
        overflow-x:auto;
      }
      .btn { text-align:center; padding:7px 5px; font-size:11px; white-space:nowrap; }
      .content { padding:10px; }
      .panel { padding:8px; border-radius:10px; }
      iframe { height:60vh; }
      #volChart, #oiChart, #gexChart, #vannaChart, #deltaDecayChart { height:340px; }
      .charts-grid { grid-template-columns:1fr; }
      .charts-grid > div { height:320px; }
      .ht-grid { grid-template-columns:1fr; }
      .ht-grid > div { height:320px; }
      .spot-grid { grid-template-columns:1fr; }
      .card { min-height:260px; }
      .unified-grid { grid-template-columns:1fr; height:auto; }
      .unified-card { min-height:300px; }
    }
    .strike-btn {
      padding:3px 8px;
      font-size:11px;
      border:1px solid var(--border);
      border-radius:6px;
      background:transparent;
      color:var(--muted);
      cursor:pointer;
    }
    .strike-btn:hover { border-color:#444; color:var(--text); }
    .strike-btn.active { background:#1a2634; border-color:#2a3a57; color:var(--text); }

    /* Sub-tabs (shared style for Charts, Historical, Trade Log) */
    .subtabs { display:flex; gap:4px; padding:8px 12px; border-bottom:1px solid var(--border); }
    .subtab-btn { padding:4px 12px; font-size:11px; font-weight:600; border:1px solid var(--border); border-radius:14px; background:transparent; color:var(--muted); cursor:pointer; transition:all .15s; }
    .subtab-btn:hover { border-color:#444; color:var(--text); }
    .subtab-btn.active { background:#1a2634; border-color:#3b82f6; color:#3b82f6; }
    .tl-filters { display:flex; gap:8px; padding:12px; border-bottom:1px solid var(--border); flex-wrap:wrap; align-items:center; }
    .tl-filters select, .tl-filters input { background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:4px 8px; font-size:11px; }
    .tl-stats { display:flex; gap:16px; padding:8px 12px; border-bottom:1px solid var(--border); font-size:12px; color:var(--muted); }
    .tl-stats .stat-val { font-weight:700; color:var(--text); }
    .tl-header { display:grid; grid-template-columns:32px 100px 32px 48px 40px 64px 72px 72px 56px 56px 44px 100px 36px; align-items:center; gap:4px; padding:6px 8px; border-bottom:2px solid var(--border); color:var(--muted); font-size:10px; font-weight:600; position:sticky; top:0; background:var(--card); z-index:1; }
    .tl-row { display:grid; grid-template-columns:32px 100px 32px 48px 40px 64px 72px 72px 56px 56px 44px 100px 36px; align-items:center; gap:4px; padding:6px 8px; border-bottom:1px solid var(--border); cursor:pointer; }
    .tl-row:hover { background:#1a1d21; }
    .tl-notes { padding:8px 12px 8px 44px; border-bottom:1px solid var(--border); display:none; }
    .tl-notes textarea { width:100%; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:4px; padding:6px; font-size:12px; resize:vertical; min-height:60px; box-sizing:border-box; }
    .tl-notes .tl-save-btn { margin-top:4px; padding:3px 10px; font-size:11px; background:#3b82f6; color:white; border:none; border-radius:4px; cursor:pointer; }
    .tl-notes .tl-save-btn:hover { background:#2563eb; }
    .setup-pill { font-size:10px; font-weight:600; padding:2px 6px; border-radius:3px; white-space:nowrap; display:inline-block; }
    /* TS SIM Log grid: #, Setup, Dir, Grade, Time, MES Entry, MES Stop, T1, T2, Result, P&L($), Dur, Status */
    .tl-header.tl-grid-sim, .tl-row.tl-grid-sim { grid-template-columns:32px 100px 32px 48px 72px 72px 72px 40px 40px 56px 64px 44px 64px; }
    /* Eval Log grid: #, Setup, Dir, Grade, Time, Qty, Entry, Stop, Result, P&L($), Dur, Status */
    .tl-header.tl-grid-eval, .tl-row.tl-grid-eval { grid-template-columns:32px 100px 32px 48px 72px 36px 72px 56px 56px 64px 44px 64px; }

    /* Playback View */
    .playback-container { display:flex; flex-direction:column; }
    .playback-info { padding:8px 0; display:flex; align-items:center; }
    .playback-detail-grid {
      display:grid;
      grid-template-columns: 1fr 1fr 1fr 1fr;
      gap:8px;
      height:320px;
      min-height:280px;
      flex-shrink:0;
    }
    .playback-card {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:10px;
      padding:8px;
      display:flex;
      flex-direction:column;
      overflow:hidden;
    }
    .playback-card h3 {
      margin:0 0 4px;
      font-size:11px;
      color:var(--muted);
      font-weight:600;
      text-transform:uppercase;
      letter-spacing:0.5px;
    }
    .playback-plot { flex:1; width:100%; min-height:0; }
    .playback-slider-container {
      padding:12px 8px;
      background:#0f1115;
      border-radius:8px;
      margin-top:8px;
    }
    #playbackSlider {
      -webkit-appearance:none;
      appearance:none;
      height:12px;
      background:#1a1d23;
      border-radius:6px;
      outline:none;
    }
    #playbackSlider::-webkit-slider-thumb {
      -webkit-appearance:none;
      appearance:none;
      width:24px;
      height:24px;
      background:#60a5fa;
      border-radius:50%;
      cursor:pointer;
      box-shadow:0 0 6px rgba(96,165,250,0.4);
    }
    #playbackSlider::-moz-range-thumb {
      width:24px;
      height:24px;
      background:#60a5fa;
      border-radius:50%;
      cursor:pointer;
      border:none;
      box-shadow:0 0 6px rgba(96,165,250,0.4);
    }
    @media (max-width: 900px) {
      .playback-detail-grid { grid-template-columns:1fr; height:auto; }
    }

    .stats-box { margin-top:14px; padding:10px; border:1px solid var(--border); border-radius:10px; background:#0f1216; }
    .stats-box h4 { margin:0 0 8px; font-size:12px; font-weight:600; }
    .stats-row { display:flex; justify-content:space-between; font-size:11px; padding:4px 0; border-bottom:1px solid var(--border); }
    .stats-row:last-child { border-bottom:none; }
    .stats-label { color:var(--muted); }
    .stats-value { color:var(--text); font-weight:500; text-align:right; }
    .stats-value.green { color:var(--green); }
    .stats-value.red { color:var(--red); }

    /* Toggle Switch */
    .toggle-switch { position:relative; display:inline-block; width:32px; height:18px; }
    .toggle-switch input { opacity:0; width:0; height:0; }
    .toggle-slider { position:absolute; cursor:pointer; top:0; left:0; right:0; bottom:0; background:#333; border-radius:18px; transition:.2s; }
    .toggle-slider:before { position:absolute; content:""; height:14px; width:14px; left:2px; bottom:2px; background:#666; border-radius:50%; transition:.2s; }
    .toggle-switch input:checked + .toggle-slider { background:#22c55e; }
    .toggle-switch input:checked + .toggle-slider:before { transform:translateX(14px); background:#fff; }

    /* Modal */
    .modal { position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.7); z-index:1000; display:flex; align-items:center; justify-content:center; }
    .modal-content { background:#1a1d21; border:1px solid var(--border); border-radius:12px; width:90%; max-width:500px; max-height:90vh; overflow:auto; }
    .modal-header { display:flex; justify-content:space-between; align-items:center; padding:16px 20px; border-bottom:1px solid var(--border); }
    .modal-header h3 { margin:0; font-size:16px; }
    .modal-close { background:none; border:none; color:var(--muted); font-size:24px; cursor:pointer; padding:0; line-height:1; }
    .modal-close:hover { color:var(--text); }
    .modal-body { padding:20px; font-size:13px; }
    .modal-footer { display:flex; justify-content:space-between; align-items:center; padding:16px 20px; border-top:1px solid var(--border); }
    /* Settings Tabs */
    .settings-tab { background:none; border:none; color:var(--muted); padding:12px 16px; cursor:pointer; font-size:13px; border-bottom:2px solid transparent; margin-bottom:-1px; }
    .settings-tab:hover { color:var(--text); }
    .settings-tab.active { color:var(--text); border-bottom-color:var(--blue); }
    .settings-panel { display:none !important; }
    .settings-panel.active { display:block !important; }
    .user-row, .message-row { display:flex; justify-content:space-between; align-items:center; padding:10px 12px; background:var(--bg); border-radius:6px; margin-bottom:8px; }
    .user-row .email { font-size:13px; }
    .user-row .badge { font-size:10px; background:var(--blue); padding:2px 6px; border-radius:4px; margin-left:8px; }
    .message-row { flex-direction:column; align-items:flex-start; }
    .message-row .msg-header { display:flex; justify-content:space-between; width:100%; margin-bottom:6px; }
    .message-row .msg-subject { font-weight:600; }
    .message-row .msg-email { color:var(--muted); font-size:11px; }
    .message-row .msg-body { font-size:12px; color:var(--muted); margin-bottom:8px; }
    .message-row .msg-date { font-size:10px; color:var(--muted); }
    .message-row.unread { border-left:3px solid var(--blue); }
    .delete-btn { background:none; border:none; color:var(--red); cursor:pointer; font-size:11px; padding:4px 8px; }
    .delete-btn:hover { text-decoration:underline; }
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">SPXW 0DTE</div>
      <div class="small">Live chain + charts</div>
      <div class="status">
        <span class="dot"></span>
        <div>
          <div style="font-weight:600; font-size:12px;">__STATUS_TEXT__</div>
          <div class="small">Last run: __LAST_TS__</div>
          <div class="small" id="dataFreshness" style="margin-top:4px">Loading...</div>
        </div>
      </div>
      <div class="nav">
        <button class="btn active" id="tabTable">Table</button>
        <button class="btn" id="tabSpot">Spot</button>
        <button class="btn" id="tabCharts">Charts</button>
        <button class="btn" id="tabEsDelta">ES Delta</button>
        <button class="btn" id="tabHistorical">Historical</button>
        <button class="btn" id="tabTradeLog">Trade Log</button>
      </div>
      <div class="small" style="margin-top:10px">Charts auto-refresh while visible.</div>
      <div class="stats-box">
        <h4>SPX Statistics</h4>
        <div id="statsContent" style="color:var(--muted);font-size:11px">Loading...</div>
      </div>
      <div style="margin-top:14px">
        <button id="alertSettingsBtn" class="strike-btn" style="padding:5px 12px;font-size:11px;width:100%">Settings</button>
      </div>
      <div style="margin-top:auto;padding-top:16px;border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center">
        <span style="font-size:11px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px" title="__USER_EMAIL__">__USER_EMAIL__</span>
        <a href="/logout" style="font-size:11px;color:var(--muted);text-decoration:none">Sign out</a>
      </div>
    </aside>

    <!-- Settings Modal -->
    <div id="alertModal" class="modal" style="display:none">
      <div class="modal-content" style="max-width:600px">
        <div class="modal-header">
          <h3>Settings</h3>
          <button id="alertModalClose" class="modal-close">&times;</button>
        </div>
        <!-- Tabs -->
        <div style="display:flex;border-bottom:1px solid var(--border);padding:0 16px">
          <button class="settings-tab active" data-tab="alerts">Alerts</button>
          <button class="settings-tab" data-tab="users">Users</button>
          <button class="settings-tab" data-tab="messages">Messages</button>
          <button class="settings-tab" data-tab="setups">Trading Setups</button>
          <button class="settings-tab" data-tab="autotrade">Auto-trade</button>
        </div>
        <!-- Alerts Tab -->
        <div class="settings-panel" id="tabPanelAlerts">
          <div class="modal-body">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid var(--border)">
              <label class="toggle-switch">
                <input type="checkbox" id="alertMasterToggle" checked>
                <span class="toggle-slider"></span>
              </label>
              <span style="font-weight:600">Alerts Enabled</span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
              <div>
                <div style="color:var(--muted);margin-bottom:8px;font-weight:600">Price Alerts</div>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alertLIS" checked> LIS (Lines in Sand)</label>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alertTarget" checked> Target</label>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alertPosGamma" checked> Max +Gamma</label>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alertNegGamma" checked> Max -Gamma</label>
              </div>
              <div>
                <div style="color:var(--muted);margin-bottom:8px;font-weight:600">Other Alerts</div>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alertParadigm" checked> Paradigm Change</label>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alertVolSpike" checked> Volume Spike</label>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alert10am" checked> 10 AM Summary</label>
                <label style="display:flex;align-items:center;gap:6px;margin:6px 0"><input type="checkbox" id="alert2pm" checked> 2 PM Summary</label>
              </div>
            </div>
            <hr style="border:none;border-top:1px solid var(--border);margin:16px 0">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
              <div>
                <div style="color:var(--muted);margin-bottom:8px;font-weight:600">Thresholds</div>
                <div style="display:flex;align-items:center;gap:6px;margin:8px 0">
                  <span>Distance:</span>
                  <input type="number" id="alertThresholdPts" value="5" min="1" max="20" style="width:50px;background:#1a1d21;border:1px solid var(--border);border-radius:4px;color:var(--text);padding:4px 6px">
                  <span>points</span>
                </div>
                <div style="display:flex;align-items:center;gap:6px;margin:8px 0">
                  <span>Vol Spike:</span>
                  <input type="number" id="alertThresholdVol" value="500" min="100" max="5000" step="100" style="width:60px;background:#1a1d21;border:1px solid var(--border);border-radius:4px;color:var(--text);padding:4px 6px">
                  <span>contracts</span>
                </div>
              </div>
              <div>
                <div style="color:var(--muted);margin-bottom:8px;font-weight:600">Cooldown</div>
                <label style="display:flex;align-items:center;gap:6px;margin:8px 0">
                  <input type="checkbox" id="alertCooldown" checked>
                  <span>Enable cooldown:</span>
                  <input type="number" id="alertCooldownMin" value="10" min="1" max="60" style="width:50px;background:#1a1d21;border:1px solid var(--border);border-radius:4px;color:var(--text);padding:4px 6px">
                  <span>min</span>
                </label>
              </div>
            </div>
          </div>
          <div class="modal-footer">
            <div id="alertStatus" style="color:var(--muted);font-size:11px"></div>
            <div style="display:flex;gap:8px">
              <button id="alertTestBtn" class="strike-btn" style="padding:6px 16px">Test Alert</button>
              <button id="alertSaveBtn" class="strike-btn" style="padding:6px 16px;background:#22c55e;border-color:#22c55e">Save</button>
            </div>
          </div>
        </div>
        <!-- Users Tab (Admin Only) -->
        <div class="settings-panel" id="tabPanelUsers">
          <div class="modal-body">
            <div style="margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid var(--border)">
              <div style="color:var(--muted);margin-bottom:12px;font-weight:600">Add New User</div>
              <div style="display:flex;gap:8px;align-items:flex-end">
                <div style="flex:1">
                  <label style="display:block;font-size:11px;color:var(--muted);margin-bottom:4px">Email</label>
                  <input type="email" id="newUserEmail" placeholder="user@email.com" style="width:100%;padding:8px;background:#1a1d21;border:1px solid var(--border);border-radius:4px;color:var(--text)">
                </div>
                <div style="flex:1">
                  <label style="display:block;font-size:11px;color:var(--muted);margin-bottom:4px">Password</label>
                  <input type="text" id="newUserPassword" placeholder="password" style="width:100%;padding:8px;background:#1a1d21;border:1px solid var(--border);border-radius:4px;color:var(--text)">
                </div>
                <button id="addUserBtn" class="strike-btn" style="padding:8px 16px;background:#22c55e;border-color:#22c55e">Add</button>
              </div>
            </div>
            <div style="color:var(--muted);margin-bottom:8px;font-weight:600">Current Users</div>
            <div id="usersList" style="max-height:200px;overflow-y:auto"></div>
          </div>
        </div>
        <!-- Messages Tab (Admin Only) -->
        <div class="settings-panel" id="tabPanelMessages">
          <div class="modal-body">
            <div style="color:var(--muted);margin-bottom:12px;font-weight:600">Access Requests</div>
            <div id="messagesList" style="max-height:300px;overflow-y:auto"></div>
          </div>
        </div>
        <!-- Trading Setups Tab -->
        <div class="settings-panel" id="tabPanelSetups">
          <div class="modal-body">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
              <div style="font-weight:600;color:var(--muted)">Setup Detection</div>
              <div style="display:flex;gap:14px">
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
                  <input type="checkbox" id="setupGexLongEnabled" style="width:16px;height:16px">
                  <span style="font-size:12px">GEX Long</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
                  <input type="checkbox" id="setupAgShortEnabled" style="width:16px;height:16px">
                  <span style="font-size:12px">AG Short</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
                  <input type="checkbox" id="setupBofaScalpEnabled" style="width:16px;height:16px">
                  <span style="font-size:12px">BofA Scalp</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
                  <input type="checkbox" id="setupAbsorptionEnabled" style="width:16px;height:16px">
                  <span style="font-size:12px">ES Absorption</span>
                </label>
              </div>
            </div>

            <div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:10px;margin-bottom:14px;font-size:11px;color:var(--muted)">
              <div style="font-weight:600;margin-bottom:4px">GEX Long — Base Conditions:</div>
              <div>Paradigm contains "GEX" &bull; Spot &ge; LIS &bull; Target-Spot &ge; 10 &bull; +GEX-Spot &ge; 10 &bull; Gap (Spot-LIS) &le; 20</div>
              <div style="font-weight:600;margin-bottom:4px;margin-top:8px">AG Short — Base Conditions:</div>
              <div>Paradigm contains "AG" &bull; Spot &lt; LIS &bull; Spot-Target &ge; 10 &bull; Spot-(-GEX) &ge; 10 &bull; Gap (LIS-Spot) &le; 20</div>
              <div style="font-weight:600;margin-bottom:4px;margin-top:8px">BofA Scalp — Base Conditions:</div>
              <div>Paradigm = BofA (not MISSY) &bull; 10:00-15:30 ET &bull; Spot within 3pts of LIS &bull; LIS width &ge; 15 &bull; LIS stable 30min</div>
              <div style="font-weight:600;margin-bottom:4px;margin-top:8px">ES Absorption — Base Conditions:</div>
              <div>Volume spike &ge; 1.5x avg &bull; Price vs CVD divergence over lookback &bull; Volland confluence (DD, paradigm, LIS)</div>
            </div>

            <div style="font-weight:600;color:var(--muted);margin-bottom:8px;font-size:12px">GEX/AG Scoring Weights (0-100, weighted average)</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px">
              <label style="font-size:11px">Support Proximity
                <input type="number" id="setupWeightSupport" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Upside Room
                <input type="number" id="setupWeightUpside" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Floor Clustering
                <input type="number" id="setupWeightFloorCluster" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Target Clustering
                <input type="number" id="setupWeightTargetCluster" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Risk / Reward
                <input type="number" id="setupWeightRR" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>

            <div style="font-weight:600;color:var(--muted);margin-bottom:8px;font-size:12px">Grade Thresholds (minimum composite score)</div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:14px">
              <label style="font-size:11px">A+
                <input type="number" id="setupGradeAPlus" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">A
                <input type="number" id="setupGradeA" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">A-Entry
                <input type="number" id="setupGradeAEntry" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>

            <div style="font-weight:600;color:var(--muted);margin-bottom:8px;font-size:12px">BofA Scalp Weights (0-100)</div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:14px">
              <label style="font-size:11px">Stability
                <input type="number" id="bofaWeightStability" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Width
                <input type="number" id="bofaWeightWidth" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Charm
                <input type="number" id="bofaWeightCharm" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Time of Day
                <input type="number" id="bofaWeightTime" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Midpoint
                <input type="number" id="bofaWeightMidpoint" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>
            <div style="font-weight:600;color:var(--muted);margin-bottom:8px;font-size:12px">BofA Scalp Parameters</div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;margin-bottom:14px">
              <label style="font-size:11px">Stop (pts)
                <input type="number" id="bofaStopDistance" min="1" max="50" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Target (pts)
                <input type="number" id="bofaTargetDistance" min="1" max="50" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Max Hold (min)
                <input type="number" id="bofaMaxHold" min="5" max="120" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Cooldown (min)
                <input type="number" id="bofaCooldown" min="5" max="120" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>

            <div style="font-weight:600;color:var(--muted);margin-bottom:8px;font-size:12px">ES Absorption Weights (0-100)</div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:14px">
              <label style="font-size:11px">Divergence
                <input type="number" id="absWeightDivergence" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Volume
                <input type="number" id="absWeightVolume" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">DD Hedging
                <input type="number" id="absWeightDD" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Paradigm
                <input type="number" id="absWeightParadigm" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">LIS Proximity
                <input type="number" id="absWeightLIS" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">LIS Side
                <input type="number" id="absWeightLISSide" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Target Dir
                <input type="number" id="absWeightTargetDir" min="0" max="100" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>
            <div style="font-weight:600;color:var(--muted);margin-bottom:8px;font-size:12px">ES Absorption Parameters</div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:8px">
              <label style="font-size:11px">Pivot Left
                <input type="number" id="absPivotLeft" min="1" max="5" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Pivot Right
                <input type="number" id="absPivotRight" min="1" max="5" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Min Vol Ratio
                <input type="number" id="absMinVolRatio" min="1" max="5" step="0.1" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;margin-bottom:14px">
              <label style="font-size:11px">CVD Z Min
                <input type="number" id="absCvdZMin" min="0.1" max="5" step="0.1" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">CVD Std Window
                <input type="number" id="absCvdStdWindow" min="5" max="50" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Vol Window
                <input type="number" id="absVolWindow" min="5" max="50" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
              <label style="font-size:11px">Cooldown (bars)
                <input type="number" id="absCooldownBars" min="1" max="50" style="width:100%;padding:4px 6px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--fg);margin-top:2px">
              </label>
            </div>

            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
              <div style="font-weight:600;color:var(--muted);font-size:12px">Recent Detections <span style="font-weight:400;font-size:10px">(click for details)</span></div>
              <button id="btnExportSetups" style="padding:3px 8px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--muted);cursor:pointer;font-size:10px">📥 Export CSV</button>
            </div>
            <div id="setupLogList" style="max-height:280px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;padding:6px;margin-bottom:14px;font-size:10px;background:var(--surface)">
              <div style="color:var(--muted);text-align:center;padding:12px">No detections yet</div>
            </div>

            <div style="display:flex;align-items:center;gap:8px;justify-content:flex-end">
              <span id="setupStatus" style="font-size:11px;color:var(--muted)"></span>
              <button id="btnTestSetup" style="padding:6px 12px;background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--fg);cursor:pointer;font-size:12px">Test Alert</button>
              <button id="btnSaveSetups" style="padding:6px 12px;background:var(--accent);border:none;border-radius:6px;color:var(--bg);cursor:pointer;font-weight:600;font-size:12px">Save</button>
            </div>
          </div>
        </div>
        <!-- Auto-trade Tab -->
        <div class="settings-panel" id="tabPanelAutotrade">
          <div class="modal-body">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">
              <div style="font-weight:600;font-size:14px">MES Auto-Trade (SIM)</div>
              <div id="autoTradeStatus" style="font-size:11px;padding:3px 10px;border-radius:4px;background:var(--surface);color:var(--muted)">Loading...</div>
            </div>
            <div style="border:1px solid var(--border);border-radius:6px;padding:12px;margin-bottom:14px;background:var(--surface)">
              <div style="font-size:11px;color:var(--muted);margin-bottom:8px">10 MES contracts per trade | T1: 5 @ +10pts | T2: 5 @ full target</div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
                  <input type="checkbox" id="atGexLong" style="width:16px;height:16px"> GEX Long
                  <span style="font-size:9px;color:var(--muted)">(split)</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
                  <input type="checkbox" id="atAgShort" style="width:16px;height:16px"> AG Short
                  <span style="font-size:9px;color:var(--muted)">(split)</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
                  <input type="checkbox" id="atBofaScalp" style="width:16px;height:16px"> BofA Scalp
                  <span style="font-size:9px;color:var(--muted)">(10 @ +10)</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
                  <input type="checkbox" id="atAbsorption" style="width:16px;height:16px"> ES Absorption
                  <span style="font-size:9px;color:var(--muted)">(10 @ +10)</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
                  <input type="checkbox" id="atParadigm" style="width:16px;height:16px"> Paradigm
                  <span style="font-size:9px;color:var(--muted)">(10 @ +10)</span>
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
                  <input type="checkbox" id="atDDExhaust" style="width:16px;height:16px"> DD Exhaust
                  <span style="font-size:9px;color:var(--muted)">(trail)</span>
                </label>
              </div>
            </div>
            <div style="font-weight:600;font-size:12px;color:var(--muted);margin-bottom:6px">Active Orders</div>
            <div id="autoTradeOrders" style="min-height:40px;border:1px solid var(--border);border-radius:6px;padding:8px;background:var(--surface);font-size:11px;color:var(--muted)">No active orders</div>
          </div>
        </div>
      </div>
    </div>

    <!-- Setup Detail Modal -->
    <div id="setupDetailModal" class="modal" style="display:none">
      <div class="modal-content" style="max-width:900px;max-height:95vh">
        <div class="modal-header">
          <h3 id="setupDetailTitle">Setup Details</h3>
          <button id="setupDetailClose" class="modal-close">&times;</button>
        </div>
        <div class="modal-body" style="padding:12px">
          <!-- Info Row -->
          <div id="setupDetailInfo" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:12px;font-size:11px">
          </div>
          <!-- Outcome Row -->
          <div id="setupDetailOutcome" style="display:flex;gap:16px;margin-bottom:12px;padding:10px;background:#0f1115;border-radius:8px;font-size:12px">
          </div>
          <!-- Chart -->
          <div id="setupDetailChart" style="height:350px;background:#0f1115;border-radius:8px"></div>
          <!-- Stats Row -->
          <div id="setupDetailStats" style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px;font-size:11px">
          </div>
          <!-- Comments -->
          <div style="margin-top:12px">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
              <span style="color:var(--muted);font-size:11px;font-weight:600">Notes / Remarks</span>
              <button id="setupDetailSaveComment" style="font-size:10px;padding:2px 10px;background:#3b82f6;color:#fff;border:none;border-radius:4px;cursor:pointer;display:none">Save</button>
              <span id="setupDetailCommentStatus" style="font-size:10px;color:#22c55e;display:none">Saved</span>
            </div>
            <textarea id="setupDetailComments" placeholder="Add your notes about this trade setup..." style="width:100%;min-height:60px;background:#1a1d21;color:var(--text);border:1px solid var(--border);border-radius:6px;padding:8px;font-size:12px;font-family:inherit;resize:vertical;box-sizing:border-box"></textarea>
          </div>
        </div>
      </div>
    </div>

    <main class="content">
      <div id="viewTable" class="panel">
        <div class="header">
          <div><strong>Live Chain Table</strong></div>
          <div class="pill">auto-refresh</div>
        </div>
        <iframe id="tableFrame" src="/table"></iframe>
      </div>

      <div id="viewCharts" class="panel" style="display:none">
        <div class="subtabs" id="chartsSubtabs">
          <button class="subtab-btn active" data-subtab="0dte">0DTE Charts</button>
          <button class="subtab-btn" data-subtab="htf">HTF Charts</button>
        </div>
        <div id="viewCharts0dte">
          <div class="header">
            <div><strong>0DTE Charts: GEX, Greeks, Volume &amp; OI</strong></div>
            <div class="pill">spot line = dotted</div>
          </div>
          <div class="charts-grid">
            <div id="gexNetChart"></div>
            <div id="gexCallPutChart"></div>
            <div id="vannaChart"></div>
            <div id="vannaOdteChart"></div>
            <div id="deltaDecayChart"></div>
            <div id="gammaOdteChart"></div>
            <div id="oiChart"></div>
            <div id="volChart"></div>
          </div>
        </div>
        <div id="viewChartsHTF" style="display:none">
          <div class="header">
            <div><strong>Volland High-Tenor Vanna &amp; Gamma</strong></div>
            <div class="pill">spot line = dotted</div>
          </div>
          <div class="ht-grid">
            <div id="weeklyVannaChart"></div>
            <div id="monthlyVannaChart"></div>
            <div id="allVannaChart"></div>
            <div id="weeklyGammaChart"></div>
            <div id="monthlyGammaChart"></div>
            <div id="allGammaChart"></div>
          </div>
        </div>
      </div>

      <div id="viewSpot" class="panel" style="display:none">
        <div class="header">
          <div><strong>Exposure View</strong></div>
          <div style="display:flex;gap:6px;align-items:center">
            <span class="pill">Strikes:</span>
            <button class="strike-btn" data-strikes="20">20</button>
            <button class="strike-btn active" data-strikes="30">30</button>
            <button class="strike-btn" data-strikes="40">40</button>
          </div>
        </div>
        <div style="padding:12px">
          <div id="statisticsPlot" style="width:100%;height:500px"></div>
          <div id="statisticsLegend" style="margin-top:10px;font-size:11px;color:var(--muted);display:flex;gap:20px;flex-wrap:wrap">
            <span><span style="color:#3b82f6">■</span> Target</span>
            <span><span style="color:#f59e0b">■</span> LIS Low</span>
            <span><span style="color:#f59e0b">■</span> LIS High</span>
            <span><span style="color:#22c55e">■</span> Max +Gamma</span>
            <span><span style="color:#ef4444">■</span> Max -Gamma</span>
          </div>
        </div>
        <div class="unified-grid">
          <div class="unified-card">
            <h3>SPX 3m</h3>
            <div id="unifiedSpxPlot" class="unified-plot"></div>
          </div>
          <div class="unified-card">
            <h3>Net GEX</h3>
            <div id="unifiedGexPlot" class="unified-plot"></div>
          </div>
          <div class="unified-card">
            <h3>Charm</h3>
            <div id="unifiedCharmPlot" class="unified-plot"></div>
          </div>
          <div class="unified-card">
            <h3>Delta Decay</h3>
            <div id="unifiedDeltaDecayPlot" class="unified-plot"></div>
          </div>
          <div class="unified-card">
            <h3>Volume</h3>
            <div id="unifiedVolPlot" class="unified-plot"></div>
          </div>
        </div>
      </div>

      <div id="viewHistorical" class="panel" style="display:none">
        <div class="subtabs" id="histSubtabs">
          <button class="subtab-btn active" data-subtab="playback">Playback</button>
          <button class="subtab-btn" data-subtab="regime">Regime Map</button>
        </div>
        <div id="viewHistPlayback">
          <div class="header">
            <div><strong>Historical Playback</strong></div>
            <div style="display:flex;gap:10px;align-items:center">
              <label style="font-size:11px;color:var(--muted)">Start Date:</label>
              <input type="date" id="playbackDate" style="background:#0f1115;border:1px solid var(--border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:11px">
              <div style="display:flex;gap:4px;background:#1a1d21;border-radius:6px;padding:2px">
                <button class="strike-btn playback-range-btn" data-days="1" style="padding:4px 10px;font-size:10px">1D</button>
                <button class="strike-btn playback-range-btn" data-days="3" style="padding:4px 10px;font-size:10px">3D</button>
                <button class="strike-btn playback-range-btn active" data-days="7" style="padding:4px 10px;font-size:10px">7D</button>
              </div>
              <button id="playbackLoad" class="strike-btn" style="padding:4px 12px">Load</button>
              <button id="playbackExportFull" class="strike-btn" style="padding:4px 12px">Export Full CSV</button>
              <button id="playbackExportSummary" class="strike-btn" style="padding:4px 12px">Export Summary CSV</button>
            </div>
          </div>
          <div class="playback-container">
            <div class="playback-info">
              <span id="playbackTimestamp" style="font-size:12px;color:var(--text)">Select a date and click Load</span>
              <span id="playbackStats" style="font-size:11px;color:var(--muted);margin-left:20px"></span>
            </div>
            <!-- Summary View (top): Price chart with key levels + stats panel -->
            <div id="playbackSummaryView">
              <div style="display:flex;gap:16px;height:420px">
                <div style="flex:2;background:#121417;border-radius:8px;padding:8px">
                  <h3 style="font-size:12px;color:var(--muted);margin:0 0 8px 0">SPX Price + Key Levels</h3>
                  <div id="playbackSummaryPlot" style="width:100%;height:calc(100% - 30px)"></div>
                </div>
                <div style="flex:1;background:#121417;border-radius:8px;padding:12px;overflow-y:auto">
                  <h3 style="font-size:12px;color:var(--muted);margin:0 0 12px 0">Statistics at Selected Time</h3>
                  <div id="playbackSummaryStats" style="font-size:13px"></div>
                </div>
              </div>
              <div style="margin-top:8px;font-size:11px;color:var(--muted);display:flex;gap:20px;flex-wrap:wrap">
                <span><span style="color:#9ca3af">--</span> Day Open</span>
                <span><span style="color:#3b82f6">■</span> Target</span>
                <span><span style="color:#f59e0b">■</span> LIS</span>
                <span><span style="color:#22c55e">■</span> Max +Gamma</span>
                <span><span style="color:#ef4444">■</span> Max -Gamma</span>
              </div>
            </div>
            <!-- Slider (middle): shared time scrubber -->
            <div class="playback-slider-container">
              <input type="range" id="playbackSlider" min="0" max="100" value="0" style="width:100%">
              <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:4px">
                <span id="playbackSliderStart">--</span>
                <span id="playbackSliderEnd">--</span>
              </div>
            </div>
            <!-- Full Detail View (bottom): Net GEX, Charm, Delta Decay, Volume cards -->
            <div id="playbackFullView" class="playback-detail-grid">
              <div class="playback-card">
                <h3>Net GEX</h3>
                <div id="playbackGexPlot" class="playback-plot"></div>
              </div>
              <div class="playback-card">
                <h3>Charm</h3>
                <div id="playbackCharmPlot" class="playback-plot"></div>
              </div>
              <div class="playback-card">
                <h3>Delta Decay</h3>
                <div id="playbackDDPlot" class="playback-plot"></div>
              </div>
              <div class="playback-card">
                <h3>Volume</h3>
                <div id="playbackVolPlot" class="playback-plot"></div>
              </div>
            </div>
          </div>
        </div>
        <div id="viewHistRegime" style="display:none">
          <div class="header">
            <div><strong>Regime Map</strong></div>
            <div style="display:flex;gap:10px;align-items:center">
              <label style="font-size:11px;color:var(--muted)">Date:</label>
              <input type="date" id="regimeMapDate" style="background:#0f1115;border:1px solid var(--border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:11px">
              <button id="regimeMapLoad" class="strike-btn" style="padding:4px 12px">Load</button>
              <span style="margin-left:8px;font-size:10px;color:var(--muted)">TF:</span>
              <button id="regimeMapTF5" class="strike-btn active" style="padding:2px 8px;font-size:10px">5m</button>
              <button id="regimeMapTF1" class="strike-btn" style="padding:2px 8px;font-size:10px">1m</button>
              <span id="regimeMapStatus" style="font-size:11px;color:var(--muted)">Select a date and click Load</span>
            </div>
          </div>
          <div style="display:flex;flex-direction:column;height:calc(100vh - 160px)">
            <div id="regimeMapPlot" style="flex:1;min-height:0"></div>
            <div style="margin-top:8px;font-size:11px;color:var(--muted);display:flex;gap:16px;flex-wrap:wrap;padding:0 8px 8px;align-items:center">
              <span><span style="display:inline-block;width:6px;height:12px;background:#22c55e;vertical-align:middle;margin-right:1px"></span><span style="display:inline-block;width:6px;height:12px;background:#ef4444;vertical-align:middle"></span> SPX</span>
              <span><span style="display:inline-block;width:16px;height:2.5px;background:#3b82f6;vertical-align:middle"></span> Target</span>
              <span><span style="display:inline-block;width:16px;height:6px;background:rgba(245,158,11,0.15);border-top:1.5px solid #f59e0b;border-bottom:1.5px solid #f59e0b;vertical-align:middle"></span> LIS Zone</span>
              <span><span style="display:inline-block;width:16px;height:1.5px;background:#22c55e;vertical-align:middle"></span> +GEX</span>
              <span><span style="display:inline-block;width:16px;height:1.5px;background:#ef4444;vertical-align:middle"></span> -GEX</span>
              <span style="margin-left:6px;font-weight:600">Paradigm:</span>
              <span><span style="display:inline-block;width:12px;height:12px;background:rgba(34,197,94,0.35);border-radius:2px;vertical-align:middle"></span> GEX</span>
              <span><span style="display:inline-block;width:12px;height:12px;background:rgba(239,68,68,0.35);border-radius:2px;vertical-align:middle"></span> Anti-GEX</span>
              <span><span style="display:inline-block;width:12px;height:12px;background:rgba(96,165,250,0.35);border-radius:2px;vertical-align:middle"></span> BofA</span>
              <span><span style="display:inline-block;width:12px;height:12px;background:rgba(168,85,247,0.35);border-radius:2px;vertical-align:middle"></span> Sidial</span>
            </div>
          </div>
        </div>
      </div>

      <!-- ES Delta View -->
      <div id="viewEsDelta" class="panel" style="display:none">
        <div class="header">
          <div><strong>ES Delta</strong></div>
          <div style="display:flex;gap:10px;align-items:center">
            <button id="esDeltaLive" class="strike-btn" style="padding:3px 10px;font-size:10px;background:#22c55e;color:#000;font-weight:600">LIVE</button>
            <span id="esDeltaStatus" style="font-size:11px;color:var(--muted)">Loading...</span>
          </div>
        </div>
        <div id="esDeltaPlot" style="height:calc(100vh - 80px)"></div>
      </div>

      <!-- Trade Log View -->
      <div id="viewTradeLog" class="panel" style="display:none;flex-direction:column;overflow:auto">
        <div class="header"><div><strong>Trade Log</strong></div><span id="tlStatus" style="font-size:11px;color:var(--muted)"></span></div>
        <div class="subtabs" id="tlSubtabs">
          <button class="subtab-btn active" data-subtab="portal">Portal Log</button>
          <button class="subtab-btn" data-subtab="tssim">TS SIM Log</button>
          <button class="subtab-btn" data-subtab="eval">Eval Log</button>
        </div>
        <div class="tl-filters">
          <select id="tlFilterSetup"><option value="">All Setups</option><option>GEX Long</option><option>AG Short</option><option>BofA Scalp</option><option>ES Absorption</option><option>DD Exhaustion</option><option>Paradigm Reversal</option></select>
          <select id="tlFilterResult"><option value="">All Results</option><option value="WIN">WIN</option><option value="LOSS">LOSS</option><option value="EXPIRED">EXPIRED</option><option value="TIMEOUT">TIMEOUT</option><option value="OPEN">OPEN</option><option value="PENDING">PENDING</option></select>
          <select id="tlFilterGrade"><option value="">All Grades</option><option>A+</option><option>A</option><option>A-Entry</option></select>
          <select id="tlFilterDate"><option value="">All Dates</option><option value="today">Today</option><option value="week">This Week</option><option value="month">This Month</option></select>
          <input type="text" id="tlSearch" placeholder="Search..." style="width:140px">
        </div>
        <div class="tl-stats" id="tlStats"></div>
        <div style="overflow-y:auto;flex:1">
          <div id="tlHeaderRow" class="tl-header">
            <span>#</span><span>Setup</span><span>Dir</span><span>Grade</span><span>Scr</span><span>Entry</span><span>Gap/RR</span><span>10p/Tgt/Stp</span><span>Result</span><span>P&L</span><span>Dur</span><span>Time</span><span></span>
          </div>
          <div id="tlBody"></div>
        </div>
      </div>

    </main>
  </div>

  <script>
    const PULL_EVERY = __PULL_MS__;

    // ===== US Eastern Time Formatting Helpers =====
    const ET_TIMEZONE = 'America/New_York';

    // Format time as HH:MM in ET
    function fmtTimeET(isoStr) {
      if (!isoStr) return '--:--';
      try {
        const d = new Date(isoStr);
        return d.toLocaleTimeString('en-US', { timeZone: ET_TIMEZONE, hour: '2-digit', minute: '2-digit', hour12: false });
      } catch { return '--:--'; }
    }

    // Format time as HH:MM:SS in ET
    function fmtTimeFullET(isoStr) {
      if (!isoStr) return '--:--:--';
      try {
        const d = new Date(isoStr);
        return d.toLocaleTimeString('en-US', { timeZone: ET_TIMEZONE, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
      } catch { return '--:--:--'; }
    }

    // Format date as MMM DD in ET
    function fmtDateShortET(isoStr) {
      if (!isoStr) return '--';
      try {
        const d = new Date(isoStr);
        return d.toLocaleDateString('en-US', { timeZone: ET_TIMEZONE, month: 'short', day: 'numeric' });
      } catch { return '--'; }
    }

    // Format date as MM/DD/YYYY in ET
    function fmtDateET(isoStr) {
      if (!isoStr) return '--';
      try {
        const d = new Date(isoStr);
        return d.toLocaleDateString('en-US', { timeZone: ET_TIMEZONE });
      } catch { return '--'; }
    }

    // Format full datetime in ET
    function fmtDateTimeET(isoStr) {
      if (!isoStr) return '--';
      try {
        const d = new Date(isoStr);
        return d.toLocaleString('en-US', { timeZone: ET_TIMEZONE });
      } catch { return '--'; }
    }

    // Format as MM/DD HH:MM in ET
    function fmtDateTimeShortET(isoStr) {
      if (!isoStr) return '--';
      try {
        const d = new Date(isoStr);
        const date = d.toLocaleDateString('en-US', { timeZone: ET_TIMEZONE, month: '2-digit', day: '2-digit' });
        const time = d.toLocaleTimeString('en-US', { timeZone: ET_TIMEZONE, hour: '2-digit', minute: '2-digit', hour12: false });
        return date + ' ' + time;
      } catch { return '--'; }
    }

    // ===== Data Freshness Indicator =====
    const dataFreshnessEl = document.getElementById('dataFreshness');
    async function fetchDataFreshness() {
      try {
        const r = await fetch('/api/data_freshness', {cache: 'no-store'});
        const data = await r.json();
        renderDataFreshness(data);
      } catch (err) {
        dataFreshnessEl.innerHTML = '<span style="color:#ef4444">Error</span>';
      }
    }
    function renderDataFreshness(data) {
      const statusColors = { ok: '#22c55e', stale: '#eab308', error: '#ef4444', closed: '#6b7280' };

      const ts = data.ts_api || {};
      const vl = data.volland || {};

      const tsColor = statusColors[ts.status] || statusColors.error;
      const vlColor = statusColors[vl.status] || statusColors.error;

      const spotStr = data.spot ? 'SPX ' + data.spot.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}) : '';
      const vixStr = data.vix ? '<span style="margin-left:12px">VIX ' + data.vix.toFixed(2) + '</span>' : '';
      const priceRow = (spotStr || vixStr) ? spotStr + vixStr + '<br>' : '';

      dataFreshnessEl.innerHTML = priceRow +
        '<span style="color:' + tsColor + '">TS:' + fmtTimeET(ts.last_update) + '</span>' +
        '<span style="margin:0 6px;color:#555">|</span>' +
        '<span style="color:' + vlColor + '">Vol:' + fmtTimeET(vl.last_update) + '</span>';
    }
    fetchDataFreshness();
    setInterval(fetchDataFreshness, PULL_EVERY);

    // ===== SPX Statistics (persists after market close) =====
    const statsContent = document.getElementById('statsContent');
    async function fetchStats() {
      try {
        const r = await fetch('/api/volland/stats', {cache: 'no-store'});
        const data = await r.json();
        renderStats(data);
      } catch (err) {
        statsContent.innerHTML = '<span style="color:#ef4444">Error: ' + err.message + '</span>';
      }
    }
    function renderStats(data) {
      if (!data || data.error) {
        statsContent.innerHTML = '<span style="color:#ef4444">' + (data?.error || 'No data') + '</span>';
        return;
      }
      const s = data.stats || {};
      let h = '';
      
      // Paradigm
      if (s.paradigm) {
        h += '<div class="stats-row"><span class="stats-label">Paradigm</span><span class="stats-value">' + s.paradigm + '</span></div>';
      }
      
      // Target
      if (s.target) {
        h += '<div class="stats-row"><span class="stats-label">Target</span><span class="stats-value">' + s.target + '</span></div>';
      }
      
      // Lines in the Sand
      if (s.lines_in_sand) {
        h += '<div class="stats-row"><span class="stats-label">Lines in Sand</span><span class="stats-value">' + s.lines_in_sand + '</span></div>';
      }
      
      // Delta Decay Hedging
      if (s.delta_decay_hedging) {
        const ddh = s.delta_decay_hedging;
        const isNeg = ddh.includes('-') || ddh.startsWith('-');
        h += '<div class="stats-row"><span class="stats-label">DD Hedging</span><span class="stats-value ' + (isNeg ? 'red' : 'green') + '">' + ddh + '</span></div>';
      }
      
      // Options Volume
      if (s.opt_volume) {
        h += '<div class="stats-row"><span class="stats-label">0DTE Volume</span><span class="stats-value">' + s.opt_volume + '</span></div>';
      }
      
      // Spot-Vol Beta
      if (s.svb_correlation != null) {
        const svb = parseFloat(s.svb_correlation);
        if (!isNaN(svb)) h += '<div class="stats-row"><span class="stats-label">Spot-Vol Beta</span><span class="stats-value">' + (svb >= 0 ? '+' : '') + svb.toFixed(2) + '</span></div>';
      }

      // If no statistics found
      if (!s.paradigm && !s.target && !s.lines_in_sand && !s.delta_decay_hedging && !s.opt_volume) {
        h += '<div style="color:var(--muted);font-size:10px">No statistics available</div>';
      }
      
      // Timestamp with staleness check (same logic as charm chart)
      let updatedLabel = fmtTimeET(data.ts) + ' ET';
      if (data.ts) {
        const ageMins = Math.round((Date.now() - new Date(data.ts).getTime()) / 60000);
        if (ageMins > 5) {
          updatedLabel += ' <span style="color:#ef4444">(stale)</span>';
        }
      }
      h += '<div class="stats-row" style="margin-top:6px;font-size:10px"><span class="stats-label">Updated</span><span class="stats-value">' + updatedLabel + '</span></div>';
      
      statsContent.innerHTML = h;
    }
    fetchStats();
    setInterval(fetchStats, 60000); // Refresh stats every 60 seconds

    // Tabs
    const tabTable=document.getElementById('tabTable'),
          tabSpot=document.getElementById('tabSpot'),
          tabCharts=document.getElementById('tabCharts'),
          tabEsDelta=document.getElementById('tabEsDelta'),
          tabHistorical=document.getElementById('tabHistorical'),
          tabTradeLog=document.getElementById('tabTradeLog');

    const viewTable=document.getElementById('viewTable'),
          viewSpot=document.getElementById('viewSpot'),
          viewCharts=document.getElementById('viewCharts'),
          viewEsDelta=document.getElementById('viewEsDelta'),
          viewHistorical=document.getElementById('viewHistorical'),
          viewTradeLog=document.getElementById('viewTradeLog');

    // Charts sub-views
    const viewCharts0dte=document.getElementById('viewCharts0dte'),
          viewChartsHTF=document.getElementById('viewChartsHTF');
    let _chartsActiveSubTab = '0dte';

    // Historical sub-views
    const viewHistPlayback=document.getElementById('viewHistPlayback'),
          viewHistRegime=document.getElementById('viewHistRegime');
    let _histActiveSubTab = 'playback';

    let tradeLogTimer = null;
    function stopTradeLog() { if(tradeLogTimer){clearInterval(tradeLogTimer);tradeLogTimer=null;} }

    function setActive(btn){
      [tabTable,tabSpot,tabCharts,tabEsDelta,tabHistorical,tabTradeLog].forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
    }
    function hideAllViews(){ viewTable.style.display='none'; viewCharts.style.display='none'; viewSpot.style.display='none'; viewHistorical.style.display='none'; viewEsDelta.style.display='none'; viewTradeLog.style.display='none'; }
    function stopAllPolling(){ stopCharts(); stopChartsHT(); stopSpot(); stopStatistics(); stopEsDelta(); stopTradeLog(); }
    function saveTab(name){ try{sessionStorage.setItem('activeTab',name);}catch(e){} }

    // Charts sub-tab switching
    function _chartsShowSubTab(sub) {
      _chartsActiveSubTab = sub;
      viewCharts0dte.style.display = sub==='0dte' ? '' : 'none';
      viewChartsHTF.style.display = sub==='htf' ? '' : 'none';
      document.querySelectorAll('#chartsSubtabs .subtab-btn').forEach(b => b.classList.toggle('active', b.dataset.subtab===sub));
      if(sub==='0dte'){ startCharts(); stopChartsHT(); } else { stopCharts(); startChartsHT(); }
      try{sessionStorage.setItem('chartsSubTab',sub);}catch(e){}
    }
    document.querySelectorAll('#chartsSubtabs .subtab-btn').forEach(btn => {
      btn.addEventListener('click', () => _chartsShowSubTab(btn.dataset.subtab));
    });

    // Historical sub-tab switching
    function _histShowSubTab(sub) {
      _histActiveSubTab = sub;
      viewHistPlayback.style.display = sub==='playback' ? '' : 'none';
      viewHistRegime.style.display = sub==='regime' ? '' : 'none';
      document.querySelectorAll('#histSubtabs .subtab-btn').forEach(b => b.classList.toggle('active', b.dataset.subtab===sub));
      if(sub==='playback') initPlayback(); else initRegimeMap();
      try{sessionStorage.setItem('histSubTab',sub);}catch(e){}
    }
    document.querySelectorAll('#histSubtabs .subtab-btn').forEach(btn => {
      btn.addEventListener('click', () => _histShowSubTab(btn.dataset.subtab));
    });

    function showTable(){ setActive(tabTable); hideAllViews(); viewTable.style.display=''; stopAllPolling(); saveTab('table'); }
    function showSpot(){ setActive(tabSpot); hideAllViews(); viewSpot.style.display=''; stopAllPolling(); startSpot(); startStatistics(); saveTab('spot'); }
    function showCharts(){ setActive(tabCharts); hideAllViews(); viewCharts.style.display=''; stopAllPolling(); _chartsShowSubTab(_chartsActiveSubTab); saveTab('charts'); }
    function showEsDelta(){ setActive(tabEsDelta); hideAllViews(); viewEsDelta.style.display=''; stopAllPolling(); startEsDelta(); saveTab('esDelta'); }
    function showHistorical(){ setActive(tabHistorical); hideAllViews(); viewHistorical.style.display=''; stopAllPolling(); _histShowSubTab(_histActiveSubTab); saveTab('historical'); }
    function showTradeLog(){ setActive(tabTradeLog); hideAllViews(); viewTradeLog.style.display='flex'; stopAllPolling(); _tlLoadActiveSubTab(); tradeLogTimer=setInterval(_tlLoadActiveSubTab,30000); saveTab('tradeLog'); }
    tabTable.addEventListener('click', showTable);
    tabSpot.addEventListener('click', showSpot);
    tabCharts.addEventListener('click', showCharts);
    tabEsDelta.addEventListener('click', showEsDelta);
    tabHistorical.addEventListener('click', showHistorical);
    tabTradeLog.addEventListener('click', showTradeLog);

    // Restore last active tab on page load
    try {
      let saved = sessionStorage.getItem('activeTab');
      // Backward compat: old tab names → new merged tabs
      if(saved==='chartsHT'){ saved='charts'; _chartsActiveSubTab='htf'; }
      if(saved==='playback'){ saved='historical'; _histActiveSubTab='playback'; }
      if(saved==='regimeMap'){ saved='historical'; _histActiveSubTab='regime'; }
      // Restore sub-tab memory
      const cSub = sessionStorage.getItem('chartsSubTab'); if(cSub) _chartsActiveSubTab = cSub;
      const hSub = sessionStorage.getItem('histSubTab'); if(hSub) _histActiveSubTab = hSub;
      const tabMap = {spot:showSpot, charts:showCharts, esDelta:showEsDelta, historical:showHistorical, tradeLog:showTradeLog};
      if(saved && tabMap[saved]) tabMap[saved]();
    } catch(e){}

    // ===== Shared fetch for options series (includes spot) =====
    async function fetchSeries(){
      const r=await fetch('/api/series',{cache:'no-store'});
      return await r.json();
    }

    // ===== Volland vanna window =====
    async function fetchVannaWindow(){
      const r = await fetch('/api/volland/vanna_window?limit=40', {cache:'no-store'});
      return await r.json();
    }

    // ===== Volland delta decay window =====
    async function fetchDeltaDecayWindow(){
      const r = await fetch('/api/volland/delta_decay_window?limit=40', {cache:'no-store'});
      return await r.json();
    }

    // ===== Main charts (2x4 grid) =====
    const gexNetDiv=document.getElementById('gexNetChart'),
          gexCallPutDiv=document.getElementById('gexCallPutChart'),
          vannaDiv=document.getElementById('vannaChart'),
          vannaOdteDiv=document.getElementById('vannaOdteChart'),
          deltaDecayDiv=document.getElementById('deltaDecayChart'),
          gammaOdteDiv=document.getElementById('gammaOdteChart'),
          oiDiv=document.getElementById('oiChart'),
          volDiv=document.getElementById('volChart');

    let chartsTimer=null;

    function verticalSpotShape(spot,yMin,yMax){
      if(spot==null) return null;
      return {type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax, line:{color:'#9aa0a6', width:2, dash:'dot'}, xref:'x', yref:'y'};
    }

    function buildLayout(title,xTitle,yTitle,spot,yMin,yMax,dtick=10){
      const shape=verticalSpotShape(spot,yMin,yMax);
      return {
        title:{text:title,font:{size:14}},
        xaxis:{title:xTitle,gridcolor:'#20242a',tickfont:{size:10},dtick:dtick},
        yaxis:{title:yTitle,gridcolor:'#20242a',tickfont:{size:10}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        font:{color:'#e6e7e9',size:11},
        margin:{t:32,r:12,b:40,l:44},
        barmode:'group',
        shapes:shape?[shape]:[]
      };
    }

    function tracesForBars(strikes,callArr,putArr,yLabel){
      return [
        {type:'bar', name:'Calls '+yLabel, x:strikes, y:callArr, marker:{color:'#22c55e'}, offsetgroup:'calls',
         hovertemplate:"Strike %{x}<br>Calls: %{y}<extra></extra>"},
        {type:'bar', name:'Puts '+yLabel,  x:strikes, y:putArr,  marker:{color:'#ef4444'}, offsetgroup:'puts',
         hovertemplate:"Strike %{x}<br>Puts: %{y}<extra></extra>"}
      ];
    }

    function tracesForGEXNet(strikes,netGEX){
      const colors = netGEX.map(v => (v >= 0 ? '#22c55e' : '#ef4444'));
      return [
        {type:'bar', name:'Net GEX', x:strikes, y:netGEX, marker:{color:colors},
         hovertemplate:"Strike %{x}<br>Net GEX: %{y:.2f}<extra></extra>"}
      ];
    }

    function tracesForGEXCallPut(strikes,callGEX,putGEX){
      return [
        {type:'bar', name:'Call GEX', x:strikes, y:callGEX, marker:{color:'#22c55e'}, offsetgroup:'call_gex',
         hovertemplate:"Strike %{x}<br>Call GEX: %{y:.2f}<extra></extra>"},
        {type:'bar', name:'Put GEX',  x:strikes, y:putGEX,  marker:{color:'#ef4444'}, offsetgroup:'put_gex',
         hovertemplate:"Strike %{x}<br>Put GEX: %{y:.2f}<extra></extra>"}
      ];
    }

    function drawVannaWindow(w, spot){
      if (!w || w.error) {
        const msg = w && w.error ? w.error : "no data";
        Plotly.react(vannaDiv, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:10,b:30},
          annotations:[{text:"Vanna error: "+msg, x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9'}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }

      const pts = w.points || [];
      if (!pts.length) {
        Plotly.react(vannaDiv, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:10,b:30},
          annotations:[{text:"No vanna points returned yet", x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9'}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }

      const strikes = pts.map(p=>p.strike);
      const vanna   = pts.map(p=>p.vanna);

      // green for +, red for -
      const colors = vanna.map(v => (v >= 0 ? '#22c55e' : '#ef4444'));

      let yMin = Math.min(...vanna);
      let yMax = Math.max(...vanna);
      if (yMin === yMax){
        const pad0 = Math.max(1, Math.abs(yMin)*0.05);
        yMin -= pad0; yMax += pad0;
      } else {
        const pad = (yMax - yMin) * 0.05;
        yMin -= pad; yMax += pad;
      }

      const shapes = [];
      if (spot != null) {
        shapes.push({
          type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax,
          xref:'x', yref:'y',
          line:{color:'#9aa0a6', width:2, dash:'dot'}
        });
      }

      const trace = {
        type:'bar',
        x: strikes,
        y: vanna,
        marker:{color: colors},
        hovertemplate:"Strike %{x}<br>Vanna %{y}<extra></extra>"
      };

      let titleText = 'Charm';
      if (w.ts_utc) {
        const dt = new Date(w.ts_utc);
        const ageMins = Math.round((Date.now() - dt.getTime()) / 60000);
        const timeStr = fmtTimeET(w.ts_utc);
        const stale = ageMins > 5 ? ' <span style="color:#ef4444">(stale)</span>' : '';
        titleText = 'Charm  <span style="font-size:11px;color:#9aa0a6">' + timeStr + ' ET' + stale + '</span>';
      }

      Plotly.react(vannaDiv, [trace], {
        title:{text:titleText, font:{size:14}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        margin:{l:55,r:10,t:32,b:40},
        xaxis:{title:'Strike', gridcolor:'#20242a', tickfont:{size:10}, dtick:10},
        yaxis:{title:'Charm',  gridcolor:'#20242a', tickfont:{size:10}, range:[yMin,yMax]},
        shapes: shapes,
        font:{color:'#e6e7e9',size:11}
      }, {displayModeBar:false,responsive:true});
    }

    function drawDeltaDecay(w, spot){
      if (!w || w.error) {
        const msg = w && w.error ? w.error : "no data";
        Plotly.react(deltaDecayDiv, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:10,b:30},
          annotations:[{text:"Delta Decay error: "+msg, x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9'}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }

      const pts = w.points || [];
      if (!pts.length) {
        Plotly.react(deltaDecayDiv, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:10,b:30},
          annotations:[{text:"No delta decay points returned yet", x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9'}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }

      const strikes = pts.map(p=>p.strike);
      const vals    = pts.map(p=>p.delta_decay);

      const colors = vals.map(v => (v >= 0 ? '#22c55e' : '#ef4444'));

      let yMin = Math.min(...vals);
      let yMax = Math.max(...vals);
      if (yMin === yMax){
        const pad0 = Math.max(1, Math.abs(yMin)*0.05);
        yMin -= pad0; yMax += pad0;
      } else {
        const pad = (yMax - yMin) * 0.05;
        yMin -= pad; yMax += pad;
      }

      const shapes = [];
      if (spot != null) {
        shapes.push({
          type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax,
          xref:'x', yref:'y',
          line:{color:'#9aa0a6', width:2, dash:'dot'}
        });
      }

      const trace = {
        type:'bar',
        x: strikes,
        y: vals,
        marker:{color: colors},
        hovertemplate:"Strike %{x}<br>Delta Decay %{y}<extra></extra>"
      };

      let titleText = 'Delta Decay';
      if (w.ts_utc) {
        const dt = new Date(w.ts_utc);
        const ageMins = Math.round((Date.now() - dt.getTime()) / 60000);
        const timeStr = fmtTimeET(w.ts_utc);
        const stale = ageMins > 5 ? ' <span style="color:#ef4444">(stale)</span>' : '';
        titleText = 'Delta Decay  <span style="font-size:11px;color:#9aa0a6">' + timeStr + ' ET' + stale + '</span>';
      }

      Plotly.react(deltaDecayDiv, [trace], {
        title:{text:titleText, font:{size:14}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        margin:{l:55,r:10,t:32,b:40},
        xaxis:{title:'Strike', gridcolor:'#20242a', tickfont:{size:10}, dtick:10},
        yaxis:{title:'Delta Decay',  gridcolor:'#20242a', tickfont:{size:10}, range:[yMin,yMax]},
        shapes: shapes,
        font:{color:'#e6e7e9',size:11}
      }, {displayModeBar:false,responsive:true});
    }

    // Generic bar chart for exposure_window data (Vanna 0DTE, Gamma 0DTE)
    function drawExposureChart(divEl, data, spot, label){
      if (!data || data.error) {
        const msg = data && data.error ? data.error : "no data";
        Plotly.react(divEl, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:30,b:30},
          annotations:[{text:label+": "+msg, x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9',size:11}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }
      const pts = data.points || [];
      if (!pts.length) {
        Plotly.react(divEl, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:30,b:30},
          annotations:[{text:label+": no points yet", x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9',size:11}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }
      const strikes = pts.map(p=>p.strike);
      const vals    = pts.map(p=>p.value);
      const colors  = vals.map(v => (v >= 0 ? '#22c55e' : '#ef4444'));
      let yMin = Math.min(...vals), yMax = Math.max(...vals);
      if (yMin === yMax){ const p0=Math.max(1,Math.abs(yMin)*0.05); yMin-=p0; yMax+=p0; }
      else { const p=(yMax-yMin)*0.05; yMin-=p; yMax+=p; }
      const shapes = [];
      if (spot != null) shapes.push({type:'line',x0:spot,x1:spot,y0:yMin,y1:yMax,xref:'x',yref:'y',line:{color:'#9aa0a6',width:2,dash:'dot'}});
      let titleText = label;
      if (data.ts_utc) {
        const dt=new Date(data.ts_utc), ageMins=Math.round((Date.now()-dt.getTime())/60000), timeStr=fmtTimeET(data.ts_utc);
        const stale = ageMins>5?' <span style="color:#ef4444">(stale)</span>':'';
        titleText = label+'  <span style="font-size:11px;color:#9aa0a6">'+timeStr+' ET'+stale+'</span>';
      }
      Plotly.react(divEl, [{type:'bar',x:strikes,y:vals,marker:{color:colors},hovertemplate:"Strike %{x}<br>"+label+" %{y}<extra></extra>"}], {
        title:{text:titleText,font:{size:14}},paper_bgcolor:'#121417',plot_bgcolor:'#0f1115',
        margin:{l:55,r:10,t:32,b:40},
        xaxis:{title:'Strike',gridcolor:'#20242a',tickfont:{size:10},dtick:10},
        yaxis:{gridcolor:'#20242a',tickfont:{size:10},range:[yMin,yMax]},
        shapes:shapes,font:{color:'#e6e7e9',size:11}
      }, {displayModeBar:false,responsive:true});
    }

    async function drawOrUpdate(){
  // 1) Fetch the fast data first (DO NOT wait for volland)
  const data = await fetchSeries();
  if (!data || !data.strikes || data.strikes.length === 0) return;

  const strikes = data.strikes, spot = data.spot;

  const vMax = Math.max(0, ...data.callVol, ...data.putVol) * 1.05;
  const oiMax= Math.max(0, ...data.callOI,  ...data.putOI ) * 1.05;
  const gNetAbs = data.netGEX.map(v=>Math.abs(v));
  const gNetMax = (gNetAbs.length ? Math.max(...gNetAbs) : 0) * 1.05;
  const gCPAbs = [...data.callGEX, ...data.putGEX].map(v=>Math.abs(v));
  const gCPMax = (gCPAbs.length ? Math.max(...gCPAbs) : 0) * 1.05;

  const gexNetLayout = buildLayout('GEX (Net)','Strike','Net GEX',spot,-gNetMax,gNetMax);
  const gexCPLayout  = buildLayout('GEX (Call & Put)','Strike','GEX',spot,-gCPMax,gCPMax);
  const volLayout = buildLayout('Volume','Strike','Volume',spot,0,vMax);
  const oiLayout  = buildLayout('Open Interest','Strike','Open Interest',spot,0,oiMax);
  gexCPLayout.barmode = 'group';

  const gexNetTraces = tracesForGEXNet(strikes, data.netGEX);
  const gexCPTraces  = tracesForGEXCallPut(strikes, data.callGEX, data.putGEX);
  const volTraces = tracesForBars(strikes, data.callVol, data.putVol, 'Vol');
  const oiTraces  = tracesForBars(strikes, data.callOI,  data.putOI,  'OI');

  const opts = {displayModeBar:false,responsive:true};
  Plotly.react(gexNetDiv,     gexNetTraces, gexNetLayout, opts);
  Plotly.react(gexCallPutDiv, gexCPTraces,  gexCPLayout,  opts);
  Plotly.react(volDiv,        volTraces,    volLayout,     opts);
  Plotly.react(oiDiv,         oiTraces,     oiLayout,      opts);

  // 2) Show loading states on first draw
  if (!window.__vannaLoadingShown) {
    window.__vannaLoadingShown = true;
    drawVannaWindow({ error: "Loading Charm..." }, spot);
    drawDeltaDecay({ error: "Loading Delta Decay..." }, spot);
    drawExposureChart(vannaOdteDiv, { error: "Loading Vanna 0DTE..." }, spot, 'Vanna 0DTE');
    drawExposureChart(gammaOdteDiv, { error: "Loading Gamma 0DTE..." }, spot, 'Gamma 0DTE');
  }

  // 3) Fetch volland charts in the background
  fetchVannaWindow()
    .then(w => drawVannaWindow(w, spot))
    .catch(err => drawVannaWindow({ error: String(err) }, spot));

  fetchDeltaDecayWindow()
    .then(w => drawDeltaDecay(w, spot))
    .catch(err => drawDeltaDecay({ error: String(err) }, spot));

  fetch('/api/volland/exposure_window?greek=vanna&expiration=TODAY&limit=40',{cache:'no-store'})
    .then(r=>r.json())
    .then(w => drawExposureChart(vannaOdteDiv, w, spot, 'Vanna 0DTE'))
    .catch(err => drawExposureChart(vannaOdteDiv, {error:String(err)}, spot, 'Vanna 0DTE'));

  fetch('/api/volland/exposure_window?greek=gamma&expiration=TODAY&limit=40',{cache:'no-store'})
    .then(r=>r.json())
    .then(w => drawExposureChart(gammaOdteDiv, w, spot, 'Gamma 0DTE'))
    .catch(err => drawExposureChart(gammaOdteDiv, {error:String(err)}, spot, 'Gamma 0DTE'));
}


    function startCharts(){
      drawOrUpdate();
      if (chartsTimer) clearInterval(chartsTimer);
      chartsTimer = setInterval(drawOrUpdate, PULL_EVERY);
    }
    function stopCharts(){
      if (chartsTimer){
        clearInterval(chartsTimer);
        chartsTimer=null;
      }
    }

    // ===== Charts HT (High-Tenor Vanna & Gamma) =====
    const htDivs = {
      weeklyVanna:  document.getElementById('weeklyVannaChart'),
      monthlyVanna: document.getElementById('monthlyVannaChart'),
      allVanna:     document.getElementById('allVannaChart'),
      weeklyGamma:  document.getElementById('weeklyGammaChart'),
      monthlyGamma: document.getElementById('monthlyGammaChart'),
      allGamma:     document.getElementById('allGammaChart'),
    };
    let chartsHTTimer = null;

    function drawHTChart(divEl, data, spot, label){
      if (!data || data.error) {
        const msg = data && data.error ? data.error : "no data";
        Plotly.react(divEl, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:30,b:30},
          annotations:[{text:label+": "+msg, x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9',size:11}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }
      const pts = data.points || [];
      if (!pts.length) {
        Plotly.react(divEl, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:30,b:30},
          annotations:[{text:label+": no points yet", x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9',size:11}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }
      const strikes = pts.map(p=>p.strike);
      const vals    = pts.map(p=>p.value);
      const colors  = vals.map(v => (v >= 0 ? '#22c55e' : '#ef4444'));

      let yMin = Math.min(...vals);
      let yMax = Math.max(...vals);
      if (yMin === yMax){
        const pad0 = Math.max(1, Math.abs(yMin)*0.05);
        yMin -= pad0; yMax += pad0;
      } else {
        const pad = (yMax - yMin) * 0.05;
        yMin -= pad; yMax += pad;
      }

      const shapes = [];
      if (spot != null) {
        shapes.push({
          type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax,
          xref:'x', yref:'y',
          line:{color:'#9aa0a6', width:2, dash:'dot'}
        });
      }

      let titleText = label;
      if (data.ts_utc) {
        const dt = new Date(data.ts_utc);
        const ageMins = Math.round((Date.now() - dt.getTime()) / 60000);
        const timeStr = fmtTimeET(data.ts_utc);
        const stale = ageMins > 5 ? ' <span style="color:#ef4444">(stale)</span>' : '';
        titleText = label + '  <span style="font-size:10px;color:#9aa0a6">' + timeStr + ' ET' + stale + '</span>';
      }

      Plotly.react(divEl, [{
        type:'bar', x:strikes, y:vals,
        marker:{color:colors},
        hovertemplate:"Strike %{x}<br>"+label+" %{y}<extra></extra>"
      }], {
        title:{text:titleText, font:{size:13}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        margin:{l:50,r:10,t:32,b:36},
        xaxis:{title:'Strike', gridcolor:'#20242a', tickfont:{size:9}, dtick:10},
        yaxis:{gridcolor:'#20242a', tickfont:{size:9}, range:[yMin,yMax]},
        shapes: shapes,
        font:{color:'#e6e7e9',size:10}
      }, {displayModeBar:false,responsive:true});
    }

    const HT_COMBOS = [
      {key:'weeklyVanna',  greek:'vanna', exp:'THIS_WEEK',       label:'Weekly Vanna'},
      {key:'monthlyVanna', greek:'vanna', exp:'THIRTY_NEXT_DAYS', label:'Monthly Vanna'},
      {key:'allVanna',     greek:'vanna', exp:'ALL',              label:'All-exp Vanna'},
      {key:'weeklyGamma',  greek:'gamma', exp:'THIS_WEEK',       label:'Weekly Gamma'},
      {key:'monthlyGamma', greek:'gamma', exp:'THIRTY_NEXT_DAYS', label:'Monthly Gamma'},
      {key:'allGamma',     greek:'gamma', exp:'ALL',              label:'All-exp Gamma'},
    ];

    async function drawOrUpdateHT(){
      let spot = null;
      try {
        const series = await fetchSeries();
        if (series && series.spot) spot = series.spot;
      } catch(e){}

      const fetches = HT_COMBOS.map(c =>
        fetch('/api/volland/exposure_window?greek='+c.greek+'&expiration='+c.exp+'&limit=40', {cache:'no-store'})
          .then(r=>r.json())
          .catch(err=>({error:String(err)}))
      );
      const results = await Promise.all(fetches);
      HT_COMBOS.forEach((c,i) => {
        drawHTChart(htDivs[c.key], results[i], spot, c.label);
      });
    }

    function startChartsHT(){
      drawOrUpdateHT();
      if (chartsHTTimer) clearInterval(chartsHTTimer);
      chartsHTTimer = setInterval(drawOrUpdateHT, PULL_EVERY);
    }
    function stopChartsHT(){
      if (chartsHTTimer){
        clearInterval(chartsHTTimer);
        chartsHTTimer=null;
      }
    }

    // ===== Exposure View: SPX Candles + GEX/Charm/Volume aligned by strike =====
    const unifiedSpxDiv = document.getElementById('unifiedSpxPlot'),
          unifiedGexDiv = document.getElementById('unifiedGexPlot'),
          unifiedCharmDiv = document.getElementById('unifiedCharmPlot'),
          unifiedDeltaDecayDiv = document.getElementById('unifiedDeltaDecayPlot'),
          unifiedVolDiv = document.getElementById('unifiedVolPlot');
    let unifiedTimer = null;
    let selectedStrikes = 30; // Default strike count
    let currentYRange = null; // Shared Y range for sync (persists across refreshes)
    let isZoomSyncing = false; // Prevent infinite loop
    let baseYRange = null; // Original range before any zoom

    // Fetch statistics levels (Target, LIS, Max Gamma)
    async function fetchStatisticsLevels() {
      const r = await fetch('/api/statistics_levels', { cache: 'no-store' });
      return await r.json();
    }

    // Fetch SPX 3-minute candles from TradeStation API
    async function fetchSPXCandles() {
      const r = await fetch('/api/spx_candles?bars=60', { cache: 'no-store' });
      return await r.json();
    }

    // Compute shared Y range from strikes centered around spot
    function computeStrikeRange(strikes, spot) {
      if (!strikes || !strikes.length) return null;

      // If user has zoomed, preserve their zoom level
      if (currentYRange) return currentYRange;

      // Center around spot and take selectedStrikes/2 above and below
      const half = Math.floor(selectedStrikes / 2);
      let above = strikes.filter(s => s >= spot).slice(0, half);
      let below = strikes.filter(s => s < spot).slice(-half);

      const selectedList = [...below, ...above].sort((a,b) => a-b);
      if (selectedList.length === 0) return { min: Math.min(...strikes), max: Math.max(...strikes) };

      let yMin = Math.min(...selectedList);
      let yMax = Math.max(...selectedList);
      const pad = (yMax - yMin) * 0.02 || 1;
      const range = { min: yMin - pad, max: yMax + pad };
      baseYRange = range; // Store as base for reset
      return range;
    }

    // Sync Y-axis zoom across all exposure charts
    function setupZoomSync(plotDiv) {
      plotDiv.on('plotly_relayout', function(eventData) {
        if (isZoomSyncing) return;

        // Check if Y-axis was changed (drag zoom or scroll zoom)
        const newYMin = eventData['yaxis.range[0]'];
        const newYMax = eventData['yaxis.range[1]'];

        if (newYMin !== undefined && newYMax !== undefined) {
          isZoomSyncing = true;
          currentYRange = { min: newYMin, max: newYMax };

          // Update all other charts
          const allDivs = [unifiedSpxDiv, unifiedGexDiv, unifiedCharmDiv, unifiedDeltaDecayDiv, unifiedVolDiv];
          allDivs.forEach(div => {
            if (div !== plotDiv && div._fullLayout) {
              Plotly.relayout(div, { 'yaxis.range': [newYMin, newYMax] });
            }
          });

          setTimeout(() => { isZoomSyncing = false; }, 100);
        }

        // Reset on double-click (autorange)
        if (eventData['yaxis.autorange']) {
          currentYRange = null;
          isZoomSyncing = true;
          const allDivs = [unifiedSpxDiv, unifiedGexDiv, unifiedCharmDiv, unifiedDeltaDecayDiv, unifiedVolDiv];
          allDivs.forEach(div => {
            if (div !== plotDiv && div._fullLayout && baseYRange) {
              Plotly.relayout(div, { 'yaxis.range': [baseYRange.min, baseYRange.max] });
            }
          });
          setTimeout(() => { isZoomSyncing = false; }, 100);
        }
      });
    }

    // Strike button click handler (set up after DOM ready)
    function setupStrikeButtons() {
      document.querySelectorAll('.strike-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          document.querySelectorAll('.strike-btn').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          selectedStrikes = parseInt(btn.dataset.strikes);
          currentYRange = null; // Reset zoom to recalculate
          baseYRange = null;
          tickUnified(); // Refresh immediately
        });
      });
    }

    // Create horizontal spot line shape
    function horizontalSpotShape(spot, xMin, xMax) {
      if (spot == null) return null;
      return {
        type: 'line',
        y0: spot, y1: spot,
        x0: xMin, x1: xMax,
        xref: 'x', yref: 'y',
        line: { color: '#60a5fa', width: 2, dash: 'dot' }
      };
    }

    // Render SPX candlestick chart (Y-axis = price aligned with strikes, on LEFT for shared axis)
    function renderUnifiedSPX(candleData, yRange, levels) {
      levels = levels || {};
      if (!candleData || candleData.error || !candleData.candles || !candleData.candles.length) {
        Plotly.react(unifiedSpxDiv, [], {
          paper_bgcolor: '#121417', plot_bgcolor: '#0f1115',
          margin: { l: 50, r: 8, t: 4, b: 24 },
          annotations: [{ text: candleData?.error || 'Loading candles...', x: 0.5, y: 0.5, xref: 'paper', yref: 'paper', showarrow: false, font: { color: '#e6e7e9' } }],
          font: { color: '#e6e7e9' }
        }, { displayModeBar: false, responsive: true, scrollZoom: true });
        return;
      }

      const candles = candleData.candles;
      // Format times to show only HH:MM in ET
      const times = candles.map(c => fmtTimeET(c.time));
      const opens = candles.map(c => c.open);
      const highs = candles.map(c => c.high);
      const lows = candles.map(c => c.low);
      const closes = candles.map(c => c.close);

      // Build horizontal lines and labels for key levels
      const shapes = [];
      const annotations = [];
      if (levels.target) {
        shapes.push({ type: 'line', y0: levels.target, y1: levels.target, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#3b82f6', width: 2 } });
        annotations.push({ x: 0.01, y: levels.target, xref: 'paper', yref: 'y', text: 'Tgt ' + Math.round(levels.target), showarrow: false, font: { color: '#3b82f6', size: 9 }, xanchor: 'left', yanchor: 'bottom' });
      }
      if (levels.lis_low) {
        shapes.push({ type: 'line', y0: levels.lis_low, y1: levels.lis_low, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#f59e0b', width: 2 } });
        annotations.push({ x: 0.01, y: levels.lis_low, xref: 'paper', yref: 'y', text: 'LIS ' + Math.round(levels.lis_low), showarrow: false, font: { color: '#f59e0b', size: 9 }, xanchor: 'left', yanchor: 'bottom' });
      }
      if (levels.lis_high) {
        shapes.push({ type: 'line', y0: levels.lis_high, y1: levels.lis_high, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#f59e0b', width: 2 } });
        annotations.push({ x: 0.01, y: levels.lis_high, xref: 'paper', yref: 'y', text: 'LIS ' + Math.round(levels.lis_high), showarrow: false, font: { color: '#f59e0b', size: 9 }, xanchor: 'left', yanchor: 'bottom' });
      }
      if (levels.max_pos_gamma) {
        shapes.push({ type: 'line', y0: levels.max_pos_gamma, y1: levels.max_pos_gamma, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#22c55e', width: 2 } });
        annotations.push({ x: 0.01, y: levels.max_pos_gamma, xref: 'paper', yref: 'y', text: '+G ' + Math.round(levels.max_pos_gamma), showarrow: false, font: { color: '#22c55e', size: 9 }, xanchor: 'left', yanchor: 'bottom' });
      }
      if (levels.max_neg_gamma) {
        shapes.push({ type: 'line', y0: levels.max_neg_gamma, y1: levels.max_neg_gamma, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#ef4444', width: 2 } });
        annotations.push({ x: 0.01, y: levels.max_neg_gamma, xref: 'paper', yref: 'y', text: '-G ' + Math.round(levels.max_neg_gamma), showarrow: false, font: { color: '#ef4444', size: 9 }, xanchor: 'left', yanchor: 'bottom' });
      }

      const trace = {
        type: 'candlestick',
        x: times,
        open: opens,
        high: highs,
        low: lows,
        close: closes,
        increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
        decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
        hoverinfo: 'x+y',
      };

      Plotly.react(unifiedSpxDiv, [trace], {
        margin: { l: 50, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: {
          title: '',
          gridcolor: '#20242a',
          tickfont: { size: 9 },
          rangeslider: { visible: false },
          type: 'category',
          nticks: 8,
          tickangle: -45
        },
        yaxis: {
          title: '',
          side: 'left',
          range: [yRange.min, yRange.max],
          gridcolor: '#20242a',
          tickfont: { size: 9 },
          dtick: 5,
          fixedrange: false
        },
        font: { color: '#e6e7e9', size: 10 },
        showlegend: false,
        shapes: shapes,
        annotations: annotations
      }, { displayModeBar: false, responsive: true, scrollZoom: true });
    }

    // Render Net GEX horizontal bar chart
    function renderUnifiedGex(strikes, netGEX, yRange, spot) {
      if (!strikes.length) return;

      const colors = netGEX.map(v => v >= 0 ? '#22c55e' : '#ef4444');
      const gMax = Math.max(1, ...netGEX.map(v => Math.abs(v))) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -gMax, gMax);
      if (spotShape) shapes.push(spotShape);

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: netGEX,
        y: strikes,
        marker: { color: colors },
        hovertemplate: 'Strike %{y}<br>Net GEX %{x:,.0f}<extra></extra>'
      };

      Plotly.react(unifiedGexDiv, [trace], {
        margin: { l: 8, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', showticklabels: false, dtick: 5, fixedrange: false },
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true, scrollZoom: true });
    }

    // Render Charm horizontal bar chart (from vanna_window)
    function renderUnifiedCharm(vannaData, yRange, spot) {
      if (!vannaData || vannaData.error || !vannaData.points || !vannaData.points.length) {
        Plotly.react(unifiedCharmDiv, [], {
          paper_bgcolor: '#121417', plot_bgcolor: '#0f1115',
          margin: { l: 8, r: 8, t: 4, b: 24 },
          annotations: [{ text: vannaData?.error || 'No charm data', x: 0.5, y: 0.5, xref: 'paper', yref: 'paper', showarrow: false, font: { color: '#e6e7e9' } }],
          font: { color: '#e6e7e9' }
        }, { displayModeBar: false, responsive: true, scrollZoom: true });
        return;
      }

      const pts = vannaData.points;
      const charmStrikes = pts.map(p => p.strike);
      const vanna = pts.map(p => p.vanna);

      const colors = vanna.map(v => v >= 0 ? '#22c55e' : '#ef4444');
      const vMax = Math.max(1, ...vanna.map(v => Math.abs(v))) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -vMax, vMax);
      if (spotShape) shapes.push(spotShape);

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: vanna,
        y: charmStrikes,
        marker: { color: colors },
        hovertemplate: 'Strike %{y}<br>Charm %{x:,.0f}<extra></extra>'
      };

      Plotly.react(unifiedCharmDiv, [trace], {
        margin: { l: 8, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', showticklabels: false, dtick: 5, fixedrange: false },
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true, scrollZoom: true });
    }

    // Render Delta Decay horizontal bar chart (from delta_decay_window)
    function renderUnifiedDeltaDecay(ddData, yRange, spot) {
      if (!ddData || ddData.error || !ddData.points || !ddData.points.length) {
        Plotly.react(unifiedDeltaDecayDiv, [], {
          paper_bgcolor: '#121417', plot_bgcolor: '#0f1115',
          margin: { l: 8, r: 8, t: 4, b: 24 },
          annotations: [{ text: ddData?.error || 'No delta decay data', x: 0.5, y: 0.5, xref: 'paper', yref: 'paper', showarrow: false, font: { color: '#e6e7e9' } }],
          font: { color: '#e6e7e9' }
        }, { displayModeBar: false, responsive: true, scrollZoom: true });
        return;
      }

      const pts = ddData.points;
      const ddStrikes = pts.map(p => p.strike);
      const ddVals = pts.map(p => p.delta_decay);

      const colors = ddVals.map(v => v >= 0 ? '#22c55e' : '#ef4444');
      const vMax = Math.max(1, ...ddVals.map(v => Math.abs(v))) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -vMax, vMax);
      if (spotShape) shapes.push(spotShape);

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: ddVals,
        y: ddStrikes,
        marker: { color: colors },
        hovertemplate: 'Strike %{y}<br>Delta Decay %{x:,.0f}<extra></extra>'
      };

      Plotly.react(unifiedDeltaDecayDiv, [trace], {
        margin: { l: 8, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', showticklabels: false, dtick: 5, fixedrange: false },
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true, scrollZoom: true });
    }

    // Render Volume horizontal bar chart (mirrored: puts left, calls right)
    function renderUnifiedVolume(strikes, callVol, putVol, yRange, spot) {
      if (!strikes.length) return;

      const putsNegative = putVol.map(v => -v);
      const vMax = Math.max(1, ...callVol, ...putVol) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -vMax, vMax);
      if (spotShape) shapes.push(spotShape);

      const traceCalls = {
        type: 'bar',
        orientation: 'h',
        name: 'Calls',
        x: callVol,
        y: strikes,
        marker: { color: '#22c55e' },
        hovertemplate: 'Strike %{y}<br>Calls %{x:,}<extra></extra>'
      };

      const tracePuts = {
        type: 'bar',
        orientation: 'h',
        name: 'Puts',
        x: putsNegative,
        y: strikes,
        marker: { color: '#ef4444' },
        hovertemplate: 'Strike %{y}<br>Puts %{customdata:,}<extra></extra>',
        customdata: putVol
      };

      Plotly.react(unifiedVolDiv, [tracePuts, traceCalls], {
        margin: { l: 8, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, range: [-vMax, vMax], zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', showticklabels: false, dtick: 5, fixedrange: false },
        barmode: 'overlay',
        showlegend: false,
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true, scrollZoom: true });
    }

    // Main tick function for exposure view
    let zoomSyncInitialized = false;
    async function tickUnified() {
      try {
        // Fetch series data (strikes, GEX, volume, spot)
        const data = await fetchSeries();
        if (!data || !data.strikes || !data.strikes.length) return;

        const strikes = data.strikes;
        const spot = data.spot;
        const yRange = computeStrikeRange(strikes, spot);
        if (!yRange) return;

        // Render GEX
        renderUnifiedGex(strikes, data.netGEX || [], yRange, spot);

        // Render Volume
        renderUnifiedVolume(strikes, data.callVol || [], data.putVol || [], yRange, spot);

        // Fetch SPX candles and statistics levels, then render
        Promise.all([fetchSPXCandles(), fetchStatisticsLevels()])
          .then(([candleData, levels]) => renderUnifiedSPX(candleData, yRange, levels))
          .catch(err => renderUnifiedSPX({ error: String(err) }, yRange, {}));

        // Fetch and render Charm (vanna_window) - async
        fetchVannaWindow()
          .then(vannaData => renderUnifiedCharm(vannaData, yRange, spot))
          .catch(err => renderUnifiedCharm({ error: String(err) }, yRange, spot));

        // Fetch and render Delta Decay - async
        fetchDeltaDecayWindow()
          .then(ddData => renderUnifiedDeltaDecay(ddData, yRange, spot))
          .catch(err => renderUnifiedDeltaDecay({ error: String(err) }, yRange, spot));

        // Setup zoom sync after first render
        if (!zoomSyncInitialized) {
          setTimeout(() => {
            [unifiedSpxDiv, unifiedGexDiv, unifiedCharmDiv, unifiedDeltaDecayDiv, unifiedVolDiv].forEach(setupZoomSync);
            zoomSyncInitialized = true;
          }, 500);
        }

      } catch (err) {
        console.error('Exposure view error:', err);
      }
    }

    let exposureInitialized = false;
    function startSpot() {
      if (!exposureInitialized) {
        setupStrikeButtons();
        exposureInitialized = true;
      }
      tickUnified();
      if (unifiedTimer) clearInterval(unifiedTimer);
      unifiedTimer = setInterval(tickUnified, PULL_EVERY);
    }

    function stopSpot() {
      if (unifiedTimer) {
        clearInterval(unifiedTimer);
        unifiedTimer = null;
      }
      stopStatistics();
    }

    // ===== Statistics View =====
    const statisticsPlot = document.getElementById('statisticsPlot');
    let statisticsTimer = null;

    async function fetchStatisticsData() {
      const [candlesRes, levelsRes] = await Promise.all([
        fetch('/api/spx_candles_1m?bars=200', {cache: 'no-store'}),
        fetch('/api/statistics_levels', {cache: 'no-store'})
      ]);
      const candles = await candlesRes.json();
      const levels = await levelsRes.json();
      return { candles: candles.candles || [], levels };
    }

    async function drawStatisticsChart() {
      try {
        const data = await fetchStatisticsData();
        const candles = data.candles;
        const levels = data.levels;

        if (!candles.length) {
          console.log('No candle data for Statistics');
          return;
        }

        // Prepare candlestick data - format times as HH:MM for category axis (no gaps)
        const times = candles.map(c => c.time.slice(11, 16));
        const opens = candles.map(c => c.open);
        const highs = candles.map(c => c.high);
        const lows = candles.map(c => c.low);
        const closes = candles.map(c => c.close);

        // Calculate Y-axis range based on levels + margin
        const levelValues = [
          levels.target,
          levels.lis_low,
          levels.lis_high,
          levels.max_pos_gamma,
          levels.max_neg_gamma
        ].filter(v => v !== null && v !== undefined);

        // Also include price range from candles
        const priceMin = Math.min(...lows);
        const priceMax = Math.max(...highs);
        levelValues.push(priceMin, priceMax);

        const yMin = Math.min(...levelValues) - 10;  // 2 strikes margin (5 each)
        const yMax = Math.max(...levelValues) + 10;

        // Build horizontal lines for levels
        const shapes = [];
        const annotations = [];

        // Target line (blue)
        if (levels.target) {
          shapes.push({
            type: 'line', y0: levels.target, y1: levels.target, x0: 0, x1: 1,
            xref: 'paper', yref: 'y', line: { color: '#3b82f6', width: 2 }
          });
          annotations.push({
            x: 1.01, y: levels.target, xref: 'paper', yref: 'y', text: 'Target ' + levels.target,
            showarrow: false, font: { color: '#3b82f6', size: 10 }, xanchor: 'left'
          });
        }

        // LIS Low line (amber)
        if (levels.lis_low) {
          shapes.push({
            type: 'line', y0: levels.lis_low, y1: levels.lis_low, x0: 0, x1: 1,
            xref: 'paper', yref: 'y', line: { color: '#f59e0b', width: 2 }
          });
          annotations.push({
            x: 1.01, y: levels.lis_low, xref: 'paper', yref: 'y', text: 'LIS ' + levels.lis_low,
            showarrow: false, font: { color: '#f59e0b', size: 10 }, xanchor: 'left'
          });
        }

        // LIS High line (amber)
        if (levels.lis_high) {
          shapes.push({
            type: 'line', y0: levels.lis_high, y1: levels.lis_high, x0: 0, x1: 1,
            xref: 'paper', yref: 'y', line: { color: '#f59e0b', width: 2 }
          });
          annotations.push({
            x: 1.01, y: levels.lis_high, xref: 'paper', yref: 'y', text: 'LIS ' + levels.lis_high,
            showarrow: false, font: { color: '#f59e0b', size: 10 }, xanchor: 'left'
          });
        }

        // Max positive gamma (green)
        if (levels.max_pos_gamma) {
          shapes.push({
            type: 'line', y0: levels.max_pos_gamma, y1: levels.max_pos_gamma, x0: 0, x1: 1,
            xref: 'paper', yref: 'y', line: { color: '#22c55e', width: 2 }
          });
          annotations.push({
            x: 1.01, y: levels.max_pos_gamma, xref: 'paper', yref: 'y', text: '+G ' + levels.max_pos_gamma,
            showarrow: false, font: { color: '#22c55e', size: 10 }, xanchor: 'left'
          });
        }

        // Max negative gamma (red)
        if (levels.max_neg_gamma) {
          shapes.push({
            type: 'line', y0: levels.max_neg_gamma, y1: levels.max_neg_gamma, x0: 0, x1: 1,
            xref: 'paper', yref: 'y', line: { color: '#ef4444', width: 2 }
          });
          annotations.push({
            x: 1.01, y: levels.max_neg_gamma, xref: 'paper', yref: 'y', text: '-G ' + levels.max_neg_gamma,
            showarrow: false, font: { color: '#ef4444', size: 10 }, xanchor: 'left'
          });
        }

        const trace = {
          type: 'candlestick',
          x: times,
          open: opens,
          high: highs,
          low: lows,
          close: closes,
          increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
          decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
          hoverinfo: 'x+y'
        };

        Plotly.react(statisticsPlot, [trace], {
          margin: { l: 50, r: 80, t: 20, b: 50 },
          paper_bgcolor: '#121417',
          plot_bgcolor: '#0f1115',
          xaxis: {
            type: 'category',
            gridcolor: '#20242a',
            tickfont: { size: 9 },
            rangeslider: { visible: false },
            nticks: 12
          },
          yaxis: {
            gridcolor: '#20242a',
            tickfont: { size: 10 },
            side: 'left',
            range: [yMin, yMax],
            dtick: 5
          },
          font: { color: '#e6e7e9', size: 10 },
          shapes: shapes,
          annotations: annotations,
          dragmode: 'zoom'
        }, {
          displayModeBar: true,
          displaylogo: false,
          modeBarButtonsToRemove: ['lasso2d', 'select2d'],
          responsive: true,
          scrollZoom: true
        });

      } catch (err) {
        console.error('Statistics view error:', err);
      }
    }

    function startStatistics() {
      drawStatisticsChart();
      if (!statisticsTimer) {
        statisticsTimer = setInterval(drawStatisticsChart, 30000);  // Update every 30s
      }
    }

    function stopStatistics() {
      if (statisticsTimer) {
        clearInterval(statisticsTimer);
        statisticsTimer = null;
      }
    }

    // ===== Playback View =====
    const playbackDateInput = document.getElementById('playbackDate'),
          playbackLoadBtn = document.getElementById('playbackLoad'),
          playbackExportFullBtn = document.getElementById('playbackExportFull'),
          playbackExportSummaryBtn = document.getElementById('playbackExportSummary'),
          playbackSlider = document.getElementById('playbackSlider'),
          playbackTimestamp = document.getElementById('playbackTimestamp'),
          playbackStats = document.getElementById('playbackStats'),
          playbackSliderStart = document.getElementById('playbackSliderStart'),
          playbackSliderEnd = document.getElementById('playbackSliderEnd'),
          playbackGexPlot = document.getElementById('playbackGexPlot'),
          playbackCharmPlot = document.getElementById('playbackCharmPlot'),
          playbackDDPlot = document.getElementById('playbackDDPlot'),
          playbackVolPlot = document.getElementById('playbackVolPlot'),
          playbackSummaryPlot = document.getElementById('playbackSummaryPlot'),
          playbackSummaryStats = document.getElementById('playbackSummaryStats');

    let playbackData = null;
    let playbackInitialized = false;
    let playbackDays = 7;

    function initPlayback() {
      if (playbackInitialized) return;
      playbackInitialized = true;

      // Default date: 7 days ago
      setPlaybackDays(7);

      playbackLoadBtn.addEventListener('click', loadPlaybackData);
      playbackExportFullBtn.addEventListener('click', exportPlaybackFullCSV);
      playbackExportSummaryBtn.addEventListener('click', exportPlaybackSummaryCSV);
      playbackSlider.addEventListener('input', onSliderChange);

      // Range buttons (1D, 3D, 7D)
      document.querySelectorAll('.playback-range-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          setPlaybackDays(parseInt(btn.dataset.days));
          loadPlaybackData();
        });
      });
    }

    function setPlaybackDays(days) {
      playbackDays = days;
      const d = new Date();
      d.setDate(d.getDate() - days);
      playbackDateInput.value = d.toISOString().split('T')[0];
      document.querySelectorAll('.playback-range-btn').forEach(b => {
        b.classList.toggle('active', parseInt(b.dataset.days) === days);
      });
    }

    async function loadPlaybackData() {
      const startDate = playbackDateInput.value;

      playbackTimestamp.textContent = 'Loading...';
      playbackLoadBtn.disabled = true;

      try {
        let url = '/api/playback/range';
        if (startDate) {
          url += '?start_date=' + startDate;
        }
        const r = await fetch(url, { cache: 'no-store' });
        const data = await r.json();

        if (data.error) {
          playbackTimestamp.textContent = 'Error: ' + data.error;
          return;
        }

        if (!data.snapshots || data.snapshots.length === 0) {
          playbackTimestamp.textContent = 'No data found for this period. Data collection starts when market is open.';
          return;
        }

        playbackData = data;
        _playbackSummaryDrawn = false;  // Reset so chart gets fresh initial render
        playbackSlider.max = data.snapshots.length - 1;
        playbackSlider.value = 0;

        // Update slider labels (ET timezone)
        playbackSliderStart.textContent = fmtDateTimeShortET(data.snapshots[0].ts) + ' ET';
        playbackSliderEnd.textContent = fmtDateTimeShortET(data.snapshots[data.snapshots.length - 1].ts) + ' ET';

        // Render both views: summary chart on top + detail cards below
        drawPlaybackSummaryChart(0);
        updatePlaybackSummaryStats(0);
        updatePlaybackSnapshot(0);

        playbackTimestamp.textContent = 'Loaded ' + data.count + ' snapshots. Drag slider to scrub through time.';
      } catch (err) {
        playbackTimestamp.textContent = 'Error: ' + err.message;
      } finally {
        playbackLoadBtn.disabled = false;
      }
    }

    function exportPlaybackFullCSV() {
      // Export full data with all strikes
      window.location.href = '/api/export/playback?load_all=true';
    }

    function exportPlaybackSummaryCSV() {
      // Export summary data (1 row per timestamp)
      window.location.href = '/api/export/playback_summary?load_all=true';
    }

    function onSliderChange() {
      if (!playbackData) return;
      const idx = parseInt(playbackSlider.value);
      drawPlaybackSummaryChart(idx);
      updatePlaybackSummaryStats(idx);
      updatePlaybackSnapshot(idx);
    }

    function updatePlaybackSnapshot(idx) {
      if (!playbackData || idx >= playbackData.snapshots.length) return;

      const snap = playbackData.snapshots[idx];

      // Get Y range from strikes
      const strikes = snap.strikes || [];
      if (!strikes.length) return;

      const yMin = Math.min(...strikes) - 2;
      const yMax = Math.max(...strikes) + 2;
      const yRange = { min: yMin, max: yMax };

      // Draw GEX
      drawPlaybackBarChart(playbackGexPlot, strikes, snap.net_gex || [], yRange, snap.spot, 'Net GEX');

      // Draw Charm
      drawPlaybackBarChart(playbackCharmPlot, strikes, snap.charm || [], yRange, snap.spot, 'Charm');

      // Draw Delta Decay
      drawPlaybackBarChart(playbackDDPlot, strikes, snap.delta_decay || [], yRange, snap.spot, 'Delta Decay');

      // Draw Volume (calls vs puts)
      drawPlaybackVolumeChart(playbackVolPlot, strikes, snap.call_vol || [], snap.put_vol || [], yRange, snap.spot);
    }

    function drawPlaybackBarChart(div, strikes, values, yRange, spot, label) {
      if (!strikes.length) return;

      const colors = values.map(v => v >= 0 ? '#22c55e' : '#ef4444');
      const vMax = Math.max(1, ...values.map(v => Math.abs(v))) * 1.1;

      const shapes = [];
      if (spot) {
        shapes.push({
          type: 'line', y0: spot, y1: spot, x0: -vMax, x1: vMax,
          xref: 'x', yref: 'y',
          line: { color: '#60a5fa', width: 2, dash: 'dot' }
        });
      }

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: values,
        y: strikes,
        marker: { color: colors },
        hovertemplate: 'Strike %{y}<br>' + label + ': %{x:,.0f}<extra></extra>'
      };

      Plotly.react(div, [trace], {
        margin: { l: 8, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { gridcolor: '#20242a', tickfont: { size: 9 }, zeroline: true, zerolinecolor: '#333' },
        yaxis: { range: [yRange.min, yRange.max], gridcolor: '#20242a', showticklabels: false, dtick: 5 },
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true });
    }

    function drawPlaybackVolumeChart(div, strikes, callVol, putVol, yRange, spot) {
      if (!strikes.length) return;

      const putsNegative = putVol.map(v => -v);
      const vMax = Math.max(1, ...callVol, ...putVol) * 1.1;

      const shapes = [];
      if (spot) {
        shapes.push({
          type: 'line', y0: spot, y1: spot, x0: -vMax, x1: vMax,
          xref: 'x', yref: 'y',
          line: { color: '#60a5fa', width: 2, dash: 'dot' }
        });
      }

      const traceCalls = {
        type: 'bar', orientation: 'h', name: 'Calls',
        x: callVol, y: strikes,
        marker: { color: '#22c55e' },
        hovertemplate: 'Strike %{y}<br>Calls: %{x:,}<extra></extra>'
      };

      const tracePuts = {
        type: 'bar', orientation: 'h', name: 'Puts',
        x: putsNegative, y: strikes,
        marker: { color: '#ef4444' },
        hovertemplate: 'Strike %{y}<br>Puts: %{customdata:,}<extra></extra>',
        customdata: putVol
      };

      Plotly.react(div, [tracePuts, traceCalls], {
        margin: { l: 8, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { gridcolor: '#20242a', tickfont: { size: 9 }, range: [-vMax, vMax], zeroline: true, zerolinecolor: '#333' },
        yaxis: { range: [yRange.min, yRange.max], gridcolor: '#20242a', showticklabels: false, dtick: 5 },
        barmode: 'overlay',
        showlegend: false,
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true });
    }

    // ===== Playback Summary View Functions =====
    let _playbackSummaryDrawn = false;  // true after initial Plotly.react
    let _playbackSummaryTimes = [];     // cached time labels for relayout

    // Build shapes + annotations for a given snapshot index
    function _buildPlaybackOverlay(idx) {
      const snaps = playbackData.snapshots;
      const snap = snaps[idx];
      const stats = snap.stats || {};
      const gexData = snap.net_gex || [];
      const strikes = snap.strikes || [];

      // Find max +GEX and -GEX strikes
      let maxPosGexStrike = null, maxNegGexStrike = null;
      let maxPosVal = 0, maxNegVal = 0;
      for (let i = 0; i < strikes.length && i < gexData.length; i++) {
        if (gexData[i] > maxPosVal) { maxPosVal = gexData[i]; maxPosGexStrike = strikes[i]; }
        if (gexData[i] < maxNegVal) { maxNegVal = gexData[i]; maxNegGexStrike = strikes[i]; }
      }

      // Parse target
      let target = null;
      if (stats.target) {
        const tMatch = String(stats.target).replace(/[$,]/g, '').match(/([\d.]+)/);
        if (tMatch) target = parseFloat(tMatch[1]);
      }

      // Parse LIS
      let lisLow = null, lisHigh = null;
      if (stats.lis) {
        const lisStr = String(stats.lis).replace(/[$,]/g, '');
        const dashMatch = lisStr.match(/([\d.]+)\s*[-–]\s*([\d.]+)/);
        if (dashMatch) {
          lisLow = parseFloat(dashMatch[1]);
          lisHigh = parseFloat(dashMatch[2]);
        } else {
          const slashMatch = lisStr.match(/([\d.]+)\s*\/\s*([\d.]+)/);
          if (slashMatch) {
            lisLow = parseFloat(slashMatch[1]);
            lisHigh = parseFloat(slashMatch[2]);
          } else {
            const single = lisStr.match(/([\d.]+)/);
            if (single) lisLow = parseFloat(single[1]);
          }
        }
      }

      // Day open price for this snapshot's date
      const currentDateET = fmtDateET(snap.ts);
      let dayOpenPrice = snap.spot;
      for (let i = 0; i < snaps.length; i++) {
        if (fmtDateET(snaps[i].ts) === currentDateET) {
          dayOpenPrice = snaps[i].spot;
          break;
        }
      }

      const shapes = [];
      const annotations = [];

      // Day Open (gray dashed)
      shapes.push({ type: 'line', y0: dayOpenPrice, y1: dayOpenPrice, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#9ca3af', width: 1, dash: 'dash' } });
      annotations.push({ x: 1.01, y: dayOpenPrice, xref: 'paper', yref: 'y', text: 'Open ' + Math.round(dayOpenPrice), showarrow: false, font: { color: '#9ca3af', size: 10 }, xanchor: 'left' });

      // Target (blue)
      if (target) {
        shapes.push({ type: 'line', y0: target, y1: target, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#3b82f6', width: 2 } });
        annotations.push({ x: 1.01, y: target, xref: 'paper', yref: 'y', text: 'Tgt ' + Math.round(target), showarrow: false, font: { color: '#3b82f6', size: 10 }, xanchor: 'left' });
      }

      // LIS (amber)
      if (lisLow) {
        shapes.push({ type: 'line', y0: lisLow, y1: lisLow, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#f59e0b', width: 2 } });
        annotations.push({ x: 1.01, y: lisLow, xref: 'paper', yref: 'y', text: 'LIS ' + Math.round(lisLow), showarrow: false, font: { color: '#f59e0b', size: 10 }, xanchor: 'left' });
      }
      if (lisHigh && lisHigh !== lisLow) {
        shapes.push({ type: 'line', y0: lisHigh, y1: lisHigh, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#f59e0b', width: 2 } });
        annotations.push({ x: 1.01, y: lisHigh, xref: 'paper', yref: 'y', text: 'LIS ' + Math.round(lisHigh), showarrow: false, font: { color: '#f59e0b', size: 10 }, xanchor: 'left' });
      }

      // Max +GEX (green)
      if (maxPosGexStrike) {
        shapes.push({ type: 'line', y0: maxPosGexStrike, y1: maxPosGexStrike, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#22c55e', width: 2 } });
        annotations.push({ x: 1.01, y: maxPosGexStrike, xref: 'paper', yref: 'y', text: '+G ' + maxPosGexStrike, showarrow: false, font: { color: '#22c55e', size: 10 }, xanchor: 'left' });
      }

      // Max -GEX (red)
      if (maxNegGexStrike) {
        shapes.push({ type: 'line', y0: maxNegGexStrike, y1: maxNegGexStrike, x0: 0, x1: 1, xref: 'paper', yref: 'y', line: { color: '#ef4444', width: 2 } });
        annotations.push({ x: 1.01, y: maxNegGexStrike, xref: 'paper', yref: 'y', text: '-G ' + maxNegGexStrike, showarrow: false, font: { color: '#ef4444', size: 10 }, xanchor: 'left' });
      }

      // Current position marker (red vertical)
      const xLabel = _playbackSummaryTimes[idx];
      shapes.push({ type: 'line', x0: xLabel, x1: xLabel, y0: 0, y1: 1, xref: 'x', yref: 'paper', line: { color: '#ef4444', width: 2, dash: 'solid' } });

      return { shapes, annotations, dayOpenPrice };
    }

    // Initial draw: builds candlestick + sets Y-range once
    function drawPlaybackSummaryChart(idx) {
      if (!playbackData || !playbackData.snapshots.length) return;

      // On slider scrub (chart already drawn): only update overlays, don't touch axes
      if (_playbackSummaryDrawn) {
        _updatePlaybackSummaryOverlay(idx);
        return;
      }

      const snaps = playbackData.snapshots;

      // Build candlestick data (once — cached in Plotly)
      const times = [];
      const opens = [];
      const highs = [];
      const lows = [];
      const closes = [];

      for (let i = 0; i < snaps.length; i++) {
        const curr = snaps[i].spot;
        const prev = i > 0 ? snaps[i - 1].spot : curr;
        times.push(fmtDateTimeShortET(snaps[i].ts));
        opens.push(prev);
        closes.push(curr);
        highs.push(Math.max(prev, curr) + Math.abs(curr - prev) * 0.1);
        lows.push(Math.min(prev, curr) - Math.abs(curr - prev) * 0.1);
      }
      _playbackSummaryTimes = times;

      // Initial Y-range: centered on first day's open ±50
      const overlay = _buildPlaybackOverlay(idx);
      const yMin = overlay.dayOpenPrice - 50;
      const yMax = overlay.dayOpenPrice + 50;

      const trace = {
        type: 'candlestick',
        x: times,
        open: opens,
        high: highs,
        low: lows,
        close: closes,
        increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
        decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
        hoverinfo: 'x+y'
      };

      Plotly.react(playbackSummaryPlot, [trace], {
        margin: { l: 50, r: 80, t: 20, b: 50 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: {
          type: 'category',
          gridcolor: '#20242a',
          tickfont: { size: 9 },
          rangeslider: { visible: false },
          nticks: 12
        },
        yaxis: {
          gridcolor: '#20242a',
          tickfont: { size: 10 },
          side: 'left',
          range: [yMin, yMax],
          dtick: 5
        },
        font: { color: '#e6e7e9', size: 10 },
        shapes: overlay.shapes,
        annotations: overlay.annotations,
        dragmode: 'zoom'
      }, {
        displayModeBar: true,
        displaylogo: false,
        modeBarButtonsToRemove: ['lasso2d', 'select2d'],
        responsive: true,
        scrollZoom: true
      });

      _playbackSummaryDrawn = true;

      // Update text displays
      _updatePlaybackSummaryText(snaps[idx]);
    }

    // Slider scrub: only update shapes/annotations (preserves user zoom/autoscale)
    function _updatePlaybackSummaryOverlay(idx) {
      const snap = playbackData.snapshots[idx];
      const overlay = _buildPlaybackOverlay(idx);

      Plotly.relayout(playbackSummaryPlot, {
        shapes: overlay.shapes,
        annotations: overlay.annotations
      });

      _updatePlaybackSummaryText(snap);
    }

    // Update timestamp + stats bar text
    function _updatePlaybackSummaryText(snap) {
      playbackTimestamp.textContent = fmtDateET(snap.ts) + ' ' + fmtTimeFullET(snap.ts) + ' ET | SPX: ' + (snap.spot ? snap.spot.toFixed(2) : 'N/A');
      if (snap.stats) {
        const s = snap.stats;
        let statsHtml = '';
        if (s.paradigm) statsHtml += 'Paradigm: ' + s.paradigm + ' | ';
        if (s.target) statsHtml += 'Target: ' + s.target + ' | ';
        if (s.lis) statsHtml += 'LIS: ' + s.lis + ' | ';
        if (s.dd_hedging) statsHtml += 'DD: ' + s.dd_hedging;
        playbackStats.textContent = statsHtml;
      } else {
        playbackStats.textContent = '';
      }
    }

    function updatePlaybackSummaryStats(idx) {
      if (!playbackData || idx >= playbackData.snapshots.length) return;

      const snap = playbackData.snapshots[idx];
      const stats = snap.stats || {};
      const gexData = snap.net_gex || [];
      const strikes = snap.strikes || [];

      // Find max +GEX and -GEX
      let maxPosGexStrike = null, maxNegGexStrike = null;
      let maxPosVal = 0, maxNegVal = 0;
      for (let i = 0; i < strikes.length && i < gexData.length; i++) {
        if (gexData[i] > maxPosVal) { maxPosVal = gexData[i]; maxPosGexStrike = strikes[i]; }
        if (gexData[i] < maxNegVal) { maxNegVal = gexData[i]; maxNegGexStrike = strikes[i]; }
      }

      // Calculate total volumes
      const callVol = (snap.call_vol || []).reduce((a, b) => a + b, 0);
      const putVol = (snap.put_vol || []).reduce((a, b) => a + b, 0);
      const totalVol = callVol + putVol;

      let html = '<div style="display:flex;flex-direction:column;gap:6px">';

      // SPX Spot
      html += '<div style="padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">SPX Spot</div>';
      html += '<div style="font-size:16px;font-weight:600;color:var(--text)">' + (snap.spot ? snap.spot.toFixed(2) : 'N/A') + '</div>';
      html += '</div>';

      // Paradigm
      html += '<div style="padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">Paradigm</div>';
      html += '<div style="font-size:12px;color:var(--text)">' + (stats.paradigm || 'N/A') + '</div>';
      html += '</div>';

      // Target + LIS side by side
      html += '<div style="display:flex;gap:6px">';
      html += '<div style="flex:1;padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">Target</div>';
      html += '<div style="font-size:12px;color:#3b82f6">' + (stats.target || 'N/A') + '</div>';
      html += '</div>';
      html += '<div style="flex:1;padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">LIS</div>';
      html += '<div style="font-size:12px;color:#f59e0b">' + (stats.lis || 'N/A') + '</div>';
      html += '</div>';
      html += '</div>';

      // Max +GEX / -GEX side by side
      html += '<div style="display:flex;gap:6px">';
      html += '<div style="flex:1;padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">Max +GEX</div>';
      html += '<div style="font-size:12px;color:#22c55e">' + (maxPosGexStrike || 'N/A') + '</div>';
      html += '</div>';
      html += '<div style="flex:1;padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">Max -GEX</div>';
      html += '<div style="font-size:12px;color:#ef4444">' + (maxNegGexStrike || 'N/A') + '</div>';
      html += '</div>';
      html += '</div>';

      // DD Hedging
      html += '<div style="padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">DD Hedging</div>';
      const ddHedging = stats.dd_hedging || 'N/A';
      const ddColor = ddHedging.includes('-') ? '#ef4444' : '#22c55e';
      html += '<div style="font-size:12px;color:' + ddColor + '">' + ddHedging + '</div>';
      html += '</div>';

      // 0DTE Volume
      html += '<div style="padding:6px 8px;background:#1a1d21;border-radius:5px">';
      html += '<div style="font-size:9px;color:var(--muted);margin-bottom:2px">0DTE Volume</div>';
      html += '<div style="font-size:12px;color:var(--text)">' + totalVol.toLocaleString() + '</div>';
      html += '<div style="font-size:9px;color:var(--muted)">C: ' + callVol.toLocaleString() + ' | P: ' + putVol.toLocaleString() + '</div>';
      html += '</div>';

      html += '</div>';
      playbackSummaryStats.innerHTML = html;
    }

    // ===== ES Delta (Range Bars) =====
    let esDeltaInterval = null;
    let esDeltaLiveMode = true;       // true = auto-scale to latest, false = user has zoomed/panned
    let esDeltaUserRanges = null;     // saved axis ranges when user interacts
    const esDeltaPlot = document.getElementById('esDeltaPlot');
    const esDeltaStatus = document.getElementById('esDeltaStatus');
    const esDeltaLiveBtn = document.getElementById('esDeltaLive');

    function _esDeltaSetLive(live) {
      esDeltaLiveMode = live;
      if (live) {
        esDeltaUserRanges = null;
        esDeltaLiveBtn.style.background = '#22c55e';
        esDeltaLiveBtn.style.color = '#000';
        esDeltaLiveBtn.textContent = 'LIVE';
      } else {
        esDeltaLiveBtn.style.background = '#333';
        esDeltaLiveBtn.style.color = '#aaa';
        esDeltaLiveBtn.textContent = 'PAUSED';
      }
    }

    // Click Live button to re-enable auto-scale
    esDeltaLiveBtn.addEventListener('click', () => { _esDeltaSetLive(true); drawEsDelta(); });

    // Detect user zoom/pan via plotly_relayout
    let esDeltaPlotReady = false;
    function _esDeltaAttachRelayout() {
      if (esDeltaPlotReady) return;
      esDeltaPlotReady = true;
      esDeltaPlot.on('plotly_relayout', (ev) => {
        if (!ev) return;
        // Ignore relayout events triggered by our own Plotly.react
        if (ev['_fromDraw']) return;
        // Check if user changed any axis range
        const keys = Object.keys(ev);
        const userChanged = keys.some(k => k.match(/^[xy]axis\d*\.range/) || k === 'xaxis.range[0]' || k === 'xaxis.range[1]');
        if (userChanged) {
          _esDeltaSetLive(false);
          // Save current ranges from the plot
          const la = esDeltaPlot.layout;
          esDeltaUserRanges = {
            x: la.xaxis && la.xaxis.range ? [...la.xaxis.range] : null,
            y: la.yaxis && la.yaxis.range ? [...la.yaxis.range] : null,
            y2: la.yaxis2 && la.yaxis2.range ? [...la.yaxis2.range] : null,
            y3: la.yaxis3 && la.yaxis3.range ? [...la.yaxis3.range] : null,
            y4: la.yaxis4 && la.yaxis4.range ? [...la.yaxis4.range] : null,
          };
        }
      });
    }

    function stopEsDelta() {
      if (esDeltaInterval) { clearInterval(esDeltaInterval); esDeltaInterval = null; }
    }
    function startEsDelta() {
      stopEsDelta();
      _esDeltaSetLive(true);
      esDeltaPlotReady = false;
      drawEsDelta();
      esDeltaInterval = setInterval(drawEsDelta, 5000);
    }
    async function drawEsDelta() {
      try {
        const [r, levelsR] = await Promise.all([
          fetch('/api/es/delta/rangebars?range=5', {cache:'no-store'}),
          fetch('/api/statistics_levels', {cache:'no-store'}).catch(() => null),
        ]);
        const raw = await r.json();
        if (raw.error) { esDeltaStatus.textContent = raw.error; return; }
        // Handle both {bars, signals} and legacy array responses
        const bars = raw.bars || raw;
        const signals = raw.signals || [];
        const levels = levelsR ? await levelsR.json().catch(() => null) : null;
        if (!bars.length) { esDeltaStatus.textContent = 'No data'; return; }

        const n = bars.length;
        const xs = bars.map(b => b.idx);
        // Tick labels: show time of bar start
        const tickTexts = bars.map(b => fmtTimeET(b.ts_start));
        const opens = bars.map(b => b.open);
        const highs = bars.map(b => b.high);
        const lows = bars.map(b => b.low);
        const closes = bars.map(b => b.close);

        // Colors per bar
        const candleColors = bars.map(b => b.close >= b.open ? '#22c55e' : '#ef4444');
        const volColors = bars.map(b => b.close >= b.open ? 'rgba(34,197,94,0.6)' : 'rgba(239,68,68,0.6)');
        const deltaColors = bars.map(b => b.delta >= 0 ? 'rgba(34,197,94,0.7)' : 'rgba(239,68,68,0.7)');

        // Build hover text with timestamp for all traces
        const hoverTexts = bars.map(b => {
          const t = fmtTimeET(b.ts_start);
          return t+' | O:'+b.open.toFixed(2)+' H:'+b.high.toFixed(2)+
                 ' L:'+b.low.toFixed(2)+' C:'+b.close.toFixed(2)+
                 '<br>Vol:'+b.volume.toLocaleString()+' Delta:'+b.delta.toLocaleString()+
                 ' CVD:'+b.cvd.toLocaleString();
        });

        // Trace 1: Candlestick (price)
        const traceCandle = {
          x: xs, open: opens, high: highs, low: lows, close: closes,
          type: 'candlestick', yaxis: 'y',
          increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
          decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
          text: hoverTexts, hoverinfo: 'text',
          name: 'Price',
        };

        // Trace 2: Volume bars
        const traceVol = {
          x: xs, y: bars.map(b => b.volume), type: 'bar', yaxis: 'y2',
          marker: { color: volColors }, name: 'Volume',
          text: bars.map((b,i) => fmtTimeET(b.ts_start)+' | Vol: '+b.volume.toLocaleString()), hoverinfo: 'text',
        };

        // Trace 3: Delta bars
        const traceDelta = {
          x: xs, y: bars.map(b => b.delta), type: 'bar', yaxis: 'y3',
          marker: { color: deltaColors }, name: 'Delta',
          text: bars.map((b,i) => fmtTimeET(b.ts_start)+' | Delta: '+b.delta.toLocaleString()), hoverinfo: 'text',
        };

        // Trace 4: CVD candles
        const traceCVD = {
          x: xs,
          open: bars.map(b => b.cvd_open), high: bars.map(b => b.cvd_high),
          low: bars.map(b => b.cvd_low), close: bars.map(b => b.cvd_close),
          type: 'candlestick', yaxis: 'y4', name: 'CVD',
          increasing: { line: { color: '#06b6d4' }, fillcolor: '#06b6d4' },
          decreasing: { line: { color: '#f97316' }, fillcolor: '#f97316' },
          text: bars.map((b,i) => fmtTimeET(b.ts_start)+' | CVD: '+b.cvd.toLocaleString()), hoverinfo: 'text',
        };

        // Show every ~10th tick label to avoid overlap
        const tickStep = Math.max(1, Math.floor(n / 15));
        const tickVals = xs.filter((_, i) => i % tickStep === 0);
        const tickLabels = tickTexts.filter((_, i) => i % tickStep === 0);

        // Right padding: extend x range 8 bars beyond last data point
        const xPad = 8;
        const xRangeMax = n - 1 + xPad;
        // Default view: last 50% of bars for better readability
        const xRangeMin = Math.max(-0.5, Math.floor(n / 2) - 0.5);

        // If user has zoomed/panned, preserve their view; otherwise auto-scale
        const useUserRange = !esDeltaLiveMode && esDeltaUserRanges;

        const layout = {
          paper_bgcolor: '#121417', plot_bgcolor: '#0f1115',
          font: { color: '#e6e7e9', size: 10 },
          margin: { l: 10, r: 60, t: 20, b: 30 },
          xaxis: {
            gridcolor: '#1a1d21', tickfont: { size: 9 },
            rangeslider: { visible: false },
            tickvals: tickVals, ticktext: tickLabels,
            range: useUserRange && esDeltaUserRanges.x ? esDeltaUserRanges.x : [xRangeMin, xRangeMax],
            fixedrange: false,
          },
          yaxis:  { domain: [0.42, 1.0],  side: 'right', gridcolor: '#1a1d21', tickformat: '.2f', fixedrange: false },
          yaxis2: { domain: [0.28, 0.40], side: 'right', gridcolor: '#1a1d21', title: '', showticklabels: true, tickfont: {size:9}, fixedrange: false },
          yaxis3: { domain: [0.14, 0.26], side: 'right', gridcolor: '#1a1d21', zeroline: true, zerolinecolor: '#555', showticklabels: true, tickfont: {size:9}, fixedrange: false },
          yaxis4: { domain: [0.0, 0.12],  side: 'right', gridcolor: '#1a1d21', showticklabels: true, tickfont: {size:9}, fixedrange: false },
          hovermode: 'x unified',
          dragmode: 'pan',
          showlegend: false,
        };

        // Preserve y-axis ranges if user has zoomed
        if (useUserRange) {
          if (esDeltaUserRanges.y) layout.yaxis.range = esDeltaUserRanges.y;
          if (esDeltaUserRanges.y2) layout.yaxis2.range = esDeltaUserRanges.y2;
          if (esDeltaUserRanges.y3) layout.yaxis3.range = esDeltaUserRanges.y3;
          if (esDeltaUserRanges.y4) layout.yaxis4.range = esDeltaUserRanges.y4;
        }

        // Panel label annotations
        layout.annotations = [
          { text: 'Price', xref: 'paper', yref: 'paper', x: 0.01, y: 0.99, showarrow: false, font: {size:10, color:'#888'} },
          { text: 'Volume', xref: 'paper', yref: 'paper', x: 0.01, y: 0.39, showarrow: false, font: {size:10, color:'#888'} },
          { text: 'Delta', xref: 'paper', yref: 'paper', x: 0.01, y: 0.25, showarrow: false, font: {size:10, color:'#888'} },
          { text: 'CVD', xref: 'paper', yref: 'paper', x: 0.01, y: 0.115, showarrow: false, font: {size:10, color:'#888'} },
        ];

        // Last price label on the y-axis (outside plot area)
        const lastBar = bars[n-1];
        const lastColor = lastBar.close >= lastBar.open ? '#22c55e' : '#ef4444';
        layout.annotations.push({
          text: ' '+lastBar.close.toFixed(2)+' ',
          xref: 'paper', yref: 'y', x: 1.005, y: lastBar.close, xanchor: 'left',
          showarrow: false, font: {size:10, color: '#fff'},
          bgcolor: lastColor, borderpad: 3, bordercolor: lastColor,
        });
        // Dashed horizontal line at last price
        layout.shapes = (layout.shapes || []).concat({
          type: 'line', xref: 'paper', yref: 'y',
          x0: 0, x1: 1, y0: lastBar.close, y1: lastBar.close,
          line: { color: lastColor, width: 1, dash: 'dot' },
        });

        // SPX key levels converted to ES prices
        if (levels && levels.spot) {
          const offset = lastBar.close - levels.spot;
          const lvlDefs = [
            ['target',        '#3b82f6', 'Tgt'],
            ['lis_low',       '#f59e0b', 'LIS'],
            ['lis_high',      '#f59e0b', 'LIS'],
            ['max_pos_gamma', '#22c55e', '+G'],
            ['max_neg_gamma', '#ef4444', '-G'],
          ];
          lvlDefs.forEach(([key, color, label]) => {
            if (!levels[key]) return;
            const esLvl = levels[key] + offset;
            layout.shapes.push({
              type: 'line', y0: esLvl, y1: esLvl, x0: 0, x1: 1,
              xref: 'paper', yref: 'y',
              line: { color: color, width: 1.5, dash: 'dash' },
            });
            layout.annotations.push({
              x: 0.01, y: esLvl, xref: 'paper', yref: 'y',
              text: label + ' ' + Math.round(esLvl),
              showarrow: false, font: { color: color, size: 9 },
              xanchor: 'left', yanchor: 'bottom',
            });
          });
        }

        // Absorption signal markers (grade A and A+ get chart markers)
        if (signals && signals.length) {
          signals.forEach(sig => {
            if (sig.grade === 'C' || sig.grade === 'B') return;
            const isBull = sig.direction === 'bullish';
            const color = isBull ? '#22c55e' : '#ef4444';
            const yPos = isBull ? sig.low - 2 : sig.high + 2;
            const arrow = isBull ? '\u25b2' : '\u25bc';
            const label = (sig.grade === 'A+' ? '\u2b50' : '') + sig.grade + ' ' + sig.score;
            layout.annotations.push({
              x: sig.bar_idx, y: yPos,
              xref: 'x', yref: 'y',
              text: '<b>' + arrow + ' ' + label + '</b>',
              showarrow: true, arrowhead: 2, arrowsize: 1, arrowcolor: color,
              ay: isBull ? 25 : -25, ax: 0,
              font: { size: 10, color: color },
              bgcolor: 'rgba(0,0,0,0.8)', bordercolor: color, borderpad: 3,
            });
          });
        }

        Plotly.react(esDeltaPlot, [traceCandle, traceVol, traceDelta, traceCVD], layout, {responsive:true, displayModeBar:false, scrollZoom:true});
        _esDeltaAttachRelayout();

        // Status text
        const sessionDelta = lastBar.cvd;
        const statusParts = [
          'Last: ' + lastBar.close.toFixed(2),
          'CVD: ' + (sessionDelta >= 0 ? '+' : '') + sessionDelta.toLocaleString(),
          'Bars: ' + n,
        ];
        if (signals.length) {
          const last = signals[signals.length - 1];
          const dir = last.direction === 'bullish' ? '\u25b2' : '\u25bc';
          statusParts.push(dir + ' ' + last.grade + '(' + last.score + '/' + last.max_score + ')');
        }
        esDeltaStatus.textContent = statusParts.join(' | ');
      } catch(e) {
        esDeltaStatus.textContent = 'Error: ' + e.message;
      }
    }

    // ===== Regime Map =====
    const regimeMapDateInput = document.getElementById('regimeMapDate');
    const regimeMapLoadBtn = document.getElementById('regimeMapLoad');
    const regimeMapStatus = document.getElementById('regimeMapStatus');
    const regimeMapPlot = document.getElementById('regimeMapPlot');
    let regimeMapInitialized = false;
    let regimeMapInterval = 5;
    const regimeMapTF5 = document.getElementById('regimeMapTF5');
    const regimeMapTF1 = document.getElementById('regimeMapTF1');

    function initRegimeMap() {
      if (regimeMapInitialized) return;
      regimeMapInitialized = true;
      // Default to today's date
      const d = new Date();
      regimeMapDateInput.value = d.toISOString().split('T')[0];
      regimeMapLoadBtn.addEventListener('click', loadRegimeMapData);
      regimeMapTF5.addEventListener('click', () => {
        if (regimeMapInterval === 5) return;
        regimeMapInterval = 5;
        regimeMapTF5.classList.add('active');
        regimeMapTF1.classList.remove('active');
        loadRegimeMapData();
      });
      regimeMapTF1.addEventListener('click', () => {
        if (regimeMapInterval === 1) return;
        regimeMapInterval = 1;
        regimeMapTF1.classList.add('active');
        regimeMapTF5.classList.remove('active');
        loadRegimeMapData();
      });
    }

    async function loadRegimeMapData() {
      const dateStr = regimeMapDateInput.value;
      if (!dateStr) {
        regimeMapStatus.textContent = 'Please select a date.';
        return;
      }
      regimeMapStatus.textContent = 'Loading...';

      try {
        const [snapRes, candleRes] = await Promise.all([
          fetch('/api/playback/range?start_date=' + dateStr, { cache: 'no-store' }),
          fetch('/api/spx_candles_date?date=' + dateStr + '&interval=' + regimeMapInterval, { cache: 'no-store' })
        ]);
        const data = await snapRes.json();
        let candles = [];
        try {
          const cData = await candleRes.json();
          if (cData.candles && cData.candles.length > 0) candles = cData.candles;
        } catch (e) {
          console.warn('[RegimeMap] Candle fetch failed, using synthesized:', e);
        }

        if (data.error) {
          regimeMapStatus.textContent = 'Error: ' + data.error;
          return;
        }

        if (!data.snapshots || data.snapshots.length === 0) {
          regimeMapStatus.textContent = 'No data found for this date.';
          return;
        }

        // Filter to selected date only (market hours filter applied in drawRegimeMap after ET conversion)
        const targetDate = dateStr; // YYYY-MM-DD
        const daySnaps = data.snapshots.filter(s => {
          const d = new Date(s.ts);
          const etDate = d.toLocaleDateString('en-CA', { timeZone: ET_TIMEZONE }); // YYYY-MM-DD format
          return etDate === targetDate;
        });

        if (daySnaps.length === 0) {
          regimeMapStatus.textContent = 'No data found for ' + dateStr + '. Try a trading day.';
          return;
        }

        const candleLabel = candles.length > 0 ? ' | ' + candles.length + ' candles (' + regimeMapInterval + 'm)' : ' | synth candles';
        regimeMapStatus.textContent = daySnaps.length + ' snapshots' + candleLabel + ' for ' + dateStr;
        drawRegimeMap(daySnaps, candles);
      } catch (err) {
        regimeMapStatus.textContent = 'Error: ' + err.message;
      }
    }

    function getParadigmColor(paradigm, opacity) {
      const a = opacity || 0.25;
      if (!paradigm) return 'rgba(156,163,175,' + a + ')';
      const p = paradigm.toUpperCase();
      if (p.includes('BOFA')) return 'rgba(96,165,250,' + a + ')';
      if (p.includes('SIDIAL')) return 'rgba(168,85,247,' + a + ')';
      if (p.includes('ANTI')) return 'rgba(239,68,68,' + a + ')';  // Anti-GEX (check before GEX)
      if (p.includes('GEX')) return 'rgba(34,197,94,' + a + ')';   // GEX
      return 'rgba(156,163,175,' + a + ')';
    }

    function buildParadigmBands(snaps) {
      const shapes = [];
      if (!snaps.length) return shapes;

      let bandStart = 0;
      let currentParadigm = (snaps[0].stats || {}).paradigm || '';

      for (let i = 1; i <= snaps.length; i++) {
        const nextParadigm = i < snaps.length ? ((snaps[i].stats || {}).paradigm || '') : '';
        if (nextParadigm !== currentParadigm || i === snaps.length) {
          const x0 = snaps[bandStart].ts;
          // Extend band to the NEXT snapshot's time so single-snapshot bands have width
          const x1 = i < snaps.length ? snaps[i].ts : snaps[i - 1].ts;
          shapes.push({
            type: 'rect',
            x0: x0, x1: x1,
            y0: 0, y1: 1,
            xref: 'x', yref: 'paper',
            fillcolor: getParadigmColor(currentParadigm),
            line: { width: 0 },
            layer: 'below'
          });
          // Vertical divider at paradigm transitions
          if (i < snaps.length && bandStart > 0) {
            shapes.push({
              type: 'line',
              x0: x0, x1: x0, y0: 0, y1: 1,
              xref: 'x', yref: 'paper',
              line: { color: 'rgba(255,255,255,0.2)', width: 1, dash: 'dot' },
              layer: 'below'
            });
          }
          if (i < snaps.length) {
            bandStart = i;
            currentParadigm = nextParadigm;
          }
        }
      }
      return shapes;
    }

    function parseTarget(stats) {
      if (!stats.target) return null;
      const m = String(stats.target).replace(/[$,]/g, '').match(/([\d.]+)/);
      return m ? parseFloat(m[1]) : null;
    }

    function parseLIS(stats) {
      const raw = stats.lis || stats.lines_in_sand;
      if (!raw) return { low: null, high: null };
      const s = String(raw).replace(/[$,]/g, '');
      const dm = s.match(/([\d.]+)\s*[-–]\s*([\d.]+)/);
      if (dm) return { low: parseFloat(dm[1]), high: parseFloat(dm[2]) };
      const sm = s.match(/([\d.]+)\s*\/\s*([\d.]+)/);
      if (sm) return { low: parseFloat(sm[1]), high: parseFloat(sm[2]) };
      const sg = s.match(/([\d.]+)/);
      return { low: sg ? parseFloat(sg[1]) : null, high: null };
    }

    function buildLevelTraces(snaps) {
      const traces = [];
      const annotations = [];
      if (!snaps.length) return { traces, annotations };

      // Helper: one point per snapshot, hv step interpolation, continuous across paradigms
      function makeTrace(fn, color, width, name) {
        const xs = [], ys = [];
        let hasVal = false;
        for (let i = 0; i < snaps.length; i++) {
          const val = fn(snaps[i]);
          if (val === null) {
            if (hasVal) { xs.push(null); ys.push(null); hasVal = false; }
            continue;
          }
          xs.push(snaps[i].ts); ys.push(val);
          hasVal = true;
        }
        if (xs.some(v => v !== null)) {
          traces.push({
            type: 'scatter', mode: 'lines', x: xs, y: ys,
            line: { color: color, width: width, shape: 'hv' },
            name: name, showlegend: false,
            hovertemplate: name + ': %{y:.0f}<extra></extra>'
          });
        }
      }

      // LIS zone: extract low & high together so nulls align for fill
      const lisXs = [], lisLowYs = [], lisHighYs = [];
      let lisHasVal = false;
      for (let i = 0; i < snaps.length; i++) {
        const lis = parseLIS(snaps[i].stats || {});
        if (lis.low === null) {
          if (lisHasVal) { lisXs.push(null); lisLowYs.push(null); lisHighYs.push(null); lisHasVal = false; }
          continue;
        }
        lisXs.push(snaps[i].ts);
        lisLowYs.push(lis.low);
        lisHighYs.push(lis.high && lis.high !== lis.low ? lis.high : lis.low);
        lisHasVal = true;
      }
      if (lisXs.some(v => v !== null)) {
        traces.push({
          type: 'scatter', mode: 'lines', x: lisXs, y: lisLowYs,
          line: { color: '#f59e0b', width: 1.5, shape: 'hv' },
          name: 'LIS Low', showlegend: false,
          hovertemplate: 'LIS: %{y:.0f}<extra></extra>'
        });
        traces.push({
          type: 'scatter', mode: 'lines', x: lisXs, y: lisHighYs,
          line: { color: '#f59e0b', width: 1.5, shape: 'hv' },
          fill: 'tonexty', fillcolor: 'rgba(245,158,11,0.08)',
          name: 'LIS High', showlegend: false,
          hovertemplate: 'LIS: %{y:.0f}<extra></extra>'
        });
      }

      // Target (thicker, prominent)
      makeTrace(s => parseTarget(s.stats || {}), '#3b82f6', 2.5, 'Target');

      // +GEX / -GEX (thinner)
      makeTrace(s => {
        const g = s.net_gex || [], st = s.strikes || [];
        let best = null, bv = 0;
        for (let j = 0; j < st.length && j < g.length; j++) if (g[j] > bv) { bv = g[j]; best = st[j]; }
        return best;
      }, '#22c55e', 1.5, '+GEX');
      makeTrace(s => {
        const g = s.net_gex || [], st = s.strikes || [];
        let best = null, bv = 0;
        for (let j = 0; j < st.length && j < g.length; j++) if (g[j] < bv) { bv = g[j]; best = st[j]; }
        return best;
      }, '#ef4444', 1.5, '-GEX');

      // Right-edge labels
      const last = snaps[snaps.length - 1];
      const ls = last.stats || {};
      const lt = last.ts;
      const tv = parseTarget(ls);
      if (tv) annotations.push({ x: lt, y: tv, xref: 'x', yref: 'y', text: 'Tgt ' + Math.round(tv), showarrow: false, font: { color: '#3b82f6', size: 10 }, xanchor: 'left', xshift: 5 });
      const lv = parseLIS(ls);
      if (lv.low) annotations.push({ x: lt, y: lv.low, xref: 'x', yref: 'y', text: 'LIS ' + Math.round(lv.low), showarrow: false, font: { color: '#f59e0b', size: 10 }, xanchor: 'left', xshift: 5 });
      if (lv.high && lv.high !== lv.low) annotations.push({ x: lt, y: lv.high, xref: 'x', yref: 'y', text: 'LIS ' + Math.round(lv.high), showarrow: false, font: { color: '#f59e0b', size: 10 }, xanchor: 'left', xshift: 5 });
      const lg = last.net_gex || [], lst = last.strikes || [];
      let pg = null, ng = null, pv = 0, nv = 0;
      for (let j = 0; j < lst.length && j < lg.length; j++) {
        if (lg[j] > pv) { pv = lg[j]; pg = lst[j]; }
        if (lg[j] < nv) { nv = lg[j]; ng = lst[j]; }
      }
      if (pg) annotations.push({ x: lt, y: pg, xref: 'x', yref: 'y', text: '+G ' + pg, showarrow: false, font: { color: '#22c55e', size: 10 }, xanchor: 'left', xshift: 5 });
      if (ng) annotations.push({ x: lt, y: ng, xref: 'x', yref: 'y', text: '-G ' + ng, showarrow: false, font: { color: '#ef4444', size: 10 }, xanchor: 'left', xshift: 5 });

      return { traces, annotations };
    }

    function drawRegimeMap(snaps, candles) {
      // Convert snapshot timestamps from UTC ISO to naive ET strings
      // Uses formatToParts for guaranteed zero-padded output across all browsers
      const _etFmt = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
      });
      for (let i = 0; i < snaps.length; i++) {
        const p = {};
        for (const {type, value} of _etFmt.formatToParts(new Date(snaps[i].ts))) p[type] = value;
        snaps[i].ts = p.year + '-' + p.month + '-' + p.day + 'T' + p.hour + ':' + p.minute + ':' + p.second;
      }

      // Filter to market hours (9:30-16:00 ET) using guaranteed HH:MM format
      snaps = snaps.filter(s => {
        const hhmm = s.ts.substring(11, 16);
        return hhmm >= '09:30' && hhmm <= '16:00';
      });
      if (!snaps.length) return;

      // Per-field forward-fill: each stats field carries forward independently
      // Handles both field names: lis (new) and lines_in_sand (old data)
      const ff = {};
      for (let i = 0; i < snaps.length; i++) {
        const s = snaps[i].stats || {};
        if (s.paradigm) ff.paradigm = s.paradigm;
        if (s.target) ff.target = s.target;
        if (s.lis || s.lines_in_sand) ff.lis = s.lis || s.lines_in_sand;
        if (s.dd_hedging || s.delta_decay_hedging) ff.dd_hedging = s.dd_hedging || s.delta_decay_hedging;
        snaps[i].stats = Object.assign({}, ff);
      }

      // Debug: log stats summary so we can see what data exists
      console.log('[RegimeMap] Forward-filled stats summary:');
      for (let i = 0; i < snaps.length; i++) {
        const st = snaps[i].stats || {};
        const ts = snaps[i].ts;
        const etTime = new Date(ts).toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit' });
        if (i === 0 || i === snaps.length - 1 || i % 10 === 0) {
          console.log('  [' + i + '] ' + etTime + ' paradigm=' + (st.paradigm || 'NONE') + ' target=' + (st.target || 'NONE') + ' lis=' + (st.lis || 'NONE'));
        }
      }

      // --- Candlestick trace ---
      let times, opens, highs, lows, closes;

      if (candles && candles.length > 0) {
        // Real OHLC candles from TradeStation API
        times = candles.map(c => c.time);
        opens = candles.map(c => c.open);
        highs = candles.map(c => c.high);
        lows = candles.map(c => c.low);
        closes = candles.map(c => c.close);
        console.log('[RegimeMap] Using ' + candles.length + ' real candles');
      } else {
        // Fallback: synthesized from spot snapshots
        times = []; opens = []; highs = []; lows = []; closes = [];
        for (let i = 0; i < snaps.length; i++) {
          const curr = snaps[i].spot;
          const prev = i > 0 ? snaps[i - 1].spot : curr;
          times.push(snaps[i].ts);
          opens.push(prev);
          closes.push(curr);
          highs.push(Math.max(prev, curr) + Math.abs(curr - prev) * 0.1);
          lows.push(Math.min(prev, curr) - Math.abs(curr - prev) * 0.1);
        }
        console.log('[RegimeMap] Using synthesized candles (API unavailable)');
      }

      const candleTrace = {
        type: 'candlestick',
        x: times, open: opens, high: highs, low: lows, close: closes,
        increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
        decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
        name: 'SPX', showlegend: false,
        hoverinfo: 'x+y'
      };

      const allPrices = [...highs, ...lows];
      const priceMin = Math.min(...allPrices) - 10;
      const priceMax = Math.max(...allPrices) + 10;

      // --- Level traces (LIS zone, Target, GEX) ---
      const { traces: levelTraces, annotations } = buildLevelTraces(snaps);

      // --- Paradigm background bands (subtle) ---
      const paradigmShapes = buildParadigmBands(snaps);

      // --- Paradigm labels centered in each band ---
      let curPar = (snaps[0].stats || {}).paradigm || '';
      let bandStart = 0;
      for (let i = 1; i <= snaps.length; i++) {
        const par = i < snaps.length ? ((snaps[i].stats || {}).paradigm || '') : '';
        if (par !== curPar || i === snaps.length) {
          if (curPar) {
            const t0 = new Date(snaps[bandStart].ts).getTime();
            const t1 = i < snaps.length ? new Date(snaps[i].ts).getTime() : new Date(snaps[snaps.length - 1].ts).getTime();
            annotations.push({
              x: new Date((t0 + t1) / 2).toISOString(),
              y: 1.0, xref: 'x', yref: 'paper',
              text: curPar, showarrow: false,
              font: { color: '#e6e7e9', size: 9 },
              bgcolor: 'rgba(0,0,0,0.5)', borderpad: 3,
              yanchor: 'top', yshift: -4
            });
          }
          bandStart = i;
          curPar = par;
        }
      }

      // Traces: levels first (below), candles on top
      // Candles first (bottom), level lines on top (visible over candle bodies)
      const allTraces = [candleTrace, ...levelTraces];

      Plotly.react(regimeMapPlot, allTraces, {
        margin: { l: 55, r: 80, t: 30, b: 50 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: {
          type: 'date',
          gridcolor: '#1a1d21',
          tickfont: { size: 10 },
          tickformat: '%H:%M',
          dtick: 1800000,
          range: [times[0].substring(0, 11) + '09:25:00', times[0].substring(0, 11) + '16:05:00'],
          rangeslider: { visible: false }
        },
        yaxis: {
          gridcolor: '#1a1d21',
          tickfont: { size: 10 },
          side: 'left',
          range: [priceMin, priceMax],
          dtick: 5
        },
        font: { color: '#e6e7e9', size: 10 },
        shapes: paradigmShapes,
        annotations: annotations,
        hovermode: 'closest',
        dragmode: 'zoom'
      }, {
        displayModeBar: true,
        displaylogo: false,
        modeBarButtonsToRemove: ['lasso2d', 'select2d'],
        responsive: true,
        scrollZoom: true
      });
    }

    // ===== Settings Modal =====
    const isAdmin = __IS_ADMIN__;
    const alertModal = document.getElementById('alertModal');
    const alertSettingsBtn = document.getElementById('alertSettingsBtn');
    const alertModalClose = document.getElementById('alertModalClose');
    const alertMasterToggle = document.getElementById('alertMasterToggle');
    const alertLIS = document.getElementById('alertLIS');
    const alertTarget = document.getElementById('alertTarget');
    const alertPosGamma = document.getElementById('alertPosGamma');
    const alertNegGamma = document.getElementById('alertNegGamma');
    const alertParadigm = document.getElementById('alertParadigm');
    const alertVolSpike = document.getElementById('alertVolSpike');
    const alert10am = document.getElementById('alert10am');
    const alert2pm = document.getElementById('alert2pm');
    const alertThresholdPts = document.getElementById('alertThresholdPts');
    const alertThresholdVol = document.getElementById('alertThresholdVol');
    const alertCooldown = document.getElementById('alertCooldown');
    const alertCooldownMin = document.getElementById('alertCooldownMin');
    const alertTestBtn = document.getElementById('alertTestBtn');
    const alertSaveBtn = document.getElementById('alertSaveBtn');
    const alertStatus = document.getElementById('alertStatus');

    // Settings tabs
    const settingsTabs = document.querySelectorAll('.settings-tab');
    const settingsPanels = document.querySelectorAll('.settings-panel');

    // Show/hide admin tabs
    settingsTabs.forEach(tab => {
      if ((tab.dataset.tab === 'users' || tab.dataset.tab === 'messages' || tab.dataset.tab === 'autotrade') && !isAdmin) {
        tab.style.display = 'none';
      }
    });

    // Tab switching
    settingsTabs.forEach(tab => {
      tab.addEventListener('click', () => {
        settingsTabs.forEach(t => t.classList.remove('active'));
        settingsPanels.forEach(p => p.classList.remove('active'));
        tab.classList.add('active');
        const panelId = 'tabPanel' + tab.dataset.tab.charAt(0).toUpperCase() + tab.dataset.tab.slice(1);
        const panel = document.getElementById(panelId);
        if (panel) panel.classList.add('active');

        // Load data when switching tabs
        if (tab.dataset.tab === 'users') loadUsers();
        if (tab.dataset.tab === 'messages') loadMessages();
        if (tab.dataset.tab === 'setups') loadSetupSettings();
        if (tab.dataset.tab === 'autotrade') loadAutoTradeStatus();
      });
    });

    // Set first tab active
    document.getElementById('tabPanelAlerts').classList.add('active');

    async function loadAlertSettings() {
      try {
        const r = await fetch('/api/alerts/settings', { cache: 'no-store' });
        const s = await r.json();
        alertMasterToggle.checked = s.enabled;
        alertLIS.checked = s.lis_enabled;
        alertTarget.checked = s.target_enabled;
        alertPosGamma.checked = s.max_pos_gamma_enabled;
        alertNegGamma.checked = s.max_neg_gamma_enabled;
        alertParadigm.checked = s.paradigm_change_enabled;
        alertVolSpike.checked = s.volume_spike_enabled;
        alert10am.checked = s.summary_10am_enabled;
        alert2pm.checked = s.summary_2pm_enabled;
        alertThresholdPts.value = s.threshold_points;
        alertThresholdVol.value = s.threshold_volume;
        alertCooldown.checked = s.cooldown_enabled;
        alertCooldownMin.value = s.cooldown_minutes;
      } catch (err) {
        console.error('Failed to load alert settings:', err);
      }
    }

    async function saveAlertSettings() {
      alertStatus.textContent = 'Saving...';
      try {
        const params = new URLSearchParams({
          enabled: alertMasterToggle.checked,
          lis_enabled: alertLIS.checked,
          target_enabled: alertTarget.checked,
          max_pos_gamma_enabled: alertPosGamma.checked,
          max_neg_gamma_enabled: alertNegGamma.checked,
          paradigm_change_enabled: alertParadigm.checked,
          volume_spike_enabled: alertVolSpike.checked,
          summary_10am_enabled: alert10am.checked,
          summary_2pm_enabled: alert2pm.checked,
          threshold_points: alertThresholdPts.value,
          threshold_volume: alertThresholdVol.value,
          cooldown_enabled: alertCooldown.checked,
          cooldown_minutes: alertCooldownMin.value,
        });
        const r = await fetch('/api/alerts/settings?' + params.toString(), { method: 'POST' });
        const data = await r.json();
        if (data.status === 'ok') {
          alertStatus.textContent = 'Saved ✓';
          alertStatus.style.color = '#22c55e';
        } else {
          alertStatus.textContent = 'Error saving';
          alertStatus.style.color = '#ef4444';
        }
      } catch (err) {
        alertStatus.textContent = 'Error: ' + err.message;
        alertStatus.style.color = '#ef4444';
      }
      setTimeout(() => { alertStatus.textContent = ''; }, 3000);
    }

    async function testAlert() {
      alertStatus.textContent = 'Sending test...';
      try {
        const r = await fetch('/api/alerts/test', { method: 'POST' });
        const data = await r.json();
        if (data.status === 'ok') {
          alertStatus.textContent = 'Test sent ✓';
          alertStatus.style.color = '#22c55e';
        } else {
          alertStatus.textContent = data.message;
          alertStatus.style.color = '#ef4444';
        }
      } catch (err) {
        alertStatus.textContent = 'Error: ' + err.message;
        alertStatus.style.color = '#ef4444';
      }
      setTimeout(() => { alertStatus.textContent = ''; }, 3000);
    }

    // ===== User Management (Admin) =====
    async function loadUsers() {
      const list = document.getElementById('usersList');
      if (!list) return;
      list.innerHTML = '<div style="color:var(--muted)">Loading...</div>';
      try {
        const r = await fetch('/api/users', { cache: 'no-store' });
        const users = await r.json();
        if (users.error) {
          list.innerHTML = '<div style="color:var(--red)">' + users.error + '</div>';
          return;
        }
        list.innerHTML = users.map(u => `
          <div class="user-row">
            <div>
              <span class="email">${u.email}</span>
              ${u.is_admin ? '<span class="badge">Admin</span>' : ''}
            </div>
            ${!u.is_admin ? '<button class="delete-btn" onclick="deleteUser('+u.id+')">Delete</button>' : ''}
          </div>
        `).join('');
      } catch (err) {
        list.innerHTML = '<div style="color:var(--red)">Error loading users</div>';
      }
    }

    async function addUser() {
      const emailInput = document.getElementById('newUserEmail');
      const passInput = document.getElementById('newUserPassword');
      const email = emailInput.value.trim();
      const password = passInput.value;
      if (!email || !password) {
        alert('Please enter email and password');
        return;
      }
      try {
        const r = await fetch('/api/users?email=' + encodeURIComponent(email) + '&password=' + encodeURIComponent(password), { method: 'POST' });
        const data = await r.json();
        if (data.error) {
          alert(data.error);
        } else {
          emailInput.value = '';
          passInput.value = '';
          loadUsers();
        }
      } catch (err) {
        alert('Error adding user');
      }
    }

    window.deleteUser = async function(id) {
      if (!confirm('Delete this user?')) return;
      try {
        const r = await fetch('/api/users/' + id, { method: 'DELETE' });
        const data = await r.json();
        if (data.error) {
          alert(data.error);
        } else {
          loadUsers();
        }
      } catch (err) {
        alert('Error deleting user');
      }
    };

    const addUserBtn = document.getElementById('addUserBtn');
    if (addUserBtn) addUserBtn.addEventListener('click', addUser);

    // ===== Messages Management (Admin) =====
    async function loadMessages() {
      const list = document.getElementById('messagesList');
      if (!list) return;
      list.innerHTML = '<div style="color:var(--muted)">Loading...</div>';
      try {
        const r = await fetch('/api/messages', { cache: 'no-store' });
        const msgs = await r.json();
        if (msgs.error) {
          list.innerHTML = '<div style="color:var(--red)">' + msgs.error + '</div>';
          return;
        }
        if (msgs.length === 0) {
          list.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px">No messages</div>';
          return;
        }
        list.innerHTML = msgs.map(m => `
          <div class="message-row ${m.is_read ? '' : 'unread'}">
            <div class="msg-header">
              <span class="msg-subject">${m.subject || 'No subject'}</span>
              <button class="delete-btn" onclick="deleteMessage(${m.id})">Delete</button>
            </div>
            <div class="msg-email">From: ${m.email}</div>
            <div class="msg-body">${m.message || ''}</div>
            <div class="msg-date">${fmtDateTimeET(m.created_at)} ET</div>
          </div>
        `).join('');
      } catch (err) {
        list.innerHTML = '<div style="color:var(--red)">Error loading messages</div>';
      }
    }

    window.deleteMessage = async function(id) {
      if (!confirm('Delete this message?')) return;
      try {
        const r = await fetch('/api/messages/' + id, { method: 'DELETE' });
        const data = await r.json();
        if (data.error) {
          alert(data.error);
        } else {
          loadMessages();
        }
      } catch (err) {
        alert('Error deleting message');
      }
    };

    // ====== Setup Detector Settings ======
    async function loadSetupSettings() {
      try {
        const r = await fetch('/api/setup/settings', { cache: 'no-store' });
        const s = await r.json();
        document.getElementById('setupGexLongEnabled').checked = s.gex_long_enabled !== false;
        document.getElementById('setupAgShortEnabled').checked = s.ag_short_enabled !== false;
        document.getElementById('setupBofaScalpEnabled').checked = s.bofa_scalp_enabled !== false;
        document.getElementById('setupAbsorptionEnabled').checked = s.absorption_enabled !== false;
        document.getElementById('setupWeightSupport').value = s.weight_support ?? 20;
        document.getElementById('setupWeightUpside').value = s.weight_upside ?? 20;
        document.getElementById('setupWeightFloorCluster').value = s.weight_floor_cluster ?? 20;
        document.getElementById('setupWeightTargetCluster').value = s.weight_target_cluster ?? 20;
        document.getElementById('setupWeightRR').value = s.weight_rr ?? 20;
        const gt = s.grade_thresholds || {};
        document.getElementById('setupGradeAPlus').value = gt['A+'] ?? 90;
        document.getElementById('setupGradeA').value = gt['A'] ?? 75;
        document.getElementById('setupGradeAEntry').value = gt['A-Entry'] ?? 60;
        // BofA Scalp
        document.getElementById('bofaWeightStability').value = s.bofa_weight_stability ?? 20;
        document.getElementById('bofaWeightWidth').value = s.bofa_weight_width ?? 20;
        document.getElementById('bofaWeightCharm').value = s.bofa_weight_charm ?? 20;
        document.getElementById('bofaWeightTime').value = s.bofa_weight_time ?? 20;
        document.getElementById('bofaWeightMidpoint').value = s.bofa_weight_midpoint ?? 20;
        document.getElementById('bofaStopDistance').value = s.bofa_stop_distance ?? 12;
        document.getElementById('bofaTargetDistance').value = s.bofa_target_distance ?? 10;
        document.getElementById('bofaMaxHold').value = s.bofa_max_hold_minutes ?? 30;
        document.getElementById('bofaCooldown').value = s.bofa_cooldown_minutes ?? 40;
        // ES Absorption
        document.getElementById('absWeightDivergence').value = s.abs_weight_divergence ?? 25;
        document.getElementById('absWeightVolume').value = s.abs_weight_volume ?? 25;
        document.getElementById('absWeightDD').value = s.abs_weight_dd ?? 10;
        document.getElementById('absWeightParadigm').value = s.abs_weight_paradigm ?? 10;
        document.getElementById('absWeightLIS').value = s.abs_weight_lis ?? 10;
        document.getElementById('absWeightLISSide').value = s.abs_weight_lis_side ?? 10;
        document.getElementById('absWeightTargetDir').value = s.abs_weight_target_dir ?? 10;
        document.getElementById('absPivotLeft').value = s.abs_pivot_left ?? 2;
        document.getElementById('absPivotRight').value = s.abs_pivot_right ?? 2;
        document.getElementById('absMinVolRatio').value = s.abs_min_vol_ratio ?? 1.4;
        document.getElementById('absCvdZMin').value = s.abs_cvd_z_min ?? 0.5;
        document.getElementById('absCvdStdWindow').value = s.abs_cvd_std_window ?? 20;
        document.getElementById('absVolWindow').value = s.abs_vol_window ?? 10;
        document.getElementById('absCooldownBars').value = s.abs_cooldown_bars ?? 10;
      } catch (err) {
        console.error('Failed to load setup settings', err);
      }
      loadSetupLog();
    }

    // ====== Auto Trade Status ======
    const _atToggleMap = {
      'atGexLong': 'GEX Long', 'atAgShort': 'AG Short', 'atBofaScalp': 'BofA Scalp',
      'atAbsorption': 'ES Absorption', 'atParadigm': 'Paradigm Reversal', 'atDDExhaust': 'DD Exhaustion',
    };

    async function loadAutoTradeStatus() {
      try {
        const r = await fetch('/api/auto-trade/status', { cache: 'no-store' });
        const s = await r.json();
        const badge = document.getElementById('autoTradeStatus');
        if (s.enabled) {
          badge.textContent = `ON | ${s.symbol} x${s.total_qty || 10} | ${s.active_count} active`;
          badge.style.color = '#22c55e';
        } else {
          badge.textContent = 'DISABLED';
          badge.style.color = '#ef4444';
        }
        const toggles = s.toggles || {};
        for (const [elId, name] of Object.entries(_atToggleMap)) {
          const el = document.getElementById(elId);
          if (el) el.checked = !!toggles[name];
        }
        const ordersEl = document.getElementById('autoTradeOrders');
        const orders = s.active_orders || {};
        const keys = Object.keys(orders);
        if (keys.length === 0) {
          ordersEl.textContent = 'No active orders';
        } else {
          ordersEl.innerHTML = keys.map(k => {
            const o = orders[k];
            const dir = o.direction?.toLowerCase().includes('long') ? 'LONG' : 'SHORT';
            const fill = o.fill_price ? `@ ${o.fill_price}` : 'pending';
            const t1 = o.t1_filled ? 'T1 filled' : `T1: ${o.first_target_price || '-'}`;
            const t2 = o.t2_filled ? 'T2 filled' : (o.full_target_price ? `T2: ${o.full_target_price}` : 'T2: trail');
            const qty = o.stop_qty != null ? `qty: ${o.stop_qty}` : '';
            return `<div style="padding:3px 0;border-bottom:1px solid var(--border)">${o.setup_name} ${dir} ${fill} | ${t1} | ${t2} | stop: ${o.current_stop} | ${qty}</div>`;
          }).join('');
        }
      } catch (err) {
        console.error('Failed to load auto-trade status', err);
      }
    }

    async function toggleAutoTrade(setupName, enabled) {
      try {
        const params = new URLSearchParams({ setup_name: setupName, enabled });
        await fetch('/api/auto-trade/toggle?' + params, { method: 'POST' });
      } catch (err) {
        console.error('Auto-trade toggle error', err);
      }
    }

    for (const [elId, name] of Object.entries(_atToggleMap)) {
      document.getElementById(elId)?.addEventListener('change', (e) => {
        toggleAutoTrade(name, e.target.checked);
      });
    }

    async function saveSetupSettings() {
      const status = document.getElementById('setupStatus');
      status.textContent = 'Saving...';
      try {
        const params = new URLSearchParams({
          gex_long_enabled: document.getElementById('setupGexLongEnabled').checked,
          ag_short_enabled: document.getElementById('setupAgShortEnabled').checked,
          bofa_scalp_enabled: document.getElementById('setupBofaScalpEnabled').checked,
          absorption_enabled: document.getElementById('setupAbsorptionEnabled').checked,
          weight_support: document.getElementById('setupWeightSupport').value,
          weight_upside: document.getElementById('setupWeightUpside').value,
          weight_floor_cluster: document.getElementById('setupWeightFloorCluster').value,
          weight_target_cluster: document.getElementById('setupWeightTargetCluster').value,
          weight_rr: document.getElementById('setupWeightRR').value,
          grade_a_plus: document.getElementById('setupGradeAPlus').value,
          grade_a: document.getElementById('setupGradeA').value,
          grade_a_entry: document.getElementById('setupGradeAEntry').value,
          bofa_weight_stability: document.getElementById('bofaWeightStability').value,
          bofa_weight_width: document.getElementById('bofaWeightWidth').value,
          bofa_weight_charm: document.getElementById('bofaWeightCharm').value,
          bofa_weight_time: document.getElementById('bofaWeightTime').value,
          bofa_weight_midpoint: document.getElementById('bofaWeightMidpoint').value,
          bofa_stop_distance: document.getElementById('bofaStopDistance').value,
          bofa_target_distance: document.getElementById('bofaTargetDistance').value,
          bofa_max_hold_minutes: document.getElementById('bofaMaxHold').value,
          bofa_cooldown_minutes: document.getElementById('bofaCooldown').value,
          abs_weight_divergence: document.getElementById('absWeightDivergence').value,
          abs_weight_volume: document.getElementById('absWeightVolume').value,
          abs_weight_dd: document.getElementById('absWeightDD').value,
          abs_weight_paradigm: document.getElementById('absWeightParadigm').value,
          abs_weight_lis: document.getElementById('absWeightLIS').value,
          abs_weight_lis_side: document.getElementById('absWeightLISSide').value,
          abs_weight_target_dir: document.getElementById('absWeightTargetDir').value,
          abs_pivot_left: document.getElementById('absPivotLeft').value,
          abs_pivot_right: document.getElementById('absPivotRight').value,
          abs_vol_window: document.getElementById('absVolWindow').value,
          abs_min_vol_ratio: document.getElementById('absMinVolRatio').value,
          abs_cvd_z_min: document.getElementById('absCvdZMin').value,
          abs_cvd_std_window: document.getElementById('absCvdStdWindow').value,
          abs_cooldown_bars: document.getElementById('absCooldownBars').value,
        });
        const r = await fetch('/api/setup/settings?' + params, { method: 'POST' });
        const data = await r.json();
        status.textContent = data.status === 'ok' ? 'Saved' : 'Error';
        setTimeout(() => { status.textContent = ''; }, 2000);
      } catch (err) {
        status.textContent = 'Error';
        setTimeout(() => { status.textContent = ''; }, 2000);
      }
    }

    // ===== Trade Log Tab =====
    let _tradeLogData = [];
    let _tlActiveSubTab = 'portal';
    let _tsSimData = [];
    let _evalLogData = [];
    const _tlPillColors = {'GEX Long':'#22c55e','AG Short':'#ef4444','BofA Scalp':'#a78bfa','ES Absorption':'#f59e0b','DD Exhaustion':'#6b7280','Paradigm Reversal':'#06b6d4'};
    const _tlGradeColors = {'A+':'#22c55e','A':'#3b82f6','A-Entry':'#eab308'};

    // Trade Log sub-tab switching
    document.querySelectorAll('#tlSubtabs .subtab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('#tlSubtabs .subtab-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        _tlActiveSubTab = btn.dataset.subtab;
        _tlLoadActiveSubTab();
      });
    });
    function _tlLoadActiveSubTab() {
      if (_tlActiveSubTab === 'portal') loadTradeLogFull();
      else if (_tlActiveSubTab === 'tssim') loadTsSimLog();
      else if (_tlActiveSubTab === 'eval') loadEvalLog();
    }

    async function loadTradeLogFull() {
      try {
        const r = await fetch('/api/setup/log_with_outcomes?limit=500', {cache:'no-store'});
        _tradeLogData = await r.json();
        if (!Array.isArray(_tradeLogData)) _tradeLogData = [];
        renderTradeLog();
        document.getElementById('tlStatus').textContent = _tradeLogData.length + ' signals loaded';
      } catch(err) {
        console.error('loadTradeLogFull error:', err);
        document.getElementById('tlBody').innerHTML = '<div style="color:var(--red);padding:12px">Error loading trade log</div>';
      }
    }

    function _tlGetFiltered() {
      const fSetup = document.getElementById('tlFilterSetup').value;
      const fResult = document.getElementById('tlFilterResult').value;
      const fGrade = document.getElementById('tlFilterGrade').value;
      const fDate = document.getElementById('tlFilterDate').value;
      const fSearch = document.getElementById('tlSearch').value.toLowerCase().trim();
      const now = new Date();
      const todayET = new Date(now.toLocaleString('en-US',{timeZone:'America/New_York'}));
      const todayStr = todayET.getFullYear()+'-'+String(todayET.getMonth()+1).padStart(2,'0')+'-'+String(todayET.getDate()).padStart(2,'0');

      return _tradeLogData.filter(l => {
        if (fSetup && l.setup_name !== fSetup) return false;
        if (fGrade && l.grade !== fGrade) return false;

        // Result filter — prefer DB-stored outcome_result (consistent with outcome_pnl)
        if (fResult) {
          const o = l.outcome || {};
          let res = '';
          if (l.outcome_result) res = l.outcome_result;
          else if (o.first_event === 'pending') res = 'PENDING';
          else if (o.first_event === '10pt' || o.first_event === 'target' || o.first_event === '15pt') res = 'WIN';
          else if (o.first_event === 'stop') res = 'LOSS';
          else if (o.first_event === 'miss') res = 'EXPIRED';
          else if (o.first_event === 'timeout') res = 'TIMEOUT';
          else res = 'OPEN';
          if (res !== fResult) return false;
        }

        // Date filter
        if (fDate && l.ts) {
          const d = new Date(l.ts);
          const dET = new Date(d.toLocaleString('en-US',{timeZone:'America/New_York'}));
          if (fDate === 'today') {
            const dStr = dET.getFullYear()+'-'+String(dET.getMonth()+1).padStart(2,'0')+'-'+String(dET.getDate()).padStart(2,'0');
            if (dStr !== todayStr) return false;
          } else if (fDate === 'week') {
            const diff = (todayET - dET) / 86400000;
            if (diff > 7) return false;
          } else if (fDate === 'month') {
            if (dET.getMonth() !== todayET.getMonth() || dET.getFullYear() !== todayET.getFullYear()) return false;
          }
        }

        // Text search
        if (fSearch) {
          const hay = [l.setup_name, l.grade, l.direction, l.comments, l.spot?.toString(), l.outcome_result].filter(Boolean).join(' ').toLowerCase();
          if (!hay.includes(fSearch)) return false;
        }
        return true;
      });
    }

    function renderTradeLog() {
      // Restore portal header/grid
      const hdr = document.getElementById('tlHeaderRow');
      hdr.className = 'tl-header';
      hdr.innerHTML = '<span>#</span><span>Setup</span><span>Dir</span><span>Grade</span><span>Scr</span><span>Entry</span><span>Gap/RR</span><span>10p/Tgt/Stp</span><span>Result</span><span>P&L</span><span>Dur</span><span>Time</span><span></span>';
      const filtered = _tlGetFiltered();

      // Stats — prefer DB-stored outcome_result (always set for resolved trades)
      let wins=0, losses=0, totalPnl=0, pnlCount=0;
      filtered.forEach(l => {
        if (l.outcome_result === 'WIN') wins++;
        else if (l.outcome_result === 'LOSS') losses++;
        else if (l.outcome_result === 'EXPIRED') {
          const epnl = l.outcome_pnl || 0;
          if (epnl > 0) wins++; else if (epnl < 0) losses++;
        } else {
          // Fallback: use historical calculator hit_target/hit_stop (not first_event)
          const oo = l.outcome || {};
          if (oo.hit_target) wins++;
          else if (oo.hit_stop) losses++;
        }
        if (l.outcome_pnl != null) { totalPnl += l.outcome_pnl; pnlCount++; }
      });
      const wr = (wins+losses) > 0 ? ((wins/(wins+losses))*100).toFixed(0) : '--';
      const pnlColor = totalPnl >= 0 ? '#22c55e' : '#ef4444';
      const pnlStr = pnlCount > 0 ? ((totalPnl >= 0 ? '+' : '') + totalPnl.toFixed(1)) : '--';
      document.getElementById('tlStats').innerHTML =
        '<span>Total: <span class="stat-val">'+filtered.length+'</span></span>' +
        '<span>Wins: <span class="stat-val" style="color:#22c55e">'+wins+'</span></span>' +
        '<span>Losses: <span class="stat-val" style="color:#ef4444">'+losses+'</span></span>' +
        '<span>WR: <span class="stat-val">'+wr+'%</span></span>' +
        '<span>Net P&L: <span class="stat-val" style="color:'+pnlColor+'">'+pnlStr+'</span></span>';

      // Table body
      const body = document.getElementById('tlBody');
      if (filtered.length === 0) {
        body.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px">No matching signals</div>';
        return;
      }

      let html = '';
      filtered.forEach((l, i) => {
        const isAbs = l.setup_name === 'ES Absorption';
        const isBofa = l.setup_name === 'BofA Scalp';
        const pillColor = _tlPillColors[l.setup_name] || '#888';
        const dir = isAbs ? (l.direction === 'bullish' ? '▲' : '▼') : (l.direction === 'long' ? '▲' : '▼');
        const dirColor = (l.direction === 'long' || l.direction === 'bullish') ? '#22c55e' : '#ef4444';
        const gradeColor = _tlGradeColors[l.grade] || '#888';
        const entry = isAbs ? (l.abs_es_price || l.spot)?.toFixed(2) : l.spot?.toFixed(0);
        const gapRr = isAbs ? ((l.abs_vol_ratio||0).toFixed(1)+'x') : ((l.gap_to_lis?.toFixed(1)||'--')+' / '+(l.rr_ratio?.toFixed(1)||'--')+'x');

        // 10p/Tgt/Stp
        const o = l.outcome || {};
        const tgtLabel = isBofa ? '15p' : '10p';
        const has10pt = o.hit_10pt === true ? '✓' : (o.hit_10pt === false ? '✗' : '–');
        const hasTgt = o.hit_target === true ? '✓' : (o.hit_target === false ? '✗' : '–');
        const hasStop = o.hit_stop === true ? '✗' : (o.hit_stop === false ? '✓' : '–');
        const c10 = o.hit_10pt ? '#22c55e' : (o.hit_10pt === false ? '#888' : '#555');
        const cTgt = o.hit_target ? '#22c55e' : (o.hit_target === false ? '#888' : '#555');
        const stopIsLoss = o.hit_stop && o.first_event === 'stop';
        const cStop = stopIsLoss ? '#ef4444' : (o.hit_stop === false ? '#22c55e' : '#888');

        // Result — prefer DB-stored outcome_result, then historical calculator
        // Only show OPEN if neither target nor stop was hit yet
        let result = '';
        if (l.outcome_result) {
          const rc = l.outcome_result === 'WIN' ? '#22c55e' : l.outcome_result === 'LOSS' ? '#ef4444' : '#888';
          result = '<span style="color:'+rc+';font-weight:700">'+l.outcome_result+'</span>';
        } else if (o.hit_target) {
          result = '<span style="color:#22c55e;font-weight:700">WIN</span>';
        } else if (o.hit_stop) {
          result = '<span style="color:#ef4444;font-weight:700">LOSS</span>';
        } else {
          result = '<span style="color:#3b82f6;font-weight:600">OPEN</span>';
        }

        // P&L
        let pnl = '--';
        let pnlC = '#888';
        if (l.outcome_pnl != null) { pnl = (l.outcome_pnl >= 0 ? '+' : '') + l.outcome_pnl.toFixed(1); pnlC = l.outcome_pnl >= 0 ? '#22c55e' : '#ef4444'; }
        else if (o.timeout_pnl != null) { pnl = (o.timeout_pnl >= 0 ? '+' : '') + o.timeout_pnl.toFixed(1); pnlC = o.timeout_pnl >= 0 ? '#22c55e' : '#ef4444'; }

        // Duration in minutes
        const em = l.outcome_elapsed_min || (o && o.elapsed_min);
        const durStr = em != null ? (em >= 60 ? Math.floor(em/60)+'h'+String(em%60).padStart(2,'0') : em+'m') : '--';

        const time = fmtTimeET(l.ts);
        const date = fmtDateShortET(l.ts);
        const hasNotes = l.comments && l.comments.trim().length > 0;
        const noteIcon = hasNotes ? '💬' : '📝';

        html += '<div class="tl-row" data-id="'+l.id+'" data-idx="'+i+'">' +
          '<span style="color:var(--muted)">'+(i+1)+'</span>' +
          '<span class="setup-pill" style="background:'+pillColor+'22;color:'+pillColor+'">'+l.setup_name+'</span>' +
          '<span style="color:'+dirColor+';font-weight:700;text-align:center">'+dir+'</span>' +
          '<span style="color:'+gradeColor+';font-weight:600">'+l.grade+'</span>' +
          '<span style="color:var(--muted)">'+l.score+'</span>' +
          '<span style="color:var(--text)">'+(entry||'--')+'</span>' +
          '<span style="color:var(--muted);font-size:10px">'+gapRr+'</span>' +
          '<span style="font-size:10px"><span style="color:'+c10+'">'+has10pt+'</span> <span style="color:'+cTgt+'">'+hasTgt+'</span> <span style="color:'+cStop+'">'+hasStop+'</span></span>' +
          '<span style="font-size:10px">'+result+'</span>' +
          '<span style="color:'+pnlC+';font-size:10px">'+pnl+'</span>' +
          '<span style="color:var(--muted);font-size:9px">'+durStr+'</span>' +
          '<span style="color:var(--muted);font-size:9px">'+date+' '+time+'</span>' +
          '<span class="tl-note-icon" data-idx="'+i+'" style="cursor:pointer;text-align:center" title="Notes">'+noteIcon+'</span>' +
        '</div>';
        html += '<div class="tl-notes" id="tlNotes'+i+'">' +
          '<textarea id="tlNotesText'+i+'">'+(l.comments||'').replace(/</g,'&lt;')+'</textarea>' +
          '<button class="tl-save-btn" data-id="'+l.id+'" data-idx="'+i+'">Save</button> <span id="tlNotesStatus'+i+'" style="font-size:10px;color:var(--muted)"></span>' +
        '</div>';
      });
      body.innerHTML = html;

      // Event listeners
      body.querySelectorAll('.tl-row').forEach(row => {
        row.addEventListener('click', (e) => {
          if (e.target.closest('.tl-note-icon')) return;
          showSetupDetail(row.dataset.id);
        });
      });
      body.querySelectorAll('.tl-note-icon').forEach(icon => {
        icon.addEventListener('click', (e) => {
          e.stopPropagation();
          const idx = icon.dataset.idx;
          const notesDiv = document.getElementById('tlNotes'+idx);
          notesDiv.style.display = notesDiv.style.display === 'block' ? 'none' : 'block';
        });
      });
      body.querySelectorAll('.tl-save-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
          e.stopPropagation();
          const logId = btn.dataset.id;
          const idx = btn.dataset.idx;
          const text = document.getElementById('tlNotesText'+idx).value;
          const status = document.getElementById('tlNotesStatus'+idx);
          try {
            const r = await fetch('/api/setup/log/'+logId+'/comment', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({comments:text})});
            if (r.ok) {
              status.textContent = 'Saved';
              status.style.color = '#22c55e';
              const entry = _tradeLogData.find(d => String(d.id) === String(logId));
              if (entry) entry.comments = text;
              const noteIcon = body.querySelector('.tl-note-icon[data-idx="'+idx+'"]');
              if (noteIcon) noteIcon.textContent = text.trim() ? '💬' : '📝';
            } else { status.textContent = 'Error'; status.style.color = '#ef4444'; }
          } catch(err) { status.textContent = 'Error'; status.style.color = '#ef4444'; }
          setTimeout(() => { status.textContent = ''; }, 2000);
        });
      });
    }

    // ===== TS SIM Log =====
    async function loadTsSimLog() {
      try {
        const r = await fetch('/api/auto-trade/log?limit=200', {cache:'no-store'});
        _tsSimData = await r.json();
        if (!Array.isArray(_tsSimData)) _tsSimData = [];
        renderTsSimLog();
        document.getElementById('tlStatus').textContent = _tsSimData.length + ' MES trades loaded';
      } catch(err) {
        console.error('loadTsSimLog error:', err);
        document.getElementById('tlBody').innerHTML = '<div style="color:var(--red);padding:12px">Error loading TS SIM log</div>';
      }
    }
    function _tsSimGetFiltered() {
      const fSetup = document.getElementById('tlFilterSetup').value;
      const fResult = document.getElementById('tlFilterResult').value;
      const fGrade = document.getElementById('tlFilterGrade').value;
      const fDate = document.getElementById('tlFilterDate').value;
      const fSearch = document.getElementById('tlSearch').value.toLowerCase().trim();
      const now = new Date();
      const todayET = new Date(now.toLocaleString('en-US',{timeZone:'America/New_York'}));
      const todayStr = todayET.getFullYear()+'-'+String(todayET.getMonth()+1).padStart(2,'0')+'-'+String(todayET.getDate()).padStart(2,'0');
      return _tsSimData.filter(l => {
        if (fSetup && l.setup_name !== fSetup) return false;
        if (fGrade && l.grade !== fGrade) return false;
        if (fResult) {
          let res = l.outcome_result || (l.status === 'closed' ? '' : 'OPEN');
          if (res !== fResult) return false;
        }
        if (fDate && l.ts) {
          const d = new Date(l.ts);
          const dET = new Date(d.toLocaleString('en-US',{timeZone:'America/New_York'}));
          if (fDate === 'today') { const dStr = dET.getFullYear()+'-'+String(dET.getMonth()+1).padStart(2,'0')+'-'+String(dET.getDate()).padStart(2,'0'); if (dStr !== todayStr) return false; }
          else if (fDate === 'week') { if ((todayET - dET)/86400000 > 7) return false; }
          else if (fDate === 'month') { if (dET.getMonth()!==todayET.getMonth()||dET.getFullYear()!==todayET.getFullYear()) return false; }
        }
        if (fSearch) {
          const hay = [l.setup_name, l.grade, l.direction, l.status].filter(Boolean).join(' ').toLowerCase();
          if (!hay.includes(fSearch)) return false;
        }
        return true;
      });
    }
    function renderTsSimLog() {
      const hdr = document.getElementById('tlHeaderRow');
      hdr.className = 'tl-header tl-grid-sim';
      hdr.innerHTML = '<span>#</span><span>Setup</span><span>Dir</span><span>Grade</span><span>Time</span><span>MES Entry</span><span>MES Stop</span><span>T1</span><span>T2</span><span>Result</span><span>P&L</span><span>Dur</span><span>Status</span>';
      const filtered = _tsSimGetFiltered();
      let wins=0,losses=0,dollarPnl=0,dollarN=0,ptsPnl=0,ptsN=0;
      filtered.forEach(l => {
        if (l.outcome_result==='WIN') wins++;
        else if (l.outcome_result==='LOSS') losses++;
        if (l.mes_pnl!=null) { dollarPnl += l.mes_pnl; dollarN++; }
        else if (l.outcome_pnl!=null) { ptsPnl += l.outcome_pnl; ptsN++; }
      });
      const wr = (wins+losses)>0 ? ((wins/(wins+losses))*100).toFixed(0) : '--';
      let pnlHtml = '--';
      if (dollarN>0) {
        const c = dollarPnl>=0?'#22c55e':'#ef4444';
        pnlHtml = '<span style="color:'+c+'">'+(dollarPnl>=0?'+$':'$')+dollarPnl.toFixed(0)+'</span>';
        if (ptsN>0) pnlHtml += ' <span style="color:var(--muted);font-size:9px">+ '+(ptsPnl>=0?'+':'')+ptsPnl.toFixed(1)+'pt legacy</span>';
      } else if (ptsN>0) {
        const c = ptsPnl>=0?'#22c55e':'#ef4444';
        pnlHtml = '<span style="color:'+c+'">'+(ptsPnl>=0?'+':'')+ptsPnl.toFixed(1)+' pts</span> <span style="color:var(--muted);font-size:8px">(portal)</span>';
      }
      document.getElementById('tlStats').innerHTML =
        '<span>Total: <span class="stat-val">'+filtered.length+'</span></span>' +
        '<span>Wins: <span class="stat-val" style="color:#22c55e">'+wins+'</span></span>' +
        '<span>Losses: <span class="stat-val" style="color:#ef4444">'+losses+'</span></span>' +
        '<span>WR: <span class="stat-val">'+wr+'%</span></span>' +
        '<span>Net P&L: '+pnlHtml+'</span>';
      const body = document.getElementById('tlBody');
      if (filtered.length===0) { body.innerHTML='<div style="color:var(--muted);text-align:center;padding:20px">No MES trades</div>'; return; }
      let html='';
      filtered.forEach((l,i) => {
        const pillColor = _tlPillColors[l.setup_name]||'#888';
        const dir = (l.direction==='long'||l.direction==='bullish') ? '\u25B2' : '\u25BC';
        const dirColor = (l.direction==='long'||l.direction==='bullish') ? '#22c55e' : '#ef4444';
        const gradeColor = _tlGradeColors[l.grade]||'#888';
        const fill = l.fill_price!=null ? l.fill_price.toFixed(2) : '--';
        const stop = l.current_stop!=null ? l.current_stop.toFixed(2) : '--';
        const t1 = l.t1_filled ? '\u2705' : (l.first_target_price!=null ? l.first_target_price.toFixed(0) : '--');
        const t2 = l.t2_filled ? '\u2705' : (l.full_target_price!=null ? l.full_target_price.toFixed(0) : 'trail');
        let result='<span style="color:#3b82f6;font-weight:600">OPEN</span>';
        if (l.outcome_result) {
          const rc = l.outcome_result==='WIN' ? '#22c55e' : l.outcome_result==='LOSS' ? '#ef4444' : '#888';
          result = '<span style="color:'+rc+';font-weight:700">'+l.outcome_result+'</span>';
        } else if (l.status==='closed') {
          result = '<span style="color:var(--muted)">CLOSED</span>';
        }
        let pnl='--', pnlC='#888';
        if (l.mes_pnl!=null) {
          pnl = '$'+(l.mes_pnl>=0?'+':'')+l.mes_pnl.toFixed(0);
          pnlC = l.mes_pnl>=0 ? '#22c55e' : '#ef4444';
        } else if (l.outcome_pnl!=null) {
          pnl = (l.outcome_pnl>=0?'+':'')+l.outcome_pnl.toFixed(1)+'pt';
          pnlC = l.outcome_pnl>=0 ? '#22c55e' : '#ef4444';
        }
        const em = l.outcome_elapsed_min;
        const durStr = em!=null ? (em>=60?Math.floor(em/60)+'h'+String(em%60).padStart(2,'0'):em+'m') : '--';
        const time = fmtTimeET(l.ts);
        const date = fmtDateShortET(l.ts);
        const statusColor = l.status==='filled'?'#f59e0b':l.status==='closed'?'var(--muted)':'#3b82f6';
        html += '<div class="tl-row tl-grid-sim">' +
          '<span style="color:var(--muted)">'+(i+1)+'</span>' +
          '<span class="setup-pill" style="background:'+pillColor+'22;color:'+pillColor+'">'+l.setup_name+'</span>' +
          '<span style="color:'+dirColor+';font-weight:700;text-align:center">'+dir+'</span>' +
          '<span style="color:'+gradeColor+';font-weight:600">'+l.grade+'</span>' +
          '<span style="color:var(--muted);font-size:9px">'+date+' '+time+'</span>' +
          '<span style="color:var(--text)">'+fill+'</span>' +
          '<span style="color:var(--muted)">'+stop+'</span>' +
          '<span style="font-size:10px;text-align:center">'+t1+'</span>' +
          '<span style="font-size:10px;text-align:center">'+t2+'</span>' +
          '<span style="font-size:10px">'+result+'</span>' +
          '<span style="color:'+pnlC+';font-size:10px">'+pnl+'</span>' +
          '<span style="color:var(--muted);font-size:9px">'+durStr+'</span>' +
          '<span style="color:'+statusColor+';font-size:9px;text-transform:uppercase">'+l.status+'</span>' +
        '</div>';
      });
      body.innerHTML = html;
    }

    // ===== Eval Log =====
    async function loadEvalLog() {
      try {
        const r = await fetch('/api/eval/log?limit=200', {cache:'no-store'});
        _evalLogData = await r.json();
        if (!Array.isArray(_evalLogData)) _evalLogData = [];
        renderEvalLog();
        document.getElementById('tlStatus').textContent = _evalLogData.length + ' eval signals loaded';
      } catch(err) {
        console.error('loadEvalLog error:', err);
        document.getElementById('tlBody').innerHTML = '<div style="color:var(--red);padding:12px">Error loading eval log</div>';
      }
    }
    function _evalGetFiltered() {
      const fSetup = document.getElementById('tlFilterSetup').value;
      const fResult = document.getElementById('tlFilterResult').value;
      const fGrade = document.getElementById('tlFilterGrade').value;
      const fDate = document.getElementById('tlFilterDate').value;
      const fSearch = document.getElementById('tlSearch').value.toLowerCase().trim();
      const now = new Date();
      const todayET = new Date(now.toLocaleString('en-US',{timeZone:'America/New_York'}));
      const todayStr = todayET.getFullYear()+'-'+String(todayET.getMonth()+1).padStart(2,'0')+'-'+String(todayET.getDate()).padStart(2,'0');
      return _evalLogData.filter(l => {
        if (fSetup && l.setup_name !== fSetup) return false;
        if (fGrade && l.grade !== fGrade) return false;
        if (fResult) {
          const res = l.outcome_result || 'OPEN';
          if (res !== fResult) return false;
        }
        if (fDate && l.ts) {
          const d = new Date(l.ts);
          const dET = new Date(d.toLocaleString('en-US',{timeZone:'America/New_York'}));
          if (fDate === 'today') { const dStr = dET.getFullYear()+'-'+String(dET.getMonth()+1).padStart(2,'0')+'-'+String(dET.getDate()).padStart(2,'0'); if (dStr !== todayStr) return false; }
          else if (fDate === 'week') { if ((todayET - dET)/86400000 > 7) return false; }
          else if (fDate === 'month') { if (dET.getMonth()!==todayET.getMonth()||dET.getFullYear()!==todayET.getFullYear()) return false; }
        }
        if (fSearch) {
          const hay = [l.setup_name, l.grade, l.direction].filter(Boolean).join(' ').toLowerCase();
          if (!hay.includes(fSearch)) return false;
        }
        return true;
      });
    }
    function renderEvalLog() {
      const hdr = document.getElementById('tlHeaderRow');
      hdr.className = 'tl-header tl-grid-eval';
      hdr.innerHTML = '<span>#</span><span>Setup</span><span>Dir</span><span>Grade</span><span>Time</span><span>Qty</span><span>Entry</span><span>Stop</span><span>Result</span><span>P&L</span><span>Dur</span><span>Status</span>';
      const filtered = _evalGetFiltered();
      let wins=0,losses=0,totalPnl=0,pnlCount=0;
      filtered.forEach(l => {
        if (l.outcome_result==='WIN') wins++;
        else if (l.outcome_result==='LOSS') losses++;
        if (l.outcome_pnl!=null) { totalPnl += l.outcome_pnl; pnlCount++; }
      });
      const wr = (wins+losses)>0 ? ((wins/(wins+losses))*100).toFixed(0) : '--';
      const pnlColor = totalPnl>=0 ? '#22c55e' : '#ef4444';
      const pnlStr = pnlCount>0 ? ((totalPnl>=0?'+':'')+totalPnl.toFixed(1)) : '--';
      document.getElementById('tlStats').innerHTML =
        '<span>Total: <span class="stat-val">'+filtered.length+'</span></span>' +
        '<span>Wins: <span class="stat-val" style="color:#22c55e">'+wins+'</span></span>' +
        '<span>Losses: <span class="stat-val" style="color:#ef4444">'+losses+'</span></span>' +
        '<span>WR: <span class="stat-val">'+wr+'%</span></span>' +
        '<span>Net P&L: <span class="stat-val" style="color:'+pnlColor+'">'+pnlStr+' pts</span></span>' +
        '<span style="color:var(--muted);font-size:8px;font-style:italic;margin-left:8px">P&L is portal SPX pts, not actual execution</span>';
      const body = document.getElementById('tlBody');
      if (filtered.length===0) { body.innerHTML='<div style="color:var(--muted);text-align:center;padding:20px">No eval signals</div>'; return; }
      let html='';
      filtered.forEach((l,i) => {
        const pillColor = _tlPillColors[l.setup_name]||'#888';
        const dir = (l.direction==='long'||l.direction==='bullish') ? '\u25B2' : '\u25BC';
        const dirColor = (l.direction==='long'||l.direction==='bullish') ? '#22c55e' : '#ef4444';
        const gradeColor = _tlGradeColors[l.grade]||'#888';
        const entry = l.entry_price ? l.entry_price.toFixed(2) : '--';
        const stopPts = l.stop_pts || '--';
        let result='<span style="color:#3b82f6;font-weight:600">OPEN</span>';
        if (l.outcome_result) {
          const rc = l.outcome_result==='WIN' ? '#22c55e' : l.outcome_result==='LOSS' ? '#ef4444' : '#888';
          result = '<span style="color:'+rc+';font-weight:700">'+l.outcome_result+'</span>';
        }
        let pnl='--', pnlC='#888';
        if (l.outcome_pnl!=null) {
          pnl = (l.outcome_pnl>=0?'+':'')+l.outcome_pnl.toFixed(1);
          pnlC = l.outcome_pnl>=0 ? '#22c55e' : '#ef4444';
        }
        const em = l.outcome_elapsed_min;
        const durStr = em!=null ? (em>=60?Math.floor(em/60)+'h'+String(em%60).padStart(2,'0'):em+'m') : '--';
        const time = fmtTimeET(l.ts);
        const date = fmtDateShortET(l.ts);
        const status = l.outcome_result || 'OPEN';
        const statusColor = status==='WIN'?'#22c55e':status==='LOSS'?'#ef4444':'#3b82f6';
        html += '<div class="tl-row tl-grid-eval">' +
          '<span style="color:var(--muted)">'+(i+1)+'</span>' +
          '<span class="setup-pill" style="background:'+pillColor+'22;color:'+pillColor+'">'+l.setup_name+'</span>' +
          '<span style="color:'+dirColor+';font-weight:700;text-align:center">'+dir+'</span>' +
          '<span style="color:'+gradeColor+';font-weight:600">'+l.grade+'</span>' +
          '<span style="color:var(--muted);font-size:9px">'+date+' '+time+'</span>' +
          '<span style="color:var(--text);text-align:center">'+l.qty+'</span>' +
          '<span style="color:var(--text)">'+entry+'</span>' +
          '<span style="color:var(--muted)">'+stopPts+'pt</span>' +
          '<span style="font-size:10px">'+result+'</span>' +
          '<span style="color:'+pnlC+';font-size:10px">'+pnl+'</span>' +
          '<span style="color:var(--muted);font-size:9px">'+durStr+'</span>' +
          '<span style="color:'+statusColor+';font-size:9px;font-weight:600">'+status+'</span>' +
        '</div>';
      });
      body.innerHTML = html;
    }

    // Wire up filter changes
    function _tlRerender() {
      if (_tlActiveSubTab === 'portal') renderTradeLog();
      else if (_tlActiveSubTab === 'tssim') renderTsSimLog();
      else if (_tlActiveSubTab === 'eval') renderEvalLog();
    }
    ['tlFilterSetup','tlFilterResult','tlFilterGrade','tlFilterDate'].forEach(id => {
      document.getElementById(id).addEventListener('change', _tlRerender);
    });
    document.getElementById('tlSearch').addEventListener('input', _tlRerender);

    async function loadSetupLog() {
      const list = document.getElementById('setupLogList');
      if (!list) return;
      try {
        const r = await fetch('/api/setup/log_with_outcomes?limit=30', { cache: 'no-store' });
        const logs = await r.json();
        if (!logs || logs.length === 0) {
          list.innerHTML = '<div style="color:var(--muted);text-align:center;padding:12px">No detections yet</div>';
          return;
        }
        const gradeColor = { 'A+': '#22c55e', 'A': '#3b82f6', 'A-Entry': '#eab308' };
        // Header row
        const header = `<div style="display:grid;grid-template-columns:28px 50px 28px 55px 60px 70px 50px 1fr;align-items:center;gap:4px;padding:4px 2px;border-bottom:2px solid var(--border);color:var(--muted);font-size:9px;font-weight:600">
          <span>Dir</span><span>Grade</span><span>Scr</span><span>SPX</span><span>Gap/RR</span><span>10p Tgt Stp</span><span>Result</span><span style="text-align:right">Time</span>
        </div>`;
        list.innerHTML = header + logs.map(l => {
          const time = fmtTimeET(l.ts);
          const date = fmtDateShortET(l.ts);
          const color = gradeColor[l.grade] || '#888';
          const bell = l.notified ? '&#128276;' : '';
          const isBofa = l.setup_name === 'BofA Scalp';
          const isAbs = l.setup_name === 'ES Absorption';
          const dir = isAbs ? (l.direction === 'bullish' ? '▲' : '▼') : (l.direction === 'long' ? '▲' : '▼');
          const dirColor = (l.direction === 'long' || l.direction === 'bullish') ? '#22c55e' : '#ef4444';
          const o = l.outcome || {};
          const tgtLabel = isBofa ? '15p' : '10p';
          const has10pt = o.hit_10pt === true ? '✓' : (o.hit_10pt === false ? '✗' : '–');
          const hasTgt = o.hit_target === true ? '✓' : (o.hit_target === false ? '✗' : '–');
          const hasStop = o.hit_stop === true ? '✗' : (o.hit_stop === false ? '✓' : '–');
          const c10 = o.hit_10pt ? '#22c55e' : (o.hit_10pt === false ? '#888' : '#555');
          const cTgt = o.hit_target ? '#22c55e' : (o.hit_target === false ? '#888' : '#555');
          const stopIsLoss = o.hit_stop && o.first_event === 'stop';
          const cStop = stopIsLoss ? '#ef4444' : (o.hit_stop === false ? '#22c55e' : '#888');
          let result = '';
          if (l.outcome_result) {
            const rc = l.outcome_result === 'WIN' ? '#22c55e' : l.outcome_result === 'LOSS' ? '#ef4444' : '#888';
            result = '<span style="color:'+rc+';font-weight:700">'+l.outcome_result+'</span>';
          } else if (o.hit_target) {
            result = '<span style="color:#22c55e;font-weight:700">WIN</span>';
          } else if (o.hit_stop) {
            result = '<span style="color:#ef4444;font-weight:700">LOSS</span>';
          } else {
            result = '<span style="color:#3b82f6;font-size:8px;font-weight:600">OPEN</span>';
          }
          const nameTag = isBofa ? '<span style="color:#a78bfa;font-size:7px;font-weight:600">BofA</span>' : isAbs ? '<span style="color:#f59e0b;font-size:7px;font-weight:600">Abs</span>' : '';
          return `<div class="setup-log-row" data-id="${l.id}" style="display:grid;grid-template-columns:28px 50px 28px 55px 60px 70px 50px 1fr;align-items:center;gap:4px;padding:4px 2px;border-bottom:1px solid var(--border);cursor:pointer" onmouseover="this.style.background='#1a1d21'" onmouseout="this.style.background='transparent'">
            <span style="color:${dirColor};font-weight:700;text-align:center">${dir}${nameTag ? '<br>' + nameTag : ''}</span>
            <span style="color:${color};font-weight:600">${l.grade}</span>
            <span style="color:var(--muted)">${l.score}</span>
            <span style="color:var(--text)">${isAbs ? (l.abs_es_price || l.spot)?.toFixed(2) : l.spot?.toFixed(0)}</span>
            <span style="color:var(--muted);font-size:9px">${isAbs ? (l.abs_vol_ratio || 0).toFixed(1) + 'x vol' : (l.gap_to_lis?.toFixed(1) + ' / ' + l.rr_ratio?.toFixed(1) + 'x')}</span>
            <span style="font-size:9px"><span style="color:${c10}">${has10pt}</span> <span style="color:${cTgt}">${hasTgt}</span> <span style="color:${cStop}">${hasStop}</span></span>
            <span style="font-size:9px">${result}</span>
            <span style="color:var(--muted);font-size:9px;text-align:right">${date} ${time} ${bell}</span>
          </div>`;
        }).join('');
        // Add click handlers
        list.querySelectorAll('.setup-log-row').forEach(row => {
          row.addEventListener('click', () => showSetupDetail(row.dataset.id));
        });
      } catch (err) {
        list.innerHTML = '<div style="color:var(--red)">Error loading log</div>';
        console.error('loadSetupLog error:', err);
      }
    }

    async function showSetupDetail(logId) {
      const modal = document.getElementById('setupDetailModal');
      const title = document.getElementById('setupDetailTitle');
      const info = document.getElementById('setupDetailInfo');
      const outcome = document.getElementById('setupDetailOutcome');
      const chart = document.getElementById('setupDetailChart');
      const stats = document.getElementById('setupDetailStats');

      const commentsBox = document.getElementById('setupDetailComments');
      const saveBtn = document.getElementById('setupDetailSaveComment');
      const statusSpan = document.getElementById('setupDetailCommentStatus');

      title.textContent = 'Loading...';
      info.innerHTML = '';
      outcome.innerHTML = '';
      stats.innerHTML = '';
      commentsBox.value = '';
      saveBtn.style.display = 'none';
      statusSpan.style.display = 'none';
      modal.style.display = 'flex';

      try {
        const r = await fetch('/api/setup/log/' + logId + '/outcome', { cache: 'no-store' });
        const data = await r.json();

        if (data.error) {
          title.textContent = 'Error: ' + data.error;
          return;
        }

        const e = data.entry;
        const o = data.outcome || {};
        const lv = data.levels || {};

        // Title
        const isBofa = e.setup_name === 'BofA Scalp';
        const isAbs = e.setup_name === 'ES Absorption';
        const dir = isAbs ? (e.direction === 'bullish' ? 'BUY ▲' : 'SELL ▼') : (e.direction === 'long' ? 'LONG ▲' : 'SHORT ▼');
        const dirColor = (e.direction === 'long' || e.direction === 'bullish') ? '#22c55e' : '#ef4444';
        const setupLabel = isBofa ? 'BofA Scalp ' : isAbs ? 'ES Absorption ' : '';
        const priceLabel = isAbs ? '@ ES ' : '@ SPX ';
        const displayPrice = isAbs ? (e.abs_es_price || e.spot)?.toFixed(2) : e.spot?.toFixed(0);
        title.innerHTML = setupLabel + '<span style="color:' + dirColor + '">' + dir + '</span> ' + e.grade + ' ' + priceLabel + displayPrice;

        // Info grid
        const infoItems = isAbs ? [
          ['Time', fmtDateTimeET(e.ts) + ' ET'],
          ['ES Entry', (lv.abs_es_price || e.abs_es_price)?.toFixed(2)],
          ['10pt Target', lv.ten_pt?.toFixed(2) || '–'],
          ['Vol Target', lv.target_es?.toFixed(2) || '10pt only'],
          ['Stop (-12)', lv.stop?.toFixed(2) || '–'],
          ['Vol Ratio', (e.abs_vol_ratio || 0).toFixed(1) + 'x'],
          ['Paradigm', e.paradigm || '–'],
          ['LIS (SPX)', e.lis?.toFixed(0) || '–'],
          ['Score', e.score + '/100'],
          ['Div', e.support_score],
          ['Vol', e.upside_score],
          ['DD', e.floor_cluster_score],
        ] : isBofa ? [
          ['Time', fmtDateTimeET(e.ts) + ' ET'],
          ['Paradigm', e.paradigm || '–'],
          ['Entry', e.spot?.toFixed(2)],
          ['LIS', e.lis?.toFixed(0) + (lv.lis_upper ? ' – ' + lv.lis_upper?.toFixed(0) : '')],
          ['Width', e.bofa_lis_width?.toFixed(0) + 'pts'],
          ['Target (+10)', lv.bofa_target_level?.toFixed(0) || lv.ten_pt?.toFixed(0)],
          ['Stop (-12)', lv.stop?.toFixed(0)],
          ['Max Hold', (lv.bofa_max_hold_minutes || 30) + 'min'],
          ['Gap', e.gap_to_lis?.toFixed(1)],
          ['Score', e.score],
        ] : [
          ['Time', fmtDateTimeET(e.ts) + ' ET'],
          ['Paradigm', e.paradigm || '–'],
          ['Entry', e.spot?.toFixed(2)],
          ['LIS', e.lis?.toFixed(0)],
          ['Target', e.target?.toFixed(0)],
          ['10pt Level', lv.ten_pt?.toFixed(0)],
          ['Stop Level', lv.stop?.toFixed(0)],
          ['Gap', e.gap_to_lis?.toFixed(1)],
          ['R:R', e.rr_ratio?.toFixed(1) + 'x'],
          ['+GEX', lv.max_plus_gex?.toFixed(0)],
          ['-GEX', lv.max_minus_gex?.toFixed(0)],
          ['Score', e.score],
        ];
        info.innerHTML = infoItems.map(([k, v]) => '<div style="background:#1a1d21;padding:6px 8px;border-radius:4px"><div style="color:var(--muted);font-size:9px">' + k + '</div><div style="color:var(--text);font-weight:600">' + (v || '–') + '</div></div>').join('');

        // Outcome row
        if (isAbs) {
          if (o.no_data || o.error) {
            outcome.innerHTML = '<div style="color:var(--muted);text-align:center;padding:8px;font-size:11px">ES Absorption — ' + (o.error || 'no ES range bar data for outcome') + '</div>';
          } else {
            const ac10 = o.hit_10pt ? '#22c55e' : '#888';
            const acTgt = o.hit_target ? '#22c55e' : '#888';
            const aStopIsLoss = o.hit_stop && o.first_event === 'stop';
            const acStop = aStopIsLoss ? '#ef4444' : (o.hit_stop ? '#888' : '#22c55e');
            const aStopLabel = o.hit_stop ? (aStopIsLoss ? '✗ STOPPED' : 'STOPPED (BE)') : '✓ SAFE';
            const isPending = o.first_event === 'pending';
            const isBofaPara = o.is_bofa_paradigm;
            outcome.innerHTML = `
              <div style="flex:1;text-align:center">
                <div style="color:var(--muted);font-size:10px">10pt Target</div>
                <div style="color:${isPending ? '#888' : ac10};font-size:18px;font-weight:700">${isPending ? '⏳' : (o.hit_10pt ? '✓ HIT' : '✗ MISS')}</div>
                ${o.time_to_10pt ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.time_to_10pt) + ' ET</div>' : ''}
              </div>
              <div style="flex:1;text-align:center">
                <div style="color:var(--muted);font-size:10px">${isBofaPara ? '10pt (BofA)' : 'Vol Target'}</div>
                <div style="color:${isPending ? '#888' : acTgt};font-size:18px;font-weight:700">${isPending ? '⏳' : (o.hit_target ? '✓ HIT' : '✗ MISS')}</div>
                ${o.time_to_target ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.time_to_target) + ' ET</div>' : ''}
              </div>
              <div style="flex:1;text-align:center">
                <div style="color:var(--muted);font-size:10px">Stop</div>
                <div style="color:${isPending ? '#888' : acStop};font-size:18px;font-weight:700">${isPending ? '⏳' : aStopLabel}</div>
                ${o.time_to_stop ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.time_to_stop) + ' ET</div>' : ''}
              </div>
              <div style="flex:1;text-align:center">
                <div style="color:var(--muted);font-size:10px">Max Profit</div>
                <div style="color:#22c55e;font-size:18px;font-weight:700">+${o.max_profit?.toFixed(1) || 0}</div>
                ${o.max_profit_ts ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.max_profit_ts) + ' ET</div>' : ''}
              </div>
              <div style="flex:1;text-align:center">
                <div style="color:var(--muted);font-size:10px">Max Loss</div>
                <div style="color:#ef4444;font-size:18px;font-weight:700">${o.max_loss?.toFixed(1) || 0}</div>
                ${o.max_loss_ts ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.max_loss_ts) + ' ET</div>' : ''}
              </div>
            `;
          }
        } else {
        const c10 = o.hit_10pt ? '#22c55e' : '#888';
        const cTgt = o.hit_target ? '#22c55e' : '#888';
        const stopIsLoss = o.hit_stop && o.first_event === 'stop';
        const cStop = stopIsLoss ? '#ef4444' : (o.hit_stop ? '#888' : '#22c55e');
        const stopLabel = o.hit_stop ? (stopIsLoss ? '✗ STOPPED' : 'STOPPED (BE)') : '✓ SAFE';
        const tgtPtLabel = isBofa ? '10pt Target' : '10pt Target';
        const hasTimeout = o.first_event === 'timeout';
        const timeoutPnl = o.timeout_pnl || 0;
        outcome.innerHTML = `
          <div style="flex:1;text-align:center">
            <div style="color:var(--muted);font-size:10px">${tgtPtLabel}</div>
            <div style="color:${c10};font-size:18px;font-weight:700">${o.hit_10pt ? '✓ HIT' : '✗ MISS'}</div>
            ${o.time_to_10pt ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.time_to_10pt) + ' ET</div>' : ''}
          </div>
          ${isBofa ? `<div style="flex:1;text-align:center">
            <div style="color:var(--muted);font-size:10px">Timeout</div>
            <div style="color:${hasTimeout ? (timeoutPnl >= 0 ? '#22c55e' : '#ef4444') : '#888'};font-size:18px;font-weight:700">${hasTimeout ? (timeoutPnl >= 0 ? '+' : '') + timeoutPnl.toFixed(1) : '–'}</div>
          </div>` : `<div style="flex:1;text-align:center">
            <div style="color:var(--muted);font-size:10px">Full Target</div>
            <div style="color:${cTgt};font-size:18px;font-weight:700">${o.hit_target ? '✓ HIT' : '✗ MISS'}</div>
            ${o.time_to_target ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.time_to_target) + ' ET</div>' : ''}
          </div>`}
          <div style="flex:1;text-align:center">
            <div style="color:var(--muted);font-size:10px">Stop</div>
            <div style="color:${cStop};font-size:18px;font-weight:700">${stopLabel}</div>
            ${o.time_to_stop ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.time_to_stop) + ' ET</div>' : ''}
          </div>
          <div style="flex:1;text-align:center">
            <div style="color:var(--muted);font-size:10px">Max Profit</div>
            <div style="color:#22c55e;font-size:18px;font-weight:700">+${o.max_profit?.toFixed(1) || 0}</div>
            ${o.max_profit_ts ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.max_profit_ts) + ' ET</div>' : ''}
          </div>
          <div style="flex:1;text-align:center">
            <div style="color:var(--muted);font-size:10px">Max Loss</div>
            <div style="color:#ef4444;font-size:18px;font-weight:700">${o.max_loss?.toFixed(1) || 0}</div>
            ${o.max_loss_ts ? '<div style="color:var(--muted);font-size:9px">' + fmtTimeET(o.max_loss_ts) + ' ET</div>' : ''}
          </div>
        `;
        } // end else (non-absorption outcome)

        // Draw chart
        if (isAbs && data.es_bars && data.es_bars.length > 0) {
          // ES Absorption: render ES range bar candlestick + CVD chart
          const esBars = data.es_bars;

          // Find signal bar by closest price match (robust float comparison)
          const sigPrice = e.abs_es_price || e.spot;
          let sigBarIdx = -1;
          if (sigPrice) {
            let minDist = Infinity;
            for (let i = 0; i < esBars.length; i++) {
              const d = Math.abs(esBars[i].close - sigPrice);
              if (d < minDist) { minDist = d; sigBarIdx = i; }
            }
            // Also try matching by timestamp (more reliable)
            if (e.ts) {
              const sigTs = new Date(e.ts).getTime();
              let minTsDist = Infinity;
              for (let i = 0; i < esBars.length; i++) {
                const barTs = new Date(esBars[i].ts_end).getTime();
                const d = Math.abs(barTs - sigTs);
                if (d < minTsDist) { minTsDist = d; sigBarIdx = i; }
              }
            }
          }

          // Window: show signal bar ± 30 bars for context, minimum 20 bars before
          const contextBefore = 30, contextAfter = 30;
          const winStart = Math.max(0, sigBarIdx - contextBefore);
          const winEnd = Math.min(esBars.length, sigBarIdx + contextAfter + 1);
          const visibleBars = esBars.slice(winStart, winEnd);
          const barLabels = visibleBars.map(b => b.idx.toString());

          // Price candlestick trace
          const priceTrace = {
            type: 'candlestick',
            x: barLabels,
            open: visibleBars.map(b => b.open),
            high: visibleBars.map(b => b.high),
            low: visibleBars.map(b => b.low),
            close: visibleBars.map(b => b.close),
            increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
            decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
            name: 'ES Price',
            yaxis: 'y'
          };

          // CVD line trace (secondary y-axis)
          const cvdTrace = {
            type: 'scatter', mode: 'lines',
            x: barLabels,
            y: visibleBars.map(b => b.cvd),
            line: { color: '#60a5fa', width: 1.5 },
            name: 'CVD',
            yaxis: 'y2'
          };

          const shapes = [];
          const annots = [];

          // Mark signal bar with vertical line
          if (sigBarIdx >= winStart && sigBarIdx < winEnd) {
            const sigLabel = esBars[sigBarIdx].idx.toString();
            shapes.push({ type:'line', x0:sigLabel, x1:sigLabel, y0:0, y1:1, yref:'paper', line:{color:'#f59e0b',width:3,dash:'solid'} });
            const isBull = e.direction === 'bullish';
            const arrow = isBull ? '▲ BUY' : '▼ SELL';
            annots.push({ x:sigLabel, y:1, yref:'paper', text:arrow + ' ' + e.grade, showarrow:false, font:{color:'#f59e0b',size:11,weight:'bold'}, yanchor:'bottom' });
          }

          // Entry price level line
          if (lv.entry) {
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:lv.entry, y1:lv.entry, line:{color:'#f59e0b',width:2,dash:'solid'} });
            annots.push({ x:barLabels[0], y:lv.entry, text:'Entry ' + lv.entry?.toFixed(2), showarrow:false, font:{color:'#f59e0b',size:10}, xanchor:'left' });
          }
          // 10pt target line
          if (lv.ten_pt) {
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:lv.ten_pt, y1:lv.ten_pt, line:{color:'#22c55e',width:1,dash:'dash'} });
            annots.push({ x:barLabels[barLabels.length-1], y:lv.ten_pt, text:'10pt', showarrow:false, font:{color:'#22c55e',size:9}, xanchor:'right' });
          }
          // Volland target (ES converted)
          if (lv.target_es && lv.target_es !== lv.ten_pt) {
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:lv.target_es, y1:lv.target_es, line:{color:'#10b981',width:1,dash:'dot'} });
            annots.push({ x:barLabels[barLabels.length-1], y:lv.target_es, text:'Tgt', showarrow:false, font:{color:'#10b981',size:9}, xanchor:'right' });
          }
          // Stop line
          if (lv.stop) {
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:lv.stop, y1:lv.stop, line:{color:'#ef4444',width:2,dash:'dash'} });
            annots.push({ x:barLabels[barLabels.length-1], y:lv.stop, text:'Stop', showarrow:false, font:{color:'#ef4444',size:9}, xanchor:'right' });
          }
          // SPX→ES converted levels (LIS, +GEX, -GEX)
          const esOffset = lv.offset || 0;
          if (lv.lis && esOffset) {
            const esLIS = lv.lis + esOffset;
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:esLIS, y1:esLIS, line:{color:'#f97316',width:1,dash:'dot'} });
            annots.push({ x:barLabels[0], y:esLIS, text:'LIS ' + esLIS.toFixed(0), showarrow:false, font:{color:'#f97316',size:9}, xanchor:'left' });
          }
          if (lv.max_plus_gex && esOffset) {
            const esGP = lv.max_plus_gex + esOffset;
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:esGP, y1:esGP, line:{color:'#22c55e',width:1,dash:'dot'} });
            annots.push({ x:barLabels[0], y:esGP, text:'+G ' + esGP.toFixed(0), showarrow:false, font:{color:'#22c55e',size:9}, xanchor:'left' });
          }
          if (lv.max_minus_gex && esOffset) {
            const esGM = lv.max_minus_gex + esOffset;
            shapes.push({ type:'line', x0:barLabels[0], x1:barLabels[barLabels.length-1], y0:esGM, y1:esGM, line:{color:'#ef4444',width:1,dash:'dot'} });
            annots.push({ x:barLabels[0], y:esGM, text:'-G ' + esGM.toFixed(0), showarrow:false, font:{color:'#ef4444',size:9}, xanchor:'left' });
          }

          Plotly.react(chart, [priceTrace, cvdTrace], {
            margin: { l:50, r:50, t:20, b:40 },
            paper_bgcolor: '#0f1115',
            plot_bgcolor: '#0a0c0f',
            xaxis: { type:'category', gridcolor:'#1a1d21', tickfont:{size:9,color:'#888'}, tickangle:0, nticks:10, title:{text:'Bar #',font:{size:9,color:'#666'}} },
            yaxis: { gridcolor:'#1a1d21', tickfont:{size:10,color:'#888'}, side:'left', title:{text:'ES Price',font:{size:9,color:'#666'}} },
            yaxis2: { overlaying:'y', side:'right', gridcolor:'transparent', tickfont:{size:9,color:'#60a5fa'}, title:{text:'CVD',font:{size:9,color:'#60a5fa'}}, showgrid:false },
            font: { color:'#e6e7e9' },
            shapes: shapes,
            annotations: annots,
            showlegend: true,
            legend: { x:0, y:1.1, orientation:'h', font:{size:10,color:'#888'} }
          }, { displayModeBar:false, responsive:true });
        } else if (data.prices && data.prices.length > 0) {
          const times = data.prices.map(p => fmtTimeET(p.ts));
          const spots = data.prices.map(p => p.spot);

          // Build candlestick data from consecutive prices
          const opens = [], highs = [], lows = [], closes = [];
          for (let i = 0; i < spots.length; i++) {
            const curr = spots[i];
            const prev = i > 0 ? spots[i - 1] : curr;
            opens.push(prev);
            closes.push(curr);
            highs.push(Math.max(prev, curr) + Math.abs(curr - prev) * 0.1);
            lows.push(Math.min(prev, curr) - Math.abs(curr - prev) * 0.1);
          }

          // Candlestick trace
          const trace = {
            type: 'candlestick',
            x: times,
            open: opens,
            high: highs,
            low: lows,
            close: closes,
            increasing: { line: { color: '#22c55e' }, fillcolor: '#22c55e' },
            decreasing: { line: { color: '#ef4444' }, fillcolor: '#ef4444' },
            name: 'Price'
          };

          // Get entry time and find the candle that contains it.
          // Each candle at times[i] covers the period times[i-1] -> times[i],
          // so we need the first snapshot AT or AFTER the entry (full timestamp comparison).
          const entryTimeET = fmtTimeET(e.ts);
          const entryMs = new Date(e.ts).getTime();
          let entryLabel = times[times.length - 1];
          for (let i = 0; i < data.prices.length; i++) {
            if (new Date(data.prices[i].ts).getTime() >= entryMs) {
              entryLabel = times[i];
              break;
            }
          }

          // Horizontal level lines + vertical entry time line
          const shapes = [];
          const annotations = [];

          // Vertical line at entry time (using closest available time)
          shapes.push({ type:'line', x0:entryLabel, x1:entryLabel, y0:0, y1:1, yref:'paper', line:{color:'#f59e0b',width:3,dash:'solid'} });
          annotations.push({ x:entryLabel, y:1, yref:'paper', text:'▼ ENTRY ' + entryTimeET + ' ET', showarrow:false, font:{color:'#f59e0b',size:11,weight:'bold'}, yanchor:'bottom' });

          // Entry level (horizontal)
          shapes.push({ type:'line', x0:times[0], x1:times[times.length-1], y0:lv.entry, y1:lv.entry, line:{color:'#f59e0b',width:2,dash:'solid'} });
          annotations.push({ x:times[0], y:lv.entry, text:'Entry ' + lv.entry?.toFixed(0), showarrow:false, font:{color:'#f59e0b',size:10}, xanchor:'left' });

          // 10pt level
          if (lv.ten_pt) {
            shapes.push({ type:'line', x0:times[0], x1:times[times.length-1], y0:lv.ten_pt, y1:lv.ten_pt, line:{color:'#22c55e',width:1,dash:'dash'} });
            annotations.push({ x:times[times.length-1], y:lv.ten_pt, text:'10pt', showarrow:false, font:{color:'#22c55e',size:9}, xanchor:'right' });
          }

          // Target
          if (lv.target) {
            shapes.push({ type:'line', x0:times[0], x1:times[times.length-1], y0:lv.target, y1:lv.target, line:{color:'#10b981',width:1,dash:'dot'} });
            annotations.push({ x:times[times.length-1], y:lv.target, text:'Target', showarrow:false, font:{color:'#10b981',size:9}, xanchor:'right' });
          }

          // Stop level
          if (lv.stop) {
            shapes.push({ type:'line', x0:times[0], x1:times[times.length-1], y0:lv.stop, y1:lv.stop, line:{color:'#ef4444',width:2,dash:'dash'} });
            annotations.push({ x:times[times.length-1], y:lv.stop, text:'Stop', showarrow:false, font:{color:'#ef4444',size:9}, xanchor:'right' });
          }

          // LIS
          if (lv.lis) {
            shapes.push({ type:'line', x0:times[0], x1:times[times.length-1], y0:lv.lis, y1:lv.lis, line:{color:'#f97316',width:1,dash:'dot'} });
            annotations.push({ x:times[0], y:lv.lis, text:isBofa ? 'LIS Low' : 'LIS', showarrow:false, font:{color:'#f97316',size:9}, xanchor:'left' });
          }
          // BofA: upper LIS line
          if (isBofa && lv.lis_upper) {
            shapes.push({ type:'line', x0:times[0], x1:times[times.length-1], y0:lv.lis_upper, y1:lv.lis_upper, line:{color:'#f97316',width:1,dash:'dot'} });
            annotations.push({ x:times[0], y:lv.lis_upper, text:'LIS High', showarrow:false, font:{color:'#f97316',size:9}, xanchor:'left' });
          }

          Plotly.react(chart, [trace], {
            margin: { l:50, r:10, t:20, b:40 },
            paper_bgcolor: '#0f1115',
            plot_bgcolor: '#0a0c0f',
            xaxis: { type:'category', gridcolor:'#1a1d21', tickfont:{size:9,color:'#888'}, tickangle:-45, nticks:15 },
            yaxis: { gridcolor:'#1a1d21', tickfont:{size:10,color:'#888'}, side:'left' },
            font: { color:'#e6e7e9' },
            shapes: shapes,
            annotations: annotations,
            showlegend: false
          }, { displayModeBar:false, responsive:true });

          // Add markers for max profit/loss points (using ET times)
          if (o.max_profit_ts) {
            const mpLabel = fmtTimeET(o.max_profit_ts);
            const mpIdx = times.indexOf(mpLabel);
            const mpPrice = mpIdx >= 0 ? spots[mpIdx] : (e.direction === 'long' ? lv.entry + o.max_profit : lv.entry - o.max_profit);
            Plotly.addTraces(chart, { type:'scatter', mode:'markers', x:[mpLabel], y:[mpPrice], marker:{color:'#22c55e',size:12,symbol:'triangle-up'}, name:'Max Profit', showlegend:false });
          }
          if (o.max_loss_ts) {
            const mlLabel = fmtTimeET(o.max_loss_ts);
            const mlIdx = times.indexOf(mlLabel);
            const mlPrice = mlIdx >= 0 ? spots[mlIdx] : (e.direction === 'long' ? lv.entry + o.max_loss : lv.entry - o.max_loss);
            Plotly.addTraces(chart, { type:'scatter', mode:'markers', x:[mlLabel], y:[mlPrice], marker:{color:'#ef4444',size:12,symbol:'triangle-down'}, name:'Max Loss', showlegend:false });
          }
        } else {
          chart.innerHTML = '<div style="color:var(--muted);text-align:center;padding:100px 20px">No price data available for this period</div>';
        }

        // Stats
        const scoreLabels = isAbs
          ? [['Divergence', e.support_score], ['Volume', e.upside_score], ['DD Hedging', e.floor_cluster_score], ['Paradigm', e.target_cluster_score], ['LIS Prox', e.rr_score]]
          : isBofa
          ? [['Stability', e.support_score], ['Width', e.upside_score], ['Charm', e.floor_cluster_score], ['Time of Day', e.target_cluster_score], ['Midpoint', e.rr_score]]
          : [['Support', e.support_score], ['Upside', e.upside_score], ['Floor Cluster', e.floor_cluster_score], ['Target Cluster', e.target_cluster_score], ['R:R Score', e.rr_score]];
        const scoreRows = scoreLabels.map(([k, v]) => '<div>' + k + ': <span style="color:var(--text)">' + (v || '–') + '</span></div>').join('');
        const bonusRow = (isBofa || isAbs) ? '' : '<div>First Hour: <span style="color:var(--text)">' + (e.first_hour ? 'Yes (+10)' : 'No') + '</span></div>';
        let summaryLabel = '';
        if (l.outcome_result) {
          if (l.outcome_result === 'WIN') summaryLabel = '<span style="color:#22c55e;font-weight:700;font-size:14px">✓ WINNER</span>';
          else if (l.outcome_result === 'LOSS') summaryLabel = '<span style="color:#ef4444;font-weight:700;font-size:14px">✗ LOSER</span>';
          else if (l.outcome_result === 'EXPIRED') {
            const tp = l.outcome_pnl || 0;
            summaryLabel = tp >= 0
              ? '<span style="color:#22c55e;font-weight:700;font-size:14px">⏱ EXPIRED +' + tp.toFixed(1) + '</span>'
              : '<span style="color:#ef4444;font-weight:700;font-size:14px">⏱ EXPIRED ' + tp.toFixed(1) + '</span>';
          } else summaryLabel = '<span style="color:#888;font-weight:700;font-size:14px">' + l.outcome_result + '</span>';
        } else if (o.hit_target) {
          summaryLabel = '<span style="color:#22c55e;font-weight:700;font-size:14px">✓ WINNER</span>';
        } else if (o.hit_stop) {
          summaryLabel = '<span style="color:#ef4444;font-weight:700;font-size:14px">✗ LOSER</span>';
        } else {
          summaryLabel = '<span style="color:#3b82f6;font-weight:700;font-size:14px">OPEN</span>';
        }
        stats.innerHTML = `
          <div style="background:#1a1d21;padding:10px;border-radius:6px">
            <div style="font-weight:600;margin-bottom:6px;color:var(--muted)">Score Breakdown</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:10px">
              ${scoreRows}
              ${bonusRow}
            </div>
          </div>
          <div style="background:#1a1d21;padding:10px;border-radius:6px">
            <div style="font-weight:600;margin-bottom:6px;color:var(--muted)">Trade Summary</div>
            <div style="font-size:10px">
              <div>First Event: <span style="color:${evtColor};font-weight:600">${(firstEvt.toUpperCase() || 'NONE')}</span></div>
              ${isAbs ? '<div>Bars After Signal: <span style="color:var(--text)">' + (o.bars_after || 0) + '</span></div>' : '<div>Data Points: <span style="color:var(--text)">' + (o.price_count || 0) + '</span></div>'}
              <div style="margin-top:6px;padding-top:6px;border-top:1px solid var(--border)">
                ${summaryLabel}
              </div>
            </div>
          </div>
        `;
        // Comments
        commentsBox.value = e.comments || '';
        saveBtn.style.display = 'inline-block';
        saveBtn.onclick = async () => {
          saveBtn.disabled = true;
          saveBtn.textContent = 'Saving...';
          try {
            await fetch('/api/setup/log/' + logId + '/comment', {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({ comments: commentsBox.value })
            });
            statusSpan.style.display = 'inline';
            statusSpan.textContent = 'Saved';
            statusSpan.style.color = '#22c55e';
            setTimeout(() => { statusSpan.style.display = 'none'; }, 2000);
          } catch (err) {
            statusSpan.style.display = 'inline';
            statusSpan.textContent = 'Error saving';
            statusSpan.style.color = '#ef4444';
          }
          saveBtn.disabled = false;
          saveBtn.textContent = 'Save';
        };
      } catch (err) {
        title.textContent = 'Error loading details';
        console.error('showSetupDetail error:', err);
      }
    }

    // Setup detail modal close handlers
    document.getElementById('setupDetailClose').addEventListener('click', () => {
      document.getElementById('setupDetailModal').style.display = 'none';
    });
    document.getElementById('setupDetailModal').addEventListener('click', (e) => {
      if (e.target.id === 'setupDetailModal') {
        document.getElementById('setupDetailModal').style.display = 'none';
      }
    });

    async function testSetupAlert() {
      const status = document.getElementById('setupStatus');
      status.textContent = 'Sending...';
      try {
        const r = await fetch('/api/setup/test', { method: 'POST' });
        const data = await r.json();
        status.textContent = data.status === 'ok' ? 'Sent!' : (data.message || 'Error');
        setTimeout(() => { status.textContent = ''; }, 3000);
      } catch (err) {
        status.textContent = 'Error';
        setTimeout(() => { status.textContent = ''; }, 2000);
      }
    }

    document.getElementById('btnSaveSetups').addEventListener('click', saveSetupSettings);
    document.getElementById('btnTestSetup').addEventListener('click', testSetupAlert);
    document.getElementById('btnExportSetups').addEventListener('click', () => {
      window.location.href = '/api/setup/export';
    });

    alertSaveBtn.addEventListener('click', saveAlertSettings);
    alertTestBtn.addEventListener('click', testAlert);
    alertMasterToggle.addEventListener('change', saveAlertSettings);

    // Modal open/close
    alertSettingsBtn.addEventListener('click', () => { alertModal.style.display = 'flex'; });
    alertModalClose.addEventListener('click', () => { alertModal.style.display = 'none'; });
    alertModal.addEventListener('click', (e) => { if (e.target === alertModal) alertModal.style.display = 'none'; });

    // Load alert settings on page load
    loadAlertSettings();

    // default
    showTable();
  </script>
</body>
</html>
"""

# ====== TABLE ENDPOINT ======
@app.get("/table")
def html_table(session: str = Cookie(default=None)):
    # Require authentication
    user = get_current_user(session)
    if not user:
        return HTMLResponse("<html><body style='background:#0b0c10;color:#e6e7e9;font-family:system-ui;padding:20px'>Please <a href='/' style='color:#60a5fa'>login</a> to view data.</body></html>")

    ts  = last_run_status.get("ts") or ""
    msg = last_run_status.get("msg") or ""
    parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
    exp  = parts.get("exp", "")
    spot_str = parts.get("spot", "")
    rows = parts.get("rows", "")

    with _df_lock:
        df_src = None if (latest_df is None or latest_df.empty) else latest_df.copy()

    if df_src is None or df_src.empty:
        body_html = "<p>No data yet. If market is open, it will appear within ~30s.</p>"
    else:
        base = df_src
        wanted = [
            "C_Volume","C_OpenInterest","C_IV","C_Gamma","C_Delta","C_Last",
            "Strike",
            "P_Last","P_Delta","P_Gamma","P_IV","P_OpenInterest","P_Volume",
        ]
        df = base[wanted].copy()
        df.columns = [
            "Volume","Open Int","IV","Gamma","Delta","LAST",
            "Strike",
            "LAST","Delta","Gamma","IV","Open Int","Volume",
        ]
        try:
            spot_val = float(spot_str)
        except:
            spot_val = None
        atm_idx = None
        if spot_val:
            try:
                atm_idx = (df["Strike"] - spot_val).abs().idxmin()
            except:
                pass

        comma_cols = {"Volume", "Open Int"}
        def fmt_val(col, v):
            if pd.isna(v):
                return ""
            if col in comma_cols:
                try:
                    f = float(v)
                    return f"{int(f):,}" if abs(f - int(f)) < 1e-9 else f"{f:,.2f}"
                except:
                    return str(v)
            return str(v)

        thead = "<tr>" + "".join(f"<th>{h}</th>" for h in df.columns) + "</tr>"
        trs = []
        for i, row in enumerate(df.itertuples(index=False), start=0):
            cls = ' class="atm"' if (atm_idx is not None and i == atm_idx) else ""
            tds = [f"<td>{fmt_val(col, v)}</td>" for col, v in zip(df.columns, row)]
            trs.append(f"<tr{cls}>" + "".join(tds) + "</tr>")
        body_html = f'<table class="table"><thead>{thead}</thead><tbody>{"".join(trs)}</tbody></table>'

    html = (TABLE_HTML_TEMPLATE
            .replace("__TS__", ts)
            .replace("__EXP__", exp)
            .replace("__SPOT__", spot_str)
            .replace("__ROWS__", rows)
            .replace("__BODY__", body_html)
            .replace("__PULL_MS__", str(PULL_EVERY * 1000)))
    return Response(content=html, media_type="text/html")

# ====== AUTHENTICATION ENDPOINTS ======
@app.get("/", response_class=HTMLResponse)
def root(request: Request, session: str = Cookie(default=None)):
    """Show login page or redirect to dashboard if logged in."""
    user = get_current_user(session)
    if user:
        return RedirectResponse(url="/dashboard", status_code=302)
    html = LOGIN_HTML_TEMPLATE.replace("__ERROR__", "")
    return HTMLResponse(html)

@app.post("/login")
def login(request: Request, email: str = Form(...), password: str = Form(...)):
    """Handle login form submission."""
    if not engine:
        html = LOGIN_HTML_TEMPLATE.replace("__ERROR__", '<div class="error">Database not available</div>')
        return HTMLResponse(html, status_code=500)

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT id, password_hash FROM users WHERE email = :email"),
                {"email": email.lower().strip()}
            ).mappings().first()

            if row and verify_password(password, row["password_hash"]):
                session_token = create_session(row["id"])
                response = RedirectResponse(url="/dashboard", status_code=302)
                response.set_cookie(
                    key="session",
                    value=session_token,
                    max_age=SESSION_MAX_AGE,
                    httponly=True,
                    samesite="lax"
                )
                return response
    except Exception as e:
        print(f"[auth] login error: {e}", flush=True)

    html = LOGIN_HTML_TEMPLATE.replace("__ERROR__", '<div class="error">Invalid email or password</div>')
    return HTMLResponse(html, status_code=401)

@app.get("/logout")
def logout():
    """Log out and redirect to login page."""
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("session")
    return response

@app.get("/favicon.ico")
@app.get("/favicon.png")
def favicon():
    """Serve favicon."""
    import pathlib
    favicon_path = pathlib.Path(__file__).parent.parent / "0dte-alpha-favicon.png"
    if favicon_path.exists():
        return FileResponse(favicon_path, media_type="image/png")
    return Response(status_code=404)

@app.get("/request-access", response_class=HTMLResponse)
def request_access_page():
    """Show the request access form."""
    html = REQUEST_ACCESS_HTML.replace("__MESSAGE__", "")
    return HTMLResponse(html)

@app.post("/request-access")
def submit_access_request(email: str = Form(...), subject: str = Form(...), message: str = Form(...)):
    """Handle access request form submission."""
    if not engine:
        html = REQUEST_ACCESS_HTML.replace("__MESSAGE__", '<div class="success">Unable to submit request. Please try again later.</div>')
        return HTMLResponse(html, status_code=500)

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO contact_messages (email, subject, message)
                VALUES (:email, :subject, :message)
            """), {"email": email.strip(), "subject": subject.strip(), "message": message.strip()})

        html = REQUEST_ACCESS_HTML.replace("__MESSAGE__", '<div class="success">Your request has been submitted. We\'ll get back to you soon!</div>')
        return HTMLResponse(html)
    except Exception as e:
        print(f"[auth] request submission error: {e}", flush=True)
        html = REQUEST_ACCESS_HTML.replace("__MESSAGE__", '<div class="success">Error submitting request. Please try again.</div>')
        return HTMLResponse(html, status_code=500)

# ====== USER MANAGEMENT API (Admin Only) ======
@app.get("/api/users")
def list_users(session: str = Cookie(default=None)):
    """List all users (admin only)."""
    user = get_current_user(session)
    if not user or not user.get("is_admin"):
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    if not engine:
        return JSONResponse({"error": "Database not available"}, status_code=500)

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT id, email, is_admin, created_at FROM users ORDER BY created_at DESC"
            )).mappings().all()
            return [{
                "id": r["id"],
                "email": r["email"],
                "is_admin": r["is_admin"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None
            } for r in rows]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/users")
def add_user(email: str = Query(...), password: str = Query(...), session: str = Cookie(default=None)):
    """Add a new user (admin only)."""
    user = get_current_user(session)
    if not user or not user.get("is_admin"):
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    if not engine:
        return JSONResponse({"error": "Database not available"}, status_code=500)

    try:
        with engine.begin() as conn:
            # Check if user already exists
            existing = conn.execute(
                text("SELECT id FROM users WHERE email = :email"),
                {"email": email.lower().strip()}
            ).first()
            if existing:
                return JSONResponse({"error": "User already exists"}, status_code=400)

            password_hash = hash_password(password)
            conn.execute(text("""
                INSERT INTO users (email, password_hash, is_admin)
                VALUES (:email, :hash, FALSE)
            """), {"email": email.lower().strip(), "hash": password_hash})

        return {"status": "ok", "message": "User added successfully"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/users/{user_id}")
def delete_user(user_id: int, session: str = Cookie(default=None)):
    """Delete a user (admin only)."""
    user = get_current_user(session)
    if not user or not user.get("is_admin"):
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    # Prevent self-deletion
    if user["id"] == user_id:
        return JSONResponse({"error": "Cannot delete your own account"}, status_code=400)

    if not engine:
        return JSONResponse({"error": "Database not available"}, status_code=500)

    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})
        return {"status": "ok", "message": "User deleted"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ====== CONTACT MESSAGES API (Admin Only) ======
@app.get("/api/messages")
def list_messages(session: str = Cookie(default=None)):
    """List all contact messages (admin only)."""
    user = get_current_user(session)
    if not user or not user.get("is_admin"):
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    if not engine:
        return JSONResponse({"error": "Database not available"}, status_code=500)

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT id, email, subject, message, is_read, created_at FROM contact_messages ORDER BY created_at DESC"
            )).mappings().all()
            return [{
                "id": r["id"],
                "email": r["email"],
                "subject": r["subject"],
                "message": r["message"],
                "is_read": r["is_read"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None
            } for r in rows]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/messages/{msg_id}/read")
def mark_message_read(msg_id: int, session: str = Cookie(default=None)):
    """Mark a message as read (admin only)."""
    user = get_current_user(session)
    if not user or not user.get("is_admin"):
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    if not engine:
        return JSONResponse({"error": "Database not available"}, status_code=500)

    try:
        with engine.begin() as conn:
            conn.execute(text("UPDATE contact_messages SET is_read = TRUE WHERE id = :id"), {"id": msg_id})
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/messages/{msg_id}")
def delete_message(msg_id: int, session: str = Cookie(default=None)):
    """Delete a message (admin only)."""
    user = get_current_user(session)
    if not user or not user.get("is_admin"):
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    if not engine:
        return JSONResponse({"error": "Database not available"}, status_code=500)

    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM contact_messages WHERE id = :id"), {"id": msg_id})
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ====== DASHBOARD ENDPOINT ======
@app.get("/dashboard", response_class=HTMLResponse)
def spxw_dashboard(session: str = Cookie(default=None)):
    # Check authentication
    user = get_current_user(session)
    if not user:
        return RedirectResponse(url="/", status_code=302)

    open_now = market_open_now()
    status_text = "Market OPEN" if open_now else "Market CLOSED"
    status_color = "#10b981" if open_now else "#ef4444"

    last_ts  = last_run_status.get("ts")  or ""
    last_msg = last_run_status.get("msg") or ""

    html = (DASH_HTML_TEMPLATE
            .replace("__STATUS_COLOR__", status_color)
            .replace("__STATUS_TEXT__", status_text)
            .replace("__LAST_TS__", str(last_ts))
            .replace("__LAST_MSG__", str(last_msg))
            .replace("__PULL_MS__", str(PULL_EVERY * 1000))
            .replace("__USER_EMAIL__", user["email"])
            .replace("__IS_ADMIN__", "true" if user.get("is_admin") else "false"))
    return HTMLResponse(html)
