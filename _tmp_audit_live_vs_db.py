"""Audit: compare live in-memory state vs DB after the 23:24 ET db_init-skip startup."""
import json
import requests
import sqlalchemy as sa

BASE = "https://0dtealpha.com"
DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

s = requests.Session()
s.post(f"{BASE}/login", data={"email": "faisal.a.d@msn.com", "password": "Mpc1234@@20"}, allow_redirects=False)

live_alerts = s.get(f"{BASE}/api/alerts/settings").json()
live_setup = s.get(f"{BASE}/api/setup/settings").json()

e = sa.create_engine(DB)
with e.connect() as c:
    db_alerts = dict(c.execute(sa.text("SELECT * FROM alert_settings WHERE id=1")).mappings().first())
    db_setup = dict(c.execute(sa.text("SELECT * FROM setup_settings WHERE id=1")).mappings().first())
    cooldown_row = c.execute(sa.text(
        "SELECT trade_date FROM setup_cooldowns ORDER BY trade_date DESC LIMIT 1")).mappings().first()
    open_trades = c.execute(sa.text(
        "SELECT id, setup_name, direction, outcome_result FROM setup_log "
        "WHERE (ts AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date "
        "AND outcome_result IS NULL ORDER BY id DESC LIMIT 10")).mappings().all()
    today_counts = c.execute(sa.text(
        "SELECT count(*) AS total, count(*) FILTER (WHERE outcome_result IS NOT NULL) AS resolved "
        "FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date")).mappings().first()

print("=== ALERT SETTINGS: live vs DB ===")
db_alerts.pop("id", None)
mism = {k: (live_alerts.get(k), v) for k, v in db_alerts.items() if live_alerts.get(k) != v}
print("MATCH" if not mism else f"MISMATCH: {mism}")

print("\n=== SETUP SETTINGS: live vs DB scalar columns ===")
checked, mm = 0, []
for k, v in db_setup.items():
    if k in ("id",) or isinstance(v, (dict, list)) or v is None:
        continue
    if k in live_setup:
        checked += 1
        lv = live_setup[k]
        try:
            same = float(lv) == float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else lv == v
        except (TypeError, ValueError):
            same = lv == v
        if not same:
            mm.append((k, lv, v))
print(f"checked {checked} scalar keys -> " + ("ALL MATCH" if not mm else f"MISMATCH: {mm}"))

print("\n=== SETUP SETTINGS: JSONB sub-settings (bofa/absorption/paradigm_rev/skew etc.) ===")
for col in db_setup:
    v = db_setup[col]
    if isinstance(v, str) and v.startswith("{"):
        try:
            v = json.loads(v)
        except Exception:
            continue
    if isinstance(v, dict):
        sub_mm = []
        for kk, vv in v.items():
            for cand in (f"{col.replace('_settings','')}_{kk}", kk):
                if cand in live_setup:
                    lv = live_setup[cand]
                    try:
                        same = float(lv) == float(vv) if isinstance(vv, (int, float)) and not isinstance(vv, bool) else lv == vv
                    except (TypeError, ValueError):
                        same = lv == vv
                    if not same:
                        sub_mm.append((cand, lv, vv))
                    break
        print(f"{col}: " + ("match" if not sub_mm else f"MISMATCH {sub_mm}"))

print("\n=== COOLDOWNS ===")
print(f"latest setup_cooldowns row: {dict(cooldown_row) if cooldown_row else 'none'}")

print("\n=== TODAY'S SETUP ACTIVITY ===")
print(f"signals today: {today_counts['total']}, resolved: {today_counts['resolved']}")
print(f"open (unresolved) trades: {[dict(r) for r in open_trades]}")
