import os
import json
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT sl.id,
               (sl.ts AT TIME ZONE 'America/New_York')::text as et_ts,
               sl.setup_name,
               sl.direction,
               sl.grade,
               rto.state
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.ts >= NOW() - INTERVAL '14 days'
        ORDER BY sl.ts DESC
    """)).fetchall()

print(f"Total real trades last 14d: {len(rows)}")
print()
print(f"{'id':>5} {'et_time':<19} {'setup':<14} {'dir':<6} {'gr':<3} {'acct':<11} {'fill':>8} {'stop':>8} {'close_px':>8} {'reason':<15} {'pts':>7}")

total_pts = 0.0
wins = losses = open_cnt = 0
by_day = {}

for r in rows:
    sid, ets, setup, direction, grade, st = r
    if not isinstance(st, dict):
        try:
            st = json.loads(st)
        except Exception:
            st = {}
    fill = st.get('fill_price')
    stop_fill = st.get('stop_fill_price')
    target_fill = st.get('target_fill_price') or st.get('target_price')
    reason = st.get('close_reason') or st.get('status')
    acct = st.get('account_id')

    close_px = stop_fill
    if reason in ('target_filled', 'target_hit'):
        close_px = st.get('target_fill_price') or st.get('target_price')
    # pts (directional)
    pts = None
    if fill is not None and close_px is not None and direction:
        if direction == 'long':
            pts = float(close_px) - float(fill)
        else:
            pts = float(fill) - float(close_px)
        total_pts += pts
        day = ets[:10]
        by_day.setdefault(day, 0.0)
        by_day[day] += pts
        if pts > 0:
            wins += 1
        else:
            losses += 1
    elif st.get('status') in ('pending', 'filled', 'open'):
        open_cnt += 1

    fill_s = f"{fill:.2f}" if fill is not None else "-"
    stop_s = f"{stop_fill:.2f}" if stop_fill is not None else "-"
    close_s = f"{close_px:.2f}" if close_px is not None else "-"
    pts_s = f"{pts:+.2f}" if pts is not None else "-"
    print(f"{sid:>5} {str(ets)[:19]:<19} {str(setup)[:14]:<14} {str(direction)[:6]:<6} {str(grade)[:3]:<3} {str(acct)[:11]:<11} {fill_s:>8} {stop_s:>8} {close_s:>8} {str(reason)[:15]:<15} {pts_s:>7}")

print()
print(f"TOTAL: {wins}W / {losses}L closed, {open_cnt} open | net pts = {total_pts:+.2f}")
print()
print("BY DAY:")
for d in sorted(by_day.keys()):
    print(f"  {d}: {by_day[d]:+.2f} pts")
