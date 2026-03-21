"""Simulate eval_trader full-day PnL for March 16, 2026."""
from datetime import datetime, timedelta

# March 16 signals from setup_log (ts in UTC)
signals = [
    (813, "2026-03-16 13:46:04", "Skew Charm", "short", "B", 6713.44, -1, "WIN", 14.5),
    (814, "2026-03-16 13:56:36", "Skew Charm", "long", "C", 6706.60, 3, "WIN", 9.1),
    (815, "2026-03-16 14:00:36", "BofA Scalp", "short", "A", 6711.61, -3, "LOSS", -10.4),
    (816, "2026-03-16 14:28:41", "ES Absorption", "bullish", "B", 6718.53, 3, "LOSS", -8.0),
    (817, "2026-03-16 14:20:07", "DD Exhaustion", "long", "A-Entry", 6716.16, 3, "LOSS", -12.0),
    (818, "2026-03-16 14:25:06", "DD Exhaustion", "short", "A", 6719.05, -1, "WIN", 18.8),
    (819, "2026-03-16 14:27:37", "Skew Charm", "short", "B", 6718.14, -1, "WIN", 16.9),
    (820, "2026-03-16 14:34:11", "Skew Charm", "long", "C", 6704.11, 3, "WIN", 6.5),
    (821, "2026-03-16 14:52:47", "DD Exhaustion", "long", "A-Entry", 6695.68, 3, "WIN", -1.0),
    (822, "2026-03-16 15:04:37", "Skew Charm", "long", "C", 6700.60, 3, "WIN", 8.2),
    (823, "2026-03-16 15:07:07", "Paradigm Reversal", "long", "A-Entry", 6697.87, 3, "WIN", 10.0),
    (824, "2026-03-16 15:08:59", "ES Absorption", "bullish", "B", 6697.80, 3, "WIN", 10.0),
    (825, "2026-03-16 15:37:40", "ES Absorption", "bearish", "B", 6714.60, -3, "WIN", 10.0),
    (826, "2026-03-16 15:30:37", "DD Exhaustion", "long", "A-Entry", 6712.20, 3, "LOSS", -12.0),
    (827, "2026-03-16 15:34:37", "AG Short", "short", "A-Entry", 6714.41, -3, "WIN", 13.2),
    (828, "2026-03-16 15:35:07", "Skew Charm", "long", "C", 6713.70, 3, "LOSS", -20.0),
    (829, "2026-03-16 15:58:11", "Paradigm Reversal", "long", "A-Entry", 6699.26, 3, "LOSS", -15.0),
    (830, "2026-03-16 16:00:38", "DD Exhaustion", "long", "A", 6696.86, 3, "LOSS", -12.0),
    (831, "2026-03-16 16:06:06", "Skew Charm", "long", "B", 6692.98, 3, "WIN", 7.3),
    (832, "2026-03-16 16:11:07", "BofA Scalp", "short", "A+", 6700.39, -3, "WIN", 10.0),
    (833, "2026-03-16 16:20:03", "ES Absorption", "bullish", "B", 6696.70, 3, "LOSS", -8.0),
    (834, "2026-03-16 16:40:06", "DD Exhaustion", "long", "A", 6690.67, 3, "WIN", 13.2),
    (835, "2026-03-16 16:40:38", "Skew Charm", "long", "B", 6689.07, 3, "WIN", 9.3),
    (836, "2026-03-16 17:28:37", "BofA Scalp", "short", "A+", 6704.35, -3, "EXPIRED", -7.1),
    (837, "2026-03-16 17:10:38", "Skew Charm", "long", "B", 6700.57, 3, "WIN", 6.7),
    (838, "2026-03-16 17:14:37", "Skew Charm", "short", "B", 6703.75, -1, "LOSS", -20.0),
    (839, "2026-03-16 17:17:06", "DD Exhaustion", "short", "A", 6700.53, -1, "LOSS", -12.0),
    (840, "2026-03-16 17:41:10", "ES Absorption", "bullish", "B", 6698.79, 1, "LOSS", -8.0),
    (841, "2026-03-16 17:30:37", "DD Exhaustion", "long", "A-Entry", 6702.44, 3, "LOSS", -12.0),
    (842, "2026-03-16 17:42:06", "Skew Charm", "long", "B", 6699.06, 3, "WIN", 7.5),
    (843, "2026-03-16 17:58:54", "DD Exhaustion", "long", "A-Entry", 6698.13, 3, "WIN", 18.3),
    (844, "2026-03-16 17:59:25", "BofA Scalp", "short", "A", 6698.13, -3, "EXPIRED", -8.3),
    (845, "2026-03-16 18:07:55", "Skew Charm", "long", "B", 6704.80, 3, "WIN", 13.5),
    (846, "2026-03-16 18:12:09", "BofA Scalp", "long", "A+", 6702.68, 1, "EXPIRED", 7.9),
    (847, "2026-03-16 18:13:39", "DD Exhaustion", "short", "A", 6702.77, -1, "LOSS", -12.0),
    (848, "2026-03-16 18:21:09", "Skew Charm", "short", "A-Entry", 6707.68, -1, "LOSS", -20.0),
    (849, "2026-03-16 18:28:49", "ES Absorption", "bearish", "B", 6707.03, -1, "LOSS", -8.0),
    (850, "2026-03-16 18:43:39", "DD Exhaustion", "short", "A+", 6709.59, -1, "LOSS", -12.0),
    (851, "2026-03-16 18:52:39", "Skew Charm", "short", "A", 6709.18, -1, "LOSS", -20.0),
    (852, "2026-03-16 19:08:39", "DD Exhaustion", "long", "A", 6703.09, 1, "WIN", 15.8),
    (853, "2026-03-16 19:09:09", "Skew Charm", "long", "A", 6705.41, 3, "WIN", 13.2),
    (854, "2026-03-16 19:35:32", "ES Absorption", "bullish", "B", 6699.94, 3, "EXPIRED", 0.5),
    (855, "2026-03-16 19:40:39", "Skew Charm", "long", "A", 6700.00, 3, "EXPIRED", -0.7),
]

# Sort by timestamp
signals.sort(key=lambda x: x[1])

# Config
ENABLED = {"ES Absorption", "AG Short", "Skew Charm", "Paradigm Reversal", "DD Exhaustion"}
STOP_PTS = {"ES Absorption": 8, "AG Short": 12, "Skew Charm": 12, "Paradigm Reversal": 12, "DD Exhaustion": 12}
QTY = 8
MES_PT = 5  # $5/pt/contract
COMM = 2.16

# Cutoff: 15:20 CT = 20:20 UTC (CDT)
CUTOFF_UTC = datetime(2026, 3, 16, 20, 20)

def est_hold_min(name, outcome, pnl):
    """Estimate hold duration in minutes."""
    if outcome == "EXPIRED":
        return 120
    stop = STOP_PTS.get(name, 12)
    if outcome == "LOSS":
        return 8 if stop <= 8 else 12
    if abs(pnl) <= 10:
        return 15
    return 25

def is_long(direction):
    return direction in ("long", "bullish")

def greek_filter(name, direction, alignment):
    if is_long(direction):
        return alignment >= 2
    if name == "Skew Charm":
        return True
    if name == "AG Short":
        return True
    if name == "DD Exhaustion" and alignment != 0:
        return True
    return False

print("=" * 95)
print("EVAL TRADER SIMULATION - March 16, 2026 (full day from market open)")
print(f"Config: qty={QTY}, comm=${COMM}/ct")
print("=" * 95)

trades = []
in_pos = False
pos_close_time = None
daily_pnl = 0.0
n_blocked_pos = 0
n_blocked_filter = 0
n_blocked_disabled = 0
n_blocked_cutoff = 0

for sid, ts_str, name, dirn, grade, spot, align, outcome, pnl in signals:
    ts = datetime.fromisoformat(ts_str)

    if in_pos and pos_close_time and ts >= pos_close_time:
        in_pos = False

    if name not in ENABLED:
        n_blocked_disabled += 1
        continue

    if not greek_filter(name, dirn, align):
        n_blocked_filter += 1
        continue

    if ts >= CUTOFF_UTC:
        n_blocked_cutoff += 1
        continue

    if in_pos:
        n_blocked_pos += 1
        continue

    # TAKE TRADE
    hold_min = est_hold_min(name, outcome, pnl)
    pos_close_time = ts + timedelta(minutes=hold_min)
    in_pos = True

    comm = QTY * COMM * 2
    gross = pnl * QTY * MES_PT
    net = gross - comm
    daily_pnl += net

    dir_str = "LONG" if is_long(dirn) else "SHORT"
    ts_et = ts - timedelta(hours=4)
    trades.append((sid, ts_et.strftime("%H:%M"), name, dir_str, grade, outcome, pnl, gross, net, daily_pnl))

print(f"\n{'#':>4} {'ET':>5} {'Setup':20s} {'Dir':6s} {'Grd':8s} {'Result':8s} {'Pts':>6s} {'Gross$':>8s} {'Net$':>8s} {'Cumul$':>8s}")
print("-" * 95)
for sid, ts_et, name, dir_str, grade, outcome, pnl, gross, net, cumul in trades:
    marker = "***" if pnl > 0 else "   "
    print(f"{sid:4d} {ts_et:>5} {name:20s} {dir_str:6s} {grade:8s} {outcome:8s} {pnl:+6.1f} {gross:+8.0f} {net:+8.0f} {cumul:+8.0f} {marker}")

wins = sum(1 for t in trades if t[6] > 0)
losses = sum(1 for t in trades if t[6] < 0)
total_comm = len(trades) * QTY * COMM * 2
print("-" * 95)
print(f"TRADES TAKEN: {len(trades)} | WINS: {wins} | LOSSES: {losses} | WR: {wins/len(trades)*100:.0f}%")
print(f"NET PnL: ${daily_pnl:+,.0f} (comm: ${total_comm:,.0f})")
print(f"BLOCKED: {n_blocked_pos} in-position, {n_blocked_disabled} disabled, {n_blocked_filter} Greek filter, {n_blocked_cutoff} past cutoff")
