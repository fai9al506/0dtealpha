"""Analyze Mar 23: identify deploy-caused trades using ACTUAL V11 filter results from DB."""

GRADE_RANK = {"A+": 3, "A": 2, "A-Entry": 1, "B": 0, "C": 0, "LOG": 0}

# V11-passed trades from DB (39 trades, P&L = +234.6)
# Format: (id, time_min_from_930, setup, direction, grade, gap, pnl)
v11_passed = [
    (1038, 15, "Skew Charm", "short", "A", 0, -14.0),
    (1041, 43, "DD Exhaustion", "short", "B", 0, -12.0),
    (1043, 46, "Skew Charm", "short", "B", 0, -14.0),
    (1047, 68, "AG Short", "short", "A+", 3.6, -11.9),
    (1053, 77, "Skew Charm", "long", "B", 0, 7.8),
    (1057, 93, "AG Short", "short", "A+", 14.9, 19.2),
    (1059, 102, "Skew Charm", "long", "A+", 0, -14.0),
    (1065, 147, "AG Short", "short", "A+", 17.6, 11.9),
    (1066, 149, "DD Exhaustion", "short", "B", 0, 12.8),
    (1068, 152, "AG Short", "short", "B", 15.8, 12.3),
    (1071, 158, "DD Exhaustion", "short", "A", 0, 14.1),
    (1069, 160, "AG Short", "short", "A", 0.1, 11.6),
    (1072, 169, "AG Short", "short", "A", 11.8, 13.4),
    (1075, 174, "DD Exhaustion", "short", "A+", 0, -1.0),
    (1076, 176, "Skew Charm", "short", "A", 0, 7.4),
    (1078, 182, "DD Exhaustion", "short", "A+", 0, -12.0),
    (1082, 197, "Skew Charm", "long", "C", 0, -14.0),
    (1083, 206, "AG Short", "short", "B", 2.9, 5.0),
    (1084, 210, "AG Short", "short", "B", 2.5, 5.0),
    (1087, 219, "AG Short", "short", "B", 2.8, 5.0),
    (1090, 223, "AG Short", "short", "A+", 3.6, 5.0),
    (1092, 232, "AG Short", "short", "B", 15.8, -20.0),
    (1095, 233, "Skew Charm", "long", "A", 0, 17.5),
    (1096, 240, "AG Short", "short", "A+", 6.1, -20.0),
    (1099, 244, "Skew Charm", "long", "A", 0, 7.0),
    (1102, 274, "AG Short", "short", "A+", 18.1, 5.0),
    (1104, 280, "AG Short", "short", "A+", 20.0, 16.7),
    (1110, 301, "AG Short", "short", "A", 18.8, 17.2),
    (1111, 310, "AG Short", "short", "A+", 7.8, 11.7),
    (1114, 318, "AG Short", "short", "A+", 6.5, 11.9),
    (1119, 329, "AG Short", "short", "A+", 5.2, 12.0),
    (1123, 337, "AG Short", "short", "A+", 8.1, 20.6),
    (1125, 339, "AG Short", "short", "A+", 8.7, 20.3),
    (1127, 342, "AG Short", "short", "A+", 11.0, 19.1),
    (1129, 347, "AG Short", "short", "A+", 17.0, 15.6),
    (1132, 351, "AG Short", "short", "A", 15.8, 15.3),
    (1135, 376, "AG Short", "short", "A", 4.5, 18.4),
    (1136, 377, "AG Short", "short", "A", 6.8, 17.2),
    (1137, 384, "AG Short", "short", "A+", 7.3, 11.5),
]

# Verify total
total_v11 = sum(t[6] for t in v11_passed)
print(f"V11 current total: {total_v11:+.1f} ({len(v11_passed)} trades)")
print()

# Split by setup type
ag_trades = [t for t in v11_passed if t[2] == "AG Short"]
dd_trades = [t for t in v11_passed if t[2] == "DD Exhaustion"]
sc_trades = [t for t in v11_passed if t[2] == "Skew Charm"]

# === AG Short cooldown simulation ===
def sim_ag(trades):
    last_grade = None
    last_gap = None
    last_time = -999
    legit, delete = [], []
    for tid, t, setup, d, grade, gap, pnl in trades:
        fire = False
        if last_time >= 0 and (t - last_time) >= 25:
            last_grade = None
            last_gap = None
        if last_grade is None:
            fire = True
        elif GRADE_RANK.get(grade, 0) > GRADE_RANK.get(last_grade, 0):
            fire = True
        elif last_gap is not None and (last_gap - gap) > 2:
            fire = True
        if fire:
            last_grade = grade
            last_gap = gap
            last_time = t
            legit.append((tid, grade, gap, pnl))
        else:
            delete.append((tid, grade, gap, pnl))
    return legit, delete

# === Time-based cooldown simulation (DD, SC) ===
def sim_time(trades, cd=30):
    last = {}
    legit, delete = [], []
    for tid, t, setup, d, grade, gap, pnl in trades:
        if (t - last.get(d, -999)) >= cd:
            last[d] = t
            legit.append((tid, grade, pnl))
        else:
            delete.append((tid, grade, pnl))
    return legit, delete

ag_l, ag_d = sim_ag(ag_trades)
dd_l, dd_d = sim_time(dd_trades, 30)
sc_l, sc_d = sim_time(sc_trades, 30)

print("=== AG Short (V11 passed) ===")
print(f"  Legitimate: {len(ag_l)}, P&L = {sum(t[3] for t in ag_l):+.1f}")
for tid, g, gap, pnl in ag_l:
    print(f"    OK #{tid}  {g:<4s}  gap={gap:5.1f}  {pnl:+.1f}")
print(f"  Deploy-caused: {len(ag_d)}, P&L = {sum(t[3] for t in ag_d):+.1f}")
for tid, g, gap, pnl in ag_d:
    print(f"    XX #{tid}  {g:<4s}  gap={gap:5.1f}  {pnl:+.1f}")

print(f"\n=== DD Exhaustion (V11 passed) ===")
print(f"  Legitimate: {len(dd_l)}, P&L = {sum(t[2] for t in dd_l):+.1f}")
for tid, g, pnl in dd_l:
    print(f"    OK #{tid}  {g:<4s}  {pnl:+.1f}")
print(f"  Deploy-caused: {len(dd_d)}, P&L = {sum(t[2] for t in dd_d):+.1f}")
for tid, g, pnl in dd_d:
    print(f"    XX #{tid}  {g:<4s}  {pnl:+.1f}")

print(f"\n=== Skew Charm (V11 passed) ===")
print(f"  Legitimate: {len(sc_l)}, P&L = {sum(t[2] for t in sc_l):+.1f}")
for tid, g, pnl in sc_l:
    print(f"    OK #{tid}  {g:<4s}  {pnl:+.1f}")
print(f"  Deploy-caused: {len(sc_d)}, P&L = {sum(t[2] for t in sc_d):+.1f}")
for tid, g, pnl in sc_d:
    print(f"    XX #{tid}  {g:<4s}  {pnl:+.1f}")

# Grand totals
all_del = [t[0] for t in ag_d] + [t[0] for t in dd_d] + [t[0] for t in sc_d]
del_pnl = sum(t[3] for t in ag_d) + sum(t[2] for t in dd_d) + sum(t[2] for t in sc_d)
keep_pnl = total_v11 - del_pnl

print("\n" + "=" * 60)
print(f"V11 SUMMARY:")
print(f"  Current:   {len(v11_passed)} trades, P&L = {total_v11:+.1f}")
print(f"  To delete: {len(all_del)} trades, P&L = {del_pnl:+.1f}")
print(f"  Corrected: {len(v11_passed) - len(all_del)} trades, P&L = {keep_pnl:+.1f}")
print(f"\n  DELETE IDs: {sorted(all_del)}")
