import psycopg2

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# Get a snapshot where aggregatedCharm is NOT 0
cur.execute("""
    SELECT ts, payload->'statistics'->>'aggregatedCharm' as agg_charm
    FROM volland_snapshots
    WHERE payload->'statistics'->>'aggregatedCharm' IS NOT NULL
      AND payload->'statistics'->>'aggregatedCharm' != '0'
    ORDER BY ts DESC LIMIT 1
""")
snap = cur.fetchone()
ts, agg_charm = snap
agg = float(agg_charm)
print(f"Snapshot: {ts}")
print(f"aggregatedCharm from API: {agg:,.0f}")
print()

# Get per-strike charm exposure points near that time
cur.execute("""
    SELECT strike, value
    FROM volland_exposure_points
    WHERE greek = 'charm'
      AND ts_utc >= %s - interval '2 minutes'
      AND ts_utc <= %s + interval '2 minutes'
    ORDER BY strike ASC
""", (ts, ts))
rows = cur.fetchall()

total = float(sum(r[1] for r in rows))
print(f"Per-strike charm: {len(rows)} strikes")
print(f"Sum of strikes:   {total:,.0f}")
print(f"aggregatedCharm:  {agg:,.0f}")
print(f"Difference:       {total - agg:,.0f}")
if agg != 0:
    print(f"Ratio:            {total / agg:.4f}")
    print(f"Match:            {'YES' if abs(total - agg) / abs(agg) < 0.05 else 'NO'}")

print()
print("Top 5 strikes by |value|:")
for strike, val in sorted(rows, key=lambda r: abs(r[1]), reverse=True)[:5]:
    print(f"  {strike}: {val:+,.0f}")

conn.close()
