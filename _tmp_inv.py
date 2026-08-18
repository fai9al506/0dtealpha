import os, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
# coverage of feature fields on live_pass longs, full history
feats=['spot','lis','target','max_plus_gex','max_minus_gex','gap_to_lis','upside','vix','overvix',
       'vix3m','vix_vix3m_ratio','vanna_all','vanna_weekly','vanna_monthly','spot_vol_beta','greek_alignment',
       'v13_gex_above','v13_dd_near','vanna_cliff_side','vanna_peak_side','vanna_regime','paradigm','first_hour']
cur.execute("""SELECT count(*) FROM setup_log WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL""")
N=cur.fetchone()[0]; print("total live_pass longs (all history):",N)
cur.execute("""SELECT min(date(ts AT TIME ZONE 'America/New_York')),max(date(ts AT TIME ZONE 'America/New_York'))
   FROM setup_log WHERE live_pass=true AND direction IN ('long','bullish')""")
print("date range:",cur.fetchone())
for f in feats:
    cur.execute(f"""SELECT count({f}) FROM setup_log WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL""")
    n=cur.fetchone()[0]; print(f"  {f:<20} pop={n:>4} ({n/N*100:>3.0f}%)")
# sample values for structural fields
cur.execute("""SELECT id, spot, lis, target, max_plus_gex, max_minus_gex, gap_to_lis, upside, paradigm, vanna_regime, vanna_cliff_side, vanna_peak_side, v13_gex_above, v13_dd_near, spot_vol_beta
  FROM setup_log WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
    AND ts AT TIME ZONE 'America/New_York' >= '2026-07-06' ORDER BY ts LIMIT 6""")
print("\nsample structural values:")
for r in cur.fetchall(): print("  ",r)
c.close()
