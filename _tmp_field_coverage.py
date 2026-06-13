import os
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(text("""
      SELECT to_char((ts AT TIME ZONE 'America/New_York'),'YYYY-MM') mo,
        COUNT(*) n,
        COUNT(spot_vol_beta) svb, COUNT(vanna_regime) vr, COUNT(overvix) ov,
        COUNT(vix) vix, COUNT(outcome_elapsed_min) elap,
        COUNT(v13_gex_above) gexa, COUNT(v13_dd_near) ddn,
        COUNT(vanna_cliff_side) vcs, COUNT(greek_alignment) ga
      FROM setup_log GROUP BY 1 ORDER BY 1
    """)).fetchall()
    print(f"{'mo':<8}{'n':>6}{'svb':>6}{'vanReg':>7}{'ovix':>6}{'vix':>6}{'elap':>6}{'gexAb':>6}{'ddNr':>6}{'vCliff':>7}{'align':>6}")
    for r in rows:
        print(f"{r[0]:<8}{r[1]:>6}{r[2]:>6}{r[3]:>7}{r[4]:>6}{r[5]:>6}{r[6]:>6}{r[7]:>6}{r[8]:>6}{r[9]:>7}{r[10]:>6}")
    # sample distinct vanna_regime + spot_vol_beta range
    print("\nvanna_regime distinct:", [r[0] for r in conn.execute(text("SELECT DISTINCT vanna_regime FROM setup_log WHERE vanna_regime IS NOT NULL")).fetchall()])
    svb = conn.execute(text("SELECT MIN(spot_vol_beta),MAX(spot_vol_beta),AVG(spot_vol_beta) FROM setup_log WHERE spot_vol_beta IS NOT NULL")).fetchone()
    print("spot_vol_beta min/max/avg:", [round(float(x),3) if x is not None else None for x in svb])
