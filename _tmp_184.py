import os, pandas as pd
from sqlalchemy import create_engine, text
csv=pd.read_csv(r"C:/Users/Faisa/Downloads/trade_log_2026-06-24.csv",encoding='utf-8-sig'); csv.columns=[c.strip() for c in csv.columns]
ids=[int(x) for x in csv['ID']]
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    r=conn.execute(text("SELECT id, outcome_pnl, mes_sim_outcome_pnl FROM setup_log WHERE id = ANY(:ids)"),{"ids":ids}).mappings().all()
chain=sum(float(x['outcome_pnl']) for x in r)
mes=sum(float(x['mes_sim_outcome_pnl']) if x['mes_sim_outcome_pnl'] is not None else float(x['outcome_pnl']) for x in r)
print(f"\n120 portal IDs: chain(outcome_pnl) sum={chain:+.1f}  |  mes_sim(fallback chain) sum={mes:+.1f}")
print(f"user quoted shown total = -184.0  (CSV P&L col = {csv['P&L'].sum():+.1f})")
