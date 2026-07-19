"""Fill mes_sim gaps using the PRODUCTION simulator (app/mes_sim_backfill), in-memory only."""
import os, sys, json
sys.path.insert(0,"G:/My Drive/Python/MyProject/GitHub/0dtealpha")
os.environ.setdefault("DATABASE_URL",os.environ["DATABASE_URL"])
from sqlalchemy import create_engine, text
from app import mes_sim_backfill as M

# GEX Long runs real_trader's standard config: SL14, BE@10 + act10/gap5.
# BE never binds when be_trigger==trail_act and gap<act, so params match SC exactly.
M.V14_WHITELIST = set(M.V14_WHITELIST) | {"GEX Long"}
M._DEFAULT_PARAMS["GEX Long"] = {"sl":14,"be_trigger":None,"be_lock":0,"trail_act":10,"trail_gap":5}

ENG=create_engine(os.environ["DATABASE_URL"])
def fill(where, params):
    """Return {setup_log_id: mes_pts} computed by the production simulator."""
    out={}
    with ENG.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        rows=c.execute(text(f"""
          SELECT id, ts, setup_name, direction, spot, trail_sl, trail_activation, trail_gap,
                 abs_es_price, outcome_target_level
          FROM setup_log WHERE {where} ORDER BY ts"""), params).fetchall()
    ok=err=0
    for r in rows:
        try:
            sim=M.compute_mes_sim_outcome(
                ENG, setup_log_id=r[0], setup_name=r[2], direction=r[3],
                signal_ts=r[1], spx_spot=float(r[4]),
                trail_sl=float(r[5]) if r[5] is not None else None,
                trail_activation=float(r[6]) if r[6] is not None else None,
                trail_gap=float(r[7]) if r[7] is not None else None,
                signal_es_price=float(r[8]) if r[8] is not None else None)
            if sim and sim.get("mes_sim_outcome_pnl") is not None:
                out[r[0]]=float(sim["mes_sim_outcome_pnl"]); ok+=1
            else: err+=1
        except Exception as e:
            err+=1
    print(f"  computed {ok}, unavailable {err}, of {len(rows)}", file=sys.stderr)
    return out

if __name__=="__main__":
    import inspect
    print(inspect.signature(M.compute_mes_sim_outcome))
