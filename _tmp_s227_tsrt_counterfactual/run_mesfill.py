from mesfill import fill
import json
# every GEX Long / missing-mes row in the study windows
g = fill("mes_sim_outcome_pnl IS NULL AND ts >= '2026-05-15' AND setup_name IN "
         "('GEX Long','Skew Charm','AG Short','DD Exhaustion','VIX Divergence','ES Absorption','Vanna Pivot Bounce')", {})
json.dump({str(k):v for k,v in g.items()}, open("mesfill_cache.json","w"))
print(f"cached {len(g)} newly-simulated mes outcomes")
