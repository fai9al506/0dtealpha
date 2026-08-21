import os, sys, importlib.util, io, contextlib, statistics
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)
CAP=5161; r=m.run("base012",2,2)
dl=sorted(r["daily"].values(),reverse=True); n=len(dl); tot=sum(dl)
net=tot*0.81
print(f"  window {os.environ['CF_START']} -> 2026-08-06 | {n} sessions | {len(m.cands)} candidates")
print(f"    gross ${tot:,.0f}   trades {r['n']}  WR {r['wr']:.0f}%   MaxDD ${r['dd']:,.0f} ({abs(r['dd'])/CAP*100:.0f}% of equity)")
print(f"    x0.81 broker capture = ${net:,.0f}  ->  ${net/n*21:,.0f}/month")
print(f"    green {sum(1 for x in dl if x>0)}/{n}   median day ${statistics.median(dl):,.0f}"
      f"   ex-top-3 ${tot-sum(dl[:3]):,.0f} (${(tot-sum(dl[:3]))*0.81/(n-3)*21:,.0f}/mo net)")
