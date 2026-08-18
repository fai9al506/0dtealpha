import os, statistics, importlib.util, sys
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
import io, contextlib
with contextlib.redirect_stdout(io.StringIO()):
    spec.loader.exec_module(m)
print(f"window {os.environ.get('CF_START')} -> 2026-08-06   ({len(m.cands)} candidates)")
for pol in ("base012","sb012","sb001"):
    for cap in (3,2):
        r=m.run(pol,cap,cap); dl=sorted(r["daily"].values(), reverse=True); n=len(dl); tot=sum(dl)
        hc=r["n"]*0.18*5*1.5
        print(f"  {pol:<8} cap{cap}/{cap}: ${tot:>7,.0f} | top1 {dl[0]/tot*100:>3.0f}% top3 {sum(dl[:3])/tot*100:>3.0f}%"
              f" | median day ${statistics.median(dl):>6,.0f} | green {sum(1 for x in dl if x>0)}/{n}"
              f" | ex-top3 ${tot-sum(dl[:3]):>6,.0f} | net of haircut ${tot-hc:>7,.0f} = ${(tot-hc)/n*21:>6,.0f}/mo")
