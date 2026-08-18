import os, sys, importlib.util, io, contextlib
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
mm=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(mm)
CAPITAL=5161
W=os.environ.get('CF_START')
print(f"  window {W} -> 2026-08-06 ({len(mm.cands)} candidates, {len(set(r['ts'].astimezone(mm.ET).date() for r in mm.cands))} sessions)")
print(f"    {'config':<34}{'total$':>9}{'MaxDD':>9}{'DD%cap':>8}{'$/mo':>9}")
pols=[("base1","1 MES flat, no basket")]
if W>='2026-06-01': pols.append(("base012","+ basket 2x sizing"))
for pol,lab in pols:
    for cap in (1,2,3):
        r=mm.run(pol,cap,cap); n=len(r['daily'])
        print(f"    {lab+' cap'+str(cap)+'/'+str(cap):<34}{r['total']:>9,.0f}{r['dd']:>9,.0f}"
              f"{abs(r['dd'])/CAPITAL*100:>7.0f}%{r['total']/n*21:>9,.0f}")
