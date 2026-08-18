import os, sys, importlib.util, io, contextlib
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)
print(f"window {os.environ.get('CF_START')} -> 2026-08-06  ({len(m.cands)} candidates)  WITH basket sizing")
for pol in ("base012","sb012"):
    print(f"\n  {pol}:")
    print(f"    {'cap':<7}{'total$':>9}{'trades':>8}{'WR':>6}{'MaxDD':>9}{'ret/DD':>8}")
    for cap in (1,2,3,4,5):
        r=m.run(pol,cap,cap)
        print(f"    {str(cap)+'/'+str(cap):<7}{r['total']:>9,.0f}{r['n']:>8}{r['wr']:>5.0f}%{r['dd']:>9,.0f}"
              f"{(r['total']/abs(r['dd']) if r['dd'] else 0):>8.1f}")
