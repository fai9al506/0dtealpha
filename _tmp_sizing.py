import os, sys, importlib.util, io, contextlib, statistics
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)
W=os.environ.get('CF_START')
print(f"\n############ {W} -> 2026-08-06   ({len(m.cands)} V16-base candidates, GEX OFF) ############")
for cap in (2,3):
    print(f"\n  --- cap {cap}/{cap} ---")
    print(f"  {'policy':<26}{'total$':>9}{'MaxDD':>9}{'ret/DD':>8}{'green':>8}{'top3%':>7}{'medDay':>8}")
    ref=None
    for pol,lab in [("base1","1 MES flat (no basket)"),("flat2","2 MES flat (pure leverage)"),
                    ("base012","basket SIZING 2x on confirm"),("sb012","basket BLOCK+size (live)")]:
        r=m.run(pol,cap,cap); dl=sorted(r['daily'].values(),reverse=True); tot=sum(dl); n=len(dl)
        if ref is None: ref=r
        pm = f"{tot/ref['total']:.2f}x" if pol!="base1" else "  --"
        pd_ = f"{abs(r['dd'])/abs(ref['dd']):.2f}x" if pol!="base1" else "  --"
        print(f"  {lab:<26}{tot:>9,.0f}{r['dd']:>9,.0f}{tot/abs(r['dd']):>8.1f}"
              f"{sum(1 for x in dl if x>0):>5}/{n:<2}{sum(dl[:3])/tot*100:>6.0f}%{statistics.median(dl):>8,.0f}"
              f"   [P&L {pm}, DD {pd_}]")
