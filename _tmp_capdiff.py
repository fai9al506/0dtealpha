import os, sys, importlib.util, io, contextlib, collections
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)
POL=os.environ.get("POL","base012")
r2=m.run(POL,2,2); r3=m.run(POL,3,3)
days=sorted(set(r2["daily"])|set(r3["daily"]))
diffs=[(r3["daily"].get(d,0)-r2["daily"].get(d,0), d) for d in days]
tot=sum(x for x,_ in diffs)
pos=[x for x,_ in diffs if x>1]; neg=[x for x,_ in diffs if x<-1]
print(f"{POL}: cap3/3 minus cap2/2  over {len(days)} sessions   net ${tot:,.0f}")
print(f"  days better {len(pos)} (+${sum(pos):,.0f})   days worse {len(neg)} (-${abs(sum(neg)):,.0f})   flat {len(days)-len(pos)-len(neg)}")
print("  worst days for cap 3:")
for x,d in sorted(diffs)[:8]: print(f"    {d}  ${x:>8,.0f}   (cap2 ${r2['daily'].get(d,0):>7,.0f} -> cap3 ${r3['daily'].get(d,0):>7,.0f})")
print("  best days for cap 3:")
for x,d in sorted(diffs)[-5:]: print(f"    {d}  ${x:>8,.0f}   (cap2 ${r2['daily'].get(d,0):>7,.0f} -> cap3 ${r3['daily'].get(d,0):>7,.0f})")
# equity paths
c2=c3=0; p2=p3=0; dd2=dd3=0
for d in days:
    c2+=r2["daily"].get(d,0); c3+=r3["daily"].get(d,0)
    p2=max(p2,c2); p3=max(p3,c3); dd2=min(dd2,c2-p2); dd3=min(dd3,c3-p3)
print(f"  MaxDD cap2 ${dd2:,.0f}   cap3 ${dd3:,.0f}")
# when did each MaxDD happen
c2=c3=0;p2=p3=0;w2=w3=None;b2=b3=0
for d in days:
    c2+=r2["daily"].get(d,0); c3+=r3["daily"].get(d,0); p2=max(p2,c2); p3=max(p3,c3)
    if c2-p2<b2: b2=c2-p2; w2=d
    if c3-p3<b3: b3=c3-p3; w3=d
print(f"  cap2 DD trough on {w2}   cap3 DD trough on {w3}")
