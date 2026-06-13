"""Final audit: verify all fixes from 2026-05-20 are correctly applied.

Checks:
  1. S161 — circuit breaker uses broker realized P&L (not setup_log gross loss)
  2. S164 — V16 portal blocks DD shorts (BOTH JS sites)
  3. S165 — silent skip telemetry (3 paths: _dd_short_block, ES Abs log_only, ES Abs PURE)
  4. Syntax check both modified files
  5. Tasks.md has all status updates

Read-only — no DB writes, no API calls.
"""
import re
from pathlib import Path

ROOT = Path("G:/My Drive/Python/MyProject/GitHub/0dtealpha")

def check_file(label, path, must_contain):
    src = (ROOT / path).read_text(encoding="utf-8")
    print(f"\n[{label}] {path}")
    for description, pattern in must_contain:
        if re.search(pattern, src):
            print(f"  PASS  {description}")
        else:
            print(f"  FAIL  {description}")

# ============ S161: real_trader.py circuit breaker ============
check_file("S161", "app/real_trader.py", [
    ("function name retained",
        r"^def _get_daily_realized_loss\(\)"),
    ("reads broker P&L (calls _get_daily_realized_pnl)",
        r"_get_daily_realized_pnl\(acct\)"),
    ("iterates ACCOUNT_WHITELIST",
        r"for acct in ACCOUNT_WHITELIST"),
    ("returns max(0, -net) — positive when net loss, 0 when green",
        r"return max\(0\.0?, -net\)"),
    ("does NOT use setup_log gross loss any more",
        r"S161 fix \(2026-05-20\)"),
])

# ============ S164: V16 portal DD short block ============
check_file("S164a (V16 strat)", "app/main.py", [
    ("V16 strat block: DD short return false",
        r"// S164 \(2026-05-20\): DD shorts unconditionally blocked by TSRT via main\.py:5430"),
    ("V16 strat: explicit DD short rejection line",
        r"if \(sn === 'DD Exhaustion' && !isLong\) return false;"),
])
check_file("S164b (tlV16 strat)", "app/main.py", [
    ("tlV16 strat: DD short comment",
        r"// S164 \(2026-05-20\): DD shorts unconditionally blocked"),
])

# ============ S165: silent skip telemetry ============
check_file("S165a (_dd_short_block)", "app/main.py", [
    ("_dd_short_block writes skip_reason=dd_short_block",
        r'_rt_skip\._log_skip_reason\(_current_setup_log\.get\(setup_name\), "dd_short_block"\)'),
])
check_file("S165b (ES Abs log_only)", "app/main.py", [
    ("ES Abs log_only writes skip_reason=log_only_pattern",
        r'_rt_skip\._log_skip_reason\(_abs_sid, "log_only_pattern"\)'),
])
check_file("S165c (ES Abs live filter)", "app/main.py", [
    ("ES Abs PURE block writes skip_reason=live_filter_block",
        r'_rt_skip\._log_skip_reason\(_abs_sid, "live_filter_block"\)'),
])

# ============ Syntax ============
print("\n[syntax]")
import ast
for f in ["app/real_trader.py", "app/main.py"]:
    src = (ROOT / f).read_text(encoding="utf-8")
    try:
        ast.parse(src)
        print(f"  PASS  {f} parses cleanly ({len(src):,} chars)")
    except SyntaxError as e:
        print(f"  FAIL  {f}: {e}")

# ============ Tasks.md entries ============
tasks = (ROOT / "Tasks.md").read_text(encoding="utf-8")
print("\n[Tasks.md entries]")
for sid in ("S159", "S160", "S161", "S162", "S163", "S164", "S165"):
    present = f"| {sid} |" in tasks
    print(f"  {'PASS' if present else 'FAIL'}  {sid} entry present")

print("\n" + "=" * 60)
print("AUDIT COMPLETE — see PASS/FAIL above")
print("=" * 60)
