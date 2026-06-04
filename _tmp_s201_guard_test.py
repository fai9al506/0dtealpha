"""S201 underwater-stack guard unit test — all scenarios, including fail-open."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("UNDERWATER_STACK_BLOCK_ENABLED", "true")
from app import real_trader as rt

def setup(orders):
    rt._active_orders.clear()
    rt._active_orders.update(orders)

PASS = []
def check(name, expect_block, setup_name="Skew Charm", is_long=True, es_price=7580.0):
    res = rt._underwater_stack_check(setup_name, is_long, es_price)
    ok = (res is not None) == expect_block
    PASS.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  -> {res}")

# 1. Jun 3 case: 2 filled SC longs underwater -> BLOCK
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7590.75),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7595.50),
})
check("2 open SC longs underwater (Jun3)", True, es_price=7580.0)

# 2. Same but in PROFIT (May 12 case) -> ALLOW
check("2 open SC longs in profit (May12)", False, es_price=7600.0)

# 3. Net positive mix (one up, one down, net > 0) -> ALLOW
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7570.0),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7588.0),
})
check("mixed, net +2 pts", False, es_price=7580.0)

# 4. Net negative mix -> BLOCK
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7575.0),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7595.0),
})
check("mixed, net -10 pts", True, es_price=7580.0)

# 5. Only 1 open underwater -> ALLOW (need >= 2)
setup({1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7595.0)})
check("only 1 open", False, es_price=7580.0)

# 6. 2 open but DIFFERENT setup -> ALLOW
setup({
    1: dict(setup_name="ES Absorption", direction="long", status="filled", fill_price=7595.0),
    2: dict(setup_name="ES Absorption", direction="long", status="filled", fill_price=7596.0),
})
check("different setup open", False, es_price=7580.0)

# 7. 2 open same setup but OPPOSITE direction -> ALLOW
setup({
    1: dict(setup_name="Skew Charm", direction="short", status="filled", fill_price=7560.0),
    2: dict(setup_name="Skew Charm", direction="short", status="filled", fill_price=7565.0),
})
check("opposite direction open", False, es_price=7580.0)

# 8. SHORTS: 2 SC shorts underwater (price rose above fills) -> BLOCK
check("2 SC shorts underwater", True, setup_name="Skew Charm", is_long=False, es_price=7580.0)
# 9. SHORTS in profit (price fell) -> ALLOW
check("2 SC shorts in profit", False, setup_name="Skew Charm", is_long=False, es_price=7550.0)

# 10. 'bullish' direction string variant counts as long -> BLOCK
setup({
    1: dict(setup_name="ES Absorption", direction="bullish", status="filled", fill_price=7595.0),
    2: dict(setup_name="ES Absorption", direction="bullish", status="filled", fill_price=7596.0),
})
check("'bullish' direction counts", True, setup_name="ES Absorption", es_price=7580.0)

# 11. pending_limit / pending_entry NOT counted -> ALLOW
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="pending_limit", fill_price=None),
    2: dict(setup_name="Skew Charm", direction="long", status="pending_entry", fill_price=None),
    3: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7595.0),
})
check("pendings excluded, only 1 filled", False, es_price=7580.0)

# 12. closed orders NOT counted -> ALLOW
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="closed", fill_price=7595.0),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7596.0),
})
check("closed excluded", False, es_price=7580.0)

# 13. fail-open: corrupt fill_price string -> no crash, ALLOW (fail-open)
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price="garbage"),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7596.0),
})
check("corrupt fill_price fail-open", False, es_price=7580.0)

# 14. es_price None/0 -> ALLOW (fail-open)
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7595.0),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7596.0),
})
check("es_price=None fail-open", False, es_price=None)
check("es_price=0 fail-open", False, es_price=0)

# 15. env kill switch -> ALLOW even when underwater
os.environ["UNDERWATER_STACK_BLOCK_ENABLED"] = "false"
check("env kill switch", False, es_price=7580.0)
os.environ["UNDERWATER_STACK_BLOCK_ENABLED"] = "true"

# 16. boundary: exactly breakeven (unreal == 0) -> ALLOW (strict < 0)
setup({
    1: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7580.0),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7580.0),
})
check("exactly breakeven allows", False, es_price=7580.0)

# 17. missing setup_name key in order dict -> no crash
setup({
    1: dict(direction="long", status="filled", fill_price=7595.0),
    2: dict(setup_name="Skew Charm", direction="long", status="filled", fill_price=7596.0),
})
check("missing keys tolerated", False, es_price=7580.0)

print(f"\n{sum(PASS)}/{len(PASS)} passed")
sys.exit(0 if all(PASS) else 1)
