"""Income projection based on actual 91-trade history"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # Get daily PnL breakdown
    daily = conn.execute(text("""
        SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
               COUNT(*) as trades,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               ROUND(SUM(outcome_pnl)::numeric, 1) as daily_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)).mappings().all()

    print("DAILY P&L BREAKDOWN")
    print("=" * 60)
    total_pts = 0
    total_trades = 0
    trading_days = 0
    for d in daily:
        pnl = float(d['daily_pnl'])
        total_pts += pnl
        total_trades += d['trades']
        trading_days += 1
        print(f"  {d['trade_date']}  {d['trades']:>2} trades  {d['wins']:>2}W  {pnl:>+7.1f} pts  cumul={total_pts:>+7.1f}")

    avg_daily_pts = total_pts / trading_days
    avg_trades_per_day = total_trades / trading_days

    print(f"\n{'=' * 60}")
    print(f"Trading days: {trading_days}")
    print(f"Total trades: {total_trades}")
    print(f"Total PnL: {total_pts:+.1f} pts")
    print(f"Avg daily PnL: {avg_daily_pts:+.1f} pts/day")
    print(f"Avg trades/day: {avg_trades_per_day:.1f}")

    # ES contract = $50/pt, MES = $5/pt
    ES_PT_VALUE = 50.0
    SAR_RATE = 3.75  # USD to SAR

    print(f"\n{'=' * 60}")
    print("INCOME PROJECTIONS (based on {:.1f} pts/day avg)".format(avg_daily_pts))
    print("Assuming ~21 trading days/month")
    print("=" * 60)

    monthly_pts = avg_daily_pts * 21

    for contracts in [1, 2, 3, 4, 5, 6]:
        monthly_usd = monthly_pts * ES_PT_VALUE * contracts
        monthly_sar = monthly_usd * SAR_RATE
        yearly_usd = monthly_usd * 12
        yearly_sar = monthly_sar * 12
        print(f"\n  {contracts} ES contract(s):")
        print(f"    Monthly: {monthly_pts:+.0f} pts x ${ES_PT_VALUE:.0f} x {contracts} = ${monthly_usd:>+10,.0f} USD / {monthly_sar:>+12,.0f} SAR")
        print(f"    Yearly:  ${yearly_usd:>+10,.0f} USD / {yearly_sar:>+12,.0f} SAR")

    # Time to $1M
    print(f"\n{'=' * 60}")
    print("TIME TO $1,000,000 USD")
    print("=" * 60)
    for contracts in [2, 3, 4, 5, 6]:
        monthly_usd = monthly_pts * ES_PT_VALUE * contracts
        if monthly_usd > 0:
            months_to_1m = 1_000_000 / monthly_usd
            years = months_to_1m / 12
            print(f"  {contracts} ES: ${monthly_usd:>+8,.0f}/mo -> {months_to_1m:.1f} months ({years:.1f} years)")
        else:
            print(f"  {contracts} ES: negative monthly, N/A")

    # Conservative scenario: exclude GEX Long (worst performer)
    no_gex = conn.execute(text("""
        SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
               ROUND(SUM(outcome_pnl)::numeric, 1) as daily_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND setup_name != 'GEX Long'
        GROUP BY DATE(ts AT TIME ZONE 'America/New_York')
        ORDER BY trade_date ASC
    """)).mappings().all()

    no_gex_total = sum(float(d['daily_pnl']) for d in no_gex)
    no_gex_days = len(no_gex)
    no_gex_avg = no_gex_total / no_gex_days if no_gex_days > 0 else 0

    print(f"\n{'=' * 60}")
    print("CONSERVATIVE: Excluding GEX Long ({:.1f} pts/day avg)".format(no_gex_avg))
    print("=" * 60)
    monthly_pts_c = no_gex_avg * 21
    for contracts in [2, 3, 4, 5, 6]:
        monthly_usd = monthly_pts_c * ES_PT_VALUE * contracts
        monthly_sar = monthly_usd * SAR_RATE
        if monthly_usd > 0:
            months_to_1m = 1_000_000 / monthly_usd
            years = months_to_1m / 12
            print(f"  {contracts} ES: ${monthly_usd:>+8,.0f}/mo ({monthly_sar:>+10,.0f} SAR) -> $1M in {months_to_1m:.1f}mo ({years:.1f}yr)")

    # Worst daily drawdown
    print(f"\n{'=' * 60}")
    print("RISK: WORST DAILY DRAWDOWNS")
    print("=" * 60)
    worst_days = sorted(daily, key=lambda d: float(d['daily_pnl']))[:5]
    for d in worst_days:
        pnl = float(d['daily_pnl'])
        for c in [2, 4, 6]:
            usd = pnl * ES_PT_VALUE * c
            print(f"  {d['trade_date']} {pnl:>+6.1f} pts -> {c} ES = ${usd:>+8,.0f}", end="")
        print()

    # Capital requirements
    print(f"\n{'=' * 60}")
    print("CAPITAL REQUIREMENTS (ES overnight margin ~$15,400/contract)")
    print("=" * 60)
    for c in [2, 3, 4, 5, 6]:
        margin = c * 15400
        print(f"  {c} ES: ${margin:>8,} margin required")
