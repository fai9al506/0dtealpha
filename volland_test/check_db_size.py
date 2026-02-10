# volland_test/check_db_size.py
# Check current database size and estimate costs

import os
import sys

# Try to load DATABASE_URL from .env or environment
DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL:
    # Try to read from parent directory .env file
    env_file = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("DATABASE_URL="):
                    DATABASE_URL = line.strip().split("=", 1)[1].strip('"').strip("'")
                    break

if not DATABASE_URL:
    print("=" * 60)
    print("DATABASE_URL not found!")
    print("=" * 60)
    print()
    print("Options:")
    print("1. Set environment variable:")
    print('   set DATABASE_URL=postgresql://user:pass@host:port/db')
    print()
    print("2. Or enter it now:")
    DATABASE_URL = input("DATABASE_URL: ").strip()
    if not DATABASE_URL:
        print("No DATABASE_URL provided. Exiting.")
        sys.exit(1)

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    print("Installing psycopg...")
    os.system("pip install psycopg[binary]")
    import psycopg
    from psycopg.rows import dict_row


def check_database():
    print()
    print("=" * 60)
    print("DATABASE SIZE REPORT")
    print("=" * 60)

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Total database size
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS total_size")
            row = cur.fetchone()
            total_size = row["total_size"]
            print(f"\nTotal Database Size: {total_size}")

            # Get raw bytes for calculations
            cur.execute("SELECT pg_database_size(current_database()) AS bytes")
            total_bytes = cur.fetchone()["bytes"]
            total_gb = total_bytes / (1024 ** 3)

            print()
            print("-" * 60)
            print("TABLE SIZES")
            print("-" * 60)

            # Size per table
            cur.execute("""
                SELECT
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS size,
                    pg_total_relation_size(schemaname || '.' || tablename) AS bytes
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
            """)
            tables = cur.fetchall()

            print(f"{'Table':<35} {'Size':>15}")
            print("-" * 50)
            for t in tables:
                print(f"{t['tablename']:<35} {t['size']:>15}")

            print()
            print("-" * 60)
            print("ROW COUNTS")
            print("-" * 60)

            # Row counts for volland tables
            volland_tables = [
                "volland_exposure_points",
                "volland_snapshots",
                "chain_snapshots"
            ]

            print(f"{'Table':<35} {'Rows':>15}")
            print("-" * 50)

            for table in volland_tables:
                try:
                    cur.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
                    count = cur.fetchone()["cnt"]
                    print(f"{table:<35} {count:>15,}")
                except Exception as e:
                    print(f"{table:<35} {'(not found)':>15}")

            # Volland exposure points details
            print()
            print("-" * 60)
            print("VOLLAND EXPOSURE POINTS BREAKDOWN")
            print("-" * 60)

            try:
                cur.execute("""
                    SELECT
                        greek,
                        expiration_option,
                        COUNT(*) as count,
                        MIN(ts_utc) as oldest,
                        MAX(ts_utc) as newest
                    FROM volland_exposure_points
                    GROUP BY greek, expiration_option
                    ORDER BY greek, expiration_option
                """)
                breakdown = cur.fetchall()

                print(f"{'Greek':<10} {'Expiration':<15} {'Rows':>12} {'Oldest':>12} {'Newest':>12}")
                print("-" * 65)
                for b in breakdown:
                    greek = b['greek'] or '(null)'
                    exp = b['expiration_option'] or '(null)'
                    oldest = str(b['oldest'])[:10] if b['oldest'] else '-'
                    newest = str(b['newest'])[:10] if b['newest'] else '-'
                    print(f"{greek:<10} {exp:<15} {b['count']:>12,} {oldest:>12} {newest:>12}")
            except Exception as e:
                print(f"Could not get breakdown: {e}")

            # Data age
            print()
            print("-" * 60)
            print("DATA AGE")
            print("-" * 60)

            try:
                cur.execute("""
                    SELECT
                        MIN(ts_utc) as oldest,
                        MAX(ts_utc) as newest,
                        MAX(ts_utc) - MIN(ts_utc) as span
                    FROM volland_exposure_points
                """)
                age = cur.fetchone()
                print(f"Oldest record: {age['oldest']}")
                print(f"Newest record: {age['newest']}")
                print(f"Data span: {age['span']}")
            except:
                pass

            # Cost estimates
            print()
            print("=" * 60)
            print("COST ESTIMATES (Railway ~$0.25/GB/month)")
            print("=" * 60)

            monthly_cost = total_gb * 0.25
            print(f"\nCurrent size: {total_gb:.3f} GB")
            print(f"Current monthly cost: ${monthly_cost:.2f}")

            print()
            print("-" * 60)
            print("PROJECTED COSTS (if adding ~1 GB/month with V2)")
            print("-" * 60)

            projections = [
                ("Now", total_gb),
                ("1 month", total_gb + 1),
                ("6 months", total_gb + 6),
                ("1 year", total_gb + 12),
                ("2 years", total_gb + 24),
                ("5 years", total_gb + 60),
            ]

            print(f"{'Period':<15} {'Size':>10} {'Monthly Cost':>15}")
            print("-" * 45)
            for period, size in projections:
                cost = size * 0.25
                print(f"{period:<15} {size:>9.1f} GB ${cost:>13.2f}")

            print()
            print("=" * 60)
            print("REPORT COMPLETE")
            print("=" * 60)


if __name__ == "__main__":
    try:
        check_database()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    print()
    input("Press Enter to close...")
