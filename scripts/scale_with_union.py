#!/usr/bin/env python3
"""Scale table using UNION ALL approach - more memory efficient than CROSS JOIN."""

import sys
import os
import duckdb
import time
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print("Usage: python scale_with_union.py <multiplier> [strategy]")
        print("  multiplier: How many times to replicate the data")
        print("  strategy: 'recursive' (default) or 'iterative'")
        print("\nExample: python scale_with_union.py 10 recursive")
        print("\nStrategies:")
        print("  recursive: Uses recursive doubling (fastest for powers of 2)")
        print("  iterative: Builds incrementally (more controlled memory usage)")
        sys.exit(1)

    multiplier = int(sys.argv[1])
    strategy = sys.argv[2] if len(sys.argv) > 2 else "recursive"

    # Load .env
    env_file = Path('.env')
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and '=' in line and not line.startswith('#'):
                key, value = line.split('=', 1)
                key = key.strip().replace('export ', '')
                value = value.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, value)

    token = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('motherduck_token')
    if not token:
        print("Error: MOTHERDUCK_TOKEN not found in environment or .env file")
        sys.exit(1)

    print(f"Connecting to MotherDuck...")
    con = duckdb.connect('md:contoso_benchmark', config={'motherduck_token': token})

    # Check current size
    current_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]
    expected_count = current_count * multiplier

    print(f"\n📊 Current table size: {current_count:,} rows")
    print(f"🎯 Target table size: {expected_count:,} rows")
    print(f"📈 Multiplication factor: {multiplier}x")
    print(f"⚙️  Strategy: {strategy}\n")

    if expected_count > 10_000_000_000:
        print(f"⚠️  WARNING: Creating {expected_count:,} rows will take significant time!")
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Scaling cancelled.")
            return

    start_time = time.perf_counter()

    try:
        if strategy == "recursive":
            scale_recursive(con, multiplier)
        else:
            scale_iterative(con, multiplier)

        # Verify count
        actual_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled_new').fetchone()[0]

        # Replace old table
        print("📝 Replacing old table...")
        con.execute('DROP TABLE main.contoso_sales_24b_scaled')
        con.execute('ALTER TABLE main.contoso_sales_24b_scaled_new RENAME TO contoso_sales_24b_scaled')

        elapsed = time.perf_counter() - start_time

        print(f"\n✅ Scaling completed successfully!")
        print(f"📊 Final row count: {actual_count:,}")
        print(f"⏱️  Time taken: {elapsed:.2f} seconds")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        elapsed = time.perf_counter() - start_time
        print(f"⏱️  Failed after {elapsed:.2f} seconds")

def scale_recursive(con, multiplier):
    """
    Recursive doubling strategy:
    - Most efficient for powers of 2
    - Uses fewer intermediate steps
    - E.g., for 8x: 1 -> 2 -> 4 -> 8
    """
    print("⏱️  Using recursive doubling strategy...")

    # Start with the original table
    con.execute('''
    CREATE OR REPLACE TABLE main.contoso_sales_24b_scaled_new AS
    SELECT * FROM main.contoso_sales_24b_scaled
    ''')

    current_multiplier = 1

    # Double until we exceed the target
    while current_multiplier * 2 <= multiplier:
        print(f"  📈 Doubling: {current_multiplier}x -> {current_multiplier * 2}x")
        con.execute('''
        INSERT INTO main.contoso_sales_24b_scaled_new
        SELECT * FROM main.contoso_sales_24b_scaled_new
        ''')
        current_multiplier *= 2

    # Add the remainder if needed
    remainder = multiplier - current_multiplier
    if remainder > 0:
        print(f"  📈 Adding remainder: {current_multiplier}x -> {multiplier}x")

        # For small remainders, add directly
        if remainder <= current_multiplier:
            con.execute(f'''
            INSERT INTO main.contoso_sales_24b_scaled_new
            SELECT * FROM main.contoso_sales_24b_scaled
            LIMIT (SELECT COUNT(*) * {remainder} FROM main.contoso_sales_24b_scaled)
            ''')
        else:
            # For larger remainders, add in chunks
            while remainder > 0:
                chunk = min(remainder, current_multiplier)
                con.execute(f'''
                INSERT INTO main.contoso_sales_24b_scaled_new
                SELECT * FROM main.contoso_sales_24b_scaled
                LIMIT (SELECT COUNT(*) * {chunk} FROM main.contoso_sales_24b_scaled)
                ''')
                remainder -= chunk

def scale_iterative(con, multiplier):
    """
    Iterative strategy:
    - More predictable memory usage
    - Adds one copy at a time
    - Better for non-power-of-2 multipliers
    """
    print("⏱️  Using iterative strategy...")

    # Create initial table with UNION ALL
    print(f"  📈 Building query with {multiplier} UNION ALL statements...")

    # Build the UNION ALL query
    union_parts = ["SELECT * FROM main.contoso_sales_24b_scaled"]
    for i in range(1, multiplier):
        union_parts.append("UNION ALL SELECT * FROM main.contoso_sales_24b_scaled")

    # Execute as single query
    query = f"""
    CREATE OR REPLACE TABLE main.contoso_sales_24b_scaled_new AS
    {' '.join(union_parts)}
    """

    print(f"  ⚙️  Executing combined UNION ALL query...")
    con.execute(query)

if __name__ == "__main__":
    main()