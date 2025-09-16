#!/usr/bin/env python3
"""Quick script to scale an already-scaled table further."""

import sys
import os
import duckdb
import time
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print("Usage: python scale_further.py <multiplier>")
        print("Example: python scale_further.py 10  # Scales current table by 10x")
        sys.exit(1)

    multiplier = int(sys.argv[1])

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
    print(f"📈 Multiplication factor: {multiplier}x\n")

    if expected_count > 10_000_000_000:
        print(f"⚠️  WARNING: Creating {expected_count:,} rows will take significant time!")
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Scaling cancelled.")
            return

    print(f"⏱️  Creating scaled table...")
    start_time = time.perf_counter()

    try:
        # Create new scaled table
        con.execute(f'''
        CREATE OR REPLACE TABLE main.contoso_sales_24b_scaled_new AS
        SELECT original.*
        FROM main.contoso_sales_24b_scaled AS original
        CROSS JOIN (
            SELECT generate_series AS additional_replicate_id
            FROM generate_series(1, {multiplier})
        ) AS replicator
        ''')

        # Verify count
        actual_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled_new').fetchone()[0]

        # Replace old table
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

if __name__ == "__main__":
    main()