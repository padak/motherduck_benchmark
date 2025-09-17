#!/usr/bin/env python3
"""
Optimized incremental scaling to 24B rows.
Builds a 1B row temp table ONCE and reuses it for all inserts.
"""

import os
import duckdb
import time
import math
from pathlib import Path
from datetime import datetime

def format_number(n):
    """Format large numbers with commas."""
    return f"{n:,}"

def print_timestamp(message):
    """Print message with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def main():
    print("="*70)
    print("OPTIMIZED INCREMENTAL SCALING TO 24 BILLION ROWS")
    print("="*70)

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
        print("❌ Error: MOTHERDUCK_TOKEN not found")
        return

    print_timestamp("🔗 Connecting to MotherDuck...")
    con = duckdb.connect('md:contoso_benchmark', config={'motherduck_token': token})

    try:
        # Check current state
        print_timestamp("📊 Checking current table sizes...")

        base_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_240k').fetchone()[0]
        current_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]

        print(f"  • Base table (240k): {format_number(base_count)} rows")
        print(f"  • Current scaled table: {format_number(current_count)} rows")

        target_count = 24_000_000_000

        if current_count >= target_count:
            print("\n✅ Already at or above target size!")
            return

        # Calculate what we need to reach the next billion
        billion = 1_000_000_000
        current_billions = current_count / billion

        # Check if we need to round to the nearest billion
        remainder = current_count % billion

        # If not exactly on a billion boundary, round to nearest billion
        if remainder != 0:
            if remainder < 500_000_000:
                # Round down - remove some rows (not supported, so we round up)
                rows_to_next_billion = billion - remainder
                print(f"\n📏 Current count: {format_number(current_count)} ({current_billions:.2f}B)")
                print(f"  • Rounding UP to {math.ceil(current_count / billion)}B")
                print(f"  • Adding {format_number(rows_to_next_billion)} rows to reach even billion")
            else:
                # Round up to next billion
                rows_to_next_billion = billion - remainder
                print(f"\n📏 Current count: {format_number(current_count)} ({current_billions:.2f}B)")
                print(f"  • Rounding UP to {math.ceil(current_count / billion)}B")
                print(f"  • Adding {format_number(rows_to_next_billion)} rows to reach even billion")

            # Create small temp table for rounding
            multiplier_small = rows_to_next_billion // base_count
            if multiplier_small > 0:
                print_timestamp(f"Creating rounding table with {format_number(multiplier_small * base_count)} rows...")
                create_temp_table(con, base_count, multiplier_small, "temp_round")

                print_timestamp("Inserting rounding batch...")
                con.execute('INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_round')
                con.execute('DROP TABLE main.temp_round')

                # Update current count
                current_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]
                print(f"  ✅ Rounded to: {format_number(current_count)} rows")
            else:
                print(f"  ⚠️  Need less than base table size, skipping rounding")
        else:
            print(f"\n✅ Current count is exactly {current_count // billion}B - no rounding needed")

        # Now calculate how many billions we need
        billions_needed = (target_count - current_count) // billion

        print(f"\n🎯 Target: {format_number(target_count)} rows")
        print(f"📈 Current: {format_number(current_count)} rows")
        print(f"📊 Need to add: {format_number(billions_needed)} billion-row batches")

        if billions_needed == 0:
            print("\n✅ Close enough to target! Less than 1B rows needed.")
            return

        # Ask for confirmation
        cooldown_seconds = 15
        print(f"\n⏱️  Plan: Build 1B row table once, then insert it {billions_needed} times")
        print(f"⏱️  Cooldown between inserts: {cooldown_seconds} seconds")
        print(f"⏱️  Estimated time: ~{billions_needed * (cooldown_seconds + 60) // 60} minutes")

        response = input("\nProceed? (yes/no): ")
        if response.lower() != "yes":
            print("❌ Operation cancelled.")
            return

        overall_start = time.time()

        # STEP 1: Build the 1B row temp table ONCE
        print(f"\n{'='*70}")
        print_timestamp("🔨 BUILDING REUSABLE 1B ROW TABLE (one-time operation)")
        print(f"{'='*70}")

        # Check if temp_1b already exists
        try:
            existing_count = con.execute('SELECT COUNT(*) FROM main.temp_1b').fetchone()[0]
            print(f"  ✅ Found existing temp_1b table with {format_number(existing_count)} rows")
            if existing_count != billion:
                print(f"  ⚠️  Size mismatch, rebuilding...")
                con.execute('DROP TABLE main.temp_1b')
                create_1b_table(con, base_count)
        except:
            print_timestamp("  Building new 1B row table...")
            create_1b_table(con, base_count)

        # Verify the 1B table
        temp_count = con.execute('SELECT COUNT(*) FROM main.temp_1b').fetchone()[0]
        print(f"  ✅ Reusable table ready: {format_number(temp_count)} rows")

        # STEP 2: Insert the 1B table multiple times with cooldowns
        print(f"\n{'='*70}")
        print(f"📦 INSERTING 1B ROWS {billions_needed} TIMES")
        print(f"{'='*70}")

        for batch_num in range(1, billions_needed + 1):
            batch_start = time.time()

            expected_after = current_count + billion
            progress = (expected_after / target_count) * 100

            print(f"\n[Batch {batch_num}/{billions_needed}]")
            print(f"  Current: {format_number(current_count)} → Target: {format_number(expected_after)}")

            print_timestamp("  Inserting 1B rows...")
            con.execute('INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_1b')

            # Verify new count
            current_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]
            batch_elapsed = time.time() - batch_start

            print_timestamp(f"  ✅ Complete in {batch_elapsed:.1f}s")
            print(f"  📊 New total: {format_number(current_count)} rows ({progress:.1f}%)")

            # Cooldown (except for last batch)
            if batch_num < billions_needed:
                print_timestamp(f"  💤 Cooling down for {cooldown_seconds} seconds...")
                time.sleep(cooldown_seconds)

        # STEP 3: Cleanup and final verification
        print(f"\n{'='*70}")
        print_timestamp("🧹 CLEANUP AND VERIFICATION")
        print(f"{'='*70}")

        # Keep or delete the 1B table?
        print("\n💾 Keep the 1B temp table for future use? (yes/no)")
        keep_response = input("  Response: ")
        if keep_response.lower() != "yes":
            print_timestamp("  Dropping temp_1b table...")
            con.execute('DROP TABLE IF EXISTS main.temp_1b')
            print("  ✅ Temp table removed")
        else:
            print("  ✅ Temp table kept for future use")

        # Update view
        print_timestamp("📝 Updating view...")
        con.execute('''
            CREATE OR REPLACE VIEW main.contoso_sales_24b AS
            SELECT * FROM main.contoso_sales_24b_scaled
        ''')
        print("  ✅ View updated")

        # Final stats
        final_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]
        total_elapsed = time.time() - overall_start

        print(f"\n{'='*70}")
        print("✅ SCALING COMPLETE!")
        print(f"{'='*70}")
        print(f"📊 Final table size: {format_number(final_count)} rows")
        print(f"⏱️  Total time: {total_elapsed/60:.1f} minutes")

        if final_count >= target_count:
            print(f"🎯 Target of {format_number(target_count)} rows achieved!")
        else:
            shortfall = target_count - final_count
            print(f"📈 {format_number(shortfall)} rows short of target")
            print("   Run again to add more billions if needed.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Cleaning up...")
        try:
            # Don't delete temp_1b - it can be reused!
            con.execute("DROP TABLE IF EXISTS main.temp_10x")
            con.execute("DROP TABLE IF EXISTS main.temp_100x")
            print("✅ Cleanup completed (kept temp_1b for reuse)")
        except:
            pass

def create_temp_table(con, base_count, multiplier, table_name):
    """Create a temp table with specified multiplier of base rows."""
    if multiplier <= 10:
        # Direct UNION ALL
        unions = ["SELECT * FROM main.contoso_sales_240k"]
        for i in range(1, multiplier):
            unions.append("UNION ALL SELECT * FROM main.contoso_sales_240k")

        con.execute(f'''
            CREATE OR REPLACE TABLE main.{table_name} AS
            {' '.join(unions)}
        ''')
    else:
        # Build progressively
        print_timestamp("    Building 10x base...")
        con.execute('''
            CREATE OR REPLACE TABLE main.temp_10x AS
            SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
            UNION ALL SELECT * FROM main.contoso_sales_240k
        ''')

        remaining = multiplier // 10
        if remaining <= 10:
            unions = ["SELECT * FROM main.temp_10x"]
            for i in range(1, remaining):
                unions.append("UNION ALL SELECT * FROM main.temp_10x")

            con.execute(f'''
                CREATE OR REPLACE TABLE main.{table_name} AS
                {' '.join(unions)}
            ''')
        else:
            # Need 100x
            print_timestamp("    Building 100x base...")
            con.execute('''
                CREATE OR REPLACE TABLE main.temp_100x AS
                SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
                UNION ALL SELECT * FROM main.temp_10x
            ''')

            # Build final table
            remaining_100s = multiplier // 100
            con.execute(f'CREATE OR REPLACE TABLE main.{table_name} AS SELECT * FROM main.temp_100x')

            for i in range(1, remaining_100s):
                if i % 10 == 0:
                    print_timestamp(f"    Adding chunk {i}/{remaining_100s}...")
                con.execute(f'INSERT INTO main.{table_name} SELECT * FROM main.temp_100x')

            con.execute('DROP TABLE IF EXISTS main.temp_100x')

        con.execute('DROP TABLE IF EXISTS main.temp_10x')

def create_1b_table(con, base_count):
    """Create the 1B row table efficiently."""
    # 1B = 240k * 4,167 (approximately)
    multiplier = 1_000_000_000 // base_count  # Should be 4166

    print_timestamp(f"  Building with multiplier {multiplier}x of base table...")

    # Build progressively: 10x -> 100x -> 1000x -> final
    print_timestamp("  Step 1: Building 10x (2.4M rows)...")
    con.execute('''
        CREATE OR REPLACE TABLE main.temp_10x AS
        SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
        UNION ALL SELECT * FROM main.contoso_sales_240k
    ''')

    print_timestamp("  Step 2: Building 100x (24M rows)...")
    con.execute('''
        CREATE OR REPLACE TABLE main.temp_100x AS
        SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
        UNION ALL SELECT * FROM main.temp_10x
    ''')

    print_timestamp("  Step 3: Building 1000x (240M rows)...")
    con.execute('''
        CREATE OR REPLACE TABLE main.temp_1000x AS
        SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
        UNION ALL SELECT * FROM main.temp_100x
    ''')

    # Now we have 240M rows, need to get to ~1B
    # 1B / 240M = ~4.17, so we need 4 copies plus a bit
    print_timestamp("  Step 4: Building final 1B table...")
    con.execute('''
        CREATE OR REPLACE TABLE main.temp_1b AS
        SELECT * FROM main.temp_1000x
        UNION ALL SELECT * FROM main.temp_1000x
        UNION ALL SELECT * FROM main.temp_1000x
        UNION ALL SELECT * FROM main.temp_1000x
    ''')

    # Add the remainder to get closer to 1B
    remaining_rows = 1_000_000_000 - (240_000_000 * 4)  # Should be 40M
    if remaining_rows > 0:
        chunks_needed = remaining_rows // 24_000_000  # How many 100x chunks
        print_timestamp(f"  Step 5: Adding {chunks_needed} more chunks to reach 1B...")
        for i in range(chunks_needed):
            con.execute('INSERT INTO main.temp_1b SELECT * FROM main.temp_100x')

    # Clean up intermediate tables
    print_timestamp("  Cleaning up intermediate tables...")
    con.execute('DROP TABLE IF EXISTS main.temp_10x')
    con.execute('DROP TABLE IF EXISTS main.temp_100x')
    con.execute('DROP TABLE IF EXISTS main.temp_1000x')

if __name__ == "__main__":
    main()