#!/usr/bin/env python3
"""
Incrementally scale from 9.6B to 24B rows with cooldown periods.
Adds data in chunks to avoid timeouts and memory issues.
"""

import os
import duckdb
import time
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
    print("INCREMENTAL SCALING TO 24 BILLION ROWS")
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
        total_to_add = target_count - current_count

        print(f"\n🎯 Target: {format_number(target_count)} rows")
        print(f"📈 Need to add: {format_number(total_to_add)} rows")

        if current_count >= target_count:
            print("\n✅ Already at or above target size!")
            return

        # Define incremental steps
        # First batch: 1.4B, then 13 batches of 1B each
        batches = []

        # Calculate how many rows we need based on current count
        if current_count == 9_600_000_000:
            # Starting from 9.6B, need to add 14.4B total
            batches = [
                ("Batch 1", 1_400_000_000),  # First batch: 1.4B
            ]
            # Add 13 batches of 1B each
            for i in range(2, 15):
                batches.append((f"Batch {i}", 1_000_000_000))
        else:
            # Generic approach: divide remaining into reasonable chunks
            chunk_size = 1_000_000_000  # 1B per chunk
            remaining = total_to_add
            batch_num = 1
            while remaining > 0:
                this_batch = min(chunk_size, remaining)
                batches.append((f"Batch {batch_num}", this_batch))
                remaining -= this_batch
                batch_num += 1

        print(f"\n📋 Execution plan: {len(batches)} batches")
        for name, rows in batches[:5]:  # Show first 5
            print(f"  • {name}: {format_number(rows)} rows")
        if len(batches) > 5:
            print(f"  • ... and {len(batches)-5} more batches")

        cooldown_seconds = 15
        print(f"\n⏱️  Cooldown between batches: {cooldown_seconds} seconds")

        response = input("\nProceed with incremental scaling? (yes/no): ")
        if response.lower() != "yes":
            print("❌ Operation cancelled.")
            return

        overall_start = time.time()
        rows_added_total = 0

        # Process each batch
        for batch_idx, (batch_name, rows_to_add) in enumerate(batches, 1):
            print(f"\n{'='*70}")
            print_timestamp(f"📦 {batch_name} ({batch_idx}/{len(batches)})")
            print(f"  Adding {format_number(rows_to_add)} rows...")

            # Calculate multiplier for this batch
            multiplier = rows_to_add // base_count
            if multiplier * base_count != rows_to_add:
                print(f"  ⚠️  Adjusting to {format_number(multiplier * base_count)} rows (multiple of base)")

            try:
                batch_start = time.time()

                # Step 1: Create temporary table with the batch data using UNION ALL
                print_timestamp("  Creating temporary batch table using UNION ALL...")

                # For large multipliers, build up progressively
                if multiplier <= 10:
                    # Small multiplier - direct UNION ALL
                    unions = ["SELECT * FROM main.contoso_sales_240k"]
                    for i in range(1, multiplier):
                        unions.append("UNION ALL SELECT * FROM main.contoso_sales_240k")

                    con.execute(f'''
                        CREATE OR REPLACE TABLE main.temp_batch AS
                        {' '.join(unions)}
                    ''')
                else:
                    # Large multiplier - build progressively to avoid memory issues
                    # First create a 10x table
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

                    if multiplier <= 100:
                        # Build from 10x
                        remaining = multiplier // 10
                        unions = ["SELECT * FROM main.temp_10x"]
                        for i in range(1, remaining):
                            unions.append("UNION ALL SELECT * FROM main.temp_10x")

                        con.execute(f'''
                            CREATE OR REPLACE TABLE main.temp_batch AS
                            {' '.join(unions)}
                        ''')
                    else:
                        # For very large multipliers, build 100x first
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

                        # Now build the final batch
                        remaining = multiplier // 100
                        con.execute('CREATE OR REPLACE TABLE main.temp_batch AS SELECT * FROM main.temp_100x')

                        for i in range(1, remaining):
                            print_timestamp(f"    Adding chunk {i+1}/{remaining}...")
                            con.execute('INSERT INTO main.temp_batch SELECT * FROM main.temp_100x')

                        # Clean up intermediate tables
                        con.execute('DROP TABLE IF EXISTS main.temp_100x')

                    # Clean up 10x table
                    con.execute('DROP TABLE IF EXISTS main.temp_10x')

                # Step 2: Insert into main table
                print_timestamp("  Inserting batch into main table...")
                con.execute('''
                    INSERT INTO main.contoso_sales_24b_scaled
                    SELECT * FROM main.temp_batch
                ''')

                # Step 3: Clean up temp table
                con.execute('DROP TABLE main.temp_batch')

                batch_elapsed = time.time() - batch_start
                rows_added_total += multiplier * base_count

                # Get current count
                new_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]

                print_timestamp(f"  ✅ Batch complete in {batch_elapsed:.1f}s")
                print(f"  📊 Current total: {format_number(new_count)} rows")
                print(f"  📈 Progress: {new_count/target_count*100:.1f}%")

                # Cooldown period (except for last batch)
                if batch_idx < len(batches):
                    print_timestamp(f"  💤 Cooling down for {cooldown_seconds} seconds...")
                    time.sleep(cooldown_seconds)

            except Exception as e:
                print_timestamp(f"  ❌ Batch failed: {e}")
                print("  Attempting to continue with next batch...")
                time.sleep(cooldown_seconds * 2)  # Longer cooldown after error
                continue

        # Final verification
        print(f"\n{'='*70}")
        print_timestamp("🔍 Final verification...")

        final_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]
        total_elapsed = time.time() - overall_start

        print(f"\n✅ SCALING COMPLETE!")
        print(f"📊 Final table size: {format_number(final_count)} rows")
        print(f"📈 Total rows added: {format_number(rows_added_total)}")
        print(f"⏱️  Total time: {total_elapsed/60:.1f} minutes")

        if final_count >= target_count:
            print(f"🎯 Target of {format_number(target_count)} rows achieved!")
        else:
            shortfall = target_count - final_count
            print(f"⚠️  {format_number(shortfall)} rows short of target")

        # Update view
        print_timestamp("📝 Updating view...")
        con.execute('''
            CREATE OR REPLACE VIEW main.contoso_sales_24b AS
            SELECT * FROM main.contoso_sales_24b_scaled
        ''')
        print("✅ View updated successfully")

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        print("Cleaning up...")
        try:
            con.execute("DROP TABLE IF EXISTS main.temp_batch")
            print("✅ Cleanup completed")
        except:
            pass

if __name__ == "__main__":
    main()