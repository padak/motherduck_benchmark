#!/usr/bin/env python3
"""Scale from 19.2B rows to exactly 24B rows by adding 4.8B rows."""

import os
import duckdb
import time
from pathlib import Path

def format_number(n):
    """Format large numbers with commas."""
    return f"{n:,}"

def main():
    print("="*60)
    print("SCALING TO 24 BILLION ROWS")
    print("="*60)

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
        print("❌ Error: MOTHERDUCK_TOKEN not found in environment or .env file")
        return

    print("🔗 Connecting to MotherDuck...")
    con = duckdb.connect('md:contoso_benchmark', config={'motherduck_token': token})

    try:
        # Check current sizes
        print("\n📊 Checking current table sizes...")

        base_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_240k').fetchone()[0]
        current_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]

        print(f"  • Base table (contoso_sales_240k): {format_number(base_count)} rows")
        print(f"  • Current scaled table: {format_number(current_count)} rows")

        target_count = 24_000_000_000
        needed_rows = target_count - current_count
        multiplier = needed_rows // base_count

        print(f"\n🎯 Target: {format_number(target_count)} rows")
        print(f"📈 Need to add: {format_number(needed_rows)} rows")
        print(f"✖️  Multiplier needed: {format_number(multiplier)}x of base table")

        if current_count >= target_count:
            print(f"\n✅ Already at or above target size!")
            return

        if abs(needed_rows - (multiplier * base_count)) > 0:
            exact_needed = multiplier * base_count
            print(f"⚠️  Note: Will create {format_number(exact_needed)} rows (not exactly {format_number(needed_rows)})")

        print(f"\n⚠️  This operation will create a {format_number(needed_rows)}-row temporary table")
        print("and then combine it with your existing data.")
        response = input("\nContinue? (yes/no): ")
        if response.lower() != "yes":
            print("❌ Operation cancelled.")
            return

        overall_start = time.perf_counter()

        # Step 1: Create 4.8B row temporary table
        print(f"\n📝 Step 1: Creating temporary table with {format_number(multiplier * base_count)} rows...")
        print(f"   Using {multiplier}x multiplier on base table...")

        start = time.perf_counter()
        con.execute(f'''
            CREATE OR REPLACE TABLE main.contoso_sales_temp_addon AS
            SELECT
                original.*,
                replicate_id
            FROM main.contoso_sales_240k AS original
            CROSS JOIN (
                SELECT generate_series AS replicate_id
                FROM generate_series(1, {multiplier})
            ) AS replicator
        ''')
        elapsed = time.perf_counter() - start

        temp_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_temp_addon').fetchone()[0]
        print(f"   ✅ Created {format_number(temp_count)} rows in {elapsed:.1f} seconds")

        # Step 2: Combine tables
        print(f"\n📝 Step 2: Combining {format_number(current_count)} + {format_number(temp_count)} rows...")
        print("   Creating final 24B table...")

        start = time.perf_counter()
        con.execute('''
            CREATE OR REPLACE TABLE main.contoso_sales_24b_final AS
            SELECT * FROM main.contoso_sales_24b_scaled
            UNION ALL
            SELECT * FROM main.contoso_sales_temp_addon
        ''')
        elapsed = time.perf_counter() - start

        final_count = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_final').fetchone()[0]
        print(f"   ✅ Combined into {format_number(final_count)} rows in {elapsed:.1f} seconds")

        # Step 3: Replace old table
        print("\n📝 Step 3: Replacing old table...")
        con.execute('DROP TABLE main.contoso_sales_24b_scaled')
        con.execute('ALTER TABLE main.contoso_sales_24b_final RENAME TO contoso_sales_24b_scaled')
        print("   ✅ Table replaced")

        # Step 4: Update view
        print("\n📝 Step 4: Updating view...")
        con.execute('''
            CREATE OR REPLACE VIEW main.contoso_sales_24b AS
            SELECT * FROM main.contoso_sales_24b_scaled
        ''')
        print("   ✅ View updated")

        # Step 5: Cleanup
        print("\n📝 Step 5: Cleaning up temporary table...")
        con.execute('DROP TABLE main.contoso_sales_temp_addon')
        print("   ✅ Temporary table removed")

        # Final verification
        print("\n✅ SUCCESS!")
        print("="*60)

        final_verify = con.execute('SELECT COUNT(*) FROM main.contoso_sales_24b_scaled').fetchone()[0]
        total_elapsed = time.perf_counter() - overall_start

        print(f"📊 Final table size: {format_number(final_verify)} rows")
        print(f"⏱️  Total time: {total_elapsed:.1f} seconds")

        if final_verify == target_count:
            print(f"🎯 Exactly {format_number(target_count)} rows achieved!")
        else:
            diff = final_verify - target_count
            print(f"📈 {format_number(abs(diff))} rows {'over' if diff > 0 else 'under'} target")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n🔄 Attempting cleanup...")
        try:
            # Try to clean up any temporary tables
            con.execute("DROP TABLE IF EXISTS main.contoso_sales_temp_addon")
            con.execute("DROP TABLE IF EXISTS main.contoso_sales_24b_final")
            print("   ✅ Cleanup completed")
        except:
            pass

if __name__ == "__main__":
    main()