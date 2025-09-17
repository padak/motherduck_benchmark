#!/usr/bin/env python3
"""Utility to load Contoso parquet samples into MotherDuck and run benchmark queries."""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import duckdb


def quote_identifier(name: str) -> str:
    """Return a SQL identifier quoted for DuckDB/MotherDuck."""
    return '"' + name.replace('"', '""') + '"'

BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR / "Performance_Test_Snowflake_Databricks"
SAMPLES_DIR = REPO_DIR / "SampleFiles"
QUERY_FILE = REPO_DIR / "code" / "query_list.sql"

DEFAULT_TEMP_DIR = Path(os.environ.get("TMPDIR", "/tmp")) / "duckdb"
DEFAULT_THREADS = 1
DEFAULT_MAX_MEMORY_MB = 256

TABLE_FILES: Sequence[Tuple[str, str]] = (
    ("contoso_stores", "contoso_stores.parquet_0_0_0.snappy.parquet"),
    ("contoso_products", "contoso_products.parquet_0_0_0.snappy.parquet"),
    ("contoso_sales_240k", "contoso_sales_240k.parquet_0_0_0.snappy.parquet"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python %(prog)s --init-db                      # Initialize database and load data
  python %(prog)s --query-all                    # Run all benchmark queries
  python %(prog)s --query 01                     # Run only Query 01
  python %(prog)s --query 01 05 10               # Run queries 01, 05, and 10
  python %(prog)s --query-all --profile          # Run with detailed resource profiling
  python %(prog)s --query 01 --explain --profile # Show query plan and resource usage
""",
    )

    # Action arguments
    action_group = parser.add_argument_group("actions")
    action_group.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database and load parquet data",
    )
    action_group.add_argument(
        "--query-all",
        action="store_true",
        help="Run all benchmark queries",
    )
    action_group.add_argument(
        "--query",
        nargs="+",
        metavar="N",
        help="Run specific query numbers (e.g., --query 01 05 10)",
    )
    action_group.add_argument(
        "--explain",
        action="store_true",
        help="Run EXPLAIN ANALYZE on queries to show query plan and statistics",
    )
    action_group.add_argument(
        "--verbose",
        action="store_true",
        help="Show query text before execution",
    )
    action_group.add_argument(
        "--show-tables",
        action="store_true",
        help="Show all tables in the database with row counts",
    )
    action_group.add_argument(
        "--scale-table",
        metavar="MULTIPLIER",
        type=int,
        help="Scale contoso_sales_240k table by given multiplier (e.g., 100000 for 24B rows)",
    )
    action_group.add_argument(
        "--use-union",
        action="store_true",
        help="Use UNION ALL instead of CROSS JOIN for scaling (more memory efficient)",
    )
    action_group.add_argument(
        "--show-storage",
        action="store_true",
        help="Show storage usage information for all databases",
    )
    action_group.add_argument(
        "--profile",
        action="store_true",
        help="Enable detailed resource profiling for queries (memory, temp files, etc.)",
    )

    # Configuration arguments
    config_group = parser.add_argument_group("configuration")
    config_group.add_argument(
        "--env-file",
        default=BASE_DIR / ".env",
        type=Path,
        help="Path to a .env-style file containing MOTHERDUCK_TOKEN",
    )
    config_group.add_argument(
        "--database",
        default="contoso_benchmark",
        help="MotherDuck database to create/use",
    )
    config_group.add_argument(
        "--schema",
        default="main",
        help="MotherDuck schema to create/use",
    )
    config_group.add_argument(
        "--query-file",
        default=QUERY_FILE,
        type=Path,
        help="SQL file with benchmark queries",
    )
    config_group.add_argument(
        "--preview-rows",
        type=int,
        default=0,
        help=(
            "Number of rows to fetch from SELECT statements for verification. "
            "Use 0 to skip fetching results (only timings will be recorded)."
        ),
    )

    # Performance arguments
    perf_group = parser.add_argument_group("performance")
    perf_group.add_argument(
        "--temp-directory",
        type=Path,
        default=DEFAULT_TEMP_DIR,
        help=(
            "Directory DuckDB should use for spill files (defaults to TMPDIR/duckdb or /tmp/duckdb)."
        ),
    )
    perf_group.add_argument(
        "--extension-directory",
        type=Path,
        default=None,
        help="Directory for DuckDB extensions (defaults to <temp-directory>/extensions).",
    )
    perf_group.add_argument(
        "--threads",
        type=int,
        default=DEFAULT_THREADS,
        help="Number of DuckDB threads to use (default: 1).",
    )
    perf_group.add_argument(
        "--max-memory-mb",
        type=int,
        default=DEFAULT_MAX_MEMORY_MB,
        help="Maximum DuckDB memory allocation in MB (default: 256).",
    )

    args = parser.parse_args()

    # Show help if no action arguments provided
    if not any([args.init_db, args.query_all, args.query, args.show_tables, args.scale_table, args.show_storage]):
        parser.print_help()
        parser.exit()

    return args


def load_env_file(env_path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not env_path.exists():
        return values

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def ensure_environment(env_path: Path) -> None:
    env_values = load_env_file(env_path)
    for key, value in env_values.items():
        os.environ.setdefault(key, value)


def connect_to_motherduck(
    database: str,
    token: str | None,
    *,
    threads: int,
    max_memory_mb: int,
    temp_directory: Path,
    extension_directory: Path,
) -> duckdb.DuckDBPyConnection:
    config = {
        "threads": threads,
        "max_memory": f"{max_memory_mb}MB",
        "temp_directory": str(temp_directory),
        "extension_directory": str(extension_directory),
    }
    if token:
        config["motherduck_token"] = token
    try:
        con = duckdb.connect("md:", config=config)
    except duckdb.Error as exc:  # pragma: no cover - surfacing clear message
        raise SystemExit(f"Failed to connect to MotherDuck: {exc}")

    con.execute(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database)}")
    con.execute(f"USE {quote_identifier(database)}")
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection, schema: str) -> str:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema)}")
    return schema


def load_parquet_tables(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    tables: Sequence[Tuple[str, str]],
) -> None:
    schema_prefix = f"{quote_identifier(schema)}."
    for table_name, file_name in tables:
        file_path = SAMPLES_DIR / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Expected sample file missing: {file_path}")
        sql = (
            f"CREATE OR REPLACE TABLE {schema_prefix}{quote_identifier(table_name)} AS "
            f"SELECT * FROM read_parquet('{file_path.as_posix()}')"
        )
        con.execute(sql)
        count = con.execute(
            f"SELECT COUNT(*) FROM {schema_prefix}{quote_identifier(table_name)}"
        ).fetchone()[0]
        print(f"Loaded {table_name} ({count} rows)")

    view_name = f"{schema_prefix}{quote_identifier('contoso_sales_24b')}"
    source_table = f"{schema_prefix}{quote_identifier('contoso_sales_240k')}"
    con.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {source_table}"
    )
    print("Created view contoso_sales_24b pointing to contoso_sales_240k")


def show_tables(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    """Display all tables with their row counts."""
    print(f"\n{'='*60}")
    print(f"📊 DATABASE TABLES in schema '{schema}'")
    print(f"{'='*60}\n")

    # Get all tables and views, excluding system views
    # System views like database_snapshots, storage_info are in MD_INFORMATION_SCHEMA
    tables_query = f"""
    SELECT
        table_name,
        table_type
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
        AND table_name NOT IN (
            'database_snapshots', 'databases', 'owned_shares',
            'query_history', 'shared_with_me', 'storage_info',
            'storage_info_history'
        )
    ORDER BY table_type, table_name
    """

    tables = con.execute(tables_query).fetchall()

    if not tables:
        print("No tables found in the database.")
        return

    print(f"{'Table Name':<30} {'Type':<10} {'Row Count':>15}")
    print("-" * 60)

    total_rows = 0
    for table_name, table_type in tables:
        try:
            # For views, check if they're actually queryable in this schema
            if table_type == "VIEW":
                # Try a simple query first to see if the view is accessible
                test_query = f"SELECT 1 FROM {quote_identifier(schema)}.{quote_identifier(table_name)} LIMIT 1"
                con.execute(test_query).fetchone()

            count_query = f"SELECT COUNT(*) FROM {quote_identifier(schema)}.{quote_identifier(table_name)}"
            row_count = con.execute(count_query).fetchone()[0]
            total_rows += row_count

            # Format large numbers with commas
            formatted_count = f"{row_count:,}"

            # Add emoji based on table type
            emoji = "📋" if table_type == "BASE TABLE" else "👁️" if table_type == "VIEW" else "📄"

            print(f"{emoji} {table_name:<28} {table_type:<10} {formatted_count:>14}")
        except Exception as e:
            # Only show error for non-system views
            error_msg = str(e)
            if "Catalog Error" in error_msg and table_type == "VIEW":
                # Skip system views that can't be accessed from this schema
                continue
            else:
                print(f"⚠️  {table_name:<28} {table_type:<10} {'Error: ' + str(e)[:20]}")

    print("-" * 60)
    print(f"📈 Total rows across all tables: {total_rows:,}\n")


def show_storage(con: duckdb.DuckDBPyConnection) -> None:
    """Display storage usage information for all databases."""
    print(f"\n{'='*80}")
    print(f"💾 STORAGE USAGE INFORMATION")
    print(f"{'='*80}\n")

    try:
        # Check if MD_INFORMATION_SCHEMA.STORAGE_INFO exists
        storage_query = """
        SELECT
            database_name,
            active_bytes,
            kept_for_cloned_bytes,
            failsafe_bytes,
            (COALESCE(active_bytes, 0) + COALESCE(failsafe_bytes, 0) + COALESCE(kept_for_cloned_bytes, 0)) as total_bytes,
            active_bytes / (1024.0 * 1024.0 * 1024.0) as active_gb,
            kept_for_cloned_bytes / (1024.0 * 1024.0 * 1024.0) as cloned_gb,
            failsafe_bytes / (1024.0 * 1024.0 * 1024.0) as failsafe_gb,
            (COALESCE(active_bytes, 0) + COALESCE(failsafe_bytes, 0) + COALESCE(kept_for_cloned_bytes, 0)) / (1024.0 * 1024.0 * 1024.0) as total_gb
        FROM MD_INFORMATION_SCHEMA.STORAGE_INFO
        ORDER BY total_bytes DESC
        """

        storage_data = con.execute(storage_query).fetchall()

        if not storage_data:
            print("No storage information available.")
            return

        # Print header
        print(f"{'Database':<25} {'Active':<12} {'Cloned':<12} {'Failsafe':<12} {'Total':<12}")
        print(f"{'Name':<25} {'(GB)':<12} {'(GB)':<12} {'(GB)':<12} {'(GB)':<12}")
        print("-" * 73)

        total_active = 0
        total_cloned = 0
        total_failsafe = 0
        total_total = 0

        # Print each database
        for row in storage_data:
            db_name = row[0] or "(unknown)"
            active_gb = row[5] or 0
            cloned_gb = row[6] or 0
            failsafe_gb = row[7] or 0
            total_gb = row[8] or 0

            # Accumulate totals
            total_active += active_gb
            total_cloned += cloned_gb
            total_failsafe += failsafe_gb
            total_total += total_gb

            # Truncate long database names
            if len(db_name) > 24:
                db_name = db_name[:21] + "..."

            print(f"{db_name:<25} {active_gb:>11.3f} {cloned_gb:>11.3f} {failsafe_gb:>11.3f} {total_gb:>11.3f}")

        # Print totals
        print("-" * 73)
        print(f"{'TOTAL':<25} {total_active:>11.3f} {total_cloned:>11.3f} {total_failsafe:>11.3f} {total_total:>11.3f}")

        # Storage lifecycle explanation
        print(f"\n📊 Storage Categories:")
        print(f"  • Active: Currently referenced data accessible by queries")
        print(f"  • Cloned: Data kept for cloned databases or shares")
        print(f"  • Failsafe: System backups retained for recovery (7 days)")

        # Cost estimation
        gb_days = total_total * 30  # Approximate monthly GB-days
        monthly_cost = gb_days * 0.0025685  # $0.0025685 per GB-day
        print(f"\n💵 Estimated Monthly Storage Cost:")
        print(f"  • Total Storage: {total_total:.3f} GB")
        print(f"  • Estimated GB-days (30-day month): {gb_days:.1f}")
        print(f"  • Estimated Cost: ${monthly_cost:.2f}")

        # Try to get historical data if available
        print(f"\n📈 Recent Storage History (last 7 days):")
        try:
            history_query = """
            SELECT
                DATE(result_ts) as date,
                SUM(total_bytes) / (1024.0 * 1024.0 * 1024.0) as total_gb
            FROM MD_INFORMATION_SCHEMA.STORAGE_INFO_HISTORY
            WHERE result_ts >= CURRENT_DATE - INTERVAL 7 DAY
            GROUP BY DATE(result_ts)
            ORDER BY date DESC
            LIMIT 7
            """

            history_data = con.execute(history_query).fetchall()

            if history_data:
                print(f"  {'Date':<12} {'Total (GB)':>12}")
                print(f"  {'-'*24}")
                for date, gb in history_data:
                    print(f"  {str(date):<12} {gb:>11.3f}")
            else:
                print("  No historical data available.")

        except Exception:
            print("  Historical data not accessible (may require admin privileges).")

    except duckdb.CatalogException as e:
        if "Table with name STORAGE_INFO does not exist" in str(e):
            print("⚠️  Storage information is not available.")
            print("    This feature requires:")
            print("    1. Organization admin privileges")
            print("    2. MotherDuck Business plan or higher")
            print("    3. Connection to MotherDuck (not local DuckDB)")
        else:
            print(f"❌ Error accessing storage information: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def scale_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    multiplier: int,
    use_union: bool = False
) -> None:
    """Scale contoso_sales_240k table by creating a larger table."""
    print(f"\n{'='*60}")
    print(f"🚀 SCALING TABLE")
    print(f"{'='*60}\n")

    source_table = f"{quote_identifier(schema)}.{quote_identifier('contoso_sales_240k')}"
    target_table = f"{quote_identifier(schema)}.{quote_identifier('contoso_sales_24b_scaled')}"

    # Check current size
    current_count = con.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
    expected_count = current_count * multiplier

    print(f"📊 Current table size: {current_count:,} rows")
    print(f"🎯 Target table size: {expected_count:,} rows")
    print(f"📈 Multiplication factor: {multiplier:,}x\n")

    # Estimate size
    size_mb = (expected_count * 100) / (1024 * 1024)  # Rough estimate: 100 bytes per row
    print(f"⚠️  Estimated table size: ~{size_mb:,.0f} MB")

    if expected_count > 1_000_000_000:
        print(f"\n⚠️  WARNING: Creating {expected_count:,} rows will take significant time and resources!")
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Scaling cancelled.")
            return

    print(f"\n⏱️  Creating scaled table {target_table}...")
    print(f"📋 Strategy: {'UNION ALL' if use_union else 'CROSS JOIN'}")
    print("This may take several minutes for large multipliers...\n")

    if use_union:
        # Memory-efficient UNION ALL approach
        print("Using memory-efficient UNION ALL strategy...")

        # Build UNION ALL query
        union_parts = [f"SELECT * FROM {source_table}"]
        for i in range(1, multiplier):
            union_parts.append(f"UNION ALL SELECT * FROM {source_table}")

        scaling_query = f"""
        CREATE OR REPLACE TABLE {target_table} AS
        {' '.join(union_parts)}
        """
    else:
        # Original CROSS JOIN approach
        scaling_query = f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT
            original.*,
            replicate_id
        FROM {source_table} AS original
        CROSS JOIN (
            SELECT generate_series AS replicate_id
            FROM generate_series(1, {multiplier})
        ) AS replicator
        ORDER BY order_date, store_id, product_id, replicate_id
        """

    start_time = time.perf_counter()
    try:
        con.execute(scaling_query)
        elapsed = time.perf_counter() - start_time

        # Verify the result
        actual_count = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]

        print(f"✅ Table created successfully!")
        print(f"📊 Final row count: {actual_count:,}")
        print(f"⏱️  Time taken: {elapsed:.2f} seconds\n")

        # Update the view to point to the new scaled table
        view_name = f"{quote_identifier(schema)}.{quote_identifier('contoso_sales_24b')}"
        con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {target_table}")
        print(f"✅ Updated view 'contoso_sales_24b' to point to the scaled table.")

    except Exception as e:
        print(f"❌ Error creating scaled table: {e}")
        elapsed = time.perf_counter() - start_time
        print(f"⏱️  Failed after {elapsed:.2f} seconds")


def get_resource_metrics(con: duckdb.DuckDBPyConnection) -> Dict[str, any]:
    """Get current resource usage metrics."""
    metrics = {}

    try:
        # Memory usage
        memory_result = con.execute("SELECT current_memory(), peak_memory()").fetchone()
        if memory_result:
            metrics['current_memory_mb'] = memory_result[0] / (1024 * 1024) if memory_result[0] else 0
            metrics['peak_memory_mb'] = memory_result[1] / (1024 * 1024) if memory_result[1] else 0
    except:
        metrics['current_memory_mb'] = 0
        metrics['peak_memory_mb'] = 0

    try:
        # Temporary files (indicates spilling to disk)
        temp_files = con.execute("SELECT COUNT(*) as count, COALESCE(SUM(size), 0) as total_size FROM duckdb_temporary_files()").fetchone()
        if temp_files:
            metrics['temp_files_count'] = temp_files[0]
            metrics['temp_files_mb'] = temp_files[1] / (1024 * 1024) if temp_files[1] else 0
    except:
        metrics['temp_files_count'] = 0
        metrics['temp_files_mb'] = 0

    try:
        # Database and buffer pool info
        db_info = con.execute("SELECT * FROM duckdb_databases() WHERE database_name = current_database()").fetchone()
        if db_info:
            metrics['database_size_mb'] = db_info[2] / (1024 * 1024) if db_info[2] else 0
    except:
        metrics['database_size_mb'] = 0

    return metrics


def parse_profiling_output(con: duckdb.DuckDBPyConnection) -> Dict[str, any]:
    """Parse the profiling output from DuckDB."""
    profile_data = {}

    try:
        # Get the last query profiling information
        profiling_result = con.execute("PRAGMA show_last_query_profiling").fetchall()

        if profiling_result:
            # Parse the profiling tree
            for row in profiling_result:
                if len(row) >= 2:
                    # Extract timing information
                    if "Total Time" in str(row):
                        profile_data['total_time'] = str(row)
                    elif "Cardinality" in str(row):
                        profile_data['rows_processed'] = str(row)

        # Try to get more detailed metrics
        try:
            json_profile = con.execute("PRAGMA profiling_output").fetchone()
            if json_profile:
                profile_data['detailed_profile'] = json_profile[0]
        except:
            pass

    except Exception as e:
        profile_data['error'] = str(e)

    return profile_data


def extract_labeled_statements(sql_text: str) -> List[Tuple[str, str]]:
    statements: List[Tuple[str, str]] = []
    buffer: List[str] = []
    current_label: str | None = None

    for raw_line in sql_text.splitlines():
        stripped = raw_line.strip()
        upper = stripped.upper()
        if not stripped:
            continue
        if stripped.startswith("--"):
            if stripped.lower().startswith("--query"):
                current_label = stripped.lstrip("-").strip().title()
            continue
        if upper.startswith("ALTER SESSION"):
            continue
        buffer.append(raw_line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer)
            if "alter session" in statement.lower():
                buffer.clear()
                current_label = None
                continue
            label = current_label or f"Statement {len(statements) + 1:02d}"
            statements.append((label, statement))
            buffer.clear()
            current_label = None

    return statements


def filter_statements(
    statements: List[Tuple[str, str]],
    query_numbers: List[str] | None,
) -> List[Tuple[str, str]]:
    """Filter statements based on query numbers."""
    if query_numbers is None:
        return statements

    filtered = []
    for label, statement in statements:
        # Extract query number from label like "Query 01"
        if "Query" in label:
            query_num = label.split()[1] if len(label.split()) > 1 else ""
            if query_num in query_numbers:
                filtered.append((label, statement))

    return filtered


def run_queries(
    con: duckdb.DuckDBPyConnection,
    statements: Iterable[Tuple[str, str]],
    preview_rows: int,
    explain: bool = False,
    verbose: bool = False,
    profile: bool = False,
) -> List[Tuple[str, float, int | None, Dict[str, any]]]:
    results: List[Tuple[str, float, int | None, Dict[str, any]]] = []

    # Enable profiling if requested
    if profile:
        con.execute("PRAGMA enable_profiling")
        con.execute("PRAGMA profiling_mode = 'detailed'")
        print("\n📊 Resource profiling enabled")

    for label, statement in statements:
        print(f"\n{'='*60}")
        print(f"🔍 {label}")
        print(f"{'='*60}")

        # Show query text if verbose
        if verbose:
            print("\n📝 Query:")
            # Truncate very long queries for display
            lines = statement.strip().split('\n')
            max_lines = 20
            if len(lines) > max_lines:
                for line in lines[:max_lines]:
                    print(f"  {line}")
                print(f"  ... ({len(lines) - max_lines} more lines)")
            else:
                for line in lines:
                    print(f"  {line}")

        # Get resource metrics before query (if profiling)
        metrics_before = get_resource_metrics(con) if profile else {}

        # Determine which query to run
        if explain:
            # Use EXPLAIN ANALYZE which runs the query and provides plan + timing
            print("\n📊 Running with EXPLAIN ANALYZE (executes query once)...")
            query_to_run = f"EXPLAIN ANALYZE {statement}"
            is_explain = True
        else:
            # Run normal query
            print(f"\n⏱️  Executing...")
            query_to_run = statement
            is_explain = False

        # Execute the query (either normal or EXPLAIN ANALYZE)
        start = time.perf_counter()
        cursor = con.execute(query_to_run)
        rowcount: int | None = None

        # Handle EXPLAIN ANALYZE output
        if is_explain:
            explain_result = cursor.fetchall()
            elapsed = time.perf_counter() - start

            print("\n📊 Query Plan with Execution Statistics:")
            # Parse the explain output to find actual execution time
            actual_query_time = elapsed  # Default to measured time

            for row in explain_result:
                if isinstance(row, tuple) and len(row) >= 2:
                    value = row[1]
                    if value:
                        # Look for total time in the output
                        if "Total Time:" in value:
                            import re
                            time_match = re.search(r'Total Time:\s*([\d.]+)s', value)
                            if time_match:
                                actual_query_time = float(time_match.group(1))

                        # Print the plan
                        for line in value.split('\n'):
                            print(f"  {line}")

            # Update elapsed with actual query time from EXPLAIN ANALYZE
            elapsed = actual_query_time

            # For EXPLAIN ANALYZE, we can't get row preview
            rowcount = None
        else:
            # Normal query execution - handle results
            if cursor.description:
                if preview_rows > 0:
                    rows = cursor.fetchmany(preview_rows)
                    rowcount = len(rows)
                    print(f"\n📋 Preview (first {rowcount} rows):")
                    # Show column headers
                    headers = [desc[0] for desc in cursor.description]
                    print(f"  {' | '.join(headers[:5])}{'...' if len(headers) > 5 else ''}")
                    print(f"  {'-' * 50}")
                    # Show first few rows
                    for i, row in enumerate(rows[:3]):
                        row_str = ' | '.join(str(val)[:20] for val in row[:5])
                        print(f"  {row_str}{'...' if len(row) > 5 else ''}")
                    if rowcount > 3:
                        print(f"  ... ({rowcount - 3} more rows)")
                else:
                    cursor.fetchone()

            # Calculate elapsed time for normal query
            elapsed = time.perf_counter() - start

        # Collect resource metrics if profiling
        resource_data = {}
        if profile:
            metrics_after = get_resource_metrics(con)

            # Calculate deltas
            resource_data = {
                'memory_used_mb': metrics_after['current_memory_mb'] - metrics_before.get('current_memory_mb', 0),
                'peak_memory_mb': metrics_after['peak_memory_mb'],
                'temp_files_count': metrics_after['temp_files_count'],
                'temp_files_mb': metrics_after['temp_files_mb'],
                'spilled_to_disk': metrics_after['temp_files_count'] > 0
            }

            # Get profiling details
            profile_data = parse_profiling_output(con)
            resource_data['profile_details'] = profile_data

            # Display resource usage
            print(f"\n📊 Resource Usage:")
            print(f"  • Memory used: {resource_data['memory_used_mb']:.2f} MB")
            print(f"  • Peak memory: {resource_data['peak_memory_mb']:.2f} MB")

            if resource_data['spilled_to_disk']:
                print(f"  • ⚠️  Spilled to disk: {resource_data['temp_files_count']} files ({resource_data['temp_files_mb']:.2f} MB)")
            else:
                print(f"  • ✅ No disk spilling")

            # If we already ran EXPLAIN ANALYZE, extract rows scanned from there
            if is_explain and 'explain_result' in locals():
                total_rows_scanned = 0
                for row in explain_result:
                    if isinstance(row, tuple) and len(row) >= 2:
                        value = str(row[1])
                        # Look for rows information in the plan
                        if "Rows" in value:
                            import re
                            # Look for patterns like "24000000000 Rows"
                            matches = re.findall(r'(\d+)\s+Rows', value)
                            if matches:
                                # Get the largest row count (usually the table scan)
                                for match in matches:
                                    total_rows_scanned = max(total_rows_scanned, int(match))

                if total_rows_scanned > 0:
                    resource_data['rows_scanned'] = total_rows_scanned
                    print(f"  • Rows scanned: {total_rows_scanned:,}")

            elif not is_explain:
                # Only run a separate EXPLAIN if we haven't already
                try:
                    # Use simple EXPLAIN (not ANALYZE) to get estimated rows
                    explain_stmt = f"EXPLAIN {statement}"
                    explain_result = con.execute(explain_stmt).fetchall()
                    total_rows_scanned = 0
                    for row in explain_result:
                        if isinstance(row, tuple) and len(row) >= 2:
                            value = str(row[1])
                            if "rows" in value.lower():
                                import re
                                matches = re.findall(r'rows=(\d+)', value.lower())
                                if matches:
                                    total_rows_scanned = max(total_rows_scanned, int(matches[0]))

                    if total_rows_scanned > 0:
                        resource_data['rows_scanned'] = total_rows_scanned
                        print(f"  • Estimated rows to scan: {total_rows_scanned:,}")

                        # Calculate efficiency if we have rowcount
                        if rowcount and rowcount > 0:
                            efficiency = (rowcount / total_rows_scanned) * 100 if total_rows_scanned > 0 else 0
                            print(f"  • Scan efficiency: {efficiency:.2f}% (returned {rowcount:,} of {total_rows_scanned:,} scanned)")
                except:
                    pass

        results.append((label, elapsed, rowcount, resource_data))
        print(f"\n✅ Completed in {elapsed:.3f} seconds")

    return results


def main() -> None:
    args = parse_args()
    ensure_environment(args.env_file)

    if args.threads < 1:
        raise SystemExit("--threads must be a positive integer")
    if args.max_memory_mb < 1:
        raise SystemExit("--max-memory-mb must be a positive integer")

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        raise SystemExit(
            "MotherDuck token not found. Set MOTHERDUCK_TOKEN in the environment or .env file."
        )

    temp_directory = args.temp_directory
    extension_directory = args.extension_directory or temp_directory / "extensions"
    for path in (temp_directory, extension_directory):
        path.mkdir(parents=True, exist_ok=True)

    con = connect_to_motherduck(
        args.database,
        token,
        threads=args.threads,
        max_memory_mb=args.max_memory_mb,
        temp_directory=temp_directory,
        extension_directory=extension_directory,
    )
    schema = ensure_schema(con, args.schema)

    # Initialize database if requested
    if args.init_db:
        print("\nInitializing database and loading data...")
        load_parquet_tables(con, schema, TABLE_FILES)
        print("Database initialization complete.\n")

    # Show tables if requested
    if args.show_tables:
        show_tables(con, schema)

    # Scale table if requested
    if args.scale_table:
        scale_table(con, schema, args.scale_table, args.use_union)

    # Show storage if requested
    if args.show_storage:
        show_storage(con)

    # Run queries if requested
    if args.query_all or args.query:
        query_text = args.query_file.read_text()
        statements = extract_labeled_statements(query_text)
        if not statements:
            raise SystemExit(f"No benchmark statements found in {args.query_file}")

        # Filter queries if specific ones requested
        if args.query:
            statements = filter_statements(statements, args.query)
            if not statements:
                raise SystemExit(f"No queries found matching: {', '.join(args.query)}")

        results = run_queries(con, statements, args.preview_rows, args.explain, args.verbose, args.profile)

        print(f"\n{'='*60}")
        print("📈 BENCHMARK SUMMARY")
        print(f"{'='*60}")
        total_time = sum(elapsed for _, elapsed, _, _ in results)
        print(f"\n⏱️  Total execution time: {total_time:.3f} seconds")
        print(f"📊 Queries executed: {len(results)}")

        if len(results) > 0:
            avg_time = total_time / len(results)
            min_time = min(elapsed for _, elapsed, _, _ in results)
            max_time = max(elapsed for _, elapsed, _, _ in results)

            print(f"\n📉 Statistics:")
            print(f"  • Average: {avg_time:.3f}s")
            print(f"  • Fastest: {min_time:.3f}s")
            print(f"  • Slowest: {max_time:.3f}s")

            # Resource statistics if profiling was enabled
            if args.profile:
                print(f"\n💾 Resource Statistics:")

                # Memory statistics
                peak_memories = [r[3]['peak_memory_mb'] for _, _, _, r in results if r]
                if peak_memories:
                    print(f"  • Peak memory usage: {max(peak_memories):.2f} MB")
                    print(f"  • Average memory: {sum(peak_memories) / len(peak_memories):.2f} MB")

                # Disk spilling statistics
                spilled_queries = [label for label, _, _, r in results if r and r.get('spilled_to_disk')]
                if spilled_queries:
                    print(f"  • ⚠️  Queries that spilled to disk: {', '.join(spilled_queries)}")
                    total_spill = sum(r[3]['temp_files_mb'] for _, _, _, r in results if r and r.get('spilled_to_disk'))
                    print(f"  • Total disk spill: {total_spill:.2f} MB")
                else:
                    print(f"  • ✅ No queries spilled to disk")

                # Scan efficiency
                efficiencies = []
                for label, _, rowcount, resource_data in results:
                    if resource_data and 'rows_scanned' in resource_data and rowcount:
                        efficiency = (rowcount / resource_data['rows_scanned']) * 100 if resource_data['rows_scanned'] > 0 else 0
                        efficiencies.append((label, efficiency))

                if efficiencies:
                    print(f"\n📊 Scan Efficiency (returned/scanned):")
                    for label, eff in sorted(efficiencies, key=lambda x: x[1], reverse=True)[:5]:
                        print(f"  • {label}: {eff:.2f}%")

        print(f"\n📋 Individual Results:")
        for label, elapsed, rowcount, resource_data in results:
            suffix = f" (previewed {rowcount} rows)" if rowcount is not None else ""
            # Add performance indicator
            if len(results) > 1:
                if elapsed == min(e for _, e, _, _ in results):
                    perf = " 🚀"
                elif elapsed == max(e for _, e, _, _ in results):
                    perf = " 🐌"
                else:
                    perf = ""
            else:
                perf = ""

            # Add resource info if profiling
            resource_suffix = ""
            if args.profile and resource_data:
                mem = resource_data.get('peak_memory_mb', 0)
                spilled = "💾" if resource_data.get('spilled_to_disk') else ""
                resource_suffix = f" [{mem:.0f}MB {spilled}]"

            print(f"  • {label}: {elapsed:.3f}s{suffix}{perf}{resource_suffix}")


if __name__ == "__main__":
    main()
