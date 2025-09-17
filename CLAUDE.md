# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a MotherDuck benchmark utility that loads Contoso parquet samples and runs performance benchmark queries. It's designed to test MotherDuck/DuckDB performance with scaled datasets up to 24 billion rows.

## Project Structure

- `motherduck_benchmark.py` - Main CLI application
- `scripts/` - Utility scripts (4 essential tools after consolidation)
  - `test_motherduck_connection.py` - Connection validation
  - `test_explain.py` - Query plan debugging
  - `optimized_scale_to_24b.py` - Efficient scaling to 24B rows
  - `simple_union_scale.sql` - SQL-only scaling alternative
- `Performance_Test_Snowflake_Databricks/` - Contains query files and sample data
- `.env` - Environment configuration (contains MOTHERDUCK_TOKEN)

## Key Commands

### Setup and Dependencies
```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Core CLI Commands
```bash
# Initialize database and load sample data
python motherduck_benchmark.py --init-db

# Show all tables with row counts
python motherduck_benchmark.py --show-tables

# Scale contoso_sales_240k table (e.g., 100000x for 24B rows)
python motherduck_benchmark.py --scale-table 100000

# Run all benchmark queries
python motherduck_benchmark.py --query-all

# Run specific queries
python motherduck_benchmark.py --query 01 05 10

# Run with verbose output and query plan
python motherduck_benchmark.py --query 01 --verbose --explain

# Preview query results
python motherduck_benchmark.py --query 01 --preview-rows 5
```

### Utility Scripts
All utility scripts are stored in the `scripts/` directory. After consolidation, we maintain 4 essential scripts:

```bash
# Test MotherDuck connection and diagnose version issues
python scripts/test_motherduck_connection.py

# Optimized scaling to 24B rows with temp table reuse
python scripts/optimized_scale_to_24b.py

# Test EXPLAIN output format
python scripts/test_explain.py

# SQL-only scaling (for MotherDuck UI users)
# Execute manually: scripts/simple_union_scale.sql
```

## Architecture

### Main Components

1. **motherduck_benchmark.py**: Core CLI application with these key functions:
   - `connect_to_motherduck()`: Establishes MotherDuck connection with configuration
   - `load_parquet_tables()`: Loads parquet files from `Performance_Test_Snowflake_Databricks/SampleFiles/`
   - `scale_table()`: Scales tables using UNION ALL approach (memory-efficient, avoids CROSS JOIN)
   - `run_queries()`: Executes benchmark queries with timing and optional EXPLAIN ANALYZE
   - `filter_statements()`: Filters specific queries from the full query list

2. **Query Processing**:
   - Queries are stored in `Performance_Test_Snowflake_Databricks/code/query_list.sql`
   - Uses `extract_labeled_statements()` to parse SQL file and identify individual queries
   - Queries are labeled as "Query 01", "Query 02", etc.
   - Already converted from Snowflake/Databricks syntax (e.g., `current_timestamp()` → `current_timestamp`)

3. **Data Model**:
   - Base tables: `contoso_stores` (67 rows), `contoso_products` (2000 rows), `contoso_sales_240k` (240K rows)
   - View: `contoso_sales_24b` - points to either `contoso_sales_240k` or scaled table
   - Scaled table: `contoso_sales_24b_scaled` - created when using `--scale-table`

### Configuration

- **Environment**: Requires `MOTHERDUCK_TOKEN` in `.env` file or environment
- **DuckDB Version**: Must be <1.4.0 (see requirements.txt)
- **Required Python Module**: `pytz` (for timezone operations in queries)
- **Default Settings**:
  - Database: `contoso_benchmark`
  - Schema: `main`
  - Threads: 1
  - Max Memory: 256MB

### Query Compatibility Notes

When converting queries from Snowflake/Databricks to DuckDB:
- `current_timestamp()` → `current_timestamp` (no parentheses)
- `ALTER SESSION` commands are automatically skipped
- DuckDB supports: DATE_TRUNC, MONTH/YEAR functions, window functions, CTEs, CASE statements
- Scaling approach differs:
  - Snowflake: `TABLE(GENERATOR(ROWCOUNT => N))`
  - Databricks: `explode(sequence(1, N))`
  - DuckDB: `generate_series(1, N)` with UNION ALL (not CROSS JOIN)

### Important Files

- `.env`: Contains `MOTHERDUCK_TOKEN` for authentication
- `Performance_Test_Snowflake_Databricks/SampleFiles/`: Contains parquet source files
- `Performance_Test_Snowflake_Databricks/code/query_list.sql`: Benchmark queries (16 queries)
- `scripts/README.md`: Detailed documentation for all utility scripts

## Best Practices

### Scaling Operations
1. **Use `optimized_scale_to_24b.py` for all scaling needs** - It handles:
   - Automatic rounding to nearest billion
   - Reusable 1B row temp table (builds once, uses many times)
   - 15-second cooldowns to prevent timeouts
   - UNION ALL approach (memory-efficient)

2. **Avoid CROSS JOIN for scaling** - Causes memory exhaustion and timeouts

3. **For manual SQL scaling** - Use `simple_union_scale.sql` in MotherDuck UI

### Known Issues
1. **Phantom system views** - Some views in main schema (database_snapshots, storage_info) cause hanging queries. These are excluded in `--show-tables`.
2. **MotherDuck timeouts** - Use scripts with built-in cooldown periods
3. **DuckDB version** - Must use version <1.4.0 for MotherDuck compatibility

### When Creating New Scripts
1. Place all scripts in `scripts/` directory
2. Consider if functionality can be added to existing scripts first
3. Use UNION ALL instead of CROSS JOIN for data multiplication
4. Include proper error handling and cleanup
5. Document in `scripts/README.md` with WHY it was created