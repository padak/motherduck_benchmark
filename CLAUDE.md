# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a MotherDuck benchmark utility that loads Contoso parquet samples and runs performance benchmark queries. It's designed to test MotherDuck/DuckDB performance with scaled datasets up to 24 billion rows.

## Project Structure

- `motherduck_benchmark.py` - Main CLI application
- `scripts/` - Utility scripts and tools (all helper scripts should be placed here)
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
All utility scripts should be stored in the `scripts/` directory.

```bash
# Test MotherDuck connection
python scripts/test_motherduck_connection.py

# Scale an already-scaled table further
python scripts/scale_further.py 10  # Scales by 10x

# Test EXPLAIN output format
python scripts/test_explain.py
```

## Architecture

### Main Components

1. **motherduck_benchmark.py**: Core CLI application with these key functions:
   - `connect_to_motherduck()`: Establishes MotherDuck connection with configuration
   - `load_parquet_tables()`: Loads parquet files from `Performance_Test_Snowflake_Databricks/SampleFiles/`
   - `scale_table()`: Scales tables using DuckDB's `generate_series()` for CROSS JOIN replication
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
  - DuckDB: `generate_series(1, N)`

### Important Files

- `.env`: Contains `MOTHERDUCK_TOKEN` for authentication
- `Performance_Test_Snowflake_Databricks/SampleFiles/`: Contains parquet source files
- `Performance_Test_Snowflake_Databricks/code/query_list.sql`: Benchmark queries (16 queries)