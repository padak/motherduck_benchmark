# MotherDuck Benchmark

A performance benchmark utility for MotherDuck/DuckDB, replicating the [Snowflake vs. Databricks performance test](https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks) using a 24 billion row dataset.

## Purpose

This project adapts the Snowflake/Databricks benchmark queries to run on MotherDuck, allowing performance comparison across these platforms. It provides tools to:
- Load Contoso sample data into MotherDuck
- Scale datasets from 240K rows up to 24B rows
- Execute 16 benchmark queries with timing and analysis
- Compare query performance with other data warehouse solutions

## Prerequisites

- Python 3.8+
- MotherDuck account and token (get one at [motherduck.com](https://motherduck.com))
- DuckDB version <1.4.0 (due to MotherDuck compatibility)
- Sample data from the original benchmark repository

## Setup

### 1. Clone the repositories

```bash
# Clone this repository
git clone https://github.com/yourusername/motherduck_benchmark.git
cd motherduck_benchmark

# Clone the original benchmark repository for sample data
git clone https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks.git
```

### 2. Set up Python environment

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure MotherDuck authentication

```bash
# Copy the environment template
cp .env.dist .env

# Edit .env and add your MotherDuck token
# Get your token from: https://motherduck.com/
```

### 4. Initialize database with sample data

```bash
# Load the Contoso sample data (240K rows)
python motherduck_benchmark.py --init-db

# View loaded tables
python motherduck_benchmark.py --show-tables
```

## Usage

### Running benchmark queries

```bash
# Run all 16 benchmark queries
python motherduck_benchmark.py --query-all

# Run specific queries
python motherduck_benchmark.py --query 01 05 10

# Run with query plan analysis
python motherduck_benchmark.py --query 01 --explain --verbose

# Preview query results
python motherduck_benchmark.py --query 01 --preview-rows 5
```

### Scaling data to 24B rows

The benchmark is designed to test performance at scale. You can progressively scale the data:

```bash
# Scale to 24M rows (100x)
python motherduck_benchmark.py --scale-table 100

# Scale to 240M rows (1000x)
python motherduck_benchmark.py --scale-table 1000

# Scale to 24B rows (100000x) - matches Snowflake/Databricks benchmark
python motherduck_benchmark.py --scale-table 100000

# Or use the utility script to scale an already-scaled table
python scripts/scale_further.py 10  # Multiplies current table by 10x
```

⚠️ **Warning**: Scaling to 24B rows requires significant time and resources. The script will prompt for confirmation when creating tables larger than 1B rows.

### Command-line options

```bash
python motherduck_benchmark.py --help
```

Key options:
- `--init-db`: Initialize database and load sample data
- `--show-tables`: Display all tables with row counts
- `--scale-table MULTIPLIER`: Scale the contoso_sales_240k table
- `--query-all`: Run all benchmark queries
- `--query N [N ...]`: Run specific query numbers
- `--explain`: Show query execution plans
- `--verbose`: Display query text before execution
- `--preview-rows N`: Fetch and display N result rows

## Benchmark Queries

The benchmark includes 16 queries testing various SQL operations:
- Window functions and rolling aggregates
- Complex joins across multiple tables
- Temporal analysis (monthly, quarterly, seasonal)
- Ranking and top-N queries
- Year-over-year comparisons
- Large-scale aggregations

Queries are stored in `Performance_Test_Snowflake_Databricks/code/query_list.sql` and have been adapted for DuckDB syntax compatibility.

## Performance Configuration

Adjust DuckDB/MotherDuck settings for optimal performance:

```bash
# Increase thread count (default: 1)
python motherduck_benchmark.py --threads 8 --query-all

# Increase memory allocation (default: 256MB)
python motherduck_benchmark.py --max-memory-mb 8192 --query-all

# Specify temp directory for spill files
python motherduck_benchmark.py --temp-directory /path/to/fast/ssd --query-all
```

## Core Application: motherduck_benchmark.py

The main CLI application provides comprehensive functionality for benchmark operations:

### Key Functions

1. **Connection Management**
   - `connect_to_motherduck()`: Establishes secure connection to MotherDuck with configurable settings
   - Handles authentication via token from environment or .env file
   - Configures DuckDB settings (threads, memory, temp directories)

2. **Data Loading**
   - `load_parquet_tables()`: Imports Contoso sample data from parquet files
   - Creates tables: contoso_stores, contoso_products, contoso_sales_240k
   - Sets up view contoso_sales_24b pointing to the active sales table

3. **Data Scaling**
   - `scale_table()`: Multiplies table rows using CROSS JOIN with generate_series()
   - Creates contoso_sales_24b_scaled with specified multiplication factor
   - Automatically updates the contoso_sales_24b view to point to scaled data
   - Provides progress tracking and confirmation for large operations

4. **Query Execution**
   - `extract_labeled_statements()`: Parses SQL file to extract individual queries
   - `filter_statements()`: Allows selective query execution by number
   - `run_queries()`: Executes queries with timing, optional EXPLAIN ANALYZE
   - Supports result preview and performance statistics

5. **Database Inspection**
   - `show_tables()`: Lists all tables/views with row counts
   - Displays table types (BASE TABLE vs VIEW)
   - Calculates total row count across database

### Configuration Options

The application supports extensive configuration through command-line arguments:
- Database connection settings (database name, schema)
- Performance tuning (threads, memory, temp directories)
- Execution modes (verbose, explain, preview)
- Action selection (init, query, scale, show)

## Project Structure

```
motherduck_benchmark/
├── motherduck_benchmark.py           # Main CLI application
├── scripts/                          # Utility scripts
│   ├── test_motherduck_connection.py # Connection testing
│   ├── scale_further.py              # Additional scaling utility
│   └── test_explain.py               # EXPLAIN output testing
├── Performance_Test_Snowflake_Databricks/  # Original benchmark (git submodule)
│   ├── code/
│   │   └── query_list.sql           # Benchmark queries
│   └── SampleFiles/                 # Parquet sample data
├── requirements.txt                  # Python dependencies
├── .env.dist                        # Environment template
├── .env                             # Your configuration (git-ignored)
└── CLAUDE.md                        # Development documentation
```

## Differences from Snowflake/Databricks

Query adaptations for DuckDB:
- `current_timestamp()` → `current_timestamp` (no parentheses)
- Scaling uses `generate_series()` instead of:
  - Snowflake: `TABLE(GENERATOR(ROWCOUNT => N))`
  - Databricks: `explode(sequence(1, N))`
- `ALTER SESSION` commands are automatically skipped

## Troubleshooting

### MotherDuck connection issues
```bash
# Test connection
python scripts/test_motherduck_connection.py
```

### DuckDB version conflicts
Ensure you're using DuckDB <1.4.0:
```bash
pip show duckdb
# If needed: pip install 'duckdb<1.4.0'
```

### Memory issues with large scales
- Increase `--max-memory-mb` parameter
- Use SSD for `--temp-directory`
- Consider scaling incrementally

## License

This project adapts the benchmark from [NickAkincilar/Performance_Test_Snowflake_Databricks](https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks). Please refer to the original repository for licensing information.