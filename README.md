<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/DuckDB_Logo-horizontal.svg">
    <source media="(prefers-color-scheme: dark)" srcset="logo/DuckDB_Logo-horizontal-dark-mode.svg">
    <img alt="DuckDB logo" src="logo/DuckDB_Logo-horizontal.svg" height="100">
  </picture>
</div>
<br>

<p align="center">
  <a href="https://github.com/duckdb/duckdb/actions"><img src="https://github.com/duckdb/duckdb/actions/workflows/Main.yml/badge.svg?branch=main" alt="Github Actions Badge"></a>
  <a href="https://discord.gg/tcvwpjfnZx"><img src="https://shields.io/discord/909674491309850675" alt="discord" /></a>
  <a href="https://github.com/duckdb/duckdb/releases/"><img src="https://img.shields.io/github/v/release/duckdb/duckdb?color=brightgreen&display_name=tag&logo=duckdb&logoColor=white" alt="Latest Release"></a>
</p>

# NUMA-Join: NUMA-Aware Join Optimizations for DuckDB

This repository contains a modified version of DuckDB with NUMA (Non-Uniform Memory Access) aware optimizations for join operations on multi-socket systems. The project focuses on optimizing database query performance in NUMA architectures by reducing cross-socket data transfers and improving memory locality.

## Benchmarking Suite

The `job-bench/` directory contains a comprehensive benchmark framework using the Join Order Benchmark (JOB) dataset to evaluate and compare different optimization strategies. It includes tools for:

- Profiling query execution plans
- Analyzing NUMA transfer patterns
- Learning optimal breaker placement
- Performance comparison across variants (baseline, no-numa(baseline+breaker), no-steal, steal)

## Getting Started

Before compiling the DuckDB, you need to adjust some constants hardcoded in the repository, which are,

1. `src/execution/operator/helper/physical_pipeline_breaker.cpp`: `96` should be replaced to number of cores on your machine.
2. `src/parallel/task_scheduler.cpp`: Make sure `pthread_setaffinity_np` bind to the correct NUMA node. The NUMA node of the cpu core bound to the thread should be the `cpu_id%2` in function `ThreadExecuteTasks`. Note that the system will use `cpu_id%2` as the NUMA node id.
3. `src/include/duckdb/storage/storage_info.hpp`: Set `STANDARD_ROW_GROUPS_SIZE` to the row group size.

Please check all branches to see the baselines we used. Just compile them as DuckDB requested.

We use some hacky global variable to set parameters, please refer to `src/include/duckdb/common/numa_config.hpp` and `job-bench/main.cpp`.

## Testing

You need to download JOB and store them into `.db` file by yourself. After that, you need to replace some `/PATH/TO/` substring under `job-bench` folder to the correct path. You also need to change some path in `job-bench/CMakeLists.txt`. Then directly run `make` under `job-bench`, then run `test.py` to get the results.

## DuckDB

DuckDB is a high-performance analytical database system. It is designed to be fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs, maps), and [several extensions designed to make SQL easier to use](https://duckdb.org/docs/guides/sql_features/friendly_sql).

DuckDB is available as a [standalone CLI application](https://duckdb.org/docs/api/cli/overview) and has clients for [Python](https://duckdb.org/docs/api/python/overview), [R](https://duckdb.org/docs/api/r), [Java](https://duckdb.org/docs/api/java), [Wasm](https://duckdb.org/docs/api/wasm/overview), etc., with deep integrations with packages such as [pandas](https://duckdb.org/docs/guides/python/sql_on_pandas) and [dplyr](https://duckdblabs.github.io/duckplyr/).

For more information on using DuckDB, please refer to the [DuckDB documentation](https://duckdb.org/docs/).

## Installation

If you want to install DuckDB, please see [our installation page](https://duckdb.org/docs/installation) for instructions.

## Data Import

For CSV files and Parquet files, data import is as simple as referencing the file in the FROM clause:

```sql
SELECT * FROM 'myfile.csv';
SELECT * FROM 'myfile.parquet';
```

Refer to our [Data Import](https://duckdb.org/docs/data/overview) section for more information.

## SQL Reference

The documentation contains a [SQL introduction and reference](https://duckdb.org/docs/sql/introduction).

## Development

For development, DuckDB requires [CMake](https://cmake.org), Python3 and a `C++11` compliant compiler. Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized debug version. You should run `make unit` and `make allunit` to verify that your version works properly after making changes. To test performance, you can run `BUILD_BENCHMARK=1 BUILD_TPCH=1 make` and then perform several standard benchmarks from the root directory by executing `./build/release/benchmark/benchmark_runner`. The details of benchmarks are in our [Benchmark Guide](benchmark/README.md).

Please also refer to our [Build Guide](https://duckdb.org/dev/building) and [Contribution Guide](CONTRIBUTING.md).

## Support

See the [Support Options](https://duckdblabs.com/support/) page.
