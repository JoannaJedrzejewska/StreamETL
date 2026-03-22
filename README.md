# StreamETL

A lightweight Python ETL pipeline for downloading, transforming, and exporting CSV data with built-in error handling and performance monitoring.

## Features

- **File Download** – fetches remote CSV files via HTTP with custom exception handling (`NotFoundError`, `AccessDeniedError`)
- **Extract** – reads CSV rows lazily using a generator (memory-efficient)
- **Transform** – computes per-row sum, mean, and detects missing values (`-` placeholders)
- **Load** – writes two output CSV files: aggregated values and missing-data index report
- **Timer decorator** – measures execution time of the `load_data` method

