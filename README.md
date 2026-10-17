# End-to-End Data Engineering Pipeline

An end-to-end ETL pipeline that extracts product data from the DummyJSON REST API, transforms and validates the records using Python, and loads them into PostgreSQL.

The pipeline is containerized with Docker and orchestrated using Apache Airflow 3.

## Tech Stack

- **Python** — Data extraction, transformation, validation and loading
- **PostgreSQL** — Data storage and ETL monitoring
- **Apache Airflow 3** — Workflow orchestration and task monitoring
- **Docker & Docker Compose** — Containerization and service management
- **Pytest** — Automated data validation tests

## Architecture

DummyJSON API → Python ETL → PostgreSQL

Apache Airflow orchestrates pipeline execution, while the `etl_runs` table records execution status, duration and processing statistics.

## Pipeline Steps

1. **Extract:** Retrieve products from the DummyJSON API.
2. **Transform:** Standardize and structure product attributes.
3. **Validate:** Check data quality, including prices, ratings, required fields and duplicate SKUs.
4. **Load:** Insert validated records into PostgreSQL.
5. **Monitor:** Record execution metrics and failures.

## Verified Execution

A successful test execution processed:

- 194 extracted products
- 194 valid products
- 0 invalid products
- 194 loaded products
- Approximately 2.64 seconds of ETL processing time

These figures represent a specific test execution, not guaranteed performance for every run.

## Current Features

- Dockerized ETL execution
- Airflow DAG with manual triggering
- PostgreSQL-based execution monitoring
- Structured Python logging
- Automated validation tests
- Exception handling

## Planned Improvements

- Separate Airflow tasks for each ETL stage
- Automated scheduling
- Post-load data quality checks
- Enhanced pipeline observability