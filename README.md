# Simple Emissions ETL

## Overview

- Clean, fully documented and type-checked code.
- Fully dockerized application.
- **Additional Feature:** Apart from covering the requirements of the assignment, it also supports optional filtering on three key fields to the approved data to export a desired portion of it as output.

### Architecture

* Pipeline (ETL) architecture pattern, with the following sequential executions steps:
    1. **Data Ingestion:** Read source files of various formats into the pipeline.
    2. **Data Normalisation:** Standardise formats (e.g., dates, units) and resolve schema mismatches.
    3. **Data Consolidation:** Merge normalised datasets into a single unified dataset.
    4. **Database Initialization:** Configure the database connection pool and validate schema readiness.
    5. **Bulk Data Loading:** Insert the consolidated dataset into the target database.
    6. **Data Validation Workflow:** Review changes (approve/reject) and enforce deletion policies.
    7. **Command-line Filter Configuration**.
    8. **Data Export:** Filter approved data and export the results as a CSV output.
* Control flow management throughout the entire pipeline with exception handling and chaining, and logging.

### Anti-patterns

I used prompts in step 6 because it seemed as the most minimal and straight-forward implementation.

## Usage

Run:

```sh
make start
```

to build the container and run the application within it.

If you want to apply filters for the output CSV file, you can pass filter arguments on any and all of the columns "sector", "region", and "year". To use it this way, run:

```sh
make start ARGS="[-h] [--sector <sector>] [--region <region>] [--year <year>]"
```

E.g.,

```
make start ARGS="--region US-NY --year 2022"
```

## Scripts

Run:

```sh
make code-quality
```

to lint, format, and type-check the entire source code.
