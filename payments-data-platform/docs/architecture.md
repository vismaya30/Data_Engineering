# Architecture

## Overview

This is a daily batch pipeline built on a **medallion architecture** (bronze → silver → gold), orchestrated by Airflow.

## Layers

| Layer | Location | Format | Content |
|---|---|---|---|
| Bronze | `s3://<bucket>/bronze/` | CSV / JSON | Raw files, as received |
| Silver | `s3://<bucket>/silver/` | Parquet | Cleaned, typed, deduplicated |
| Quarantine | `s3://<bucket>/quarantine/` | Parquet | Records that failed DQ checks, with reason |
| Gold | `s3://<bucket>/gold/` | Parquet | Reconciled, business-ready data |
| Warehouse | PostgreSQL (RDS) | Tables | Star schema built by dbt |

## Partitioning

```
s3://<bucket>/<layer>/<dataset>/ingest_date=YYYY-MM-DD/part-*.parquet
```

- Partitioning by `ingest_date` enables incremental processing and cheap backfills.
- Datasets: `transactions`, `settlements`, `merchants`.

## Daily DAG Flow

1. **generate_data**: creates the day's source files (simulating an upstream system)
2. **ingest_bronze**: uploads the raw files to S3
3. **transform_silver**: PySpark casts types, standardises currency and timestamps, and deduplicates on `transaction_id`
4. **quality_gate**: runs the validation rules. Failed rows go to quarantine, and the task fails if the error rate exceeds a threshold
5. **reconcile_gold**: matches authorisations to settlements (see `reconciliation.md`)
6. **load_warehouse**: loads the gold data into PostgreSQL staging tables
7. **dbt_run / dbt_test**: builds and tests the star schema

## Reliability Patterns

| Pattern | Implementation |
|---|---|
| Idempotency | Each run overwrites its own `ingest_date` partition. Warehouse loads use delete-insert / upsert per date |
| Incremental load | The watermark (last processed date) is stored in a control table, and only new partitions are processed |
| Retries | Airflow `retries=3`, `retry_delay=5min` |
| Backfill | `airflow dags backfill` over a date range, which is safe because runs are idempotent |
| Late-arriving data | The reconciliation window looks back T-3 days for settlements |
| Observability | Row counts and DQ results are logged per run to an `audit_log` table |
