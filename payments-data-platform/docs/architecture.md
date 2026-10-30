# Architecture

## Layers

| Layer | Location | Format | Partitioned by | Written by |
|---|---|---|---|---|
| Bronze | `bronze/<entity>/ingest_date=YYYY-MM-DD/` | JSON lines | ingest date | `src/generator/generate.py` |
| Quarantine | `quarantine/<entity>/` | Parquet | `ingest_date` | `src/transforms/silver.py` |
| Silver | `silver/transactions`, `silver/settlements`, `silver/merchants` | Parquet | `auth_date`, `settlement_date`, none | `src/transforms/silver.py` |
| Gold | `gold/reconciliation` | Parquet | `business_date` | `src/transforms/reconciliation.py` |
| Warehouse | PostgreSQL `raw` schema | tables | indexed by date | `src/transforms/load_warehouse.py` |
| Marts | PostgreSQL `staging`, `marts`, `reference` schemas | views and tables | n/a | dbt |

`DATA_ROOT` decides where the lake lives. Locally it is `./data`; on AWS it is something like `s3a://my-payments-lake`. The code paths are the same in both cases. When the root is on S3 the Spark session adds `hadoop-aws` and uses the default AWS credential chain.

## Daily flow

The Airflow DAG `payments_daily` runs once per day with `catchup=True`, `max_active_runs=1` and `depends_on_past=True`, so days are always processed in order.

1. **generate_bronze** writes three files for the run date: merchants (a full snapshot), the day's authorisations, and the settlement file that arrived that day. The settlement file contains settlements for authorisations from T-1, T-2 and T-3.
2. **build_silver** reads only that day's bronze partition, trims and casts every field, drops exact duplicates by business key, runs the data quality rules, writes rejects to quarantine and the rest to silver.
3. **reconcile** looks at the last `LATE_WINDOW_DAYS` days of approved authorisations and every settlement that arrived in the same window, and writes a status per transaction to gold.
4. **load_warehouse** copies the affected silver and gold partitions into PostgreSQL inside a single transaction and moves the watermark forward.
5. **dbt_build** runs seeds, staging views, marts and every dbt test.

## Idempotency

Every write replaces a whole partition rather than appending:

- Bronze file names are fixed per day, so a rerun overwrites the same object.
- Spark runs with `spark.sql.sources.partitionOverwriteMode=dynamic`, so `mode("overwrite")` only replaces the partitions present in the output.
- The warehouse loader deletes the date range it is about to insert and does both in one transaction.
- `fact_transactions` is an incremental dbt model using `delete+insert` on `transaction_id`.

The generator is deterministic. Every random choice is seeded from the run date or the transaction id, so regenerating a day gives byte-identical files.

## Incremental loading

The loader keeps a watermark in `raw.load_watermark`. On each run it loads from `min(watermark + 1, run_date - LATE_WINDOW_DAYS)` up to the run date. That covers two cases:

- If runs were missed, it catches up from the last loaded day.
- Even on a normal run it reloads the last few days, because reconciliation statuses for those days can still change as late settlements arrive.

The watermark only moves forward, so rerunning an old day does not rewind it.

## Late-arriving data

Settlements can land up to T+3. Gold is recomputed for the trailing window on every run, so a transaction can move from `PENDING` to `MATCHED`, `LATE` or `UNMATCHED` as more settlement files arrive. Once a business date is older than the window it stops changing.

Reconciliation only considers settlements dated on or before the run date, so rerunning an old day reproduces the result it had at the time.
