# Architecture Decision Records

## ADR-001: Medallion layout on S3

**Status:** accepted

Raw files land in bronze untouched, cleaned and validated data lives in silver, and business outputs live in gold. Keeping bronze immutable means any downstream bug can be fixed and replayed without asking the source to resend files.

## ADR-002: Partition overwrite instead of append

**Status:** accepted

Every write replaces whole date partitions using Spark's dynamic partition overwrite. Appending would make reruns produce duplicates and would need a separate cleanup step. The trade-off is that a partition that disappears from the output (for example a date with zero rows) is not deleted automatically. With daily card volumes that does not happen in practice.

## ADR-003: Custom quality checks rather than Great Expectations

**Status:** accepted

The rule set is small and every rule is a single Spark expression. Writing them directly keeps the dependency list short, makes it easy to tag each rejected row with the exact rules it broke, and runs inside the same Spark job without a second pass. Great Expectations would be worth adding once there are many more sources or a need for hosted data docs.

## ADR-004: Quarantine bad rows, fail on bad batches

**Status:** accepted

A handful of malformed records should not stop finance getting their numbers, so bad rows are moved aside and the run continues. A large share of bad rows usually means an upstream format change, and loading the remainder would give misleading totals, so the run fails above a configurable threshold.

## ADR-005: Recompute a trailing window for late data

**Status:** accepted

Settlements arrive up to T+3. Instead of tracking which transactions changed, each run recomputes reconciliation for the last three days. It is simple, idempotent and cheap at daily volumes. The window size is a single setting.

## ADR-006: PostgreSQL as the serving warehouse

**Status:** accepted

The marts are small, the consumers are a dashboard and ad hoc SQL, and RDS PostgreSQL is cheap and familiar. dbt works the same way against Redshift or Snowflake if the data grows.

## ADR-007: Loading through COPY rather than JDBC

**Status:** accepted

The loader collects each partition to the driver and streams it into PostgreSQL with `COPY`. This avoids shipping a JDBC driver jar and lets the delete and insert share one transaction. It relies on daily partitions fitting in driver memory, which holds for this volume. A larger deployment would switch to Spark JDBC writes into a staging table followed by a swap.

## ADR-008: dbt in its own virtual environment inside the Airflow image

**Status:** accepted

dbt and Airflow pin overlapping libraries. Installing dbt into `/opt/dbt` and calling it by path avoids dependency conflicts without needing a second container.
