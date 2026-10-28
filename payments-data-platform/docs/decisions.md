# Architecture Decision Records

## ADR-001: Medallion architecture (bronze / silver / gold)
**Decision:** Separate raw, cleaned and business-ready layers.
**Why:** Raw data stays replayable, debugging is easier, and each layer has a single responsibility.

## ADR-002: Parquet for silver and gold
**Decision:** Raw files stay CSV/JSON. Processed layers use Parquet.
**Why:** It's columnar and compressed, it keeps the schema, and Spark reads it efficiently.

## ADR-003: Partition by ingest_date
**Decision:** All S3 datasets are partitioned by `ingest_date`.
**Why:** It enables incremental processing, idempotent overwrites and cheap backfills.

## ADR-004: Idempotent overwrite per partition
**Decision:** Each run overwrites only its own date partition.
**Why:** Re-runs and backfills are safe and never create duplicates.

## ADR-005: dbt for warehouse modelling
**Decision:** dbt handles SQL transformations inside PostgreSQL.
**Why:** It provides version-controlled SQL, built-in tests (unique, not_null, relationships) and lineage docs.

## ADR-006: Quarantine instead of drop
**Decision:** Invalid rows are stored with a reason instead of being discarded.
**Why:** It gives an audit trail and makes it possible to fix and reprocess the rows.

## ADR-007: Synthetic data
**Decision:** Data is generated with Faker rather than taken from real payment data.
**Why:** There are no PII or GDPR concerns, and errors can be injected in a controlled way.
