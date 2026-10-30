# Data Quality

Checks run inside the silver job, after cleaning and deduplication and before anything is written to silver.

## How it works

Each entity has a list of rules. A rule is a name plus a Spark boolean expression that is true when the row is valid. All rules are evaluated on every row and the names of the ones that failed are collected into a `dq_failures` array, so a single bad row can report several problems at once.

- Rows with an empty `dq_failures` go on to silver.
- Rows with failures are written to `quarantine/<entity>/ingest_date=.../` with the failure list and a `quarantined_at` timestamp.
- A JSON summary is written to `reports/data_quality/ingest_date=YYYY-MM-DD.json`.

Quarantine is always written, even when the gate fails, so there is something to investigate.

## Rules

### Transactions

| Rule | Condition |
|---|---|
| transaction_id_not_null | id present |
| merchant_id_not_null | merchant present |
| merchant_known | merchant exists in the same day's valid merchant snapshot |
| amount_positive | amount parsed and greater than zero |
| currency_supported | GBP, EUR or USD |
| card_scheme_supported | VISA, MASTERCARD or AMEX |
| auth_status_valid | APPROVED or DECLINED |
| auth_timestamp_valid | timestamp parsed |

### Settlements

| Rule | Condition |
|---|---|
| settlement_id_not_null | id present |
| transaction_id_not_null | reference present |
| settled_amount_positive | amount parsed and greater than zero |
| currency_supported | GBP, EUR or USD |
| card_scheme_supported | VISA, MASTERCARD or AMEX |
| settlement_date_valid | date parsed |

### Merchants

| Rule | Condition |
|---|---|
| merchant_id_not_null | id present |
| merchant_name_not_null | name present |
| mcc_format | four digits |
| country_format | two upper case letters |
| currency_supported | GBP, EUR or USD |

## Gate

After evaluation the job fails the run if, for any entity:

- the reject ratio is above `MAX_REJECT_RATIO` (5% by default), or
- the feed is empty.

The one exception is the settlement feed on `PIPELINE_START_DATE`, which is legitimately empty because nothing has been authorised yet.

When the gate fails nothing is written to silver for that day, the Airflow task fails, and downstream tasks do not run. Fix the source, rerun the day, and the partition is replaced.

## Deduplication

Before the rules run, each entity is deduplicated on its business key (`transaction_id`, `settlement_id`, `merchant_id`). Rows with a null key are kept so the rules can quarantine them rather than letting them silently disappear.

Two settlements with different `settlement_id` values for the same transaction are not duplicates at this stage. They are real business records and reconciliation flags them as `DUPLICATE`.

## dbt tests

dbt adds a second layer on the warehouse side:

- `unique` and `not_null` on every key
- `accepted_values` on statuses
- `relationships` from facts to every dimension
- `assert_matched_amounts_agree`: a `MATCHED` or `LATE` row must have exactly one settlement for the authorised amount
- `assert_every_approval_reconciled`: every approved authorisation has a reconciliation row
