# Data Quality

Checks run in the silver layer, before reconciliation.

## Rules

| ID | Dataset | Rule | Severity |
|---|---|---|---|
| DQ01 | transactions | `transaction_id` not null | Reject |
| DQ02 | transactions | `transaction_id` unique (after dedup) | Reject |
| DQ03 | transactions | `amount > 0` | Reject |
| DQ04 | transactions | `currency` in allowed list | Reject |
| DQ05 | transactions | `auth_timestamp` not in the future | Reject |
| DQ06 | transactions | `merchant_id` exists in merchants | Reject |
| DQ07 | settlements | `settled_amount > 0` | Reject |
| DQ08 | settlements | `settlement_date >= auth_date` | Warn |
| DQ09 | all | Row count within ±50% of 7-day average | Warn |
| DQ10 | all | Schema matches the expected columns and types | Fail pipeline |

## Handling

- **Reject**: the row is written to `quarantine/` with columns `dq_rule_id` and `dq_reason`.
- **Warn**: the result is logged to `audit_log` and the pipeline continues.
- **Fail pipeline**: the Airflow task fails and downstream tasks are skipped.
- If rejected rows exceed **5%** of a batch, the quality gate task fails.

## Injected Errors (for testing)

The generator deliberately produces about 2–3% bad data (duplicates, negative amounts, unknown currencies, orphan merchant IDs) to show that the checks work.
