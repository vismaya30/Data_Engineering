# Reconciliation

The goal is to give every approved authorisation exactly one status, and to surface any settlement that cannot be tied back to an authorisation.

## Inputs for run date R

| Input | Filter |
|---|---|
| Approved authorisations to classify | `auth_date` between R - 3 and R |
| Settlements | `settlement_date` between R - 3 and R |
| Known authorisations, for orphan detection | `auth_date` between R - 6 and R, any status |

The window size is `LATE_WINDOW_DAYS` (default 3). Settlements are grouped by `transaction_id` and left joined to the approved authorisations.

## Statuses

Rules are applied in this order and the first match wins.

| Status | Rule |
|---|---|
| `UNMATCHED` | no settlement and the authorisation is at least `LATE_WINDOW_DAYS` old |
| `PENDING` | no settlement yet, still inside the window |
| `DUPLICATE` | more than one settlement |
| `AMOUNT_MISMATCH` | one settlement, amount differs from the authorisation |
| `LATE` | one settlement, correct amount, more than `LATE_THRESHOLD_DAYS` (default 2) after authorisation |
| `MATCHED` | one settlement, correct amount, on time |
| `ORPHAN_SETTLEMENT` | a settlement in the window whose `transaction_id` is not among known authorisations |

Declined authorisations are not reconciled. They appear in `fact_transactions` as `NOT_APPLICABLE`.

## Worked example

Run date 2026-10-20, window 3 days, late threshold 2 days.

| Transaction | Authorised | Settlements | Status |
|---|---|---|---|
| A | 10-18, 10.00 | 10-19, 10.00 | MATCHED |
| B | 10-17, 10.00 | 10-20, 10.00 | LATE |
| C | 10-18, 10.00 | 10-19, 9.50 | AMOUNT_MISMATCH |
| D | 10-18, 10.00 | 10-19, 10.00 and 10-19, 10.00 | DUPLICATE |
| E | 10-19, 10.00 | none | PENDING |
| F | 10-17, 10.00 | none | UNMATCHED |
| (none) | | 10-19, 25.00 for unknown id | ORPHAN_SETTLEMENT |

These exact cases are covered in `tests/test_reconciliation.py`.

## Output

`gold/reconciliation`, partitioned by `business_date` (the authorisation date, or the settlement date for orphans):

`business_date, transaction_id, settlement_ids, merchant_id, card_scheme, currency, auth_amount, settled_amount, settlement_count, settlement_date, days_to_settle, status, run_date, reconciled_at`

Each run overwrites the partitions in its window, so the latest status always wins and older dates settle into their final state once they leave the window.

## Known limitations

- Matching is on `transaction_id` only. Real settlement files often need fuzzy matching on amount, PAN, auth code and date when the reference is missing.
- Partial settlements and refunds are not modelled.
- Amounts are compared in the transaction currency. There is no FX conversion.
