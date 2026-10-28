# Reconciliation

The reconciliation job matches **authorisations** (transactions) with **settlements** on `transaction_id`.

## Statuses

| Status | Condition |
|---|---|
| MATCHED | Settlement found, amount equal, settled within T+2 |
| LATE_MATCH | Settlement found, amount equal, settled after T+2 |
| AMOUNT_MISMATCH | Settlement found, but \|auth − settled\| > 0.01 |
| UNMATCHED_AUTH | Approved auth with no settlement after the look-back window (T+3) |
| UNMATCHED_SETTLEMENT | Settlement with no matching auth |

## Logic

1. Read the silver transactions for `run_date` and the settlements for `run_date - 3` to `run_date`.
2. Run a full outer join on `transaction_id`.
3. Exclude DECLINED transactions, since they should never settle.
4. Assign a status using the rules above.
5. Write the result to `gold/reconciliation/ingest_date=<run_date>`.

## Outputs

- A daily break report listing all non-MATCHED records
- KPIs: match rate %, total unmatched value (EUR), average days to settle
