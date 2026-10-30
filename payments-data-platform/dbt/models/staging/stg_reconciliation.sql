select
    business_date,
    transaction_id,
    settlement_ids,
    merchant_id,
    card_scheme,
    currency,
    auth_amount,
    settled_amount,
    coalesce(settled_amount, 0) - coalesce(auth_amount, 0) as amount_difference,
    settlement_count,
    settlement_date,
    days_to_settle,
    status,
    run_date,
    reconciled_at
from {{ source('raw', 'reconciliation') }}
