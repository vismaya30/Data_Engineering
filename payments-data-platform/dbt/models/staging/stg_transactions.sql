select
    transaction_id,
    merchant_id,
    card_scheme,
    masked_pan,
    amount,
    currency,
    auth_code,
    auth_status,
    auth_status = 'APPROVED' as is_approved,
    auth_timestamp,
    auth_date
from {{ source('raw', 'transactions') }}
