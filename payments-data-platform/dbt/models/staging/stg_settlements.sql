select
    settlement_id,
    transaction_id,
    merchant_id,
    card_scheme,
    settled_amount,
    currency,
    settlement_date,
    batch_id
from {{ source('raw', 'settlements') }}
