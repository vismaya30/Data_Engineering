select
    date_key,
    auth_date,
    merchant_id,
    card_scheme,
    currency,
    count(*) as transaction_count,
    count(*) filter (where is_approved) as approved_count,
    count(*) filter (where not is_approved) as declined_count,
    sum(amount) filter (where is_approved) as approved_amount,
    sum(settled_amount) as settled_amount
from {{ ref('fact_transactions') }}
group by 1, 2, 3, 4, 5
