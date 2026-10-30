select
    to_char(r.business_date, 'YYYYMMDD')::int as date_key,
    r.business_date,
    r.merchant_id,
    r.card_scheme,
    r.currency,
    r.status,
    s.is_exception,
    count(*) as record_count,
    sum(r.auth_amount) as auth_amount,
    sum(r.settled_amount) as settled_amount,
    sum(r.amount_difference) as amount_difference
from {{ ref('stg_reconciliation') }} as r
inner join {{ ref('dim_reconciliation_status') }} as s
    on s.status = r.status
group by 1, 2, 3, 4, 5, 6, 7
