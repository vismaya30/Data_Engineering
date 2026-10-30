with settlements as (
    select * from {{ ref('stg_settlements') }}
),

transactions as (
    select transaction_id, auth_date, amount
    from {{ ref('stg_transactions') }}
),

reconciliation as (
    select transaction_id, status
    from {{ ref('stg_reconciliation') }}
    where status <> 'ORPHAN_SETTLEMENT'
)

select
    s.settlement_id,
    s.transaction_id,
    to_char(s.settlement_date, 'YYYYMMDD')::int as date_key,
    s.settlement_date,
    t.auth_date,
    s.settlement_date - t.auth_date as days_to_settle,
    s.merchant_id,
    s.card_scheme,
    s.currency,
    s.batch_id,
    s.settled_amount,
    t.amount as auth_amount,
    case
        when t.transaction_id is null then 'ORPHAN_SETTLEMENT'
        else coalesce(r.status, 'PENDING')
    end as reconciliation_status
from settlements as s
left join transactions as t
    on t.transaction_id = s.transaction_id
left join reconciliation as r
    on r.transaction_id = s.transaction_id
