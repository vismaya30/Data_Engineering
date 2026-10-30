{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

with transactions as (
    select *
    from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    where auth_date >= (select max(auth_date) from {{ this }}) - {{ var('late_window_days') }}
    {% endif %}
),

reconciliation as (
    select *
    from {{ ref('stg_reconciliation') }}
    where status <> 'ORPHAN_SETTLEMENT'
)

select
    t.transaction_id,
    to_char(t.auth_date, 'YYYYMMDD')::int as date_key,
    t.auth_date,
    t.auth_timestamp,
    t.merchant_id,
    t.card_scheme,
    t.currency,
    t.auth_status,
    t.is_approved,
    t.amount,
    r.settled_amount,
    coalesce(r.settlement_count, 0) as settlement_count,
    r.settlement_date,
    r.days_to_settle,
    case
        when not t.is_approved then 'NOT_APPLICABLE'
        else coalesce(r.status, 'PENDING')
    end as reconciliation_status
from transactions as t
left join reconciliation as r
    on r.transaction_id = t.transaction_id
