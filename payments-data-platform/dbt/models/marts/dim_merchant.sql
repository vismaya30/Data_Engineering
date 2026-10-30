select
    merchant_id,
    merchant_name,
    mcc,
    country_code,
    settlement_currency,
    onboarded_date
from {{ ref('stg_merchants') }}
