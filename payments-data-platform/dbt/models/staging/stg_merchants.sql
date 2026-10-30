select
    merchant_id,
    merchant_name,
    mcc,
    country as country_code,
    settlement_currency,
    onboarded_date
from {{ source('raw', 'merchants') }}
