select
    currency_code,
    currency_name,
    minor_units
from {{ ref('currencies') }}
