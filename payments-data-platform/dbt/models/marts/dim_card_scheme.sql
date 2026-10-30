select
    scheme_code,
    scheme_name,
    network_model
from {{ ref('card_schemes') }}
