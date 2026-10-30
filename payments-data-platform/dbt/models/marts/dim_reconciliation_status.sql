select
    status,
    description,
    is_exception
from {{ ref('reconciliation_statuses') }}
