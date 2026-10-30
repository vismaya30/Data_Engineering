select *
from {{ ref('stg_reconciliation') }}
where status in ('MATCHED', 'LATE')
  and (settlement_count <> 1 or settled_amount <> auth_amount)
