select t.transaction_id
from {{ ref('stg_transactions') }} as t
left join {{ ref('stg_reconciliation') }} as r
    on r.transaction_id = t.transaction_id
   and r.status <> 'ORPHAN_SETTLEMENT'
where t.is_approved
  and r.transaction_id is null
