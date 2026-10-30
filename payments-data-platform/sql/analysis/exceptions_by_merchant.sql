select
    m.merchant_id,
    m.merchant_name,
    a.status,
    sum(a.record_count) as records,
    sum(a.amount_difference) as amount_difference
from marts.agg_daily_reconciliation as a
join marts.dim_merchant as m
    on m.merchant_id = a.merchant_id
where a.is_exception
  and a.business_date >= current_date - 30
group by m.merchant_id, m.merchant_name, a.status
order by records desc;
