select
    card_scheme,
    days_to_settle,
    count(*) as settlements,
    round(100.0 * count(*) / sum(count(*)) over (partition by card_scheme), 2) as pct
from marts.fact_settlements
where reconciliation_status <> 'ORPHAN_SETTLEMENT'
group by card_scheme, days_to_settle
order by card_scheme, days_to_settle;
