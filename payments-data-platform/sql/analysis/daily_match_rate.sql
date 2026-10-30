select
    business_date,
    sum(record_count) filter (where status = 'MATCHED') as matched,
    sum(record_count) filter (where status <> 'ORPHAN_SETTLEMENT') as authorisations,
    round(
        100.0 * sum(record_count) filter (where status = 'MATCHED')
        / nullif(sum(record_count) filter (where status <> 'ORPHAN_SETTLEMENT'), 0),
        2
    ) as match_rate_pct
from marts.agg_daily_reconciliation
group by business_date
order by business_date desc;
