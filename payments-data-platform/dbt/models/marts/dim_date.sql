with days as (
    select generate_series(
        '{{ var("calendar_start") }}'::date,
        (current_date + interval '1 year')::date,
        interval '1 day'
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int as date_key,
    date_day,
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Mon') as month_name,
    extract(isodow from date_day)::int as day_of_week,
    to_char(date_day, 'Dy') as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from days
