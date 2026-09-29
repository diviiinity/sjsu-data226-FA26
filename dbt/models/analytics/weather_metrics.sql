with base as (
    select * from {{ ref('stg_weather') }}
),

flags as (
    select
        *,
        case when precipitation_mm < 1 then 1 else 0 end as is_dry_day
    from base
),

groups as (
    select
        *,
        sum(1 - is_dry_day) over (
            partition by city order by weather_date
            rows between unbounded preceding and current row
        ) as wet_day_count
    from flags
)

select
    city,
    latitude,
    longitude,
    weather_date,
    temp_max,
    temp_min,
    temp_avg,
    precipitation_mm,
    weather_code,

    round(avg(temp_avg) over (
        partition by city order by weather_date
        rows between 6 preceding and current row
    ), 2) as temp_7d_moving_avg,

    round(temp_avg - avg(temp_avg) over (partition by city), 2) as temp_anomaly,

    round(sum(precipitation_mm) over (
        partition by city order by weather_date
        rows between 6 preceding and current row
    ), 2) as rainfall_7d_rolling_mm,

    is_dry_day,

    case when is_dry_day = 1 then
        sum(is_dry_day) over (
            partition by city, wet_day_count order by weather_date
            rows between unbounded preceding and current row
        )
    else 0 end as dry_spell_length

from groups
