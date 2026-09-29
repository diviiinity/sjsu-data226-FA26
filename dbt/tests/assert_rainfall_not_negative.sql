-- Rainfall can never be below zero
select *
from {{ ref('weather_metrics') }}
where precipitation_mm < 0 or rainfall_7d_rolling_mm < 0
