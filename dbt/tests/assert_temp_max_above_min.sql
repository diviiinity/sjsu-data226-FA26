-- Daily max temperature should never be lower than daily min
select *
from {{ ref('weather_metrics') }}
where temp_max < temp_min
