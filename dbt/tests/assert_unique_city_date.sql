-- Each city should have only one row per date
select city, weather_date, count(*) as row_count
from {{ ref('weather_metrics') }}
group by city, weather_date
having count(*) > 1
