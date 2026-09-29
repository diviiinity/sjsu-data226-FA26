select
    city,
    latitude,
    longitude,
    "date"                                  as weather_date,
    temp_max,
    temp_min,
    round((temp_max + temp_min) / 2, 2)     as temp_avg,
    precipitation                           as precipitation_mm,
    weather_code
from {{ source('raw', 'WEATHER_TEMPERATURE') }}
