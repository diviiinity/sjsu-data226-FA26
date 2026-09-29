{% snapshot weather_snapshot %}

{{
  config(
    target_schema='ANALYTICS',
    unique_key='weather_key',
    strategy='check',
    check_cols=['temp_max', 'temp_min', 'precipitation_mm', 'weather_code']
  )
}}

select
    city || '_' || to_char(weather_date, 'YYYY-MM-DD') as weather_key,
    *
from {{ ref('stg_weather') }}

{% endsnapshot %}
