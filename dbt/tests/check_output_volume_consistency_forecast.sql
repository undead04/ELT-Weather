-- tests/assert_weather_forecast_has_min_records.sql
select 
    count(*) as total_rows
from {{ ref('int_weather_aq_forecast') }}
having count(*) < 1512