{% snapshot dim_locations_snapshot %}

{{
    config(
      target_database='weather_dw',
      target_schema='snapshots',
      unique_key='province_id',
      strategy='check',
      check_cols=['latitude', 'longitude', 'region'],
    )
}}

select * from {{ ref('dim_locations') }}

{% endsnapshot %}
