-- Airflow metadata
CREATE DATABASE airflow;
CREATE USER airflow_user WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;

-- Warehouse
CREATE DATABASE weather_dw;
CREATE USER warehouse_user WITH PASSWORD 'warehouse';
GRANT ALL PRIVILEGES ON DATABASE weather_dw TO warehouse_user;
\c weather_dw
CREATE SCHEMA IF NOT EXISTS raw;
GRANT ALL ON SCHEMA raw TO warehouse_user;
CREATE TABLE raw.raw_weather_current (
    province_id BIGINT,
    insert_time TIMESTAMP,
    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m BIGINT,
    apparent_temperature DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    time TEXT
);
CREATE TABLE raw.raw_weather_forecast (
    province_id BIGINT,
    insert_time TIMESTAMP,
    temperature_2m TEXT,
    relative_humidity_2m TEXT,
    apparent_temperature TEXT,
    uv_index TEXT,
    precipitation TEXT,
    wind_speed TEXT,
    time TEXT
);
CREATE TABLE raw.raw_aq_current (
    province_id BIGINT,
    insert_time TIMESTAMP,
    pm2_5 DOUBLE PRECISION,
    european_aqi_pm2_5 DOUBLE PRECISION,
    time TEXT
);
CREATE TABLE raw.raw_aq_forecast (
    province_id BIGINT,
    insert_time TIMESTAMP,
    pm2_5 TEXT,
    european_aqi_pm2_5 TEXT,
    time TEXT
); 

