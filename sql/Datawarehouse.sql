-- ============================
-- DIM TABLES
-- ============================
CREATE SEQUENCE date_id_seq;
CREATE SEQUENCE time_id_seq;
CREATE SEQUENCE weather_id_seq;
CREATE SEQUENCE aq_id_seq;
CREATE SEQUENCE corr_id_seq;

CREATE TABLE dim_city (
    city_id INTEGER PRIMARY KEY,
    city_name VARCHAR UNIQUE NOT NULL,
    country VARCHAR NOT NULL,
    lon DOUBLE NOT NULL,
    lat DOUBLE NOT NULL
);

CREATE INDEX dim_city_city_name_index ON dim_city(city_name);


CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY DEFAULT nextval('date_id_seq'),
    full_date DATE UNIQUE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    day_of_week VARCHAR NOT NULL
);

CREATE TABLE dim_time (
    time_id INTEGER PRIMARY KEY DEFAULT nextval('time_id_seq'),
    hour  INTEGER UNIQUE NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    time_bucket VARCHAR NOT NULL
);

-- ============================
-- FACT TABLES
-- ============================

CREATE TABLE fact_weather (
    weather_id INTEGER PRIMARY KEY DEFAULT nextval('weather_id_seq'),
    date_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    temperature DOUBLE NOT NULL,
    humidity DOUBLE NOT NULL,
    wind_speed DOUBLE NOT NULL,
    precipitation DOUBLE NOT NULL,
    weather_code VARCHAR NOT NULL,
    cloud_cover DOUBLE NOT NULL,
    rain DOUBLE NOT NULL,
    wind_direction VARCHAR NOT NULL,
    apparent_temperature DOUBLE NOT NULL,
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

CREATE TABLE fact_air_quality (
    aq_id INTEGER PRIMARY KEY DEFAULT nextval('aq_id_seq'),
    time_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    aqi DOUBLE NOT NULL,
    pm25 DOUBLE NOT NULL,
    pm10 DOUBLE NOT NULL,
    no2 DOUBLE NOT NULL,
    so2 DOUBLE NOT NULL,
    o3 DOUBLE NOT NULL,
    co DOUBLE NOT NULL,
    co2 DOUBLE NOT NULL,
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);
