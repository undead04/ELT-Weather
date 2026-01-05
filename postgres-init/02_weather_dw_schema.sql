\c weather_dw
-- ============================
-- SEQUENCES
-- ============================
CREATE SEQUENCE date_id_seq;
CREATE SEQUENCE time_id_seq;
CREATE SEQUENCE city_id_seq;
CREATE SEQUENCE weather_id_seq;
CREATE SEQUENCE aq_id_seq;

-- ============================
-- DIM TABLES
-- ============================

CREATE TABLE dim_city (
    city_id INTEGER PRIMARY KEY DEFAULT nextval('city_id_seq'),
    city_name VARCHAR(100) UNIQUE NOT NULL,
    country VARCHAR(100) NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    min_lat DOUBLE PRECISION NOT NULL,
    max_lat DOUBLE PRECISION NOT NULL,
    min_lon DOUBLE PRECISION NOT NULL,
    max_lon DOUBLE PRECISION NOT NULL,
    inseget_time TIMESTAMP NOT NULL
);

CREATE INDEX idx_dim_city_city_name ON dim_city(city_name);


CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY DEFAULT nextval('date_id_seq'),
    full_date DATE UNIQUE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    day_of_week INTEGER NOT NULL,
    inseget_time TIMESTAMP NOT NULL
);


CREATE TABLE dim_time (
    time_id INTEGER PRIMARY KEY DEFAULT nextval('time_id_seq'),
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    time_bucket VARCHAR(20) NOT NULL,
    inseget_time TIMESTAMP NOT NULL,
    UNIQUE (hour, minute, second)
);

-- ============================
-- FACT TABLES
-- ============================

CREATE TABLE fact_weather (
    weather_id INTEGER PRIMARY KEY DEFAULT nextval('weather_id_seq'),
    date_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    inseget_time TIMESTAMP NOT NULL,

    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    precipitation DOUBLE PRECISION NOT NULL,
    weather_type VARCHAR(100) NOT NULL,
    weather_code INTEGER NOT NULL,
    cloud_cover DOUBLE PRECISION NOT NULL,
    rain DOUBLE PRECISION NOT NULL,
    wind_direction VARCHAR(10) NOT NULL,
    apparent_temperature DOUBLE PRECISION NOT NULL,
    CONSTRAINT fk_weather_city FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    CONSTRAINT fk_weather_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT fk_weather_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);


CREATE TABLE fact_air_quality (
    aq_id INTEGER PRIMARY KEY DEFAULT nextval('aq_id_seq'),
    date_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    inseget_time TIMESTAMP NOT NULL,
    aqi DOUBLE PRECISION NOT NULL,
    pm25 DOUBLE PRECISION NOT NULL,
    pm10 DOUBLE PRECISION NOT NULL,
    no2 DOUBLE PRECISION NOT NULL,
    so2 DOUBLE PRECISION NOT NULL,
    o3 DOUBLE PRECISION NOT NULL,
    co DOUBLE PRECISION NOT NULL,
    co2 DOUBLE PRECISION NOT NULL,

    CONSTRAINT fk_aq_city FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    CONSTRAINT fk_aq_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT fk_aq_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

-- ============================
-- INDEXES FOR FACT TABLES
-- ============================

CREATE INDEX idx_fact_weather_date ON fact_weather(date_id);
CREATE INDEX idx_fact_weather_city ON fact_weather(city_id);

CREATE INDEX idx_fact_aq_date ON fact_air_quality(date_id);
CREATE INDEX idx_fact_aq_city ON fact_air_quality(city_id);

ALTER TABLE dim_date
ADD CONSTRAINT uq_dim_date_dmy UNIQUE (day, month, year);

ALTER TABLE dim_time
ADD CONSTRAINT uq_dim_time_hms UNIQUE (hour, minute, second);

ALTER TABLE fact_weather
ADD CONSTRAINT uq_fact_weather_dct UNIQUE (date_id, city_id, time_id);

ALTER TABLE fact_air_quality
ADD CONSTRAINT uq_fact_air_quality_dct UNIQUE (date_id, city_id, time_id);

CREATE OR REPLACE VIEW V_DIM_DATE AS 
SELECT * FROM dim_date;

CREATE OR REPLACE VIEW V_DIM_TIME AS 
SELECT * FROM dim_time;

CREATE OR REPLACE VIEW V_DIM_CITY AS 
SELECT * FROM dim_city;

CREATE OR REPLACE VIEW V_FACT_WEATHER AS 
SELECT * FROM fact_weather;

CREATE VIEW V_FACT_AQ AS 
SELECT * FROM fact_air_quality; 

