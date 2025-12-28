#!/bin/bash
set -e

echo "--- DANG KHOI TAO DATABASE LẦN ĐẦU ---"

# 1. Tạo database weather_dw
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB_DEFAULT" <<-EOSQL
    CREATE DATABASE weather_dw;
EOSQL

# 2. Chạy file SQL vào database vừa tạo
echo "--- Dang nap du lieu vao weather_dw ---"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "weather_dw" -f /docker-entrypoint-initdb.d/sql_scripts/init_warehouse.sql

echo "--- HOAN THANH KHOI TAO ---"
