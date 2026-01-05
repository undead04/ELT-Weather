SELECT
  'CREATE DATABASE weather_dw'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'weather_dw'
)\gexec
