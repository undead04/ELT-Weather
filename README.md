# ELT-Weather

This repository contains a small ELT (Extract -> Load -> Transform) pipeline collecting weather and air-quality data for Vietnamese cities.

Contents & purpose
- `scripts/extract`: collect data from public APIs
- `scripts/transform`: convert raw JSON into Parquet, calculate AQI
- `scripts/load`: insert processed data into DuckDB (`sql/weather.duckdb`)
- `scheduler`: Airflow DAGs for daily/yearly runs
- `notebooks/`: analysis & visualization examples

Quick start (developer)
1) Create a Python environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Run a step locally (example):
```bash
python main.py transform weather
```

3) Inspect processed data in `data/processed` or open the `sql/weather.duckdb` DuckDB file from the project root.

What I added / changed
- A minimal `main.py` CLI to run available ETL steps (extract/transform/load) for each domain (weather, aq, city, date, time).
- `utils/config.py` centralizes common paths.
- Populated `requirements.txt` with the libs used by the scripts.

Next improvements you can ask me to make
- Add unit tests and a CI GitHub Action to run tests and linters
- Create a `docker-compose` to run a local Airflow instance for the DAGs
- Add data quality checks and monitoring hooks

If you'd like, I can implement any of the above next — tell me which one you prefer and I'll continue.

Running the full stack with Docker (local Airflow + ETL runner)
1) Start the stack (this will build images and initialize Airflow + attach an interactive ETL container):
```bash
docker compose up --build -d
```
2) Initialize the project DuckDB schema (runs once):
```bash
docker compose run --rm etl-runner python sql/config_dw.py
```
3) Open Airflow UI: http://localhost:8080 (login: admin / admin)
4) Use the `etl-runner` container to run ETL commands interactively:
```bash
docker compose exec etl-runner bash
python main.py all weather
```
If you'd like I can implement any of the above next — tell me which one you prefer and I'll continue.

Spark & Delta Lake
------------------
This project supports PySpark + Delta Lake for scalable transformations and ACID table storage.

Quick notes:
- `requirements.txt` includes `pyspark` and `delta-spark` so transforms can run using Spark.
- A sample Spark → Delta transform is available at `scripts/transform/transform_delta.py` and writes to `data/delta/<domain>`.
- The `docker-compose.yml` mounts the `delta_data` volume for persistent Delta storage; the `etl-runner` image includes Java and PySpark so you can run delta transforms inside the container.

Example (inside the etl-runner container or local environment where PySpark is available):
```bash
python scripts/transform/transform_delta.py weather
```

If you'd like I'll add an example notebook cell that demonstrates reading the Delta table using Spark and generating interactive charts.
