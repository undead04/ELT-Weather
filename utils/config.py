from pathlib import Path

# Project root
BASE_DIR: Path = Path(__file__).resolve().parents[1]

# Common paths
DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"
LOG_DIR: Path = BASE_DIR / "logs"
SQL_DB_PATH: Path = BASE_DIR /'db'/ "weather.duckdb"
SPARK_APP_NAME: str = "elt_weather_app"
SQL_SCRIPTS_PATH:Path = BASE_DIR / 'sql'


def get_base_dir() -> Path:
    return BASE_DIR


def get_db_path() -> Path:
    """Return the path to the DuckDB file used by loaders."""
    return SQL_DB_PATH


__all__ = [
    "BASE_DIR",
    "DATA_DIR",
    "RAW_DIR",
    "PROCESSED_DIR",
    "LOG_DIR",
    "SQL_DB_PATH",
    "DELTA_DIR",
    "SPARK_APP_NAME",
    "get_base_dir",
    "get_db_path",
]
