import psycopg2
from pathlib import Path
from elt_app.utils.config import (
    POSTGRES_CONN_URI, BASE_DIR
)
from elt_app.utils.logging import get_logger

logger = get_logger("db_utils")

def execute_sql_file(sql_file_path: Path):
    """Reads and executes a SQL file."""
    if not sql_file_path.exists():
        logger.error(f"SQL file not found: {sql_file_path}")
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    logger.info(f"Executing SQL from {sql_file_path.name}...")
    
    try:
        with psycopg2.connect(POSTGRES_CONN_URI) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_content)
                conn.commit()
        logger.info(f"Successfully executed {sql_file_path.name}")
    except Exception as e:
        logger.error(f"Error executing {sql_file_path.name}: {e}")
        raise e
