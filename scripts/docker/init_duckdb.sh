#!/usr/bin/env bash
set -euo pipefail

echo "Initializing DuckDB schema (sql/Datawarehouse.sql) ..."
python sql/config_dw.py
echo "Done."
