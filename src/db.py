"""
db.py

Database helper functions for the NBA forecasting project.
Creates and initializes the SQLite database using sql/schema.sql.
"""

import sqlite3
from pathlib import Path

# Base directory = repo root (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "nba_forecasting.db"
SCHEMA_PATH = BASE_DIR / "sql" / "schema.sql"


def get_connection() -> sqlite3.Connection:
    """Create a SQLite connection with the correct path."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)  # ensure /data exists
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # lets you treat rows like dicts
    return conn


def init_db() -> None:
    """Initialize the database by running the schema.sql script."""
    with get_connection() as conn:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
        conn.commit()


if __name__ == "__main__":
    print(f"Initializing database at {DB_PATH} using schema {SCHEMA_PATH}...")
    init_db()
    print("Database initialized successfully.")
