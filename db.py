import sqlite3
from typing import Optional

from constants import DB_FILE_PATH
from logger_setup import logger


def initialise_database() -> Optional[sqlite3.Connection]:
    """Initialises the database and creates the emissions table."""
    try:
        conn = sqlite3.connect(DB_FILE_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS emissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            activity_name TEXT,
            sector TEXT,
            category TEXT,
            unit TEXT,
            kg_co2e REAL,
            kg_co2 REAL,
            kg_ch4 REAL,
            kg_n2o REAL,
            assessment_report TEXT,
            scope TEXT,
            life_cycle_assessment TEXT,
            validity_year INTEGER,
            validity_region TEXT,
            source TEXT,
            is_approved INTEGER DEFAULT 0
        );
        """
        )

        conn.commit()
        return conn
    except sqlite3.Error as e:
        logger.error("Database initialization error: %s" % e)
        return None
