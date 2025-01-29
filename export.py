import sqlite3
from typing import Optional

import pandas as pd

from logger_setup import logger


def export_approved_data(
    conn: sqlite3.Connection,
    output_file_path: str,
    sector: Optional[str] = None,
    region: Optional[str] = None,
    year: Optional[int] = None,
) -> None:
    """Exports (optionally) filtered portion of the approved data to a CSV file."""
    query = "SELECT * FROM emissions WHERE is_approved = 1"

    filters = []
    if sector is not None:
        query += " AND sector = ?"
        filters.append(sector)

    if region is not None:
        query += " AND validity_region = ?"
        filters.append(region)

    if year is not None:
        query += " AND validity_year = ?"
        filters.append(year)

    query += ";"

    try:
        approved_data = pd.read_sql_query(query, conn, params=filters)
        approved_data.to_csv(output_file_path, index=False)
    except sqlite3.Error as e:
        logger.error("Error reading from the database: %s" % e)
        raise
    except FileNotFoundError:
        logger.error("Invalid output file path.")
        raise
    except PermissionError:
        logger.error("Permission denied to write to the output file.")
        raise
    except OSError as e:
        logger.error("Error writing to the output file: %s" % e)
        raise
    except MemoryError:
        logger.error("Data frame too large.")
        raise
