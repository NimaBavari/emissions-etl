import asyncio
import os

import aiosqlite
import pandas as pd
from temporalio import activity

from constants import INPUT_DIR_PATH, DB_FILE_PATH
from logger_setup import create_or_get_logger
from utils import SourceFileParseRes, normalise_data, read_source_file, initialise_database

logger = create_or_get_logger(__name__)


@activity.defn
async def read_source_files() -> dict[str, SourceFileParseRes]:
    """Returns the data from all the source files in the data directory."""
    logger.info("Reading input files...")

    fparse_results = {}
    fnames = await asyncio.to_thread(os.listdir, INPUT_DIR_PATH)
    for fname in fnames:
        fpath = os.path.join(INPUT_DIR_PATH, fname)
        if os.path.isfile(fpath):
            fparse_res = read_source_file(fpath)
            if fparse_res is not None:
                fparse_results[fname] = fparse_res

    return fparse_results


@activity.defn
async def normalise_and_combine_data(fparse_results: dict[str, SourceFileParseRes]) -> dict[str, list]:
    """Normalises and combines all the data from given source files."""
    logger.info("Normalising data...")
    tasks = [asyncio.to_thread(normalise_data, res, fname) for fname, res in fparse_results.items()]
    normalised_data = await asyncio.gather(*tasks)
    combined_df = pd.concat(normalised_data, ignore_index=True)
    return {
        "columns": combined_df.columns.tolist(),
        "data": combined_df.values.tolist(),
        "index": combined_df.index.tolist(),
    }


@activity.defn
async def load_data_to_db(data: dict[str, list]) -> None:
    """Loads normalised data into the database."""
    logger.info("Initialising DB...")
    conn = await initialise_database()
    if conn is None:
        logger.error("Database initialisation error: %s" % e)
        return

    logger.info("Loading normalised data to DB...")
    try:
        df = pd.DataFrame(data["data"], columns=data["columns"], index=data["index"])
        tuples = list(df.itertuples(index=False, name=None))
        columns = df.columns.tolist()
        query = "INSERT INTO emissions (%s) VALUES (%s)" % (", ".join(columns), ", ".join(["?"] * len(columns)))
        await conn.executemany(query, tuples)
        await conn.commit()
    except aiosqlite.OperationalError:
        logger.error("Schema conflict. Please ensure the data is normalised.")
        raise
    except Exception as e:
        logger.error("Error loading data into the database: %s" % e)
        raise


@activity.defn
async def export_approved_data(output_file_path: str) -> None:
    """Exports the approved data to a CSV file."""
    logger.info("Exporting approved data...")
    try:
        conn = await aiosqlite.connect(DB_FILE_PATH)
        cursor = await conn.execute("SELECT * FROM emissions WHERE is_approved = 1;")
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        approved_data = pd.DataFrame(rows, columns=columns)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        await asyncio.to_thread(approved_data.to_csv, output_file_path, index=False)
    except aiosqlite.Error as e:
        logger.error("Error connecting to or reading from the database: %s" % e)
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
