import argparse
import os

import pandas as pd

from constants import EXPORT_CSV_PATH, INPUT_DIR_PATH
from data_processing import load_data_to_db, normalise_data, read_source_file
from db import initialise_database
from export import export_approved_data
from logger_setup import logger
from review import review_records

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simple emissions ETL", usage="PROG [-h] [--sector <sector>] [--region <region>] [--year <year>]"
    )
    parser.add_argument("--sector", type=str, help="Sector name to filter by")
    parser.add_argument("--region", type=str, help="Region name to filter by")
    parser.add_argument("--year", type=int, help="Year to filter by")

    logger.info("Reading input files...")

    fparse_results = {}
    for fname in os.listdir(INPUT_DIR_PATH):
        fpath = os.path.join(INPUT_DIR_PATH, fname)
        if os.path.isfile(fpath):
            fparse_res = read_source_file(fpath)
            if fparse_res is not None:
                fparse_results[fname] = fparse_res

    logger.info("Normalising data...")
    normalised_data = [normalise_data(res, fname) for fname, res in fparse_results.items()]
    combined_data = pd.concat(normalised_data, ignore_index=True)

    connection = initialise_database()
    if connection is not None:
        logger.info("Loading normalised data to DB...")
        load_data_to_db(connection, combined_data)

        logger.info("Starting the review process...")
        review_records(connection)

        aggregate_filters = {k: v for k, v in parser.parse_args()._get_kwargs() if v is not None}

        logger.info("Exporting approved data...")
        export_approved_data(connection, EXPORT_CSV_PATH, **aggregate_filters)

        logger.info("Database initialised, data loaded, and export is ready.")
