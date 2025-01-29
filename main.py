import pandas as pd

from constants import EXPORT_CSV_PATH, FILE1_PATH, FILE2_PATH, FILE3_PATH
from data_processing import load_data_to_db, normalise_data
from db import initialise_database
from export import export_approved_data
from logger_setup import logger
from review import review_records

if __name__ == "__main__":
    logger.info("Reading input files...")
    file1 = pd.ExcelFile(FILE1_PATH)
    file2 = pd.read_csv(FILE2_PATH)
    file3 = pd.ExcelFile(FILE3_PATH)

    logger.info("Normalising data...")
    file1_normalised = normalise_data(file1.parse("Sheet1"), "File1")
    file2_normalised = normalise_data(file2, "File2")
    file3_normalised = normalise_data(file3.parse("Sheet1"), "File3")

    combined_data = pd.concat([file1_normalised, file2_normalised, file3_normalised], ignore_index=True)

    connection = initialise_database()
    if connection is not None:
        logger.info("Loading normalised data to DB...")
        load_data_to_db(connection, combined_data)

        logger.info("Starting the review process...")
        review_records(connection)

        grouping_filters = {}

        sector = input("Input sector name (enter to skip): ")
        if sector:
            grouping_filters["sector"] = sector

        region = input("Input region name (enter to skip): ")
        if region:
            grouping_filters["region"] = region

        year = input("Input year (enter to skip): ")
        if year:
            grouping_filters["year"] = int(year)

        logger.info("Exporting approved data...")
        export_approved_data(connection, EXPORT_CSV_PATH, **grouping_filters)

        logger.info("Database initialised, data loaded, and export script is ready.")
