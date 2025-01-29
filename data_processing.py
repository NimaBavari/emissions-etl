import sqlite3
from typing import TypeAlias

import pandas as pd

from logger_setup import logger

ExcelLikeParseRes: TypeAlias = pd.DataFrame | dict[str, pd.DataFrame] | dict[int, pd.DataFrame]


def normalise_data(data: ExcelLikeParseRes, source_name: str) -> pd.DataFrame:
    """Normalises data based on a unified schema."""
    return pd.DataFrame(
        {
            "activity_name": data.get("Activity Name", pd.NA),
            "sector": data.get("Sector", data.get("Sector-Category", pd.NA)),
            "category": data.get("Category", pd.NA),
            "unit": data.get("Unit", pd.NA),
            "kg_co2e": data.get("Emmision (kgCO2e)", data.get("kgCO2e", pd.NA)),
            "kg_co2": data.get("kgCO2", pd.NA),
            "kg_ch4": data.get("kgCH4", pd.NA),
            "kg_n2o": data.get("kgN2O", pd.NA),
            "assessment_report": data.get("Assesment Report", pd.NA),
            "scope": data.get("Scope", pd.NA),
            "life_cycle_assessment": data.get("Life Cylce Assesment", data.get("LCA", pd.NA)),
            "validity_year": data.get("Validity Year", data.get("Year Valid From", pd.NA)),
            "validity_region": data.get("Validity Region", data.get("Region", pd.NA)),
            "source": source_name,
        }
    )


def load_data_to_db(conn: sqlite3.Connection, data: pd.DataFrame) -> None:
    """Loads normalised data into the database."""
    try:
        data.to_sql("emissions", conn, if_exists="append", index=False)
    except sqlite3.OperationalError:
        logger.error("Schema conflict. Please ensure the data is normalised.")
    except Exception as e:
        logger.error("Error loading data into the database: %s" % e)
