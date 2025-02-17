from typing import Optional, TypeAlias
from constants import DB_FILE_PATH
import pandas as pd
import aiosqlite

SourceFileParseRes: TypeAlias = pd.DataFrame | dict[str, pd.DataFrame] | dict[int, pd.DataFrame]


def read_source_file(source_path: str) -> Optional[SourceFileParseRes]:
    """Adapter for reading different file types.

    Easily extensible to support other file formats.
    """
    if source_path.endswith(".csv"):
        return pd.read_csv(source_path)

    if source_path.endswith(".xlsx") or source_path.endswith(".xls"):
        return pd.ExcelFile(source_path).parse("Sheet1")

    return None


def normalise_data(data: SourceFileParseRes, source_name: str) -> pd.DataFrame:
    """Normalises data based on a unified schema."""
    return pd.DataFrame.from_records(
        [
            {
                "activity_name": data.get("Activity Name"),
                "sector": data.get("Sector", data.get("Sector-Category")),
                "category": data.get("Category"),
                "unit": data.get("Unit"),
                "kg_co2e": data.get("Emmision (kgCO2e)", data.get("kgCO2e")),
                "kg_co2": data.get("kgCO2"),
                "kg_ch4": data.get("kgCH4"),
                "kg_n2o": data.get("kgN2O"),
                "assessment_report": data.get("Assesment Report"),
                "scope": data.get("Scope"),
                "life_cycle_assessment": data.get("Life Cylce Assesment", data.get("LCA")),
                "validity_year": data.get("Validity Year", data.get("Year Valid From")),
                "validity_region": data.get("Validity Region", data.get("Region")),
                "source": source_name,
            }
        ]
    )


async def initialise_database() -> Optional[aiosqlite.Connection]:
    """Initialises the database and creates the emissions table."""
    try:
        conn = await aiosqlite.connect(DB_FILE_PATH)
        cursor = await conn.cursor()

        await cursor.execute(
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

        await conn.commit()
        return conn
    except aiosqlite.Error as e:
        return None
