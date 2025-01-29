import sqlite3

import pandas as pd

from logger_setup import logger


def list_unapproved_records(conn: sqlite3.Connection) -> pd.DataFrame:
    """Lists all unapproved records from the database."""
    try:
        query = "SELECT * FROM emissions WHERE is_approved = 0;"
        return pd.read_sql_query(query, conn)
    except sqlite3.Error as e:
        logger.error("Error reading from the database: %s" % e)
        return pd.DataFrame()


def approve_record(conn: sqlite3.Connection, record_id: int) -> None:
    """Approves a specific record by ID."""
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE emissions SET is_approved = 1 WHERE id = ?;", (record_id,))
        conn.commit()
    except sqlite3.Error as e:
        logger.error("Error approving record %d: %s" % (record_id, e))
        raise


def reject_record(conn: sqlite3.Connection, record_id: int) -> None:
    """Rejects a specific record by deleting it from the database."""
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM emissions WHERE id = ?;", (record_id,))
        conn.commit()
    except sqlite3.Error as e:
        logger.error("Error rejecting record %d: %s" % (record_id, e))
        raise


def review_records(conn: sqlite3.Connection) -> None:
    """Command-line interface for reviewing unapproved records."""
    while True:
        unapproved = list_unapproved_records(conn)

        if unapproved.empty:
            logger.info("No unapproved records left to review.")
            break

        print("\nUnapproved Records:")
        print(unapproved)

        try:
            record_id = int(input("Enter the ID of the record to review (or -1 to exit): "))

            if record_id == -1:
                break

            action = input("Approve (a) or Reject (r) this record? ").strip().lower()
            if action == "a":
                approve_record(conn, record_id)
                logger.info("Record %d approved." % record_id)
            elif action == "r":
                reject_record(conn, record_id)
                logger.info("Record %d rejected." % record_id)
            else:
                logger.warning("Invalid action. Please enter 'a' to approve or 'r' to reject.")
        except ValueError:
            logger.warning("Invalid input. Please enter a valid record ID.")
        except sqlite3.Error as e:
            logger.error("Error processing record: %s" % e)
            raise
