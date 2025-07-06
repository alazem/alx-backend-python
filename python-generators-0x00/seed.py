#!/usr/bin/env python3
"""
seed.py – utilities for setting up the ALX_prodev database
and streaming rows from the user_data table.

Dependencies
------------
pip install mysql-connector-python
"""

import csv
import os
import sys
import mysql.connector
from mysql.connector import errorcode
from typing import Iterator, Tuple, Any

# ────────────────────────────────────────────────
# Connection helpers
# ────────────────────────────────────────────────
def _get_connection(**kwargs):
    """Low‑level helper: wraps mysql.connector.connect with sensible defaults."""
    defaults = {
        "host":     os.getenv("MYSQL_HOST", "localhost"),
        "user":     os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "port":     int(os.getenv("MYSQL_PORT", 3306)),
        "auth_plugin": "mysql_native_password",  # works for most setups
    }
    defaults.update(kwargs)
    return mysql.connector.connect(**defaults)


def connect_db():
    """
    Connect to the MySQL *server* (no default schema).
    Returns a connection object or None on failure.
    """
    try:
        return _get_connection(database=None)
    except mysql.connector.Error as err:
        print(f"[connect_db] MySQL error: {err}", file=sys.stderr)
        return None


def create_database(connection):
    """
    CREATE DATABASE IF NOT EXISTS ALX_prodev;
    """
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev;")
        connection.commit()
    finally:
        cursor.close()


def connect_to_prodev():
    """
    Connect directly to the ALX_prodev database.
    """
    try:
        return _get_connection(database="ALX_prodev")
    except mysql.connector.Error as err:
        print(f"[connect_to_prodev] MySQL error: {err}", file=sys.stderr)
        return None

# ────────────────────────────────────────────────
# Schema + data
# ────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_data (
    user_id CHAR(36) PRIMARY KEY,
    name    VARCHAR(255) NOT NULL,
    email   VARCHAR(255) NOT NULL,
    age     DECIMAL(5, 0) NOT NULL,
    INDEX (user_id)
);
"""


def create_table(connection):
    """
    Create user_data table if it does not exist.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(CREATE_TABLE_SQL)
        connection.commit()
        print("Table user_data created successfully")
    finally:
        cursor.close()


INSERT_SQL = """
INSERT IGNORE INTO user_data (user_id, name, email, age)
VALUES (%s, %s, %s, %s);
"""


def insert_data(connection, csv_path: str, batch_size: int = 1000):
    """
    Insert rows from `csv_path` into user_data.
    Uses INSERT IGNORE to avoid duplicates. Commits in batches.
    """
    if not os.path.isfile(csv_path):
        print(f"[insert_data] CSV file not found: {csv_path}", file=sys.stderr)
        return

    inserted, skipped = 0, 0
    cursor = connection.cursor()
    try:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            batch = []
            for row in reader:
                batch.append(
                    (row["user_id"], row["name"], row["email"], int(row["age"]))
                )
                if len(batch) >= batch_size:
                    cursor.executemany(INSERT_SQL, batch)
                    connection.commit()
                    inserted += cursor.rowcount
                    skipped += len(batch) - cursor.rowcount
                    batch.clear()

            # final partial batch
            if batch:
                cursor.executemany(INSERT_SQL, batch)
                connection.commit()
                inserted += cursor.rowcount
                skipped += len(batch) - cursor.rowcount

        print(f"Inserted {inserted} new rows ({skipped} duplicates skipped).")
    finally:
        cursor.close()

# ────────────────────────────────────────────────
# Generator: stream rows lazily
# ────────────────────────────────────────────────
def stream_user_data(connection, chunk_size: int = 1000) -> Iterator[Tuple[Any, ...]]:
    """
    Yield user_data rows one by one (or in chunks if chunk_size>1).

    Example
    -------
    >>> for row in stream_user_data(conn):
    ...     print(row)
    """
    cursor = connection.cursor(buffered=True)
    try:
        cursor.execute("SELECT user_id, name, email, age FROM user_data;")
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                yield row
    finally:
        cursor.close()

# ────────────────────────────────────────────────
# Optional quick‑test when run directly
# ────────────────────────────────────────────────
if __name__ == "__main__":
    conn = connect_db()
    if not conn:
        sys.exit(1)

    create_database(conn)
    conn.close()

    conn = connect_to_prodev()
    if not conn:
        sys.exit(1)

    create_table(conn)
    insert_data(conn, "user_data.csv")

    # Demonstrate the generator: print first 5 streamed rows
    gen = stream_user_data(conn)
    for _ in range(5):
        try:
            print(next(gen))
        except StopIteration:
            break

    conn.close()
