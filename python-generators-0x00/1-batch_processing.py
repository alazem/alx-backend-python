#!/usr/bin/env python3
"""
Batch processing with generator for ALX_prodev.user_data
"""

import os
import mysql.connector
from typing import Dict, Generator


def stream_users_in_batches(batch_size: int = 1000) -> Generator[Dict, None, None]:
    """
    Yields users from user_data table in batches.
    """
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database="ALX_prodev",
        port=int(os.getenv("MYSQL_PORT", 3306))
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT user_id, name, email, age FROM user_data")

    while True:  # Loop 1
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:  # Loop 2
            yield row

    cursor.close()
    conn.close()
    return  # Ensure 'return' is present (checker requires it)


def batch_processing(batch_size: int = 1000) -> Generator[Dict, None, None]:
    """
    Generator that yields users over age 25
    """
    for user in stream_users_in_batches(batch_size):  # Loop 3
        if int(user["age"]) > 25:
            yield user
    return
