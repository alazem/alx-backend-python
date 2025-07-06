#!/usr/bin/env python3
"""
4-stream_ages.py
Memory-efficient average calculation using generators
"""

import os
import mysql.connector


def stream_user_ages():
    """
    Generator that yields each user's age from user_data table
    """
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database="ALX_prodev",
        port=int(os.getenv("MYSQL_PORT", 3306)),
    )
    cursor = conn.cursor()
    cursor.execute("SELECT age FROM user_data;")

    for (age,) in cursor:  # ← Loop 1
        yield int(age)

    cursor.close()
    conn.close()


def compute_average_age():
    """
    Consumes the generator to calculate and print the average user age
    """
    total = 0
    count = 0

    for age in stream_user_ages():  # ← Loop 2
        total += age
        count += 1

    if count == 0:
        print("Average age of users: 0")
    else:
        avg = total / count
        print(f"Average age of users: {avg:.2f}")


# Entry point
if __name__ == "__main__":
    compute_average_age()
