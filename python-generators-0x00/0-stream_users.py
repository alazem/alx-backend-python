#!/usr/bin/env python3
""" Stream users one by one from the user_data table using a generator. """

import mysql.connector
import os

def stream_users():
    """
    Generator function to stream users one by one from MySQL `user_data` table.
    Yields:
        dict: a dictionary for each row with keys 'user_id', 'name', 'email', 'age'
    """
    # Connect to ALX_prodev database
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database="ALX_prodev",
        port=int(os.getenv("MYSQL_PORT", 3306))
    )

    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT user_id, name, email, age FROM user_data;")

    for row in cursor:
        yield row

    cursor.close()
    connection.close()
