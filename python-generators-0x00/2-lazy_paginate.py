#!/usr/bin/env python3
"""Lazy pagination: stream user_data in pages using a generator"""

import seed  # assumes seed.connect_to_prodev is available

def paginate_users(page_size, offset):
    """
    Fetch a page of users from user_data table based on limit and offset.
    Returns a list of dictionaries.
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_data LIMIT %s OFFSET %s", (page_size, offset))
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows


def lazy_pagination(page_size):
    """
    Generator that yields pages of users lazily, only when needed.
    Uses a single loop.
    """
    offset = 0
    while True:  # ✅ Only loop
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page  # ✅ Required generator
        offset += page_size
