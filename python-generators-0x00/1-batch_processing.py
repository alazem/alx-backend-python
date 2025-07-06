#!/usr/bin/env python3
"""
1-batch_processing.py
─────────────────────
• stream_users_in_batches(batch_size) → generator that yields DB rows **in batches**  
• batch_processing(batch_size)        → prints (streams) users whose age > 25

Constraints met
---------------
✔ Uses `yield`
✔ Total of **≤ 3 loops** (1 `while` + 2 `for`)
✔ Works with the provided 2‑main.py
"""

import os
import mysql.connector
from typing import List, Dict, Iterator


def stream_users_in_batches(batch_size: int = 1000) -> Iterator[List[Dict]]:
    """
    Yield successive lists of up to `batch_size` rows from ALX_prodev.user_data.
    Each row is a `dict` thanks to `dictionary=True` cursor.
    """
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database="ALX_prodev",
        port=int(os.getenv("MYSQL_PORT", 3306)),
    )
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT user_id, name, email, age FROM user_data;")

    while True:                                    # LOOP 1 (generator loop)
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows                                 # ← generator magic ✨

    cur.close()
    conn.close()


d
