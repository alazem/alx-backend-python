import sqlite3
import time
import functools

def with_db_connection(func)
 @functools.wraps(func)
 def wrapper
  

def retry_on_failure(retries=3, delay=2)
  def wrapper
