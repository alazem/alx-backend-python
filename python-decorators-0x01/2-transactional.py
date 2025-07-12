import functools
import sqlite3

def with_db_connection(func):
  @functools.wraps(func)
  def wrapper(*args, **kwargs)
   conn = connect.sqlite3(user.db)
    try:
      result = conn(func, *args, **kwargs)
      return result
    
