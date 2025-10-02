import os
import psycopg2
from urllib.parse import urlparse

db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL not set")

url = urlparse(db_url)
conn = psycopg2.connect(
    dbname=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
)
conn.autocommit = False

def exec_safe(sql, params=None, fetch="none"):
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            if fetch == "one":
                return cur.fetchone()
            elif fetch == "all":
                return cur.fetchall()
            return None
    except psycopg2.Error:
        conn.rollback()
        raise
