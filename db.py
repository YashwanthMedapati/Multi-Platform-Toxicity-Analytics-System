import os
import time
import psycopg2
from psycopg2 import pool
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from contextlib import contextmanager
from dotenv import load_dotenv
from utils import getlogger

# loading the environment variables
load_dotenv()
logger = getlogger("db")
# registering the psycopg2 adapter so python dicts store as JSONB in database
register_adapter(dict, Json)
DATABASE_URL = os.getenv("DATABASE_URL")
# this code sets up a PostgreSQL connection pool which lets the app reuse database connections 
# instead of making a new one for each query. This method is very helpful when the program runs 
# more than one Faktory job at the same time because it stops the database from getting too many 
# connection requests. The ThreadedConnectionPool keeps a number of active connections open. 
# The minconn setting tells the pool how many open connections to keep ready, 
# and the maxconn setting tells the pool how many connections can be open at the same time.
connection_pool = pool.ThreadedConnectionPool(
    minconn=int(os.getenv("DB_MIN_CONN")),
    maxconn=int(os.getenv("DB_MAX_CONN")),
    dsn=DATABASE_URL
)
logger.info(f"database connection pool created with min={connection_pool.minconn}, max={connection_pool.maxconn}")
# context manager to get a DB cursor from the pool.
# automatically commits on success or rolls back on error.
# added timeout and retry mechanism in case the pool is exhausted
@contextmanager
def get_cursor(commit=False, timeout=10, retry_delay=1):
    start_time = time.time()
    conn = None
    cur = None
    attempts = 0
    while True:
        try:
            conn = connection_pool.getconn()
            break
        except psycopg2.pool.PoolError:
            attempts += 1
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                try:
                    available = len(connection_pool._pool)
                    busy = connection_pool.maxconn - available
                    logger.error(
                        f"CRITICAL: pool exhausted after {timeout}s and {attempts} attempts. "
                        f"pool status: {busy}/{connection_pool.maxconn} connections in use"
                    )
                except:
                    logger.error(f"CRITICAL: pool exhausted after {timeout}s and {attempts} attempts")
                raise TimeoutError(f"no database connection available after {timeout} seconds")
            if attempts % 5 == 0:
                logger.warning(f"pool busy-> attempt {attempts} elapsed {elapsed:.1f}s")
            time.sleep(retry_delay)
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            connection_pool.putconn(conn)
            logger.debug("returned database connection to pool")