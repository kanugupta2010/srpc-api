import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

connection_pool = pooling.MySQLConnectionPool(
    pool_name    = "srpc_pool",
    pool_size    = 10,
    pool_reset_session = True,
    **DB_CONFIG
)

def get_connection():
    """
    FastAPI dependency — yields a DB connection and guarantees
    it is returned to the pool after the request completes,
    even if an exception is raised.
    """
    conn = connection_pool.get_connection()
    try:
        yield conn
    finally:
        conn.close()