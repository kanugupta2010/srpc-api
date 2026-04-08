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
    pool_name="srpc_pool",
    pool_size=10,
    **DB_CONFIG
)

def get_connection():
    return connection_pool.get_connection()
