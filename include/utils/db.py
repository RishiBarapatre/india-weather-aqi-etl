"""
include/utils/db.py
-------------------
Shared database connection helpers.

Both PostgreSQL (warehouse) and MongoDB (raw zone) connections are built
from environment variables injected by docker-compose.yaml.  No credentials
ever live in source code.

Usage:
    from include.utils.db import get_warehouse_conn, get_mongo_db

    with get_warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
"""

import os
import psycopg2
import psycopg2.extras   # for RealDictCursor — returns rows as dicts
from pymongo import MongoClient


# ---------------------------------------------------------------------------
# PostgreSQL — Data Warehouse
# ---------------------------------------------------------------------------

def get_warehouse_conn() -> psycopg2.extensions.connection:
    """
    Return a psycopg2 connection to the warehouse PostgreSQL database.

    The returned connection is NOT auto-committed — callers must explicitly
    call conn.commit() or use it as a context manager (which commits on
    clean exit and rolls back on exception).

    Example:
        with get_warehouse_conn() as conn:   # auto-commits / rolls back
            with conn.cursor() as cur:
                cur.execute("INSERT ...")
    """
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_POSTGRES_HOST"],
        port=int(os.environ.get("WAREHOUSE_POSTGRES_PORT", 5432)),
        user=os.environ["WAREHOUSE_POSTGRES_USER"],
        password=os.environ["WAREHOUSE_POSTGRES_PASSWORD"],
        dbname=os.environ["WAREHOUSE_POSTGRES_DB"],
    )


def get_warehouse_cursor(conn):
    """
    Return a RealDictCursor for the given connection.

    RealDictCursor makes each row a dict keyed by column name rather than
    a plain tuple — much safer when column order might change.

    Example:
        with get_warehouse_conn() as conn:
            with get_warehouse_cursor(conn) as cur:
                cur.execute("SELECT city_id, city_name FROM dim_city")
                rows = cur.fetchall()   # [{"city_id": 1, "city_name": "Mumbai"}, ...]
    """
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ---------------------------------------------------------------------------
# MongoDB — Raw Zone
# ---------------------------------------------------------------------------

def get_mongo_client() -> MongoClient:
    """
    Return a MongoClient connected to the raw-zone MongoDB instance.

    The client is NOT thread-safe — create one per task/thread.
    Always close it when done (or use a context manager).
    """
    username = os.environ["MONGO_INITDB_ROOT_USERNAME"]
    password = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
    host = os.environ.get("MONGO_HOST", "mongo")
    port = int(os.environ.get("MONGO_PORT", 27017))

    uri = f"mongodb://{username}:{password}@{host}:{port}/"
    return MongoClient(uri)


def get_mongo_db():
    """
    Return the raw-zone MongoDB database object.

    Example:
        db = get_mongo_db()
        db.raw_weather_readings.insert_one({...})
    """
    client = get_mongo_client()
    db_name = os.environ.get("MONGO_DB", "weather_aqi_raw")
    return client[db_name]
