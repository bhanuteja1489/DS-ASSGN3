import sqlite3
import os
from fastapi import FastAPI
import mysql.connector as conn
import time
import inspect
import threading
# DB_FILE = "example.db"
# if os.path.exists(DB_FILE):
#     os.remove(DB_FILE)

# mysql_conn = sqlite3.connect('example.db', check_same_thread=False)
# mysql_cursor = mysql_conn.cursor()

while True:
    try:
        mysql_conn = conn.connect(
            host="metadb",
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
        )
        print("connected")
        break
    
    except Exception as e:
        # print(e)
        time.sleep(0.02)
mysql_cursor = mysql_conn.cursor()

app = FastAPI()
# app = {}

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


# maps for storing locally for loadbalancer
app.server_list = {}
app.schema = None


# lock for app datastructure

appLock = threading.Lock()



def get_db(db_engine = "sql"):
    if db_engine == "sqlite":
        db_conn = sqlite3.connect('example.db')
        db_cursor = db_conn.cursor()
    else:
        db_conn= conn.connect(
                host="metadb",
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DATABASE"),
            )
        db_cursor = db_conn.cursor()
    return db_conn, db_cursor

def close_db(db_conn, db_cursor):
    db_cursor.close()
    db_conn.close()

