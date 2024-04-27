from fastapi import APIRouter
from fastapi.responses import JSONResponse
from globals import app, get_db,close_db,release_read,acquire_read
import mysql.connector as conn
import os
router = APIRouter()

@app.get("/status")
def get_status():
    try:
        acquire_read()
        mysql_conn, mysql_cursor = get_db()

        print("connected")
        get_shards_query = "SELECT ShardT.Stud_id_low, ShardT.Shard_id, ShardT.Shard_size,MapT.Server_id FROM ShardT,MapT where ShardT.Shard_id=MapT.Shard_id AND MapT.Primary_=1"
        mysql_cursor.execute(get_shards_query)  
        shard_rows = mysql_cursor.fetchall()
        # mysql_cursor.execute("Select Server_id from MapT where Primary_=1")
        # for row in shard_rows:
        #     shard_id = row[1]

        servers = {}
        for ser in app.server_list:
            get_servers = "SELECT Shard_id FROM MapT WHERE Server_id=%s"
            mysql_cursor.execute(get_servers, (ser, ))
            sh_rows = mysql_cursor.fetchall()
            servers[ser] = [sh[0] for sh in sh_rows]

        return {
            "N": len(app.server_list), # number of servers currently running
            "schema": {
                "columns":["Stud_id","Stud_name","Stud_marks"],
                "dtypes":["Number","String","String"]
            },
            "shards": [
                {col: value for col, value in zip(["Stud_id_low", "Shard_id", "Shard_size","primary_server"], row)}
                for row in shard_rows
            ],
            "servers": servers
        }
    except Exception as e:
        print("Exception:", e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    finally:
        close_db(mysql_conn, mysql_cursor)
        release_read()

