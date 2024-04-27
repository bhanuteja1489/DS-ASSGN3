from fastapi import APIRouter, Request, Body
from fastapi.responses import JSONResponse
import requests
from random import randint
from typing import Any
import threading
from globals import *

router = APIRouter()


# reader - <app.server_list,metaDB>
@app.get("/read/{server_id}")
def read_data(server_id):
    try:
        acquire_read()
        mysql_conn, mysql_cursor = get_db()

        print(f"Thread id: {threading.get_ident()}")
        get_server_shards_query = "SELECT ShardT.Stud_id_low, ShardT.Shard_id,ShardT.Shard_size from MapT where ShardT.Shard_id=MapT.Shard_id AND MapT.Server_id=%s"
        mysql_cursor.execute(get_server_shards_query, server_id)
        resp_ = mysql_cursor.fetchall()
        result = []
        for row in resp_:
            url = f"http://{server_id}:{8000}/read"
            data = {
                "shard": row[1],
                "Stud_id": {
                    "low": row[0],
                    "high": row[0] + row[2],
                },
            }

            resp = requests.post(url, json=data)
            if resp.status_code != 200:
                return JSONResponse(
                    status_code=400,
                    content={"message": "Invalid status code", "status": "failure"},
                )

            resp = resp.json()

            print('the fethced data from :',row[1],'is ',resp)
            if resp["status"] == "success":
                result [row[1]]= resp["data"]
            else:
                return JSONResponse(
                    status_code=400,
                    content={"message": "Invalid query", "status": "failure"},
                )
        print(
            "shards_queried:",
            shards_queried,
            "data:",
            result,
        )
        return {
            "shards_queried": shards_queried,
            "data": result,
            "status": "success"
        }

    except Exception as e:
        return "Some error"
    finally:
        close_db(mysql_conn, mysql_cursor)
        release_read()
