from fastapi import APIRouter, Request, HTTPException,Body
from typing import Any
import requests

from globals import *

router = APIRouter()

# reader  - <metadb>
@app.delete("/del")
def delete_entry(req: Any = Body(...)):
    try:
        acquire_read()
        mysql_conn,mysql_cursor = get_db()

        Stud_id = req["Stud_id"]
        # get the shard corresponding to the stud_id 
        mysql_cursor.execute("SELECT DISTINCT Shard_id FROM ShardT  WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s",(Stud_id,Stud_id))
        result = mysql_cursor.fetchone()
        print(result)

        if result:
            shard_id = result[0]
            
        #get lock on the shard


        #get all servers that contain the shard
            data = {
                        "shard":shard_id,
                        "Stud_id":Stud_id,
                        "secondary_servers":[]
                    }
            mysql_cursor.execute("SELECT Server_id,Primary_ FROM MapT WHERE Shard_id=%s",(shard_id,))
            rows = mysql_cursor.fetchall()
            PRIMARY_SERVER = None
            for row in rows:
                server_id ,primary = row
                if primary:
                    PRIMARY_SERVER = server_id
                else:
                    data["secondary_servers"].append(server_id)
            
            if PRIMARY_SERVER:
                result = requests.delete(f"http://{PRIMARY_SERVER}:8000/del",json=data,timeout=15)
                
                if not result.ok:
                    raise HTTPException(status_code=500,detail="Internal error")
            
            return {
                "message": f"Data entry for Stud_id:{Stud_id} deleted",
                "status" : "success"
            }
            
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500,detail="Internal error1")
    finally:
        close_db(mysql_conn,mysql_cursor)
        release_read()