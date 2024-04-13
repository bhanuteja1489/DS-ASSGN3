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

            mysql_cursor.execute("SELECT DISTINCT Server_id FROM MapT WHERE Shard_id = %s",(shard_id,))
            servers = mysql_cursor.fetchall()
            print(servers)
            if servers:
                for server in servers:
                    server = server[0]
                    payload = {
                        "shard":shard_id,
                        "Stud_id":Stud_id,
                    }
                    result = requests.delete(f"http://{server}:8000/del",json=payload,timeout=15)
                    
                    if not result.ok:
                        raise HTTPException(status_code=500,detail="Internal error")
                
                return {
                    "message": f"Data entry for Stud_id:{Stud_id} deleted",
                    "status" : "success"
                }
            
    except Exception as e:
        raise HTTPException(status_code=500,detail="Internal error1")
    finally:
        close_db(mysql_conn,mysql_cursor)
        release_read()