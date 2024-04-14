from fastapi import APIRouter, Request, HTTPException,Body
import requests 
from typing import Any
from globals import *

router = APIRouter()



# reader - <metadb>
@app.put("/update")
async def update_shard(req: Any = Body(...)):
 try:
    acquire_read()
    mysql_conn,mysql_cursor = get_db()

    Stud_id = req["Stud_id"]
    Student = req["data"]
    mysql_cursor.execute("SELECT DISTINCT Shard_id FROM ShardT  WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s",(Stud_id,Stud_id))
    result = mysql_cursor.fetchone()
    print("here")
    print(result)
    if result:
        shard_id = result[0]
        
        #get lock on the shard
        mysql_cursor.execute("SELECT Server_id,Primary_ FROM MapT WHERE Shard_id=%s",(shard_id,))
        rows = mysql_cursor.fetchall()
        PRIMARY_SERVER = None
        data = {
                    "shard":shard_id,
                    "Stud_id":Stud_id,
                    "data":Student,
                    "secondary_servers": []
                }
        for row in rows:
            server_id ,primary = row
            if primary:
                PRIMARY_SERVER = server_id
            else:
                data["secondary_servers"].append(server_id)
        
        if PRIMARY_SERVER:
            result = requests.put(f"http://{PRIMARY_SERVER}:8000/update",json=data,timeout=15)
            
            if not result.ok:
                raise HTTPException(status_code=500,detail="Internal error")
        
            return {
                "message": f"Data entry for Stud_id:{Stud_id} updated",
                "status" : "success"
            }
        else:
            return {
                "message" : "No server found",
                "status"  : "failure"
            }
 except Exception as e:
    print(e)
    return "some error"
 finally: 
    close_db(mysql_conn,mysql_cursor)
    release_read()