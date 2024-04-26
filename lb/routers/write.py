from fastapi import APIRouter, Request,Body
from fastapi.responses import JSONResponse
import requests
from requests import RequestException
from typing import Any
from globals import *
import threading
router = APIRouter()


# reader - <app.server_list,metaDB>
# TODO: need to get locks when writing to shards
@app.post("/write")
def write(req: Any=Body(...)):
    # need to map shard to server
    #print thread id  
    print(f"Thread id: {threading.get_ident()}")
    try:
        acquire_read()
        mysql_conn,mysql_cursor = get_db()

        students = req["data"]
        print(students)
        GET_SHARDS_QUERY = "SELECT * FROM ShardT"
        mysql_cursor.execute(GET_SHARDS_QUERY)
        rows = mysql_cursor.fetchall()
        shards = {row[1]: {"students":[],"attr":list(row),"server":[]} for row in rows}
        mysql_cursor.execute("SELECT * FROM MapT")
        MapT_rows =mysql_cursor.fetchall()
        print('before : ',shards)
        for MapT_row in MapT_rows:
            shard_id = MapT_row[0]
            server = MapT_row[1]
            shards[shard_id]["server"].append(server)
        print('after : ',shards)

        print(shards)
        for student in students:
            Stud_id =student["Stud_id"]
            Stud_name=student["Stud_name"]
            Stud_marks=student["Stud_marks"]
            
            for shard_id in shards:
                if shards[shard_id]["attr"][0] <= Stud_id and Stud_id <= shards[shard_id]["attr"][0]+shards[shard_id]["attr"][2]:
                    shards[shard_id]["students"].append((Stud_id,Stud_name,Stud_marks))
        
        data_written = []
        for shard_id in shards:
            # acquire the lock for this shard
            if shards[shard_id]['students'] == []:
                continue
            
            with app.locks[shard_id]:
                queries = [{"Stud_id":stud[0],"Stud_name":stud[1],"Stud_marks":stud[2]} for stud in shards[shard_id]["students"]]
                data= { "shard":shard_id,"data":queries,"secondary_servers":[]}
                curr_idx = None
                mysql_cursor.execute("SELECT Server_id,Primary_ FROM MapT WHERE Shard_id=%s",(shard_id,))
                rows = mysql_cursor.fetchall()
                PRIMARY_SERVER = None
                for row in rows:
                    server_id ,primary = row
                    if primary:
                        PRIMARY_SERVER = server_id
                    else:
                        data["secondary_servers"].append(server_id)
                print(f"Sending request to primary server: {PRIMARY_SERVER} :{shard_id}") 

                try:
                    print(data)
                    result = requests.post(f"http://{PRIMARY_SERVER}:8000/write",json=data,timeout=15)
                    if result.status_code != 200:
                        return JSONResponse(status_code=400,content={
                            "message":f"writes to shard {shard_id} failed",
                            "data entries written successfully":data_written,
                            "status":"failure"
                        })
                    print(result.json())
                except requests.RequestException as e:
                    print(e)
                    print(f"failed to write to {shard_id}....")
                    print("Continuing to write to other shards")
                    continue
                data_written.extend(queries)
        return {"message":f"{len(students)} Data entries added","status":"success"}
        
    except RequestException as e:
        print("RequestException:", e)
        return JSONResponse(status_code=500, content={"message": "Request failed", "status": "failure"})
        
    except sqlite3.Error as e: 
        print("SQLite Error:", e)
        return JSONResponse(status_code=500, content={"message": "SQLite error", "status": "failure"})
        
    except Exception as e:
        print("Other Exception:", e)
        return JSONResponse(status_code=500, content={"message": "Unexpected error", "status": "failure"})
    finally:
        close_db(mysql_conn,mysql_cursor)
        release_read()