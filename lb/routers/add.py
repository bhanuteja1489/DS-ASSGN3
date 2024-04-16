from fastapi import APIRouter, Request,Body
from fastapi.responses import JSONResponse
import time
from consistent_hashing import ConsistentHashing
import requests
from random import randint

from helpers import create_server
from globals import *
from typing import Any

router = APIRouter()

# writer to <app.server_list,metaDB>
# add code for creating locks to new shards
@app.post("/add")
def add_servers(req: Any = Body(...)):
    try:
        acquire_write()                   #acquire the write lock
        mysql_conn,mysql_cursor=get_db()  #get the database connection
        n = req['n']
        new_shards, servers = req['new_shards'], req['servers']
        if n > len(servers):
            return JSONResponse(
                status_code=400,
                content={
                    "message": "<Error> Number of new servers (n) is greater than newly added instances",
                    "status": "failure"
                }
            )
        
        # need to create random server id when not given, currently assuming n = len(servers)
        ip = {}
        for server_name in servers:
            name , ipaddr = create_server(name=server_name)
            print("Successfully created ",name)
            ip[name] = ipaddr
        
        message = "Added"
        
        for server_name in servers:
                message += f" {server_name},"
                # on success, adding this new server to the respective shards' consistent hashing object
                app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": ip[server_name]}
                for sh in servers[server_name]:
                    if sh not in app.hash_dict:
                        app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
                        # create a lock for this shard
                        app.locks[sh] = threading.Lock()
                    
                    app.hash_dict[sh].add_server(app.server_list[server_name]['index'], ip[server_name], 8000)

                    # need to handle the case when one of the server fails, we need to stop the already created servers or do something else
                    # adding entry in MapT

                    add_mapt_query = "INSERT INTO MapT VALUES (%s,%s,0)"
                    mysql_cursor.execute(add_mapt_query,(sh,server_name))
                    mysql_conn.commit()
                
                # calling the /config api of this new server
                url = f"http://{ip[server_name]}:{8000}/config"
                data = {
                    "schema": {"columns":["Stud_id","Stud_name","Stud_marks"],
                            "dtypes":["Number","String","String"]},
                    "shards": servers[server_name]
                }
                print(data)
                while True:
                    try:
                        requests.post(url, json=data)
                        break
                    except:
                        print("retry after sleeping 0.1sec")
                        time.sleep(0.1)
                
                # if the shard is an existing one ,copying it from other servers
                for sh in servers[server_name]:
                    print(f"new-shards")
                    new_shardids = [shard_info["Shard_id"] for shard_info in new_shards]
                    print(new_shardids)
                    if sh not in new_shardids:
                            mysql_cursor.execute("SELECT DISTINCT Server_id FROM MapT WHERE Shard_id=%s",(sh,))
                            sh_servers = mysql_cursor.fetchall()
                            sh_students = []
                            for sh_server in sh_servers:
                                if sh_server[0] != server_name:
                                    try:
                                        resp = requests.get(f"http://{app.server_list[sh_server[0]]['ip']}:8000/copy",json={
                                        "shards": [sh]},timeout=15)
                                        sh_students = resp.json()[sh]
                                        break
                                    except requests.RequestException as e:
                                        print(e)
                                        print(f"couldn't copy {sh} from {sh_server[0]},trying another server")
                            
                            print(f"=== Student List for {sh} ===")
                            print(sh_students)
                            print("====================")

                                # copy the shard data to the newly added server 
                            requests.post(f"http://{app.server_list[server_name]['ip']}:8000/write",json={
                                "shard":sh,
                                "curr_idx": 0, 
                                "data": sh_students
                            },timeout=15)
                            print(f"Successfully copied shard:{sh} data from ", sh_server[0]," to ", server_name)   
                                
        # adding entries in ShardT
        for shard in new_shards:
            shard_query ="INSERT INTO ShardT VALUES (%s,%s,%s,%s)"
            mysql_cursor.execute(shard_query,(shard["Stud_id_low"],shard["Shard_id"],shard["Shard_size"],shard["Stud_id_low"]))
            mysql_conn.commit()
        
        return {
            "N": len(app.server_list),
            "message": message,
            "status": "success"
        }
    except Exception as e:
         return "some error"
    finally:
        close_db(mysql_conn,mysql_cursor)
        release_write()
        