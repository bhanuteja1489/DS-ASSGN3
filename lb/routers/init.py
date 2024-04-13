from fastapi import APIRouter, Request,Body
from fastapi.responses import JSONResponse
import time
from consistent_hashing import ConsistentHashing
import requests
from random import randint
from typing import Any
from helpers import create_server
from globals import *

router = APIRouter()
 
#writer - <app.server_list,metaDB>
# add code for creating locks to new shards
@app.post("/init")
def init_system(req: Any=Body(...)):

#     "N":3
#  "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],
#  "dtypes":["Number","String","String"]}
#  "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096},
#  {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096},
#  {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size  ":4096},]
#  "servers":{"Server0":["sh1","sh2"],
#  "Server1":["sh2","sh3"],

    try:
        acquire_write()
        time.sleep(5)
        mysql_conn,mysql_cursor=get_db()

        n, schema = req['N'], req['schema']
        shards, servers = req['shards'], req['servers']
        print(servers)
        app.schema = schema

        if n != len(servers):
            return JSONResponse(
                status_code=400,
                content={
                    "message": f"value of N and number of servers don't match!",
                    "status": "failure"
                }
            )
            
        # To store the shards map with server in database
        # MapT Schema
        create_mapt_query = f"""
        CREATE TABLE IF NOT EXISTS MapT (
            Shard_id VARCHAR(255),
            Server_id VARCHAR(255)
        )
        """
        mysql_cursor.execute(create_mapt_query)
        mysql_conn.commit()

        # To create ShardT schema in database
        create_shardt_query = f"""
        CREATE TABLE IF NOT EXISTS ShardT (
            Stud_id_low INT PRIMARY KEY,
            Shard_id VARCHAR(255),
            Shard_size INT,
            valid_idx INT
        )
        """
        mysql_cursor.execute(create_shardt_query)
        mysql_conn.commit()
        
        ip={}
        for server_name in servers:
            name,ipaddr = create_server(name=server_name)
            ip[name] = ipaddr
        
        for server_name in servers:
            url = f"http://{ip[server_name]}:{8000}/config"
            print(url)
            data = {
                "schema": schema,
                "shards": servers[server_name]
            }
            print(data)
            
            while True:
                try:
                        result = requests.post(url, json=data,timeout=None)
                        print(result.ok)
                        break
                except requests.RequestException as e:
                        print("trying again")
                        time.sleep(1) # time sleep for sqlite is 2 sec and for mysql need change to 30 sec
                    
                # on success
            app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": ip[server_name], "shards": data["shards"]}
            for sh in servers[server_name]:
                if sh not in app.hash_dict:
                    app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
                    #create lock for each shard
                    app.locks[sh] = threading.Lock()
                
                app.hash_dict[sh].add_server(app.server_list[server_name]['index'], ip[server_name], 8000)
                
                ## add shard-server mapping to database
                add_mapt_query = "INSERT INTO MapT VALUES (%s, %s)"
                print(add_mapt_query)
                try:
                    mysql_cursor.execute(add_mapt_query,(sh,server_name))
                    mysql_conn.commit()
                except Exception as e:
                    print(e)
                    print("Issue is here")

        # creating all shard entries in ShardT
        for shard in shards:
            shard_query = "INSERT INTO ShardT VALUES (%s,%s,%s,%s)"

            try:
                mysql_cursor.execute(shard_query,(shard["Stud_id_low"],shard["Shard_id"],shard["Shard_size"],shard["Stud_id_low"]))
                mysql_conn.commit()
            except Exception as e:
                    print(e)
                    print("Issue is here ):")
        return {
            "message" : "Configured Database",
            "status" : "success"
        }
    except Exception as e:
        print(e)
        return "some error"
    finally:
        close_db(mysql_conn,mysql_cursor)
        print("servers:")
        print(app.server_list.keys())
        requests.post("http://shard_manager:8000/sync_app",json={
             "server_list":app.server_list,
        })
        release_write()


 