from fastapi import FastAPI, Request, HTTPException
import os
from dotenv import load_dotenv
import sqlite3
import time
import sys

from fastapi.responses import JSONResponse
import requests

current_directory = os.getcwd()
sys.path.append(current_directory)
# print(current_directory)
from log import *

# Load environment file
load_dotenv()

app = FastAPI()

server_id = os.getenv("SERVER_ID", "Server0")

# Connect to SQLite database
conn = sqlite3.connect("student.db")
cursor = conn.cursor()
logger = {}
commit = {}
primary = {}


@app.on_event("shutdown")
async def shutdown_event():
    # Close SQLite connection
    print("Cleaning up the resources")
    conn.close()


@app.get("/heartbeat")
async def heartbeat():
    return ""


# Initialzes the shards
@app.post("/config")
async def initialize_shards(request: Request):
    try:
        req = await request.json()
        # schema is a dictionary with columns as stud_id, stud_name, stud_marks and
        # dtypes as number, string, string
        schema = req["schema"]
        shards = req["shards"]
        for shard in shards:
            logger[shard] = FileLogger("logs", shard + ".log")
            commit[shard] = 0
            primary[shard] = 0
        message = ""

        # Create a table for each shard
        for i, shard in enumerate(shards):
            # Hardcoded schema for now since given data-types are wrong
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {shard} (
                {schema["columns"][0]} INTEGER PRIMARY KEY,
                {schema["columns"][1]} VARCHAR(255),
                {schema["columns"][2]} VARCHAR(255)
            )
            """
            cursor.execute(create_table_query)
            if i == 0:
                message += f"{server_id}:{shard}"
            else:
                message += f", {server_id}:{shard}"

        message += " configured"
        conn.commit()
        return {"message": message, "status": "success"}

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid request")


@app.get("/copy")
async def get_all_shards_data(request: Request):
    try:
        req = await request.json()
        shards = req["shards"]

        response = {}
        for shard in shards:
            cursor.execute(f"SELECT * FROM {shard}")
            rows = cursor.fetchall()
            response[shard] = [
                {
                    col: value
                    for col, value in zip(["Stud_id", "Stud_name", "Stud_marks"], row)
                }
                for row in rows
            ]
        response["status"] = "success"
        return response

    except sqlite3.Error as err:
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")


@app.post("/commit")
async def get_commits(request: Request):
    try:
        req = await request.json()
        shards = req["shards"]

        response = {}
        shards_commits = {}
        for shard in shards:
            if primary[shard] == 0:
                shards_commits[shard] = logger[shard].get_last_log_id()
            else:
                shards_commits[shard] = commit[shard]
        response["shards"] = shards_commits
        response["status"] = "success"
        return response

    except:
        raise HTTPException(status_code=400, detail="Invalid request")


@app.post("/primary")
async def set_primary(request: Request):
    try:
        req = await request.json()
        shards = req["shards"]
        for shard in shards:
            primary[shard] = 1
    except:
        raise HTTPException(status_code=400, detail="Invalid request")


@app.post("/read")
async def get_students_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        id_range = req["Stud_id"]
        low = id_range["low"]
        high = id_range["high"]

        cursor.execute(
            f"SELECT * FROM {shard} WHERE Stud_id >= {low} AND Stud_id <= {high}"
        )
        response = {}
        response["data"] = cursor.fetchall()

        response["status"] = "success"

        return response

    except sqlite3.Error as err:
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")


@app.post("/write")
async def add_students_data(request: Request):
    try:
        req = await request.json()
        print(req)
        shard = req["shard"]
        data = req["data"]
        for row in data:
            id = int(logger[shard].get_last_log_id()) + 1
            log = Log(id, LogType(0), row, datetime.now())
            logger[shard].add_log(log)
        if primary[shard] == 1:
            print(" I am primary")
            mod_data = {"shard": req["shard"], "data": req["data"]}
            secondary_servers = req["secondary_servers"]
            votes = 0
            for server in secondary_servers:
                try:
                    print("sent a reqest to server ", server)
                    result = requests.post(
                        f"http://{server}:8000/write", json=mod_data, timeout=15
                    )
                    print(result.json())
                    if result.status_code != 200:
                        return JSONResponse(
                            status_code=400,
                            content={
                                "message": f"writes to server {server} failed",
                                # "data entries written successfully":data_written,
                                "status": "failure",
                            },
                        )
                    else:
                        votes += 1
                        print("got a vote")
                    print(result.json())
                except requests.RequestException as e:
                    print(e)
                    print(f"failed to write to {server}....")
                    print("Continuing to write to other servers")
                    continue
            if 2 * votes >= len(secondary_servers):
                print("got enough votes")
                for row in data:
                    cursor.execute(
                        f"INSERT INTO {shard} (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
                        (row["Stud_id"], row["Stud_name"], row["Stud_marks"]),
                    )
                commit[shard] = logger[shard].get_last_log_id()

        else:
            print(" I am secondary")
            for row in data:
                cursor.execute(
                    f"INSERT INTO {shard} (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
                    (row["Stud_id"], row["Stud_name"], row["Stud_marks"]),
                )
            commit[shard] = logger[shard].get_last_log_id()

        response = {"message": "Data entries added", "status": "success"}
        conn.commit()

        return response

    except sqlite3.Error as err:
        print(err)
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=400, detail="Invalid request")


@app.put("/update")
async def update_student_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        stud_id = req["Stud_id"]
        data = req["data"]
        stud_name = data["Stud_name"]
        stud_marks = data["Stud_marks"]

        id = int(logger[shard].get_last_log_id()) + 1
        log = Log(
            id,
            LogType(1),
            {"shard": shard, "Stud_id": stud_id, "data": data},
            datetime.now(),
        )
        logger[shard].add_log(log)

        if primary[shard] == 1:
            print(" I am primary")
            secondary_servers = req["secondary_servers"]
            votes = 0
            mod_data = {"shard": shard, "Stud_id": stud_id, "data": data}

            for server in secondary_servers:
                try:
                    print("sent a reqest to server ", server)
                    result = requests.put(
                        f"http://{server}:8000/update", json=mod_data, timeout=15
                    )
                    print(result.json())
                    if result.status_code != 200:
                        return JSONResponse(
                            status_code=400,
                            content={
                                "message": f"updates to server {server} failed",
                                # "data entries written successfully":data_written,
                                "status": "failure",
                            },
                        )
                    else:
                        votes += 1
                        print("got a vote")
                    print(result.json())
                except requests.RequestException as e:
                    print(e)
                    print(f"failed to update {server}....")
                    print("Continuing to update in other servers")
                    continue
            if 2 * votes >= len(secondary_servers):
                print("got enough votes")
                cursor.execute(
                    f"UPDATE {shard} SET Stud_name = ?, Stud_marks = ? WHERE Stud_id = ?",
                    (stud_name, stud_marks, stud_id),
                )
                commit[shard] = logger[shard].get_last_log_id()
        else:
            print(" I am secondary")
            cursor.execute(
                f"UPDATE {shard} SET Stud_name = ?, Stud_marks = ? WHERE Stud_id = ?",
                (stud_name, stud_marks, stud_id),
            )
            commit[shard] = logger[shard].get_last_log_id()

        conn.commit()

        response = {
            "message": f"Data entry for Stud_id:{stud_id} updated",
            "status": "success",
        }

        return response
    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")


@app.delete("/del")
async def delete_student_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        stud_id = req["Stud_id"]

        id = int(logger[shard].get_last_log_id()) + 1
        log = Log(id, LogType(2), {"Stud_id": stud_id}, datetime.now())
        logger[shard].add_log(log)
        if primary[shard] == 1:
            print(" I am primary")
            secondary_servers = req["secondary_servers"]
            votes = 0
            mod_data = {"shard": req["shard"], "Stud_id": req["Stud_id"]}

            for server in secondary_servers:
                try:
                    print("sent a reqest to server ", server)
                    result = requests.delete(
                        f"http://{server}:8000/del", json=mod_data, timeout=15
                    )
                    print(result.json())
                    if result.status_code != 200:
                        return JSONResponse(
                            status_code=400,
                            content={
                                "message": f"delete to server {server} failed",
                                # "data entries written successfully":data_written,
                                "status": "failure",
                            },
                        )
                    else:
                        votes += 1
                        print("got a vote")
                    print(result.json())
                except requests.RequestException as e:
                    print(e)
                    print(f"failed to delete {server}....")
                    print("Continuing to delete in other servers")
                    continue
            if 2 * votes >= len(secondary_servers):
                print("got enough votes")
                cursor.execute(f"DELETE FROM {shard} WHERE Stud_id = ?", (stud_id,))
                commit[shard] = logger[shard].get_last_log_id()
        else:
            print(" I am secondary")
            cursor.execute(f"DELETE FROM {shard} WHERE Stud_id = ?", (stud_id,))
            commit[shard] = logger[shard].get_last_log_id()

        conn.commit()

        response = {
            "message": f"Data entry for Stud_id:{stud_id} removed",
            "status": "success",
        }

        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")


# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
