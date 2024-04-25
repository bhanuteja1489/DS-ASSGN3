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
        print("schema:", schema)
        print("shards: ", shards)
        print("started config")
        for shard in shards:
            logger[shard] = FileLogger("logs", str(shard) + ".log")
            commit[shard] = 0
            primary[shard] = 0
        message = ""
        print("created logger,commit and primary maps")
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
        print(e)
        raise HTTPException(status_code=400, detail="Invalid request")

@app.post("/update_local_data")
async def update_local_data(request: Request):
    try:
        req = await request.json()
        server = req["servers"]
        shard = req["shards"]
        list_to_update = req["data"]
        for row in list_to_update:
            if row["type"].lower() == "write":
                requests.post(
                    f"http://{server}:8000/write",
                    json={"shard": shard, "data": row["data"]["data"]},
                )
            elif row["type"].lower() == "delete":
                requests.delete(
                    f"http://{server}:8000/del",
                    json={"shard": shard, "Stud_id": row["data"]["Stud_id"]},
                )
            elif row["type"].lower() == "update":
                # "shard": shard, "Stud_id": stud_id, "data": data
                requests.put(
                    f"http://{server}:8000/update",
                    json={
                        "shard": shard,
                        "Stud_id": row["data"]["Stud_id"],
                        "data": row["data"]["data"],
                    },
                )
            else:
                print("Incorrect type ", row["type"])
    except:
        raise HTTPException(status_code=400, detail="Invalid request")

    return {"message": "Updated the local data successfully", "status": "success"}

@app.get("/fetch_log_data")
async def fetch_log_data(request: Request):
    try:
        req = await request.json()
        shard = req["shards"]
        PRIMARY_SERVER = req["servers"]
        ipaddr = req["own_ip"]
        try:
            resp = requests.get(
                f"http://{PRIMARY_SERVER}:8000/copy",
                json={"shards": [shard]},
            )
            resp = resp.json()
            print("response of copy : ", resp)
            students = resp[shard]
            log_data = resp["log_data_" + shard]
            commit_index = resp["commit_" + shard]
            print("=== Student List ===", flush=True)
            print(students, flush=True)
            print("====================", flush=True)
            print("=== Log Data ===", flush=True)
            print(log_data, flush=True)
            print("====================", flush=True)
            logger[shard].overwrite_file(log_data, shard)
            print(
                "last log id afte inserting log data", logger[shard].get_last_log_id()
            )
            # requests.post(
            #     f"http://{ipaddr}:8000/write",
            #     json={"shard": shard, "data": students},
            # )
            for student in students:
                # Using parameterized query to avoid SQL injection
                cursor.execute(
                    f"INSERT INTO {shard} (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
                    (student["Stud_id"], student["Stud_name"], student["Stud_marks"]),
                )

            list_to_update = logger[shard].get_requests_from_given_index(
                shard, int(commit_index) + 1
            )
            print(list_to_update)
            for row in list_to_update:
                if row["type"].lower() == "write":
                    requests.post(
                        f"http://{ipaddr}:8000/write",
                        json={"shard": shard, "data": row["data"]["data"]},
                    )
                elif row["type"].lower() == "delete":
                    requests.delete(
                        f"http://{ipaddr}:8000/del",
                        json={"shard": shard, "Stud_id": row["data"]["Stud_id"]},
                    )
                elif row["type"].lower() == "update":
                    # "shard": shard, "Stud_id": stud_id, "data": data
                    requests.put(
                        f"http://{ipaddr}:8000/update",
                        json={
                            "shard": shard,
                            "Stud_id": row["data"]["Stud_id"],
                            "data": row["data"]["data"],
                        },
                    )
                else:
                    print("Incorrect type ", row["type"])
            print(
                f"Successfully copied shard:{shard} data from ",
                PRIMARY_SERVER,
                flush=True,
            )

        except requests.RequestException as e:
            print(e)
            print(f"Request to {server_id} failed", flush=True)
            print("Trying with another server", flush=True)

    except:
        raise HTTPException(status_code=400, detail="Invalid request")

    return {"message": "Fetched the data successfully", "status": "success"}


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
            log_data = logger[shard].get_file_data(shard)
            response["log_data_" + str(shard)] = log_data
            response["commit_" + str(shard)] = commit[shard]
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
        if primary[shard] == 1:
            print(" I am primary")
            mod_data = {
                "shard": req["shard"],
                "data": req["data"],
                "commit": commit[shard],
            }
            secondary_servers = req["secondary_servers"]
            votes = 0
            for row in data:
                id = int(logger[shard].get_last_log_id()) + 1
                log = Log(id, LogType(0), row, datetime.now())
                logger[shard].add_log(log)
            for server in secondary_servers:
                while True:
                    try:
                        print("sent a reqest to server ", server)
                        result = requests.post(
                            f"http://{server}:8000/write", json=mod_data, timeout=15
                        )
                        print(result.json())
                        if result.status_code != 200:
                            print(f"write to server {server} failed")
                        else:
                            result = result.json()

                            if (
                                result["status"] == "failure"
                                and result["message"] == "Does not have uptodate"
                            ):
                                curr_commit_index = result["last_commit"]
                                logger[shard].get_requests_from_given_index(shard, int(curr_commit_index) + 1)
                                list_to_update = logger[shard].get_requests_from_given_index(shard, int(commit_index) + 1)
                                print(list_to_update)
                                resp = requests.post(
                                    f"http://{server}:8000/update_local_data",
                                    json={"shards": shard,"data":list_to_update,"servers":server},
                                )
                                resp = resp.json()
                                print(resp)

                            else:
                                votes += 1
                                print("got a vote")
                                break

                                
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
            commit_index = req["commit"]
            curr_commit_index = logger[shard].get_last_log_id()
            if commit_index != curr_commit_index:
                return {
                    "message": "Does not have upto date",
                    "last_commit": curr_commit_index,
                    "status": "failure",
                }
            for row in data:
                id = int(logger[shard].get_last_log_id()) + 1
                log = Log(id, LogType(0), row, datetime.now())
                logger[shard].add_log(log)
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
        print(f"Exception type: {type(e)}, Exception: {e}")
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


        if primary[shard] == 1:
            print(" I am primary")
            secondary_servers = req["secondary_servers"]
            votes = 0
            mod_data = {"shard": shard, "Stud_id": stud_id, "data": data,"commit": commit[shard]}

            id = int(logger[shard].get_last_log_id()) + 1
            log = Log(
                id,
                LogType(1),
                {"shard": shard, "Stud_id": stud_id, "data": data},
                datetime.now(),
            )
            logger[shard].add_log(log)
            for server in secondary_servers:
                while True:
                    try:
                        print("sent a reqest to server ", server)
                        result = requests.put(
                            f"http://{server}:8000/update", json=mod_data
                        )
                        print(result.json())
                        if result.status_code != 200:
                            print(f"update to server {server} failed")

                            # return JSONResponse(
                            #     status_code=400,
                            #     content={
                            #         "message": f"updates to server {server} failed",
                            #         # "data entries written successfully":data_written,
                            #         "status": "failure",
                            #     },
                            # )
                        else:
                            result = result.json()

                            if (
                                result["status"] == "failure"
                                and result["message"] == "Does not have uptodate"
                            ):
                                curr_commit_index = result["last_commit"]
                                logger[shard].get_requests_from_given_index(shard, int(curr_commit_index) + 1)
                                list_to_update = logger[shard].get_requests_from_given_index(shard, int(commit_index) + 1)
                                print(list_to_update)
                                resp = requests.post(
                                    f"http://{server}:8000/update_local_data",
                                    json={"shards": shard,"data":list_to_update,"servers":server},
                                )
                                resp = resp.json()
                                print(resp)

                            else:
                                votes += 1
                                print("got a vote")
                                break
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
            commit_index = req["commit"]
            curr_commit_index = logger[shard].get_last_log_id()
            if commit_index != curr_commit_index:
                return {
                    "message": "Does not have upto date",
                    "last_commit": curr_commit_index,
                    "status": "failure",
                }
            id = int(logger[shard].get_last_log_id()) + 1
            log = Log(
                id,
                LogType(1),
                {"shard": shard, "Stud_id": stud_id, "data": data},
                datetime.now(),
            )
            logger[shard].add_log(log)
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

        
        if primary[shard] == 1:
            print(" I am primary")
            secondary_servers = req["secondary_servers"]
            votes = 0
            mod_data = {"shard": req["shard"], "Stud_id": req["Stud_id"],"commit": commit[shard]}
            id = int(logger[shard].get_last_log_id()) + 1
            log = Log(id, LogType(2), {"Stud_id": stud_id}, datetime.now())
            logger[shard].add_log(log)
            for server in secondary_servers:
                while True:
                    try:
                        print("sent a reqest to server ", server)
                        result = requests.delete(
                            f"http://{server}:8000/del", json=mod_data, timeout=15
                        )
                        print(result.json())
                        if result.status_code != 200:
                            print(f"delete to server {server} failed")
                            # return JSONResponse(
                            #     status_code=400,
                            #     content={
                            #         "message": f"delete to server {server} failed",
                            #         # "data entries written successfully":data_written,
                            #         "status": "failure",
                            #     },
                            # )
                        else:
                            result = result.json()

                            if (
                                result["status"] == "failure"
                                and result["message"] == "Does not have uptodate"
                            ):
                                curr_commit_index = result["last_commit"]
                                logger[shard].get_requests_from_given_index(shard, int(curr_commit_index) + 1)
                                list_to_update = logger[shard].get_requests_from_given_index(shard, int(commit_index) + 1)
                                print(list_to_update)
                                resp = requests.post(
                                    f"http://{server}:8000/update_local_data",
                                    json={"shards": shard,"data":list_to_update,"servers":server},
                                )
                                resp = resp.json()
                                print(resp)

                            else:
                                votes += 1
                                print("got a vote")
                                break
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
            commit_index = req["commit"]
            curr_commit_index = logger[shard].get_last_log_id()
            if commit_index != curr_commit_index:
                return {
                    "message": "Does not have upto date",
                    "last_commit": curr_commit_index,
                    "status": "failure",
                }
            print(" I am secondary")
            id = int(logger[shard].get_last_log_id()) + 1
            log = Log(id, LogType(2), {"Stud_id": stud_id}, datetime.now())
            logger[shard].add_log(log)
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
