from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry
from globals import app, get_db, close_db, appLock
from helpers import create_server

# todo : need to elect a new primary leader if the primary leader is dead
CHECK_INTERVAL = 40  # 1 minute

G = {"to_remove": [], "to_add": []}


def is_alive(server):
    print(f"Checking health of {server} ........", flush=True)
    try:
        request_url = f"http://{app.server_list[server]['ip']}:{8000}/heartbeat"

        req_session = requests.Session()
        retries = Retry(
            total=3, backoff_factor=0.1, status_forcelist=[500, 501, 502, 503, 504]
        )

        req_session.mount("http://", HTTPAdapter(max_retries=retries))

        response = req_session.get(request_url, timeout=0.5)

        print(f"<+> Server {server} is alive", flush=True)

        return True
    except requests.RequestException as e:
        print(f"<!> Server {server} is dead; Error: {e}", flush=True)

        return False


def respawn_dead_server(dead_server, conn, cursor):
    global G
    new_server = f"new_{dead_server}"

    print(f"Respawning {dead_server} as {new_server} ......", flush=True)

    old_server_data = app.server_list.pop(dead_server)
    print('old servers data',old_server_data)
    name, ipaddr = create_server(name=new_server)
    shards, index = old_server_data["shards"], old_server_data["index"]

    # make /config request to the new server
    payload = {
        "schema": {
            "columns": ["Stud_id", "Stud_name", "Stud_marks"],
            "dtypes": ["Number", "String", "String"],
        },
        "shards": shards,
    }
    print(payload)
    while True:
        print('Well, I am here')
        try:
            print("respawn dead server payload : ", payload)
            result = requests.post(
                f"http://{ipaddr}:{8000}/config", json=payload, timeout=1
            )
            print(result.ok)
            break
        except requests.RequestException as e:
            print("trying again")
            sleep(1)

    app.server_list[new_server] = {"index": index, "ip": ipaddr, "shards": shards}

    print(f"New server {new_server} is created with IP {ipaddr}!", flush=True)

    print(f"Copying shard data from other servers ....... ", end=" ", flush=True)
    print(shards, flush=True)
    for sh in shards:
        # changing the hash info
        print(f".....Restoring {sh} ", flush=True)
        G["to_remove"].append((sh, index))
        G["to_add"].append((sh, index, ipaddr, 8000))
        # need to handle this in load balancer
        # app.hash_dict[sh].remove_server(index)
        # app.hash_dict[sh].add_server(index, ipaddr, 8000)
        cursor.execute(
            "Select Server_id, Shard_id, Primary_ FROM MapT WHERE Server_id=%s AND Shard_id=%s AND Primary_=1",
            (
                dead_server,
                sh,
            ),
        )
        mapt_rows = cursor.fetchall()
        print("fetched mapt rows : ", mapt_rows)
        PRIMARY_SERVER = None
        for row in mapt_rows:
            print("each row : ", row)
            if row[2] == 1:
                PRIMARY_SERVER = row[0]

        # Remove the shard - old server mapping from database       #TODO
        cursor.execute(
            "DELETE FROM MapT WHERE Server_id=%s AND Shard_id=%s",
            (
                dead_server,
                sh,
            ),
        )
        conn.commit()

        while PRIMARY_SERVER is None or PRIMARY_SERVER == dead_server:
            print("respawn dead server is calling elect primary")
            print("-------------------------------------------")
            print("-------------------------------------------")
            elect_primary()
            cursor.execute(
                "Select Server_id, Shard_id, Primary_ FROM MapT WHERE Shard_id=%s AND Primary_=1",
                (sh,),
            )
            mapt_rows = cursor.fetchall()
            print("fetched mapt rows : ", mapt_rows)
            for row in mapt_rows:
                print("each row : ", row)
                if row[2] == 1:
                    PRIMARY_SERVER = row[0]

        try:
            resp = requests.get(
                f"http://{ipaddr}:8000/fetch_log_data",
                json={"shards": sh, "servers": PRIMARY_SERVER, "own_ip": ipaddr},
            )
            print("the response is : ")
            print(resp.json())
        except requests.RequestException as e:
            print(e)
            print(f"Request to {ipaddr} failed", flush=True)

        # add the shard - new server mapping to database
        cursor.execute("INSERT INTO MapT VALUES(%s,%s,0)", (sh, new_server))
        conn.commit()
        print(
            f"Successfully inserted shard:{sh} to server:{new_server} mapping into MapT",
            flush=True,
        )

    print("Done!", flush=True)
    # clean up down by check_server_health
    return {"message": "Successfully respawned dead servers", "status": "success"}


# def check_primary_util():


def elect_primary():
    print("entered elect primary")
    try:
        print("started election")
        mysql_conn, cursor = get_db()
        cursor.execute(f"SELECT distinct Shard_id FROM MapT")
        shards_rows = cursor.fetchall()
        cursor.execute(f"Select Shard_id from MapT where Primary_=1")
        primary_shard_rows = cursor.fetchall()
        print(primary_shard_rows)
        for row in shards_rows:
            shard = row[0]
            if shard in [shard_row[0] for shard_row in primary_shard_rows]:
                print("primary is there for ", shard)
                continue
            print(shard)
            cursor.execute(f"Select Server_id from MapT where Shard_id=%s", (shard,))
            servers_with_given_shard_rows = cursor.fetchall()
            print("servers with given shard rows : ")
            print(servers_with_given_shard_rows)
            votes = {}
            for row_server in servers_with_given_shard_rows:
                server = row_server[0]
                try:
                    rsp = requests.post(
                        f"http://{server}:8000/commit", json={"shards": [shard]}
                    )
                    rsp = rsp.json()
                    print(f"Response from server {server} is {rsp}")
                    for shard_value in rsp["shards"]:
                        votes[server] = rsp["shards"][shard_value]
                    print("votes as of now : ", votes)
                except requests.RequestException as e:
                    print(str(e))
                    continue

            max_votes = max(votes.items(), key=lambda x: x[1])
            max_votes_server = max_votes[0]
            print("checking who has majority")
            print("Elected Server:", max_votes)
            rsp = requests.post(
                f"http://{max_votes_server}:8000/primary", json={"shards": [shard]}
            )
            cursor.execute(
                f"Update MapT SET Primary_=1 where Server_id=%s AND Shard_id=%s",
                (max_votes_server, shard),
            )

            mysql_conn.commit()  # at last

        return {
            "message": "Successfully elected Primaries for all shards",
            "status": "success",
        }

        # app.server_list
    except Exception as e:
        print(f"Error: {e}", flush=True)


def check_server_health():
    while 1:
        try:

            # acquire  lock on app
            appLock.acquire()
            print("Checking server health ....", flush=True)
            server_names = list(app.server_list.keys())
            print("Server names: ", flush=True)
            print(server_names)
            conn, cursor = get_db()
            for server in server_names:
                if not is_alive(server):
                    print(app.server_list)
                    respawn_dead_server(server, conn, cursor)

        except Exception as e:
            print(f"Error: {e}", flush=True)

        finally:
            close_db(conn, cursor)
            print("finished checking server health", flush=True)
            if G["to_remove"]:
                rsp = requests.post(
                    "http://lb:8000/sync_app",
                    json={
                        "server_list": app.server_list,
                        "to_remove": G["to_remove"],
                        "to_add": G["to_add"],
                        # need to add information about primary server
                    },
                )
            appLock.release()

        sleep(CHECK_INTERVAL)
