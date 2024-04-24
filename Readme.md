# Distributed Systems Assignment 2

<!-- ![System Diagram](./shardedDbSystemDiagram.png)
 -->
 <p align="center">
  <img src="./shardedDbSystemDiagram.png" alt="Description of the image">
</p>

## Server

- The server containers handle 2 different shards (one replica each) 
- This has the following endpoints:
    - `Endpoint (/config, method=POST)`
    - `Endpoint (/heartbeat, method=GET)`
    - `Endpoint (/copy, method=GET)`
    - `Endpoint (/read, method=POST)`
    - `Endpoint (/write, method=POST)`
    - `Endpoint (/update, method=POST)`
    - `Endpoint (/del, method=DELETE)`

## Loadbalancer

- The loadbalancer stores the information regarding each shard
- The mapping of replica of shard to server
- This has the following endpoints:
    - `Endpoint (/init, method=POST)`
    - `Endpoint (/status, method=GET)`
    - `Endpoint (/add, method=POST)`
    - `Endpoint (/rm, method=POST)`
    - `Endpoint (/read, method=GET)`
    - `Endpoint (/write, method=POST)`
    - `Endpoint (/update, method=PUT)`
    - `Endpoint (/del, method=DELETE)`
- If any server is down the loadbalancer spawns another server

## Shard Manager

## Client

- Client sends the requests to the loadbalancer.
- Client recieves response from the server via loadbalancer.

## Run Locally
Ensure docker, docker-compose and jupyter-notebook are installed.

Clone the project

```bash
  git clone https://github.com/Venkatasai-102/DS-Asgn2.git
```

Go to the project directory

```bash
  cd DS-Asgn2
```

Install dependencies (For Analysis part)  


```bash
  pip3 install requests, aiohttp, matplotlib, 
```

Build docker images for Loadbalancer and Server

```bash
  sudo make build
```
Start the Loadbalancer

```bash
  sudo make 
```
Now run the code from **analysis.ipynb**z (covers task A1, A2, A3) and **analysis-task-A4.ipynb** (covers task A4)

Remove all containers

```bash
  sudo make clean
```

## Analysis

1. In the default configuration, time taken for 10,000 write requests is `559.99 seconds` and time taken for 10,000 read requests is `75.85 seconds`.

2. After increasing the number of shard replicas to 7, the time taken for 10,000 write requests is `397.35 seconds` and time taken for 10,000 read requests is `55.96 seconds`.

3. After increasing the number of servers to 10 and increasing the number of shards to 6 and replicas to 8, the time taken for 10,000 write requests is `1768 seconds` and time taken for 10,000 read requests is `61.14 seconds`

4. All the endpoints are tested and everything is working perfectly fine. After dropping a server container the load balancer is successfully spawns a new container and copies the shard entries present in other replicas.


# Design Choice

We used Passive Replication

One primary is selected and it sends all write requests to other servers
Secondary servers updates their data once they get request from primary and send an ack to the primary
Once primary gets enough votes it updates data locally and commits the data
If secondary crashes then the shard manager respawns a new server. It gets the data from the primary.
If primary crashes then shard manager along with respawning new server, starts leader election. It gets the data from the newly elected leader. 

### Leader Election
All the servers send their latest commit log index to the shard manager. 
Shard manager selects one with highest commit index or any one of the server if there are multiple servers with same log maximum commited log index.
