# Build & Run

cargo build

# Docker

## Setup
docker network create --subnet=172.19.0.0/16 runiversal-net

## Build
docker build -t runiversal -f Dockerfile.init .
docker build -t runiversal .

## Local Build and Test
cargo build

cargo run --release --bin paxos &&
cargo run --release --bin paxos2pc_sim &&
cargo run --release --bin simtest
cargo run --release --bin simtest -- -i 9

cargo run --bin client 2>/dev/null
docker run -it runiversal

## Run & Stop
docker run --cap-add=NET_ADMIN -it --name=rclient4 --ip 172.19.0.4 --network=runiversal-net runiversal scripts/client -i 172.19.0.4 -e 172.19.0.10
docker run --cap-add=NET_ADMIN -it --name=runiversal10 --ip 172.19.0.10 --network=runiversal-net runiversal scripts/transact -i 172.19.0.10 -t masterbootup
docker run --cap-add=NET_ADMIN -d --name=runiversal15 --ip 172.19.0.15 --network=runiversal-net runiversal scripts/transact -i 172.19.0.15 -t freenode -f newslave -e 172.19.0.10

docker kill rclient; docker container rm rclient;
docker kill runiversal10; docker container rm runiversal10;
docker kill runiversal15; docker container rm runiversal15;

## Setup
To start up the system and create an initial client, do:

./run start

To create extra clients, do:

./run new_client 3 10
./run new_node 25 reconfig 10
./run new_node 26 newslave 10
./run new_node 27 newslave 10
./run new_node 28 newslave 10
./run new_node 29 newslave 10
./run new_node 30 newslave 10
./run new_node 31 reconfig 10
./run new_node 32 reconfig 10
./run new_node 33 reconfig 10
./run new_node 34 reconfig 10
./run new_node 35 reconfig 10

master_target 172.19.0.1

To clean up everything, do:

./run clean
./run cclean 2
./run cclean 3
./run nclean 25
./run nclean 26

## Demo
1. Run `./run start` in terminal pane. This will start the MasterGroup, 2 SlaveGroups, and a client.
2. Run `./run new_client 3` to start a new client, then do `live` to start the live view.
3. Run the "Query Examples" 
4. Kill one of the Slave leaders with `./run nclean 16` (or similar).
5. Create a Slave free node so that it can replace the one that was just killed: `./run new_node 25 reconfig 10`
6. Create 5 Slaves as newslave, e.g. `./run new_node 26 newslave 10` to show how new SlaveGroups are formed automatically.
7. Run the `./run new_node 31 reconfig 10` commands to create lots of free nodes, then kill some more nodes to show the reconfiguration.

## Query Examples

CREATE TABLE users(id INT PRIMARY KEY);
INSERT INTO users(id) VALUES (1), (2), (3);
SELECT * FROM users;

ALTER TABLE users ADD name STRING;
UPDATE users SET name = 'henry' WHERE id = 2;
SELECT * FROM users;

CREATE TABLE inventory(id INT PRIMARY KEY, name VARCHAR);
INSERT INTO inventory(id, name) VALUES (1, 'pasindu'), (2, 'hello');
SELECT id, name FROM inventory;

DROP TABLE inventory;
