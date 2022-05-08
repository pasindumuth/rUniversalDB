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
cargo run --release --bin simtest -- -i 19

cargo run --bin client 2>/dev/null
docker run -it runiversal
## Run & Stop
docker run --cap-add=NET_ADMIN -it --name=rclient --ip 172.19.0.2 --network=runiversal-net runiversal scripts/client -i 172.19.0.2
docker run --cap-add=NET_ADMIN -it --name=runiversal10 --ip 172.19.0.10 --network=runiversal-net runiversal scripts/transact -i 172.19.0.10 -t masterbootup
docker run --cap-add=NET_ADMIN -d --name=runiversal15 --ip 172.19.0.15 --network=runiversal-net runiversal scripts/transact -i 172.19.0.15 -t freenode -f newslave -e 172.19.0.10

docker kill rclient; docker container rm rclient;
docker kill runiversal10; docker container rm runiversal10;
docker kill runiversal15; docker container rm runiversal15;

## Workflow 1
1. start masters and client, run the following:
startmaster 172.19.0.10 172.19.0.11 172.19.0.12 172.19.0.13 172.19.0.14
2. start the slaves, run the following:
target 172.19.0.10
CREATE TABLE users(id INT PRIMARY KEY);
target 172.19.0.15
INSERT INTO users(id) VALUES (1), (2), (3);
SELECT id FROM users;

target 172.19.0.10
ALTER TABLE users ADD name STRING;
target 172.19.0.15
UPDATE users SET name = 'henry' WHERE id = 2;
SELECT * FROM users;

4. run the following
target 172.19.0.10
CREATE TABLE inventory(id INT PRIMARY KEY, name VARCHAR);
target 172.19.0.15
INSERT INTO inventory(id, name) VALUES (1, 'pasindu'), (2, 'hello');
SELECT id, name FROM inventory;

## Workflow 2
Run the following:

./run masters
./run client_fast
./run slaves
./run client
setmaster 172.19.0.10 172.19.0.11 172.19.0.12 172.19.0.13 172.19.0.14

After this, we can set `target` and fire SQL queries, as Workflow 1.