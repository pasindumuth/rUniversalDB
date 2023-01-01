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
To build:

./run build

To start up the system and create an initial client, do:

./run start

To create extra clients and nodes, do:

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
slave_target 172.19.0.16

To clean up everything, do:

./run clean
./run cclean 2
./run cclean 3
./run nclean 25
./run nclean 26
./run dclean

## Demo
1. Run `./run start` in terminal pane. This will start the MasterGroup, 2 SlaveGroups, and a client. Run `live` in that view
3. Run `./run new_client 3 10` to start a new client.
2. Explicitly connect to `172.19.0.15` with `slave_target 172.19.0.15` (the Leadership for the first SlaveGroup). (This is useful for showcasing node deletion later.)
4. Run the Basic/Advanced Queries.
5. Kill `172.19.0.15` with `./run nclean 15` (or similar). (This shows reconfiguration.)
6. Create a Slave free node so that it can replace the one that was just killed: `./run new_node 25 reconfig 10`
7. Explicitly connect to `172.19.0.17` with `slave_target 172.19.0.17` and then fire some queries (just to show that new leaders are actually possible to use).
8. Create 5 Slaves as newslave, e.g. `./run new_node 26 newslave 10` to show how new SlaveGroups are formed automatically.
9. Explicitly connect to `172.19.0.26` with `slave_target 172.19.0.26` and then fire some queries (just to show that new Groups are actually used).
10. Run the `./run new_node 31 reconfig 10` commands to create lots of free nodes.
11. Kill `172.19.0.26` with `./run nclean 26` (or similar). (This shows reconfiguration, immediately follows by the consumption of a free node.)
12. Explicitly connect to `172.19.0.29` with `slave_target 172.19.0.29` and then fire some queries (just to show that new Groups are actually used). 
13. Quit the live system with `q`, and call `./run dclean` to clean up.

## Basic Queries
```sql
CREATE TABLE user(id INT PRIMARY KEY);
INSERT INTO user(id) VALUES (1), (2), (3);
SELECT * FROM user;

ALTER TABLE user ADD name STRING;
UPDATE user SET name = 'henry' WHERE id = 2;
SELECT * FROM user;

CREATE TABLE inventory(id INT PRIMARY KEY, name VARCHAR);
INSERT INTO inventory(id, name) VALUES (1, 'pasindu'), (2, 'hello');
SELECT id, name FROM inventory;

DROP TABLE user;
DROP TABLE inventory;
```

## Advanced Queries

### DDL
```sql
CREATE TABLE inventory (
  product_id INT PRIMARY KEY, email VARCHAR, 
  count INT
);
-- Separate
INSERT INTO inventory (product_id, email, count) 
VALUES 
  (0, 'my_email_0', 15), 
  (1, 'my_email_1', 25);
-- Separate
CREATE TABLE user (
  email VARCHAR PRIMARY KEY, balance INT, 
);
-- Separate
INSERT INTO user (email, balance) 
VALUES 
  ('my_email_0', 50), 
  ('my_email_1', 60), 
  ('my_email_2', 70);
-- Separate
CREATE TABLE product_stock (id INT PRIMARY KEY, product_id INT,);
-- Separate
INSERT INTO product_stock (id, product_id) 
VALUES 
  (0, 0), 
  (1, 1), 
  (2, 1);
```
### DQL

```sql
-- Join
SELECT U2.email, U1.balance, product_id
FROM user AS U2 JOIN (user AS U1 LEFT JOIN inventory AS I)
    ON ((SELECT count(id) 
        FROM product_stock
        WHERE product_id = I.product_id) = 2)
    AND U1.balance <= 60
WHERE U2.email = 'my_email_0';

-- CTEs
WITH
    v1 AS (SELECT email AS e, balance * 2
            FROM  user
            WHERE email = 'my_email_0')
SELECT *
FROM v1;

-- Multi Stage
UPDATE user
SET balance = balance + 20
WHERE email = (
    SELECT email
    FROM inventory
    WHERE product_id = 1);

UPDATE inventory
SET count = count + 5
WHERE email = (
    SELECT email
    FROM user
    WHERE balance >= 80);
```