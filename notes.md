# Build & Run

cargo build

# Docker

## Setup
docker network create --subnet=172.19.0.0/16 runiversal-net

## Build
docker build -t runiversal -f Dockerfile.init .
docker build -t runiversal .

## Run & Stop
### Only Slaves
docker kill universal0; docker container rm universal0;
docker kill universal1; docker container rm universal1;
docker kill universal2; docker container rm universal2;
docker kill universal3; docker container rm universal3;
docker kill universal4; docker container rm universal4;
docker kill client; docker container rm client;

docker run -it --name=universal0 --ip 172.19.0.3 --network=runiversal-net runiversal cargo run 0 172.19.0.3
docker run -it --name=universal1 --ip 172.19.0.4 --network=runiversal-net runiversal cargo run 1 172.19.0.4 172.19.0.3
docker run -it --name=universal2 --ip 172.19.0.5 --network=runiversal-net runiversal cargo run 2 172.19.0.5 172.19.0.3 172.19.0.4
docker run -it --name=universal3 --ip 172.19.0.6 --network=runiversal-net runiversal cargo run 3 172.19.0.6 172.19.0.3 172.19.0.4 172.19.0.5
docker run -it --name=universal4 --ip 172.19.0.7 --network=runiversal-net runiversal cargo run 4 172.19.0.7 172.19.0.3 172.19.0.4 172.19.0.5 172.19.0.6

docker run -it --name=client --network=runiversal-net runiversal stack run hUniversalDB-client-exe 172.19.0.3
