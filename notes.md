# Build & Run

cargo build

# Docker

## Setup
docker network create --subnet=172.19.0.0/16 runiversal-net

## Build
docker build -t runiversal -f Dockerfile.init .
docker build -t runiversal .

## Local Build and Test
cargo build --release &&
cargo run --release --bin paxos 2>/dev/null &&
cargo run --release --bin paxos2pc_sim 2>/dev/null &&
cargo run --release --bin simtest 2>/dev/null

cargo run --bin client 2>/dev/null

## Run & Stop
docker run --cap-add=NET_ADMIN -it --name=rclient --ip 172.19.0.2 --network=runiversal-net runiversal scripts/client -i 172.19.0.2
docker run --cap-add=NET_ADMIN -it --name=runiversal10 --ip 172.19.0.10 --network=runiversal-net runiversal scripts/transact -i 172.19.0.10 -t masterbootup
docker run --cap-add=NET_ADMIN -d --name=runiversal15 --ip 172.19.0.15 --network=runiversal-net runiversal scripts/transact -i 172.19.0.15 -t freenode -f newslave -e 172.19.0.10

docker kill rclient; docker container rm rclient;
docker kill runiversal10; docker container rm runiversal10;
docker kill runiversal15; docker container rm runiversal15;

1. start masters and client, run the following:
startmaster 172.19.0.10 172.19.0.11 172.19.0.12 172.19.0.13 172.19.0.14
3. start the slaves, run the following:
target 172.19.0.10
CREATE TABLE users(id INT PRIMARY KEY);
target 172.19.0.15
INSERT INTO users(id) VALUES (1), (2), (3);
SELECT id FROM users;

## Code
Example of implementing Debug for a struct:

```rust

impl Debug for SlaveContext {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut debug_trait_builder = f.debug_struct("SlaveContext");
    let _ = debug_trait_builder.field("coord_positions", &self.coord_positions);
    let _ = debug_trait_builder.field("this_sid", &self.this_sid);
    let _ = debug_trait_builder.field("this_gid", &self.this_gid);
    let _ = debug_trait_builder.field("this_eid", &self.this_eid);
    let _ = debug_trait_builder.field("gossip", &self.gossip);
    let _ = debug_trait_builder.field("leader_map", &self.leader_map);
    let _ = debug_trait_builder.field("network_driver", &self.network_driver);
    let _ = debug_trait_builder.field("slave_bundle", &self.slave_bundle);
    let _ = debug_trait_builder.field("tablet_bundles", &self.tablet_bundles);
    debug_trait_builder.finish()
  }
}

```

Example (unrefined) of impelmenting Debug for an enum:

```rust
  impl ::core::fmt::Debug for SlaveTimerInput {
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
      match (&*self,) {
        (&SlaveTimerInput::PaxosTimerEvent(ref __self_0),) => {
          let mut debug_trait_builder = f.debug_tuple("PaxosTimerEvent");
          let _ = debug_trait_builder.field(&&(*__self_0));
          debug_trait_builder.finish()
        }
        (&SlaveTimerInput::RemoteLeaderChanged,) => {
          let mut debug_trait_builder = f.debug_tuple("RemoteLeaderChanged");
          debug_trait_builder.finish()
        }
      }
    }
  }
```


```
  let successful_queries = sorted_success_res.len() as u32;
  let mut count = 0;
  for (_, (req, res)) in sorted_success_res {
    println!(
      "         QUERY {:?}:
                Req: {:?}
                Res: {:?}",
      count, req, res
    );
    count += 1;
    context.send_query(&mut sim, req.query.as_str(), 10000, res.result);
  }
```
