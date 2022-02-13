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

docker run -it --name=client --network=runiversal-net runiversal cargo run --bin client 172.19.0.3 1610


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
