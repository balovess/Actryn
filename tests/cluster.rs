#![cfg(feature = "experimental-rpc")]

use actryn::cluster::{ClusterClient, ClusterServerBuilder};
use actryn::{ActorError, ActorRuntime, Decision, DurableActor, SqliteStorage};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::TcpListener;

struct Account;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Balance {
    value: u64,
}
#[derive(Serialize, Deserialize)]
struct Add(u64);
impl DurableActor for Account {
    const KIND: &'static str = "cluster.account.v1";
    type State = Balance;
    type Command = Add;
    type Reply = u64;
    fn initial(_key: &str) -> Balance {
        Balance { value: 0 }
    }
    fn decide(s: &Balance, c: Add) -> Result<Decision<Balance, u64>, ActorError> {
        let next = s
            .value
            .checked_add(c.0)
            .ok_or_else(|| ActorError::Rejected("overflow".into()))?;
        Ok(Decision::new(Balance { value: next }, next))
    }
}
fn key_on(shard: usize) -> String {
    for n in 0..100_000 {
        let key = format!("account:{n}");
        if SqliteStorage::shard_for(Account::KIND, &key) == shard {
            return key;
        }
    }
    panic!("failed to find key for shard {shard}")
}
fn node(
    path: &std::path::Path,
    id: &str,
    endpoint: &str,
    shards: Vec<usize>,
    ttl: Duration,
) -> (SqliteStorage, ActorRuntime, ClusterClient) {
    let store = SqliteStorage::open(path).unwrap();
    let runtime = ActorRuntime::builder()
        .storage(store.clone())
        .node_id(id)
        .shards(2)
        .claim_shards(endpoint.to_owned(), shards, ttl)
        .register_durable::<Account>()
        .unwrap()
        .build()
        .unwrap();
    let client = ClusterClient::new(runtime.clone(), store.clone(), id);
    (store, runtime, client)
}

#[tokio::test]
async fn two_owners_remote_calls_and_fenced_failover() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cluster.db");
    let lease = Duration::from_secs(2);
    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap().to_string();
    let (store_a, rt_a, client_a) = node(&path, "node-a", &addr_a, vec![0], lease);
    let server_a = ClusterServerBuilder::new()
        .register::<Account>()
        .unwrap()
        .serve(listener_a, rt_a.clone())
        .unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap().to_string();
    let (_store_b, rt_b, client_b) = node(&path, "node-b", &addr_b, vec![1], lease);
    let server_b = ClusterServerBuilder::new()
        .register::<Account>()
        .unwrap()
        .serve(listener_b, rt_b.clone())
        .unwrap();
    let key_a = key_on(0);
    let key_b = key_on(1);
    assert_eq!(
        client_a
            .command::<Account>(&key_a, "a:1", Add(4))
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        client_b
            .command::<Account>(&key_a, "a:2", Add(3))
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        client_a
            .command::<Account>(&key_b, "b:1", Add(6))
            .await
            .unwrap(),
        6
    );
    assert_eq!(client_b.read::<Account>(&key_a).await.unwrap().value, 7);
    assert_eq!(
        client_b
            .command::<Account>(&key_a, "a:2", Add(3))
            .await
            .unwrap(),
        7
    );
    assert!(matches!(
        client_b.command::<Account>(&key_a, "a:2", Add(999)).await,
        Err(ActorError::RequestIdConflict)
    ));
    // Simulate lease expiry. The runtime observes the renewal failure and closes its RPC server.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE shard_owners SET expires_at_ms=0 WHERE shard_id=0",
            [],
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(store_a.locate(0).await.unwrap().is_none());
    assert!(matches!(
        client_b.read::<Account>(&key_a).await,
        Err(ActorError::NoOwner)
    ));
    // Replacement owner gets a strictly newer epoch and reloads persisted state.
    let listener_c = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_c = listener_c.local_addr().unwrap().to_string();
    let (_store_c, rt_c, client_c) = node(&path, "node-c", &addr_c, vec![0], lease);
    let server_c = ClusterServerBuilder::new()
        .register::<Account>()
        .unwrap()
        .serve(listener_c, rt_c.clone())
        .unwrap();
    assert_eq!(
        client_b
            .command::<Account>(&key_a, "a:3", Add(5))
            .await
            .unwrap(),
        12
    );
    assert_eq!(client_c.read::<Account>(&key_a).await.unwrap().value, 12);
    assert!(matches!(
        rt_a.durable::<Account>(&key_a)
            .command("stale", Add(100))
            .await,
        Err(ActorError::OwnershipLost)
    ));
    assert_eq!(client_b.read::<Account>(&key_b).await.unwrap().value, 6);
    server_a.shutdown().await;
    server_b.shutdown().await;
    server_c.shutdown().await;
    rt_a.shutdown().await.unwrap();
    rt_b.shutdown().await.unwrap();
    rt_c.shutdown().await.unwrap();
}
