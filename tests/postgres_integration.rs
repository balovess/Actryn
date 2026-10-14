#![cfg(feature = "postgres")]

//! Run against an isolated disposable database:
//! ACTOR_TEST_DATABASE_URL=postgresql://... cargo test --features postgres --test postgres_integration

use std::time::Duration;

mod common;

use actryn::{ActorError, ActorRuntime, Decision, DurableActor, PostgresStorage};
use serde::{Deserialize, Serialize};

struct Wallet;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
struct Balance {
    amount: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct Add(u64);

impl DurableActor for Wallet {
    const KIND: &'static str = "test.postgres.wallet.v1";
    type State = Balance;
    type Command = Add;
    type Reply = u64;

    fn initial(_: &str) -> Self::State {
        Balance { amount: 0 }
    }

    fn decide(
        state: &Self::State,
        command: Add,
    ) -> Result<Decision<Self::State, Self::Reply>, ActorError> {
        let amount = state
            .amount
            .checked_add(command.0)
            .ok_or_else(|| ActorError::Rejected("overflow".into()))?;
        Ok(Decision::new(Balance { amount }, amount))
    }
}

#[tokio::test]
async fn fenced_handover_and_deduplication() {
    let url = common::database_url();
    let pool = common::pool(&url);
    let storage = PostgresStorage::new(pool.clone());
    storage.initialize_schema().await.unwrap();

    // Dedicated disposable DB required: shard ownership is global in the DB.
    let shard = common::unleased_shard(&storage).await;
    let key = common::key_on_shard(Wallet::KIND, shard);
    let test_id = common::unique_id();
    let ttl = Duration::from_secs(2);
    let a = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id(format!("pg-node-a-{test_id}"))
        .claim_shards("127.0.0.1:52001", vec![shard], ttl)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await
        .unwrap();

    assert_eq!(
        a.durable::<Wallet>(&key)
            .command("rid-1", Add(5))
            .await
            .unwrap(),
        5
    );
    // Runtime-owned renewal keeps a lease alive without a transport server.
    tokio::time::sleep(Duration::from_millis(2300)).await;
    // At most one owner can hold the same shard while its lease is live.
    let attempt = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id(format!("pg-node-b-{test_id}"))
        .claim_shards("127.0.0.1:52002", vec![shard], ttl)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await;
    assert!(matches!(attempt, Err(ActorError::OwnershipLost)));

    // Simulate a lease that expired while the owner was disconnected from its database.
    let connection = pool.get().await.unwrap();
    let shard_id = i32::try_from(shard).unwrap();
    connection
        .execute(
            "UPDATE shard_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE shard_id = $1",
            &[&shard_id],
        )
        .await
        .unwrap();
    drop(connection);
    let b = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id(format!("pg-node-b-{test_id}"))
        .claim_shards("127.0.0.1:52002", vec![shard], ttl)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await
        .unwrap();
    assert_eq!(
        b.durable::<Wallet>(&key)
            .command("rid-1", Add(5))
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        b.durable::<Wallet>(&key)
            .command("rid-2", Add(2))
            .await
            .unwrap(),
        7
    );
    assert!(matches!(
        a.durable::<Wallet>(&key).command("rid-3", Add(9)).await,
        Err(ActorError::OwnershipLost)
    ));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn postgres_requires_explicit_shards() {
    let url = common::database_url();
    let storage = PostgresStorage::new(common::pool(&url));
    let result = ActorRuntime::builder()
        .storage(storage)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await;
    assert!(matches!(result, Err(ActorError::InvalidConfiguration)));
}
