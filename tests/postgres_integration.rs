#![cfg(feature = "postgres")]

//! Run against an isolated disposable database:
//! ACTOR_TEST_DATABASE_URL=postgresql://... cargo test --features postgres --test postgres_integration

use std::time::Duration;

mod common;

use actryn::{
    ActorError, ActorRuntime, Decision, DurableActor, LOGICAL_SHARDS, PostgresStorage,
    StorageAdapter,
};
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

#[tokio::test]
async fn shard_lease_renewal_is_atomic_for_the_requested_set() {
    let url = common::database_url();
    let pool = common::pool(&url);
    let storage = PostgresStorage::new(pool.clone());
    storage.initialize_schema().await.unwrap();

    let first_shard = common::unleased_shard(&storage).await;
    let mut second_shard = None;
    for shard in 0..LOGICAL_SHARDS {
        if shard != first_shard
            && storage
                .locate(shard)
                .await
                .expect("read shard lease")
                .is_none()
        {
            second_shard = Some(shard);
            break;
        }
    }
    let second_shard =
        second_shard.expect("a second unleased shard is available in the dedicated test database");
    let test_id = common::unique_id();
    let node_a = format!("renew-node-a-{test_id}");
    let node_b = format!("renew-node-b-{test_id}");
    let lease = Duration::from_secs(30);
    let owner = storage
        .claim_selected(
            &node_a,
            "127.0.0.1:52003",
            &[first_shard, second_shard],
            lease,
        )
        .await
        .unwrap();
    let first_before = storage
        .locate(first_shard)
        .await
        .unwrap()
        .expect("first shard remains owned by node A");

    let connection = pool.get().await.unwrap();
    let second_id = i32::try_from(second_shard).unwrap();
    connection
        .execute(
            "UPDATE shard_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE shard_id = $1",
            &[&second_id],
        )
        .await
        .unwrap();
    drop(connection);
    storage
        .claim_selected(&node_b, "127.0.0.1:52004", &[second_shard], lease)
        .await
        .unwrap();

    let result = storage
        .renew_selected(owner, vec![first_shard, second_shard], lease)
        .await;
    assert!(matches!(result, Err(ActorError::OwnershipLost)));
    let first_after = storage
        .locate(first_shard)
        .await
        .unwrap()
        .expect("first shard remains owned by node A");
    assert_eq!(first_before.owner_id, first_after.owner_id);
    assert_eq!(first_before.epoch, first_after.epoch);
    assert_eq!(first_before.expires_at_ms, first_after.expires_at_ms);
    assert_eq!(
        storage
            .locate(second_shard)
            .await
            .unwrap()
            .expect("second shard is owned by node B")
            .owner_id,
        node_b
    );
}
