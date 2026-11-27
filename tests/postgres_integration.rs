#![cfg(feature = "postgres")]

//! Run against an isolated disposable database:
//! ACTOR_TEST_DATABASE_URL=postgresql://... cargo test --features postgres --test postgres_integration

use std::{
    future::pending,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

mod common;

use actryn::{
    ActorError, ActorRuntime, Decision, DurableActor, LOGICAL_SHARDS, Ownership, PostgresStorage,
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

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

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
async fn schema_migrations_are_idempotent_and_report_version() {
    let storage = PostgresStorage::new(common::pool(&common::database_url()));
    storage.migrate().await.unwrap();
    assert_eq!(storage.schema_version().await.unwrap(), 1);
    storage.migrate().await.unwrap();
    assert_eq!(storage.schema_version().await.unwrap(), 1);
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

#[tokio::test]
async fn postgres_process_crash_allows_fenced_stateful_takeover() {
    let url = common::database_url();
    let storage = PostgresStorage::new(common::pool(&url));
    storage.initialize_schema().await.unwrap();

    let shard = common::unleased_shard(&storage).await;
    let key = common::key_on_shard(Wallet::KIND, shard);
    let test_id = common::unique_id();
    let node_a = format!("crash-owner-a-{test_id}");
    let node_b = format!("crash-owner-b-{test_id}");
    let lease = Duration::from_secs(3);
    let directory = tempfile::tempdir().unwrap();
    let ready_file = directory.path().join("owner-ready.txt");
    let executable = std::env::current_exe().unwrap();
    let mut child = KillOnDrop(
        Command::new(executable)
            .args([
                "--ignored",
                "--exact",
                "postgres_crash_owner_child_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ACTOR_TEST_DATABASE_URL", &url)
            .env("ACTRYN_CRASH_CHILD_SHARD", shard.to_string())
            .env("ACTRYN_CRASH_CHILD_KEY", &key)
            .env("ACTRYN_CRASH_CHILD_NODE", &node_a)
            .env("ACTRYN_CRASH_CHILD_LEASE_MS", lease.as_millis().to_string())
            .env("ACTRYN_CRASH_CHILD_READY_FILE", &ready_file)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );

    let ready_deadline = Instant::now() + Duration::from_secs(15);
    while !ready_file.exists() {
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!("crash-owner process exited before acquiring its lease: {status}");
        }
        assert!(
            Instant::now() < ready_deadline,
            "crash-owner process did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let snapshot = std::fs::read_to_string(&ready_file).unwrap();
    let mut snapshot_lines = snapshot.lines();
    let stale_owner_id = snapshot_lines.next().unwrap().to_owned();
    let stale_epoch = snapshot_lines.next().unwrap().parse::<u64>().unwrap();
    assert_eq!(stale_owner_id, node_a);
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "crash-owner process should remain alive while its runtime owns the lease"
    );
    let active_location = storage.locate(shard).await.unwrap().unwrap();
    assert_eq!(active_location.owner_id, stale_owner_id);
    assert_eq!(active_location.epoch, stale_epoch);

    let premature_takeover = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id(node_b.clone())
        .claim_shards("127.0.0.1:52005", vec![shard], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await;
    assert!(matches!(premature_takeover, Err(ActorError::OwnershipLost)));
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "crash-owner process should still be alive before the forced crash"
    );

    child.0.kill().unwrap();
    let crash_status = child.0.wait().unwrap();
    assert!(
        !crash_status.success(),
        "the old owner must stop without running graceful shutdown"
    );

    let takeover_deadline = Instant::now() + lease + Duration::from_secs(5);
    loop {
        if storage.locate(shard).await.unwrap().is_none() {
            break;
        }
        assert!(
            Instant::now() < takeover_deadline,
            "crashed owner's lease did not expire in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let runtime_b = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id(node_b.clone())
        .claim_shards("127.0.0.1:52005", vec![shard], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await
        .unwrap();
    assert_eq!(
        runtime_b
            .durable::<Wallet>(&key)
            .command("after-process-crash", Add(2))
            .await
            .unwrap(),
        7,
        "the replacement owner continues from the state committed by the crashed process"
    );

    let mut stale_epochs = vec![0; LOGICAL_SHARDS];
    stale_epochs[shard] = stale_epoch;
    let stale_ownership = Ownership::new(stale_owner_id, stale_epochs).unwrap();
    assert!(matches!(
        storage.load(stale_ownership, Wallet::KIND, key).await,
        Err(ActorError::OwnershipLost)
    ));
    runtime_b.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "subprocess fixture for postgres_process_crash_allows_fenced_stateful_takeover"]
async fn postgres_crash_owner_child_process() {
    let url = common::database_url();
    let shard = std::env::var("ACTRYN_CRASH_CHILD_SHARD")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let key = std::env::var("ACTRYN_CRASH_CHILD_KEY").unwrap();
    let node_id = std::env::var("ACTRYN_CRASH_CHILD_NODE").unwrap();
    let lease = Duration::from_millis(
        std::env::var("ACTRYN_CRASH_CHILD_LEASE_MS")
            .unwrap()
            .parse::<u64>()
            .unwrap(),
    );
    let ready_file = std::env::var("ACTRYN_CRASH_CHILD_READY_FILE").unwrap();
    let storage = PostgresStorage::new(common::pool(&url));
    storage.initialize_schema().await.unwrap();
    let runtime = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id(node_id.clone())
        .claim_shards("127.0.0.1:52004", vec![shard], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build_async()
        .await
        .unwrap();

    assert_eq!(
        runtime
            .durable::<Wallet>(&key)
            .command("before-process-crash", Add(5))
            .await
            .unwrap(),
        5
    );
    let location = storage.locate(shard).await.unwrap().unwrap();
    assert_eq!(location.owner_id, node_id);
    std::fs::write(
        ready_file,
        format!("{}\n{}", location.owner_id, location.epoch),
    )
    .unwrap();

    pending::<()>().await;
}
