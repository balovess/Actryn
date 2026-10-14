#![cfg(feature = "postgres")]

mod common;

use std::time::Duration;

use actryn::{
    ActorError, CommitRequest, CommitResult, OutboxEffect, Ownership, PostgresStorage,
    StorageAdapter,
};
use deadpool_postgres::Pool;

const KIND: &str = "test.postgres.fault.v1";

fn request(ownership: Ownership, key: &str, request_id: &str, state: &[u8]) -> CommitRequest {
    CommitRequest {
        ownership,
        kind: KIND,
        key: key.to_owned(),
        request_id: request_id.to_owned(),
        request_hash: [0x54; 32],
        expected_version: 0,
        state: state.to_vec(),
        reply: b"committed-reply".to_vec(),
        effects: vec![OutboxEffect::new(
            "test.destination.v1",
            format!("destination-{request_id}"),
            b"committed-effect".to_vec(),
        )],
    }
}

async fn install_trigger(
    pool: &Pool,
    table: &str,
    key: &str,
    suffix: &str,
    body: &str,
) -> (String, String) {
    let trigger = format!("actryn_test_{suffix}_trigger");
    let function = format!("actryn_test_{suffix}_function");
    let sql = format!(
        "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN {body}; RETURN NEW; END; $$;
         CREATE TRIGGER {trigger} BEFORE INSERT ON {table}
         FOR EACH ROW WHEN (NEW.actor_key = '{key}') EXECUTE FUNCTION {function}();"
    );
    pool.get()
        .await
        .expect("get test database connection")
        .batch_execute(&sql)
        .await
        .expect("install fault-injection trigger");
    (trigger, function)
}

async fn remove_trigger(pool: &Pool, table: &str, trigger: &str, function: &str) {
    let sql = format!(
        "DROP TRIGGER IF EXISTS {trigger} ON {table}; DROP FUNCTION IF EXISTS {function}();"
    );
    pool.get()
        .await
        .expect("get test database connection")
        .batch_execute(&sql)
        .await
        .expect("remove fault-injection trigger");
}

#[tokio::test]
async fn postgres_rolls_back_state_receipt_and_outbox_together() {
    let url = common::database_url();
    let pool = common::pool(&url);
    let storage = PostgresStorage::new(pool.clone());
    storage.initialize_schema().await.unwrap();
    let shard = common::unleased_shard(&storage).await;
    let owner_id = format!("atomic-owner-{}", common::unique_id());
    let ownership = storage
        .claim_selected(
            &owner_id,
            "127.0.0.1:55101",
            &[shard],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let key = common::key_on_shard(KIND, shard);
    let request_id = format!("atomic-request-{}", common::unique_id());
    let (trigger, function) = install_trigger(
        &pool,
        "actor_outbox",
        &key,
        &common::unique_id().replace('-', "_"),
        "RAISE EXCEPTION 'injected outbox insert failure'",
    )
    .await;

    let result = storage
        .commit(request(
            ownership.clone(),
            &key,
            &request_id,
            b"must-rollback",
        ))
        .await;
    assert!(matches!(result, Err(ActorError::Storage(_))));
    remove_trigger(&pool, "actor_outbox", &trigger, &function).await;

    assert_eq!(
        storage
            .load(ownership.clone(), KIND, key.clone())
            .await
            .unwrap(),
        None,
        "state written before the injected failure must roll back"
    );
    assert_eq!(
        storage
            .receipt(ownership.clone(), KIND, key.clone(), request_id.clone())
            .await
            .unwrap(),
        None,
        "receipt written before the injected failure must roll back"
    );
    assert!(
        storage
            .pending_outbox(10_000)
            .await
            .unwrap()
            .iter()
            .all(|record| record.request_id != request_id)
    );

    assert!(matches!(
        storage
            .commit(request(
                ownership,
                &key,
                &request_id,
                b"committed-after-retry"
            ))
            .await
            .unwrap(),
        CommitResult::Applied
    ));
}

#[tokio::test]
async fn postgres_connection_loss_during_commit_leaves_no_partial_write() {
    let url = common::database_url();
    let suffix = common::unique_id().replace('-', "_");
    let application_name = format!("actryn_kill_{suffix}");
    let storage_pool = common::pool_with_options(&url, &application_name, None);
    let admin_pool = common::pool(&url);
    let storage = PostgresStorage::new(storage_pool.clone());
    storage.initialize_schema().await.unwrap();
    let shard = common::unleased_shard(&storage).await;
    let owner_id = format!("disconnect-owner-{suffix}");
    let ownership = storage
        .claim_selected(
            &owner_id,
            "127.0.0.1:55102",
            &[shard],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let key = common::key_on_shard(KIND, shard);
    let request_id = format!("disconnect-request-{suffix}");
    let (trigger, function) = install_trigger(
        &admin_pool,
        "actor_outbox",
        &key,
        &suffix,
        "PERFORM pg_sleep(30)",
    )
    .await;

    let commit_storage = storage.clone();
    let commit_request = request(ownership.clone(), &key, &request_id, b"interrupted");
    let commit = tokio::spawn(async move { commit_storage.commit(commit_request).await });
    let admin = admin_pool.get().await.unwrap();
    let pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(row) = admin
                .query_opt(
                    "SELECT pid FROM pg_stat_activity
                     WHERE application_name = $1 AND state = 'active'
                       AND query LIKE 'INSERT INTO actor_outbox%' LIMIT 1",
                    &[&application_name],
                )
                .await
                .unwrap()
            {
                break row.get::<_, i32>(0);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("commit reached the injected outbox stall");
    let terminated: bool = admin
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated, "the in-flight commit backend was terminated");
    let result = commit.await.unwrap();
    assert!(matches!(result, Err(ActorError::Storage(_))));
    remove_trigger(&admin_pool, "actor_outbox", &trigger, &function).await;

    assert_eq!(
        storage
            .load(ownership.clone(), KIND, key.clone())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        storage
            .receipt(ownership.clone(), KIND, key.clone(), request_id.clone())
            .await
            .unwrap(),
        None
    );
    assert!(
        admin_pool
            .get()
            .await
            .unwrap()
            .query(
                "SELECT outbox_id FROM actor_outbox WHERE request_id = $1",
                &[&request_id],
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        storage
            .commit(request(
                ownership.clone(),
                &key,
                &request_id,
                b"retry-after-disconnect",
            ))
            .await
            .unwrap(),
        CommitResult::Applied
    ));
    assert_eq!(
        storage.load(ownership, KIND, key).await.unwrap(),
        Some((1, b"retry-after-disconnect".to_vec()))
    );
}

#[tokio::test]
async fn postgres_serialization_failure_retries_the_entire_transaction() {
    let url = common::database_url();
    let suffix = common::unique_id().replace('-', "_");
    let sequence = format!("actryn_test_{suffix}_sequence");
    let pool = common::pool_with_options(
        &url,
        &format!("actryn_retry_{suffix}"),
        Some("-c default_transaction_isolation=serializable"),
    );
    let storage = PostgresStorage::new(pool.clone());
    storage.initialize_schema().await.unwrap();
    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!("CREATE SEQUENCE {sequence}"))
        .await
        .unwrap();
    let body = format!(
        "IF nextval('{sequence}'::regclass) = 1 THEN
             RAISE EXCEPTION 'injected serialization failure' USING ERRCODE = '40001';
         END IF"
    );
    let shard = common::unleased_shard(&storage).await;
    let owner_id = format!("retry-owner-{suffix}");
    let ownership = storage
        .claim_selected(
            &owner_id,
            "127.0.0.1:55103",
            &[shard],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let key = common::key_on_shard(KIND, shard);
    let request_id = format!("retry-request-{suffix}");
    let (trigger, function) = install_trigger(&pool, "actor_states", &key, &suffix, &body).await;
    let result = storage
        .commit(request(
            ownership.clone(),
            &key,
            &request_id,
            b"after-retry",
        ))
        .await
        .unwrap();
    assert!(matches!(result, CommitResult::Applied));
    remove_trigger(&pool, "actor_states", &trigger, &function).await;
    let attempts: i64 = pool
        .get()
        .await
        .unwrap()
        .query_one(&format!("SELECT last_value FROM {sequence}"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(attempts, 2, "the complete transaction must run again");
    assert_eq!(
        storage.load(ownership, KIND, key).await.unwrap(),
        Some((1, b"after-retry".to_vec()))
    );
    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!("DROP SEQUENCE IF EXISTS {sequence}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_lease_takeover_waits_for_the_fenced_row_lock() {
    let url = common::database_url();
    let pool = common::pool(&url);
    let storage = PostgresStorage::new(pool.clone());
    storage.initialize_schema().await.unwrap();
    let shard = common::unleased_shard(&storage).await;
    let suffix = common::unique_id();
    let old_owner_id = format!("old-owner-{suffix}");
    let new_owner_id = format!("new-owner-{suffix}");
    let old_ownership = storage
        .claim_selected(
            &old_owner_id,
            "127.0.0.1:55104",
            &[shard],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let key = common::key_on_shard(KIND, shard);

    let mut admin = pool.get().await.unwrap();
    admin
        .execute(
            "UPDATE shard_leases SET expires_at = clock_timestamp() - INTERVAL '1 second'
             WHERE shard_id = $1",
            &[&(shard as i32)],
        )
        .await
        .unwrap();
    let lock_tx = admin.transaction().await.unwrap();
    lock_tx
        .query_one(
            "SELECT shard_id FROM shard_leases WHERE shard_id = $1 FOR SHARE",
            &[&(shard as i32)],
        )
        .await
        .unwrap();

    let takeover_storage = storage.clone();
    let takeover_owner_id = new_owner_id.clone();
    let takeover = tokio::spawn(async move {
        takeover_storage
            .claim_selected(
                &takeover_owner_id,
                "127.0.0.1:55105",
                &[shard],
                Duration::from_secs(30),
            )
            .await
    });
    let old_write_storage = storage.clone();
    let old_write_request = request(
        old_ownership.clone(),
        &key,
        &format!("stale-write-{suffix}"),
        b"stale-write",
    );
    let old_write = tokio::spawn(async move { old_write_storage.commit(old_write_request).await });

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !takeover.is_finished(),
        "lease takeover must wait while a transaction holds FOR SHARE"
    );
    lock_tx.commit().await.unwrap();

    let new_ownership = takeover.await.unwrap().unwrap();
    assert!(
        new_ownership.epoch_for_shard(shard).unwrap()
            > old_ownership.epoch_for_shard(shard).unwrap()
    );
    assert!(matches!(
        old_write.await.unwrap(),
        Err(ActorError::OwnershipLost)
    ));
    assert!(matches!(
        storage
            .commit(request(
                new_ownership.clone(),
                &key,
                &format!("new-write-{suffix}"),
                b"new-owner-state",
            ))
            .await
            .unwrap(),
        CommitResult::Applied
    ));
    assert_eq!(
        storage.load(new_ownership, KIND, key).await.unwrap(),
        Some((1, b"new-owner-state".to_vec()))
    );
}

#[tokio::test]
async fn postgres_statement_timeout_rolls_back_and_allows_same_id_retry() {
    let url = common::database_url();
    let suffix = common::unique_id().replace('-', "_");
    let bootstrap = PostgresStorage::new(common::pool(&url));
    bootstrap.initialize_schema().await.unwrap();
    let pool = common::pool_with_options(
        &url,
        &format!("actryn_timeout_{suffix}"),
        Some("-c statement_timeout=500"),
    );
    let timeout_storage = PostgresStorage::new(pool);
    let shard = common::unleased_shard(&bootstrap).await;
    let ownership = bootstrap
        .claim_selected(
            &format!("timeout-owner-{suffix}"),
            "127.0.0.1:55106",
            &[shard],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let key = common::key_on_shard(KIND, shard);
    let request_id = format!("timeout-request-{suffix}");
    let admin_pool = common::pool(&url);
    let (trigger, function) = install_trigger(
        &admin_pool,
        "actor_outbox",
        &key,
        &suffix,
        "PERFORM pg_sleep(2)",
    )
    .await;

    let timed_out = timeout_storage
        .commit(request(ownership.clone(), &key, &request_id, b"timed-out"))
        .await;
    assert!(matches!(timed_out, Err(ActorError::Storage(_))));
    remove_trigger(&admin_pool, "actor_outbox", &trigger, &function).await;
    assert_eq!(
        bootstrap
            .load(ownership.clone(), KIND, key.clone())
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        bootstrap
            .commit(request(ownership, &key, &request_id, b"after-timeout"))
            .await
            .unwrap(),
        CommitResult::Applied
    ));
}
