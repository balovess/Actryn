//! Demonstrates injecting an application-defined storage adapter through the trait object API.
use std::{sync::Arc, time::Duration};

use actryn::{
    ActorError, ActorRuntime, CommitRequest, CommitResult, Decision, DurableActor, OutboxRecord,
    OutboxStore, Ownership, ShardLocation, SqliteStorage, StorageAdapter,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

struct Counter;

#[derive(Clone, Serialize, Deserialize)]
struct CounterState(u64);

#[derive(Serialize)]
struct Add(u64);

impl DurableActor for Counter {
    const KIND: &'static str = "example.custom_adapter.counter.v1";
    type State = CounterState;
    type Command = Add;
    type Reply = u64;

    fn initial(_key: &str) -> Self::State {
        CounterState(0)
    }

    fn decide(
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Decision<Self::State, Self::Reply>, ActorError> {
        let next = state
            .0
            .checked_add(command.0)
            .ok_or_else(|| ActorError::Rejected("counter overflow".into()))?;
        Ok(Decision::new(CounterState(next), next))
    }
}

/// Application-owned adapter that delegates to a bundled provider.
///
/// A real adapter could delegate to any database that upholds the storage contract.
#[derive(Clone)]
struct CustomSqliteAdapter(SqliteStorage);

#[async_trait]
impl OutboxStore for CustomSqliteAdapter {
    async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        OutboxStore::pending_outbox(&self.0, limit).await
    }

    async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
        OutboxStore::ack_outbox(&self.0, id).await
    }
}

#[async_trait]
impl StorageAdapter for CustomSqliteAdapter {
    fn claim_all_local(&self, node_id: &str) -> Result<Ownership, ActorError> {
        StorageAdapter::claim_all_local(&self.0, node_id)
    }

    fn claim_selected_local(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        StorageAdapter::claim_selected_local(&self.0, node_id, endpoint, shards, lease)
    }

    async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        StorageAdapter::claim_selected(&self.0, node_id, endpoint, shards, lease).await
    }

    async fn renew_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        StorageAdapter::renew_selected(&self.0, ownership, shards, lease).await
    }

    async fn release_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
    ) -> Result<(), ActorError> {
        StorageAdapter::release_selected(&self.0, ownership, shards).await
    }

    async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        StorageAdapter::locate(&self.0, shard).await
    }

    async fn check_fence(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        StorageAdapter::check_fence(&self.0, ownership, kind, key).await
    }

    async fn load(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        StorageAdapter::load(&self.0, ownership, kind, key).await
    }

    async fn receipt(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        StorageAdapter::receipt(&self.0, ownership, kind, key, request_id).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        StorageAdapter::commit(&self.0, request).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = SqliteStorage::open(directory.path().join("custom-adapter.sqlite"))?;
    let adapter: Arc<dyn StorageAdapter> = Arc::new(CustomSqliteAdapter(storage));

    let runtime = ActorRuntime::builder()
        .storage_shared(adapter)
        .standalone()
        .register_durable::<Counter>()?
        .build()?;

    let counter = runtime.durable::<Counter>("demo");
    assert_eq!(counter.command("request-1", Add(2)).await?, 2);
    assert_eq!(counter.command("request-1", Add(2)).await?, 2);
    assert!(matches!(
        counter.command("request-1", Add(99)).await,
        Err(ActorError::RequestIdConflict)
    ));
    println!("counter={}", counter.read().await?.0);

    runtime.shutdown().await?;
    Ok(())
}
