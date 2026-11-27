//! PostgreSQL durable backend.
//!
//! Safety argument: a command holds a `FOR SHARE` lock on its shard-lease row
//! until state + receipt + outbox commit. A handover acquires a conflicting row
//! update lock, increments epoch, and therefore cannot pass a concurrent commit.
//! Every validity check uses PostgreSQL's clock, never the worker clock.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use deadpool_postgres::{Object, Pool};
use tokio_postgres::{Transaction, error::SqlState};

use crate::{
    ActorError, StorageError, TimingObserver, TimingStage,
    outbox::{OutboxRecord, OutboxStore},
    storage::{
        CommitRequest, CommitResult, LOGICAL_SHARDS, Ownership, ShardLocation, StorageAdapter,
        shard_for,
    },
};

/// PostgreSQL-backed durable storage with transactional shard fencing.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: Pool,
    timing_observer: Option<Arc<dyn TimingObserver>>,
}

struct StageTimer<'a> {
    observer: Option<&'a dyn TimingObserver>,
    stage: TimingStage,
    started: Option<Instant>,
}

impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        if let (Some(observer), Some(started)) = (self.observer, self.started) {
            observer.observe(self.stage, started.elapsed());
        }
    }
}

impl From<PostgresStorage> for super::StorageBackend {
    fn from(adapter: PostgresStorage) -> Self {
        Self::new(adapter)
    }
}

fn db_error(error: impl std::fmt::Display) -> ActorError {
    ActorError::Storage(StorageError::new("postgresql", "database", error))
}

const MAX_TRANSACTION_RETRIES: usize = 3;
const SCHEMA_VERSION: u32 = 1;
const MIGRATION_LOCK_ID: i64 = 0x0041_6374_7279_0001;

#[derive(Debug)]
enum CommitFailure {
    Actor(ActorError),
    Database(tokio_postgres::Error),
    UnknownOutcome,
}

impl CommitFailure {
    fn is_retryable_transaction_failure(&self) -> bool {
        matches!(
            self,
            Self::Database(error)
                if matches!(
                    error.code(),
                    Some(code)
                        if code == &SqlState::T_R_SERIALIZATION_FAILURE
                            || code == &SqlState::T_R_DEADLOCK_DETECTED
                )
        )
    }

    fn into_actor_error(self) -> ActorError {
        match self {
            Self::Actor(error) => error,
            Self::Database(error) => match error.code() {
                Some(code) => {
                    let mut detail = StorageError::new("postgresql", "transaction", &error)
                        .with_code(code.code());
                    if code == &SqlState::T_R_SERIALIZATION_FAILURE
                        || code == &SqlState::T_R_DEADLOCK_DETECTED
                    {
                        detail = detail.retryable();
                    }
                    ActorError::Storage(detail)
                }
                None => db_error(error),
            },
            Self::UnknownOutcome => ActorError::UnknownOutcome,
        }
    }
}

impl From<ActorError> for CommitFailure {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

impl From<tokio_postgres::Error> for CommitFailure {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Database(error)
    }
}

fn as_i64(value: u64) -> Result<i64, ActorError> {
    i64::try_from(value).map_err(|_| ActorError::VersionConflict)
}

fn as_u64(value: i64) -> Result<u64, ActorError> {
    u64::try_from(value).map_err(|_| {
        ActorError::Storage(StorageError::new(
            "postgresql",
            "integer_conversion",
            "negative database version",
        ))
    })
}

fn checked_lease(lease: Duration) -> Result<i64, ActorError> {
    if lease < Duration::from_secs(1) || lease > Duration::from_secs(3600) {
        return Err(ActorError::InvalidConfiguration);
    }
    i64::try_from(lease.as_millis()).map_err(|_| ActorError::InvalidConfiguration)
}

impl PostgresStorage {
    /// The caller configures pool credentials and TLS. No implicit NoTls mode.
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            timing_observer: None,
        }
    }

    /// Enable optional timings for durable commit phases.
    ///
    /// The observer runs inline on storage tasks. Keep it fast, non-blocking,
    /// and non-panicking. With no observer configured, phase timers are not started.
    pub fn with_timing_observer(mut self, observer: Arc<dyn TimingObserver>) -> Self {
        self.timing_observer = Some(observer);
        self
    }

    fn stage_timer(&self, stage: TimingStage) -> StageTimer<'_> {
        let observer = self.timing_observer.as_deref();
        StageTimer {
            observer,
            stage,
            started: observer.map(|_| Instant::now()),
        }
    }

    /// Applies bundled infrastructure schema migrations before durable use.
    ///
    /// This method is idempotent and serialized with a PostgreSQL transaction
    /// advisory lock. It does not migrate application-owned actor state payloads.
    pub async fn migrate(&self) -> Result<(), ActorError> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            &[&MIGRATION_LOCK_ID],
        )
        .await
        .map_err(db_error)?;
        tx.execute(
            "CREATE TABLE IF NOT EXISTS actryn_schema_migrations (
                version bigint PRIMARY KEY CHECK (version > 0),
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
             )",
            &[],
        )
        .await
        .map_err(db_error)?;

        let current: Option<i64> = tx
            .query_one("SELECT MAX(version) FROM actryn_schema_migrations", &[])
            .await
            .map_err(db_error)?
            .get(0);
        match current {
            None => {
                tx.batch_execute(include_str!("../../migrations/0001_init.sql"))
                    .await
                    .map_err(db_error)?;
                tx.execute(
                    "INSERT INTO actryn_schema_migrations(version) VALUES ($1)",
                    &[&i64::from(SCHEMA_VERSION)],
                )
                .await
                .map_err(db_error)?;
            }
            Some(version) if version == i64::from(SCHEMA_VERSION) => {}
            Some(_) => return Err(ActorError::InvalidConfiguration),
        }
        tx.commit().await.map_err(db_error)
    }

    /// Returns the applied infrastructure schema version, or zero before migration.
    pub async fn schema_version(&self) -> Result<u32, ActorError> {
        let result = self
            .client()
            .await?
            .query_opt(
                "SELECT COALESCE(MAX(version), 0) FROM actryn_schema_migrations",
                &[],
            )
            .await;
        let row = match result {
            Ok(row) => row,
            Err(error) if error.code() == Some(&SqlState::UNDEFINED_TABLE) => return Ok(0),
            Err(error) => return Err(db_error(error)),
        };
        let version: i64 = row.map(|row| row.get(0)).unwrap_or(0);
        u32::try_from(version).map_err(|_| {
            ActorError::Storage(StorageError::new(
                "postgresql",
                "schema_version",
                "schema version exceeds supported range",
            ))
        })
    }

    /// Compatibility alias for [`Self::migrate`].
    pub async fn initialize_schema(&self) -> Result<(), ActorError> {
        self.migrate().await
    }

    async fn client(&self) -> Result<Object, ActorError> {
        self.pool.get().await.map_err(db_error)
    }

    /// Claim is atomic across the requested shard set. An unexpired lease is
    /// never stolen, including if it belongs to this same node ID.
    async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        if node_id.is_empty()
            || node_id.len() > 256
            || endpoint.is_empty()
            || shards.is_empty()
            || shards.iter().any(|s| *s >= LOGICAL_SHARDS)
        {
            return Err(ActorError::InvalidConfiguration);
        }
        let ttl_ms = checked_lease(lease)?;
        let mut unique = shards.to_vec();
        unique.sort_unstable();
        unique.dedup();

        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        let mut epochs = vec![0u64; LOGICAL_SHARDS];
        for shard in unique {
            let shard = shard as i32;
            let row = tx
                .query_opt(
                    "INSERT INTO shard_leases(shard_id, owner_id, endpoint, epoch, expires_at)
                 VALUES ($1, $2, $3, 1,
                         clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond'))
                 ON CONFLICT(shard_id) DO UPDATE SET
                     owner_id = EXCLUDED.owner_id,
                     endpoint = EXCLUDED.endpoint,
                     epoch = shard_leases.epoch + 1,
                     expires_at = clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond')
                 WHERE shard_leases.expires_at <= clock_timestamp()
                 RETURNING epoch",
                    &[&shard, &node_id, &endpoint, &ttl_ms],
                )
                .await
                .map_err(db_error)?;
            let Some(row) = row else {
                return Err(ActorError::OwnershipLost);
            };
            epochs[shard as usize] = as_u64(row.get::<_, i64>(0))?;
        }
        tx.commit().await.map_err(db_error)?;
        Ownership::new(node_id.to_owned(), epochs)
    }

    pub(crate) async fn renew_selected(
        &self,
        owner: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        let ttl_ms = checked_lease(lease)?;
        if shards.is_empty() || shards.iter().any(|s| *s >= LOGICAL_SHARDS) {
            return Err(ActorError::InvalidConfiguration);
        }
        let mut unique_shards = shards;
        unique_shards.sort_unstable();
        unique_shards.dedup();
        let shard_ids: Vec<i32> = unique_shards
            .iter()
            .map(|shard| i32::try_from(*shard).map_err(|_| ActorError::InvalidConfiguration))
            .collect::<Result<_, _>>()?;
        let epochs: Vec<i64> = unique_shards
            .iter()
            .map(|shard| as_i64(owner.epochs[*shard]))
            .collect::<Result<_, _>>()?;

        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        // Renew the full set in one round trip so row locks are not held across
        // sequential client/server exchanges for each shard.
        let updated = tx
            .execute(
                "UPDATE shard_leases AS leases
                 SET expires_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond')
                 FROM unnest($1::integer[], $2::bigint[]) AS requested(shard_id, epoch)
                 WHERE leases.shard_id = requested.shard_id
                   AND leases.owner_id = $4
                   AND leases.epoch = requested.epoch
                   AND leases.expires_at > clock_timestamp()",
                &[&shard_ids, &epochs, &ttl_ms, &owner.owner.as_ref()],
            )
            .await
            .map_err(db_error)?;
        if updated != unique_shards.len() as u64 {
            return Err(ActorError::OwnershipLost);
        }
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    pub(crate) async fn release_selected(
        &self,
        owner: Ownership,
        shards: Vec<usize>,
    ) -> Result<(), ActorError> {
        if shards.is_empty()
            || shards
                .iter()
                .any(|shard| *shard >= LOGICAL_SHARDS || owner.epochs[*shard] == 0)
        {
            return Err(ActorError::InvalidConfiguration);
        }
        let mut unique = shards;
        unique.sort_unstable();
        unique.dedup();

        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        for shard in unique {
            let shard_id = shard as i32;
            let epoch = as_i64(owner.epochs[shard])?;
            let updated = tx
                .execute(
                    "UPDATE shard_leases SET expires_at = clock_timestamp()
                 WHERE shard_id = $1 AND owner_id = $2 AND epoch = $3
                   AND expires_at > clock_timestamp()",
                    &[&shard_id, &owner.owner.as_ref(), &epoch],
                )
                .await
                .map_err(db_error)?;
            if updated != 1 {
                return Err(ActorError::OwnershipLost);
            }
        }
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    /// Looks up the unexpired owner of one logical shard using database time.
    pub async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        if shard >= LOGICAL_SHARDS {
            return Err(ActorError::InvalidConfiguration);
        }
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT owner_id, endpoint, epoch,
               (EXTRACT(EPOCH FROM expires_at) * 1000)::bigint
             FROM shard_leases WHERE shard_id = $1 AND expires_at > clock_timestamp()",
                &[&(shard as i32)],
            )
            .await
            .map_err(db_error)?;
        row.map(|r| {
            Ok(ShardLocation {
                shard,
                owner_id: r.get(0),
                endpoint: r.get(1),
                epoch: as_u64(r.get::<_, i64>(2))?,
                expires_at_ms: r.get(3),
            })
        })
        .transpose()
    }

    /// Validate shard ownership in a transaction. Lease handover cannot run
    /// past this row lock until the transaction ends.
    async fn fence(
        tx: &Transaction<'_>,
        owner: &Ownership,
        kind: &str,
        key: &str,
    ) -> Result<(), CommitFailure> {
        let shard = shard_for(kind, key);
        let epoch = as_i64(owner.epochs[shard])?;
        if epoch == 0 {
            return Err(ActorError::OwnershipLost.into());
        }
        let row = tx
            .query_opt(
                "SELECT owner_id, epoch FROM shard_leases WHERE shard_id = $1 FOR SHARE",
                &[&(shard as i32)],
            )
            .await?;
        let Some(row) = row else {
            return Err(ActorError::OwnershipLost.into());
        };
        if row.get::<_, String>(0).as_str() != owner.owner.as_ref() || row.get::<_, i64>(1) != epoch
        {
            return Err(ActorError::OwnershipLost.into());
        }
        // Check time *after* waiting for the row lock, using server time.
        let row = tx
            .query_one(
                "SELECT expires_at > clock_timestamp() FROM shard_leases WHERE shard_id = $1",
                &[&(shard as i32)],
            )
            .await?;
        if !row.get::<_, bool>(0) {
            return Err(ActorError::OwnershipLost.into());
        }
        Ok(())
    }

    pub(crate) async fn check_fence(
        &self,
        owner: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        Self::fence(&tx, &owner, kind, &key)
            .await
            .map_err(CommitFailure::into_actor_error)?;
        tx.commit().await.map_err(db_error)
    }

    pub(crate) async fn load(
        &self,
        owner: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        Self::fence(&tx, &owner, kind, &key)
            .await
            .map_err(CommitFailure::into_actor_error)?;
        let row = tx
            .query_opt(
                "SELECT version, payload FROM actor_states WHERE kind = $1 AND actor_key = $2",
                &[&kind, &key],
            )
            .await
            .map_err(db_error)?;
        let result = row
            .map(|r| Ok((as_u64(r.get::<_, i64>(0))?, r.get(1))))
            .transpose()?;
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }

    pub(crate) async fn receipt(
        &self,
        owner: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        Self::fence(&tx, &owner, kind, &key)
            .await
            .map_err(CommitFailure::into_actor_error)?;
        let row = tx
            .query_opt(
                "SELECT request_hash, reply FROM actor_receipts
             WHERE kind = $1 AND actor_key = $2 AND request_id = $3",
                &[&kind, &key, &request_id],
            )
            .await
            .map_err(db_error)?;
        let result = row
            .map(|r| {
                let hash: Vec<u8> = r.get(0);
                let hash = hash.try_into().map_err(|_| {
                    ActorError::Storage(StorageError::new(
                        "postgresql",
                        "receipt",
                        "invalid receipt hash",
                    ))
                })?;
                Ok((hash, r.get(1)))
            })
            .transpose()?;
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }

    /// The only path allowed to make a durable command visible. A serialization failure or
    /// deadlock restarts the complete transaction, including lease fencing and receipt lookup.
    pub(crate) async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        let _total_timer = self.stage_timer(TimingStage::StorageCommitTotal);
        for attempt in 0..=MAX_TRANSACTION_RETRIES {
            match self.commit_once(&request).await {
                Ok(result) => return Ok(result),
                Err(error) if error.is_retryable_transaction_failure() => {
                    if attempt == MAX_TRANSACTION_RETRIES {
                        return Err(error.into_actor_error());
                    }
                    tokio::time::sleep(Duration::from_millis(10_u64 << attempt)).await;
                }
                Err(error) => return Err(error.into_actor_error()),
            }
        }
        unreachable!("the bounded transaction retry loop always returns")
    }

    async fn commit_once(&self, request: &CommitRequest) -> Result<CommitResult, CommitFailure> {
        let owner = &request.ownership;
        let kind = request.kind;
        let key = &request.key;
        let request_id = &request.request_id;
        let hash = &request.request_hash;
        let expected_version = request.expected_version;
        let mut client = {
            let _timer = self.stage_timer(TimingStage::StoragePoolAcquire);
            self.client().await.map_err(CommitFailure::Actor)?
        };
        let tx = {
            let _timer = self.stage_timer(TimingStage::StorageTransactionBegin);
            client.transaction().await?
        };
        {
            let _timer = self.stage_timer(TimingStage::StorageFence);
            Self::fence(&tx, owner, kind, key).await?;
        }

        let previous = {
            let _timer = self.stage_timer(TimingStage::StorageReceiptLookup);
            tx.query_opt(
                "SELECT request_hash, reply FROM actor_receipts
             WHERE kind = $1 AND actor_key = $2 AND request_id = $3",
                &[&kind, key, request_id],
            )
            .await?
        };
        if let Some(row) = previous {
            let prior_hash: Vec<u8> = row.get(0);
            if prior_hash.as_slice() != hash.as_slice() {
                return Err(ActorError::RequestIdConflict.into());
            }
            return Ok(CommitResult::Duplicate(row.get(1)));
        }

        let expected = as_i64(expected_version)?;
        let next = as_i64(
            expected_version
                .checked_add(1)
                .ok_or(ActorError::VersionConflict)?,
        )?;
        {
            let _timer = self.stage_timer(TimingStage::StorageWrites);
            let written = if expected == 0 {
                tx.execute(
                    "INSERT INTO actor_states(kind,actor_key,version,payload)
                 VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    &[&kind, key, &next, &request.state],
                )
                .await?
            } else {
                tx.execute(
                    "UPDATE actor_states SET version = $4, payload = $5
                 WHERE kind = $1 AND actor_key = $2 AND version = $3",
                    &[&kind, key, &expected, &next, &request.state],
                )
                .await?
            };
            if written != 1 {
                return Ok(CommitResult::VersionConflict);
            }

            tx.execute(
                "INSERT INTO actor_receipts(kind,actor_key,request_id,request_hash,reply)
             VALUES($1,$2,$3,$4,$5)",
                &[&kind, key, request_id, &hash.to_vec(), &request.reply],
            )
            .await?;
            for (ordinal, effect) in request.effects.iter().enumerate() {
                let ordinal =
                    i32::try_from(ordinal).map_err(|_| ActorError::InvalidConfiguration)?;
                tx.execute(
                    "INSERT INTO actor_outbox(kind,actor_key,request_id,ordinal,
                   destination_kind,destination_key,payload)
                 VALUES($1,$2,$3,$4,$5,$6,$7)",
                    &[
                        &kind,
                        key,
                        request_id,
                        &ordinal,
                        &effect.destination_kind,
                        &effect.destination_key,
                        &effect.payload,
                    ],
                )
                .await?;
            }
        }
        let commit_result = {
            let _timer = self.stage_timer(TimingStage::StorageTransactionCommit);
            tx.commit().await
        };
        match commit_result {
            Ok(()) => Ok(CommitResult::Applied),
            Err(error) if error.code().is_none() => Err(CommitFailure::UnknownOutcome),
            Err(error) => Err(CommitFailure::Database(error)),
        }
    }

    /// Reads at most `limit` undelivered outbox records in insertion order.
    pub async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        if !(1..=10_000).contains(&limit) {
            return Err(ActorError::InvalidConfiguration);
        }
        let rows = self
            .client()
            .await?
            .query(
                "SELECT outbox_id,kind,actor_key,request_id,ordinal,destination_kind,
                    destination_key,payload
             FROM actor_outbox WHERE delivered = false ORDER BY outbox_id LIMIT $1",
                &[&(limit as i64)],
            )
            .await
            .map_err(db_error)?;
        Ok(rows
            .into_iter()
            .map(|r| OutboxRecord {
                id: r.get(0),
                origin_kind: r.get(1),
                origin_key: r.get(2),
                request_id: r.get(3),
                ordinal: i64::from(r.get::<_, i32>(4)),
                destination_kind: r.get(5),
                destination_key: r.get(6),
                payload: r.get(7),
            })
            .collect())
    }

    /// Persistently marks one outbox record as delivered.
    pub async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
        self.client()
            .await?
            .execute(
                "UPDATE actor_outbox SET delivered = true WHERE outbox_id = $1",
                &[&id],
            )
            .await
            .map_err(db_error)?;
        Ok(())
    }
}

#[async_trait]
impl OutboxStore for PostgresStorage {
    async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        PostgresStorage::pending_outbox(self, limit).await
    }

    async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
        PostgresStorage::ack_outbox(self, id).await
    }
}

#[async_trait]
impl StorageAdapter for PostgresStorage {
    async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        PostgresStorage::claim_selected(self, node_id, endpoint, shards, lease).await
    }

    async fn renew_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        PostgresStorage::renew_selected(self, ownership, shards, lease).await
    }

    async fn release_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
    ) -> Result<(), ActorError> {
        PostgresStorage::release_selected(self, ownership, shards).await
    }

    async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        PostgresStorage::locate(self, shard).await
    }

    async fn check_fence(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        PostgresStorage::check_fence(self, ownership, kind, key).await
    }

    async fn load(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        PostgresStorage::load(self, ownership, kind, key).await
    }

    async fn receipt(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        PostgresStorage::receipt(self, ownership, kind, key, request_id).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        PostgresStorage::commit(self, request).await
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn postgres_errors_have_explicit_provider_and_operation() {
        match as_u64(-1) {
            Err(ActorError::Storage(detail)) => {
                assert_eq!(detail.provider, "postgresql");
                assert_eq!(detail.operation, "integer_conversion");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
