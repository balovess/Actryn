//! PostgreSQL durable backend.
//!
//! Safety argument: a command holds a `FOR SHARE` lock on its shard-lease row
//! until state + receipt + outbox commit. A handover acquires a conflicting row
//! update lock, increments epoch, and therefore cannot pass a concurrent commit.
//! Every validity check uses PostgreSQL's clock, never the worker clock.

use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Object, Pool};
use tokio_postgres::{Transaction, error::SqlState};

use crate::{
    ActorError,
    outbox::{OutboxRecord, OutboxStore},
    storage::{
        CommitRequest, CommitResult, LOGICAL_SHARDS, Ownership, ShardLocation, StorageAdapter,
        shard_for,
    },
};

#[derive(Clone)]
pub struct PostgresStorage {
    pool: Pool,
}

impl From<PostgresStorage> for super::StorageBackend {
    fn from(adapter: PostgresStorage) -> Self {
        Self::new(adapter)
    }
}

fn db_error(error: impl std::fmt::Display) -> ActorError {
    ActorError::Storage(error.to_string())
}

const MAX_TRANSACTION_RETRIES: usize = 3;

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
                Some(code) => ActorError::Storage(format!("PostgreSQL SQLSTATE {code:?}: {error}")),
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
    u64::try_from(value).map_err(|_| ActorError::Storage("negative database version".into()))
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
        Self { pool }
    }

    /// Bootstrap for development. Production installations must use versioned
    /// migrations; this function only creates the current schema if missing.
    pub async fn initialize_schema(&self) -> Result<(), ActorError> {
        self.client()
            .await?
            .batch_execute(include_str!("../../migrations/0001_init.sql"))
            .await
            .map_err(db_error)
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
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(db_error)?;
        for shard in shards {
            let shard_id = shard as i32;
            let epoch = as_i64(owner.epochs[shard])?;
            let updated = tx
                .execute(
                    "UPDATE shard_leases SET
                    expires_at = clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond')
                 WHERE shard_id = $1 AND owner_id = $2 AND epoch = $3
                   AND expires_at > clock_timestamp()",
                    &[&shard_id, &owner.owner.as_ref(), &epoch, &ttl_ms],
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
                let hash = hash
                    .try_into()
                    .map_err(|_| ActorError::Storage("invalid receipt hash".into()))?;
                Ok((hash, r.get(1)))
            })
            .transpose()?;
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }

    /// The only path allowed to make a durable command visible. A serialization failure or
    /// deadlock restarts the complete transaction, including lease fencing and receipt lookup.
    pub(crate) async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
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
        let mut client = self.client().await.map_err(CommitFailure::Actor)?;
        let tx = client.transaction().await?;
        Self::fence(&tx, owner, kind, key).await?;

        let previous = tx
            .query_opt(
                "SELECT request_hash, reply FROM actor_receipts
             WHERE kind = $1 AND actor_key = $2 AND request_id = $3",
                &[&kind, key, request_id],
            )
            .await?;
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
            let ordinal = i32::try_from(ordinal).map_err(|_| ActorError::InvalidConfiguration)?;
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
        match tx.commit().await {
            Ok(()) => Ok(CommitResult::Applied),
            Err(error) if error.code().is_none() => Err(CommitFailure::UnknownOutcome),
            Err(error) => Err(CommitFailure::Database(error)),
        }
    }

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
