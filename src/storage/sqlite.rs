//! SQLite storage and single-host lease laboratory.
use super::{
    CommitRequest, CommitResult, LOGICAL_SHARDS, Ownership, ShardLocation, StorageAdapter,
    StorageBackend, shard_for,
};
use crate::{
    ActorError,
    outbox::{OutboxRecord, OutboxStore},
};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

// Use the same database clock for lease acquisition, renewal and fencing.
const DB_NOW: &str = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";

/// Embedded SQLite: safe for fencing tests on a *shared local filesystem*, not a
/// multi-host database or multi-node placement implementation.
#[derive(Clone)]
pub struct SqliteStorage {
    connection: Arc<Mutex<Connection>>,
}

fn db_err(e: rusqlite::Error) -> ActorError {
    ActorError::Storage(e.to_string())
}
fn join_err(e: tokio::task::JoinError) -> ActorError {
    ActorError::Storage(e.to_string())
}
fn as_i64(value: u64) -> Result<i64, ActorError> {
    i64::try_from(value).map_err(|_| ActorError::VersionConflict)
}
fn as_u64(value: i64) -> Result<u64, ActorError> {
    u64::try_from(value).map_err(|_| ActorError::Storage("negative SQLite version or epoch".into()))
}

fn check_owner(
    conn: &Connection,
    owner: &Ownership,
    kind: &str,
    key: &str,
) -> Result<(), ActorError> {
    let (shard, epoch) = owner.token(kind, key);
    let stored: Option<(String, i64)> = conn.query_row(
        "SELECT owner_id,epoch FROM shard_owners
         WHERE shard_id=?1 AND expires_at_ms > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
        params![shard], |r| Ok((r.get(0)?, r.get(1)?))
    ).optional().map_err(db_err)?;
    match stored {
        Some((id, current)) if id == owner.owner.as_ref() => {
            if as_u64(current)? == epoch {
                Ok(())
            } else {
                Err(ActorError::OwnershipLost)
            }
        }
        _ => Err(ActorError::OwnershipLost),
    }
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ActorError> {
        let conn = Connection::open(path).map_err(db_err)?;
        conn.busy_timeout(Duration::from_secs(5)).map_err(db_err)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS states (
                actor_kind TEXT NOT NULL, actor_key TEXT NOT NULL,
                version INTEGER NOT NULL, payload BLOB NOT NULL,
                PRIMARY KEY(actor_kind,actor_key)
             );
             CREATE TABLE IF NOT EXISTS receipts (
                actor_kind TEXT NOT NULL, actor_key TEXT NOT NULL,
                request_id TEXT NOT NULL, request_hash BLOB NOT NULL,
                reply BLOB NOT NULL,
                PRIMARY KEY(actor_kind,actor_key,request_id)
             );
             CREATE TABLE IF NOT EXISTS shard_owners (
                shard_id INTEGER PRIMARY KEY, owner_id TEXT NOT NULL,
                epoch INTEGER NOT NULL,
                endpoint TEXT NOT NULL DEFAULT '',
                expires_at_ms INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_kind TEXT NOT NULL, actor_key TEXT NOT NULL,
                request_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                destination_kind TEXT NOT NULL, destination_key TEXT NOT NULL,
                payload BLOB NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
                UNIQUE(actor_kind,actor_key,request_id,ordinal)
             );
             CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(delivered,id);",
        )
        .map_err(db_err)?;
        // Upgrade the v0.3 schema in place; existing claims expire, not states.
        let mut stmt = conn
            .prepare("PRAGMA table_info(shard_owners)")
            .map_err(db_err)?;
        let names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(1))
            .map_err(db_err)?
            .collect::<Result<_, _>>()
            .map_err(db_err)?;
        drop(stmt);
        if !names.iter().any(|n| n == "endpoint") {
            conn.execute_batch(
                "ALTER TABLE shard_owners ADD COLUMN endpoint TEXT NOT NULL DEFAULT '';",
            )
            .map_err(db_err)?;
        }
        if !names.iter().any(|n| n == "expires_at_ms") {
            conn.execute_batch(
                "ALTER TABLE shard_owners ADD COLUMN expires_at_ms INTEGER NOT NULL DEFAULT 0;",
            )
            .map_err(db_err)?;
        }
        Ok(Self {
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    /// Explicit fencing handover. This does NOT detect liveness, balance shards,
    /// or provide remote routing. A new claim supersedes any older instance.
    /// All 256 logical shards are claimed in one SQLite write transaction.
    pub(crate) fn claim_all(&self, node_id: &str) -> Result<Ownership, ActorError> {
        if node_id.is_empty() || node_id.len() > 256 {
            return Err(ActorError::InvalidKey);
        }
        let mut conn = self
            .connection
            .lock()
            .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)?;
        let mut epochs = Vec::with_capacity(LOGICAL_SHARDS);
        for shard in 0..LOGICAL_SHARDS {
            let updated = tx
                .execute(
                    "INSERT INTO shard_owners(shard_id,owner_id,epoch,endpoint,expires_at_ms)
                 VALUES(?1,?2,1,'',9223372036854775807)
                 ON CONFLICT(shard_id) DO UPDATE SET
                    owner_id=excluded.owner_id, epoch=shard_owners.epoch+1,
                    endpoint='', expires_at_ms=9223372036854775807
                 WHERE shard_owners.epoch < 9223372036854775807",
                    params![shard as i64, node_id],
                )
                .map_err(db_err)?;
            if updated != 1 {
                return Err(ActorError::VersionConflict);
            }
            let epoch: i64 = tx
                .query_row(
                    "SELECT epoch FROM shard_owners WHERE shard_id=?1",
                    params![shard as i64],
                    |r| r.get(0),
                )
                .map_err(db_err)?;
            epochs.push(as_u64(epoch)?);
        }
        tx.commit().map_err(db_err)?;
        Ownership::new(node_id.to_owned(), epochs)
    }

    /// Selectively acquire only unowned/expired shards. Never steals an active lease.
    /// A *new* runtime should be built after a handover so no old activation is reused.
    pub(crate) fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        if node_id.is_empty()
            || node_id.len() > 256
            || endpoint.is_empty()
            || lease < Duration::from_secs(1)
            || lease > Duration::from_secs(3600)
            || shards.is_empty()
            || shards.iter().any(|&s| s >= LOGICAL_SHARDS)
        {
            return Err(ActorError::InvalidConfiguration);
        }
        let ttl_ms =
            i64::try_from(lease.as_millis()).map_err(|_| ActorError::InvalidConfiguration)?;
        let mut unique = shards.to_vec();
        unique.sort_unstable();
        unique.dedup();
        let mut conn = self
            .connection
            .lock()
            .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_err)?;
        let mut epochs = vec![0; LOGICAL_SHARDS];
        for shard in unique {
            // Atomic conditional INSERT/UPDATE prevents stealing any unexpired lease.
            // Renewal is only allowed while current ownership remains valid.
            let sql = format!(
                "INSERT INTO shard_owners(shard_id,owner_id,epoch,endpoint,expires_at_ms)
                  VALUES(?1,?2,1,?3,({DB_NOW}) + ?4)
                  ON CONFLICT(shard_id) DO UPDATE SET
                     owner_id=excluded.owner_id, epoch=shard_owners.epoch+1,
                     endpoint=excluded.endpoint, expires_at_ms=excluded.expires_at_ms
                  WHERE shard_owners.expires_at_ms <= ({DB_NOW})
                    AND shard_owners.epoch < 9223372036854775807"
            );
            let updated = tx
                .execute(&sql, params![shard as i64, node_id, endpoint, ttl_ms])
                .map_err(db_err)?;
            if updated != 1 {
                return Err(ActorError::OwnershipLost);
            }
            let epoch: i64 = tx
                .query_row(
                    "SELECT epoch FROM shard_owners WHERE shard_id=?1",
                    params![shard as i64],
                    |r| r.get(0),
                )
                .map_err(db_err)?;
            epochs[shard] = as_u64(epoch)?;
        }
        tx.commit().map_err(db_err)?;
        Ownership::new(node_id.to_owned(), epochs)
    }

    pub(crate) async fn renew_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        let connection = self.connection.clone();
        if shards.is_empty() || lease < Duration::from_secs(1) || lease > Duration::from_secs(3600)
        {
            return Err(ActorError::InvalidConfiguration);
        }
        let ttl = i64::try_from(lease.as_millis()).map_err(|_| ActorError::InvalidConfiguration)?;
        if shards
            .iter()
            .any(|&s| s >= LOGICAL_SHARDS || ownership.epochs[s] == 0)
        {
            return Err(ActorError::InvalidConfiguration);
        }
        tokio::task::spawn_blocking(move || {
            let mut conn = connection
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_err)?;
            let sql = format!(
                "UPDATE shard_owners SET expires_at_ms=({DB_NOW})+?4
                 WHERE shard_id=?1 AND owner_id=?2 AND epoch=?3
                   AND expires_at_ms > ({DB_NOW})"
            );
            for shard in shards {
                let epoch = as_i64(ownership.epochs[shard])?;
                let updated = tx
                    .execute(
                        &sql,
                        params![shard as i64, ownership.owner.as_ref(), epoch, ttl],
                    )
                    .map_err(db_err)?;
                if updated != 1 {
                    return Err(ActorError::OwnershipLost);
                }
            }
            tx.commit().map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(join_err)?
    }

    pub async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        if shard >= LOGICAL_SHARDS {
            return Err(ActorError::InvalidConfiguration);
        }
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let conn = connection
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            let sql = format!(
                "SELECT owner_id,endpoint,epoch,expires_at_ms FROM shard_owners
                 WHERE shard_id=?1 AND expires_at_ms > ({DB_NOW})"
            );
            let location = conn
                .query_row(&sql, params![shard as i64], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                })
                .optional()
                .map_err(db_err)?;
            location
                .map(|(owner_id, endpoint, epoch, expires_at_ms)| {
                    Ok(ShardLocation {
                        shard,
                        owner_id,
                        endpoint,
                        epoch: as_u64(epoch)?,
                        expires_at_ms,
                    })
                })
                .transpose()
        })
        .await
        .map_err(join_err)?
    }

    pub fn shard_for(kind: &str, key: &str) -> usize {
        shard_for(kind, key)
    }

    pub(crate) async fn check_fence(
        &self,
        owner: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        let conn = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            check_owner(&conn, &owner, kind, &key)
        })
        .await
        .map_err(join_err)?
    }

    pub(crate) async fn load(
        &self,
        owner: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        let conn = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            // Read transaction gives one SQLite snapshot for owner check + state load.
            let tx = conn.transaction().map_err(db_err)?;
            check_owner(&tx, &owner, kind, &key)?;
            let state = tx
                .query_row(
                    "SELECT version,payload FROM states WHERE actor_kind=?1 AND actor_key=?2",
                    params![kind, key],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .optional()
                .map_err(db_err)?;
            state
                .map(|(version, payload)| Ok((as_u64(version)?, payload)))
                .transpose()
        })
        .await
        .map_err(join_err)?
    }

    pub(crate) async fn receipt(
        &self,
        owner: Ownership,
        kind: &'static str,
        key: String,
        rid: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        let conn = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            let tx = conn.transaction().map_err(db_err)?;
            check_owner(&tx, &owner, kind, &key)?;
            let found: Option<(Vec<u8>, Vec<u8>)> = tx
                .query_row(
                    "SELECT request_hash,reply FROM receipts
                 WHERE actor_kind=?1 AND actor_key=?2 AND request_id=?3",
                    params![kind, key, rid],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(db_err)?;
            found
                .map(|(hash, reply)| {
                    let hash: [u8; 32] = hash
                        .try_into()
                        .map_err(|_| ActorError::Storage("invalid stored hash".into()))?;
                    Ok((hash, reply))
                })
                .transpose()
        })
        .await
        .map_err(join_err)?
    }

    pub(crate) async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        let conn = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let CommitRequest {
                ownership: owner,
                kind,
                key,
                request_id: rid,
                request_hash: hash,
                expected_version,
                state,
                reply,
                effects,
            } = request;
            let mut conn = conn
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            // Claim/handover and commit both use BEGIN IMMEDIATE: fencing cannot
            // race between ownership verification and state/outbox write.
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_err)?;
            check_owner(&tx, &owner, kind, &key)?;
            let prior_receipt: Option<(Vec<u8>, Vec<u8>)> = tx
                .query_row(
                    "SELECT request_hash,reply FROM receipts
                 WHERE actor_kind=?1 AND actor_key=?2 AND request_id=?3",
                    params![kind, key, rid],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(db_err)?;
            if let Some((old_hash, old_reply)) = prior_receipt {
                if old_hash.as_slice() != hash.as_slice() {
                    return Err(ActorError::RequestIdConflict);
                }
                return Ok(CommitResult::Duplicate(old_reply));
            }
            let prior: Option<i64> = tx
                .query_row(
                    "SELECT version FROM states WHERE actor_kind=?1 AND actor_key=?2",
                    params![kind, key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db_err)?;
            let prior = prior.map(as_u64).transpose()?;
            if prior.unwrap_or(0) != expected_version {
                return Ok(CommitResult::VersionConflict);
            }
            let version = expected_version
                .checked_add(1)
                .ok_or_else(|| ActorError::Storage("state version overflow".into()))?;
            let expected_version = as_i64(expected_version)?;
            let version = as_i64(version)?;
            let rows = if prior.is_none() {
                tx.execute(
                    "INSERT INTO states(actor_kind,actor_key,version,payload) VALUES(?1,?2,?3,?4)",
                    params![kind, key, version, state],
                )
                .map_err(db_err)?
            } else {
                tx.execute(
                    "UPDATE states SET version=?3,payload=?4
                        WHERE actor_kind=?1 AND actor_key=?2 AND version=?5",
                    params![kind, key, version, state, expected_version],
                )
                .map_err(db_err)?
            };
            if rows != 1 {
                return Ok(CommitResult::VersionConflict);
            }
            tx.execute(
                "INSERT INTO receipts(actor_kind,actor_key,request_id,request_hash,reply)
                        VALUES(?1,?2,?3,?4,?5)",
                params![kind, key, rid, hash.to_vec(), reply],
            )
            .map_err(db_err)?;
            for (index, effect) in effects.iter().enumerate() {
                tx.execute(
                    "INSERT INTO outbox(actor_kind,actor_key,request_id,ordinal,
                    destination_kind,destination_key,payload)
                    VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        kind,
                        key,
                        rid,
                        index as i64,
                        effect.destination_kind,
                        effect.destination_key,
                        effect.payload
                    ],
                )
                .map_err(db_err)?;
            }
            tx.commit().map_err(|_| ActorError::UnknownOutcome)?;
            Ok(CommitResult::Applied)
        })
        .await
        .map_err(join_err)?
    }

    /// Read unacknowledged messages. Multiple dispatchers may see the same
    /// record; receivers MUST deduplicate using delivery_id.
    pub async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        if limit == 0 || limit > 10_000 {
            return Err(ActorError::InvalidConfiguration);
        }
        let conn = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            let mut stmt = conn
                .prepare(
                    "SELECT id,actor_kind,actor_key,request_id,ordinal,
                destination_kind,destination_key,payload FROM outbox
                WHERE delivered=0 ORDER BY id LIMIT ?1",
                )
                .map_err(db_err)?;
            let iter = stmt
                .query_map(params![limit as i64], |r| {
                    Ok(OutboxRecord {
                        id: r.get(0)?,
                        origin_kind: r.get(1)?,
                        origin_key: r.get(2)?,
                        request_id: r.get(3)?,
                        ordinal: r.get(4)?,
                        destination_kind: r.get(5)?,
                        destination_key: r.get(6)?,
                        payload: r.get(7)?,
                    })
                })
                .map_err(db_err)?;
            iter.collect::<Result<Vec<_>, _>>().map_err(db_err)
        })
        .await
        .map_err(join_err)?
    }

    pub async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
        let conn = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|_| ActorError::Storage("poisoned mutex".into()))?;
            conn.execute("UPDATE outbox SET delivered=1 WHERE id=?1", params![id])
                .map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(join_err)?
    }
}

impl From<SqliteStorage> for StorageBackend {
    fn from(adapter: SqliteStorage) -> Self {
        Self::new(adapter)
    }
}

#[async_trait]
impl OutboxStore for SqliteStorage {
    async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        SqliteStorage::pending_outbox(self, limit).await
    }

    async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
        SqliteStorage::ack_outbox(self, id).await
    }
}

#[async_trait]
impl StorageAdapter for SqliteStorage {
    fn claim_all_local(&self, node_id: &str) -> Result<Ownership, ActorError> {
        SqliteStorage::claim_all(self, node_id)
    }

    fn claim_selected_local(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        SqliteStorage::claim_selected(self, node_id, endpoint, shards, lease)
    }

    async fn claim_all(&self, node_id: &str) -> Result<Ownership, ActorError> {
        SqliteStorage::claim_all(self, node_id)
    }

    async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        SqliteStorage::claim_selected(self, node_id, endpoint, shards, lease)
    }

    async fn renew_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        SqliteStorage::renew_selected(self, ownership, shards, lease).await
    }

    async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        SqliteStorage::locate(self, shard).await
    }

    async fn check_fence(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        SqliteStorage::check_fence(self, ownership, kind, key).await
    }

    async fn load(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        SqliteStorage::load(self, ownership, kind, key).await
    }

    async fn receipt(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        SqliteStorage::receipt(self, ownership, kind, key, request_id).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        SqliteStorage::commit(self, request).await
    }
}

#[cfg(test)]
mod adapter_tests {
    use super::*;

    #[test]
    fn sqlite_uses_generic_backend_handle() {
        let storage = SqliteStorage::open(":memory:").expect("open sqlite");
        let backend = StorageBackend::new(storage);
        assert!(backend.claim_all_local("test").is_ok());
    }
}

#[cfg(test)]
mod integer_conversion_tests {
    use super::{as_i64, as_u64};
    use crate::ActorError;

    #[test]
    fn sqlite_integer_conversions_enforce_signed_range() {
        assert_eq!(as_i64(i64::MAX as u64).unwrap(), i64::MAX);
        assert!(matches!(
            as_i64(i64::MAX as u64 + 1),
            Err(ActorError::VersionConflict)
        ));
        assert_eq!(as_u64(i64::MAX).unwrap(), i64::MAX as u64);
        assert!(matches!(as_u64(-1), Err(ActorError::Storage(_))));
    }
}
