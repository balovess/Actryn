#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Actryn is a Rust library for embedding a multi-type virtual actor runtime in an application.
//!
//! One bounded scheduler per shard serves *both* durable and session actors.
//! Durable commands are serialized, deduplicated and committed before success.
//! Session commands mutate volatile memory, with no persistence guarantee.
//! See the crate README for runnable examples and `CONTRACTS.md` for the storage
//! adapter requirements and the guarantees callers can rely on.
//!
//! SQLite is single-host only. PostgreSQL provides transactional cross-host
//! ownership fencing, but remote RPC still requires a trusted loopback boundary.
//! The transport is not production-ready.

/// Loopback-only TCP protocol for laboratory testing. Never expose to untrusted networks.
#[cfg(feature = "experimental-rpc")]
pub mod cluster;
mod outbox;
mod runtime;
pub mod storage;

pub use outbox::{Decision, OutboxEffect, OutboxRecord, OutboxSink, OutboxStore, dispatch_outbox};
pub use runtime::{
    ActorRuntime, ActorRuntimeBuilder, DurableActor, DurableRef, RuntimeMetrics, SessionActor,
    SessionRef, TimingObserver, TimingStage,
};
#[cfg(feature = "postgres")]
pub use storage::PostgresStorage;
pub use storage::{
    CommitRequest, CommitResult, LOGICAL_SHARDS, Ownership, ShardLocation, SqliteStorage,
    StorageAdapter, StorageBackend, balanced_initial_shards, rendezvous_initial_shards,
};

/// Machine-readable context for a known storage failure.
///
/// `UnknownOutcome` is a separate [`ActorError`] variant because the result of
/// the operation may have committed. This type only describes failures that
/// are known not to have produced a durable commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageError {
    /// Provider name, such as `sqlite` or `postgresql`.
    pub provider: String,
    /// Operation that failed, such as `commit` or `schema_migration`.
    pub operation: String,
    /// Provider-specific error code, such as a PostgreSQL SQLSTATE.
    pub code: Option<String>,
    /// Whether retrying the same operation may succeed without changing inputs.
    pub retryable: bool,
    /// Human-readable diagnostic message.
    pub message: String,
}

impl StorageError {
    /// Creates a non-retryable storage failure.
    pub fn new(
        provider: impl Into<String>,
        operation: impl Into<String>,
        message: impl std::fmt::Display,
    ) -> Self {
        Self {
            provider: provider.into(),
            operation: operation.into(),
            code: None,
            retryable: false,
            message: message.to_string(),
        }
    }

    /// Attaches a provider-specific error code.
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// Marks the failure as safe to retry with unchanged inputs.
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.code, self.retryable) {
            (Some(code), true) => write!(
                f,
                "{} {} failed with code {code} (retryable): {}",
                self.provider, self.operation, self.message
            ),
            (Some(code), false) => write!(
                f,
                "{} {} failed with code {code}: {}",
                self.provider, self.operation, self.message
            ),
            (None, true) => write!(
                f,
                "{} {} failed (retryable): {}",
                self.provider, self.operation, self.message
            ),
            (None, false) => write!(
                f,
                "{} {} failed: {}",
                self.provider, self.operation, self.message
            ),
        }
    }
}

/// Errors returned by Actryn's public APIs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ActorError {
    /// The application rejected a command with a domain-specific reason.
    Rejected(String),
    /// A known storage operation failed; the outcome was not committed.
    Storage(StorageError),
    /// A storage commit or remote command may have taken effect, but its result was not received.
    /// Retry with the same request ID and payload; do not create a new request ID.
    UnknownOutcome,
    /// Serialization or deserialization failed.
    Codec(String),
    /// A request ID was reused with a different command payload.
    RequestIdConflict,
    /// The expected actor state version did not match the stored version.
    VersionConflict,
    /// The runtime no longer holds a valid shard lease.
    OwnershipLost,
    /// The actor's bounded mailbox was full.
    MailboxFull,
    /// The runtime's activation capacity was exhausted.
    CapacityExceeded,
    /// The runtime is shutting down and no longer accepts new work.
    ShuttingDown,
    /// An actor key, request ID, or other supplied identifier was invalid.
    InvalidKey,
    /// The actor kind was not registered with the runtime.
    UnregisteredKind,
    /// Two actor types tried to register the same kind.
    DuplicateKind,
    /// A durable actor was registered without a storage adapter.
    MissingStorage,
    /// Runtime configuration was invalid.
    InvalidConfiguration,
    /// The command, state, or actor type did not match the registered kind.
    TypeMismatch,
    /// A user callback panicked and its activation was discarded.
    HandlerPanicked,
    /// No live owner was found for a shard.
    NoOwner,
    /// The experimental cluster protocol reported a transport or framing error.
    ClusterProtocol(String),
}

impl std::fmt::Display for ActorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(reason) => write!(f, "command rejected: {reason}"),
            Self::Storage(detail) => write!(f, "storage operation failed: {detail}"),
            Self::UnknownOutcome => f.write_str(
                "storage commit outcome is unknown; retry with the same request ID and payload",
            ),
            Self::Codec(reason) => write!(f, "serialization failed: {reason}"),
            Self::RequestIdConflict => f.write_str("request ID reused with different command"),
            Self::VersionConflict => f.write_str("actor state version conflict"),
            Self::OwnershipLost => f.write_str("shard ownership lost or lease expired"),
            Self::MailboxFull => f.write_str("actor mailbox is full"),
            Self::CapacityExceeded => f.write_str("activation capacity exceeded"),
            Self::ShuttingDown => f.write_str("runtime is shutting down"),
            Self::InvalidKey => f.write_str("invalid actor key or request ID"),
            Self::UnregisteredKind => f.write_str("actor kind is not registered"),
            Self::DuplicateKind => f.write_str("actor kind registered twice"),
            Self::MissingStorage => f.write_str("durable actor requires storage"),
            Self::InvalidConfiguration => f.write_str("invalid runtime configuration"),
            Self::TypeMismatch => f.write_str("actor command or state type mismatch"),
            Self::HandlerPanicked => f.write_str("actor handler panicked"),
            Self::NoOwner => f.write_str("no live shard owner found"),
            Self::ClusterProtocol(reason) => write!(f, "cluster protocol error: {reason}"),
        }
    }
}
impl std::error::Error for ActorError {}

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, ActorError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| serde_json::to_vec(value)))
        .map_err(|_| ActorError::HandlerPanicked)?
        .map_err(|e| ActorError::Codec(e.to_string()))
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ActorError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serde_json::from_slice(bytes)
    }))
    .map_err(|_| ActorError::HandlerPanicked)?
    .map_err(|e| ActorError::Codec(e.to_string()))
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use serde::{Deserialize, Serialize, de::Deserializer, ser::Serializer};

    struct BrokenSerialize;
    impl Serialize for BrokenSerialize {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("user serializer panicked")
        }
    }

    struct BrokenDeserialize;
    impl<'de> Deserialize<'de> for BrokenDeserialize {
        fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            panic!("user deserializer panicked")
        }
    }

    #[test]
    fn user_codec_panics_are_isolated() {
        assert!(matches!(
            encode(&BrokenSerialize),
            Err(ActorError::HandlerPanicked)
        ));
        assert!(matches!(
            decode::<BrokenDeserialize>(b"null"),
            Err(ActorError::HandlerPanicked)
        ));
    }
}

#[cfg(test)]
mod storage_error_tests {
    use super::*;

    #[test]
    fn storage_error_carries_machine_readable_context() {
        let detail = StorageError::new("postgresql", "commit", "serialization failure")
            .with_code("40001")
            .retryable();
        assert_eq!(detail.provider, "postgresql");
        assert_eq!(detail.operation, "commit");
        assert_eq!(detail.code.as_deref(), Some("40001"));
        assert!(detail.retryable);

        let encoded = serde_json::to_string(&ActorError::Storage(detail)).unwrap();
        let decoded: ActorError = serde_json::from_str(&encoded).unwrap();
        match decoded {
            ActorError::Storage(detail) => assert_eq!(detail.code.as_deref(), Some("40001")),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
