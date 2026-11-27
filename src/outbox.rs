use crate::ActorError;
use async_trait::async_trait;

/// Only the operations needed by the outbox dispatcher.
/// A delivery can repeat after a crash; the sink must implement idempotency.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Return at most `limit` pending records. Adapters accept limits from 1 through 10,000.
    async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError>;
    /// Persistently acknowledge one previously returned record.
    async fn ack_outbox(&self, id: i64) -> Result<(), ActorError>;
}

/// An effect is a *durable delivery intent*, not a remotely executed call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEffect {
    /// Registered actor kind that should consume the effect.
    pub destination_kind: String,
    /// Logical actor key that should consume the effect.
    pub destination_key: String,
    /// Opaque application payload.
    pub payload: Vec<u8>,
}
impl OutboxEffect {
    /// Creates a delivery intent for one destination actor.
    pub fn new(kind: impl Into<String>, key: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            destination_kind: kind.into(),
            destination_key: key.into(),
            payload,
        }
    }
}

/// Result of one durable actor decision.
pub struct Decision<S, R> {
    /// State to install only after the durable commit succeeds.
    pub next_state: S,
    /// Reply to return to the caller after commit.
    pub reply: R,
    /// Durable delivery intents committed with the state.
    pub effects: Vec<OutboxEffect>,
}
impl<S, R> Decision<S, R> {
    /// Creates a decision without outbox effects.
    pub fn new(next_state: S, reply: R) -> Self {
        Self {
            next_state,
            reply,
            effects: vec![],
        }
    }
    /// Appends one outbox effect to the decision.
    pub fn with_effect(mut self, effect: OutboxEffect) -> Self {
        self.effects.push(effect);
        self
    }
}

/// One durable effect ready for at-least-once delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    /// Provider-specific durable record identifier used for acknowledgement.
    pub id: i64,
    /// Actor kind whose decision produced the effect.
    pub origin_kind: String,
    /// Actor key whose decision produced the effect.
    pub origin_key: String,
    /// Request ID that produced the effect.
    pub request_id: String,
    /// Zero-based effect position within the request.
    pub ordinal: i64,
    /// Actor kind that should consume the effect.
    pub destination_kind: String,
    /// Actor key that should consume the effect.
    pub destination_key: String,
    /// Opaque application payload.
    pub payload: Vec<u8>,
}
impl OutboxRecord {
    /// Stable across retried delivery of the same committed effect.
    pub fn delivery_id(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}",
            self.origin_kind.len(),
            self.origin_kind,
            self.origin_key.len(),
            self.origin_key,
            self.request_id.len(),
            self.request_id,
            self.ordinal
        )
    }
}

/// Receiver must acknowledge only once its own durable operation is complete.
/// A crash after send but before ack can cause duplicate delivery.
pub trait OutboxSink: Send + Sync {
    /// Sends one record; consumers must deduplicate by [`OutboxRecord::delivery_id`].
    fn send<'a>(
        &'a self,
        message: &'a OutboxRecord,
    ) -> impl std::future::Future<Output = Result<(), ActorError>> + Send + 'a;
}

/// Manually poll one batch; no always-on worker is required by embedded apps.
/// Returns the number of successfully acknowledged records.
pub async fn dispatch_outbox<B: OutboxStore, S: OutboxSink>(
    storage: &B,
    sink: &S,
    limit: usize,
) -> Result<usize, ActorError> {
    let pending = storage.pending_outbox(limit).await?;
    let mut sent = 0;
    for message in pending {
        sink.send(&message).await?;
        storage.ack_outbox(message.id).await?;
        sent += 1;
    }
    Ok(sent)
}
