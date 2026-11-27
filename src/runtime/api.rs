//! Runtime construction and typed actor references.
//!
//! Internal `expect` calls after construction rely on validation in
//! `ActorRuntimeBuilder::validate`; request identifiers and keys are checked
//! before they reach the scheduler. User-controlled failures are returned as
//! [`crate::ActorError`].
use super::actor::{DurableActor, DurableKind, ErasedKind, Mode, SessionActor, SessionKind};
use super::metrics::{RuntimeMetrics, ShardMetrics};
use super::scheduler::{ActorId, CommandRequest, Envelope, Ingress, Request, Shard};
use super::timing::{SharedTimingObserver, TimingObserver, TimingStage};
use crate::{
    ActorError, encode,
    storage::{Ownership, StorageBackend, balanced_initial_shards, rendezvous_initial_shards},
};
use futures::stream::FuturesUnordered;
use sha2::{Digest, Sha256};
use std::{
    any::TypeId,
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_LEASE_RELEASE_WAIT: Duration = Duration::from_secs(5);

struct TimingGuard<'a> {
    observer: Option<&'a dyn TimingObserver>,
    stage: TimingStage,
    started: Option<Instant>,
}

impl Drop for TimingGuard<'_> {
    fn drop(&mut self) {
        if let (Some(observer), Some(started)) = (self.observer, self.started) {
            observer.observe(self.stage, started.elapsed());
        }
    }
}

/// Configures and builds an [`ActorRuntime`].
pub struct ActorRuntimeBuilder {
    kinds: HashMap<&'static str, Arc<dyn ErasedKind>>,
    storage: Option<StorageBackend>,
    node_id: String,
    shards: usize,
    ingress_capacity: usize,
    mailbox_capacity: usize,
    activation_limit: usize,
    idle_timeout: Duration,
    eviction_interval: Duration,
    shard_claim: Option<ShardClaim>,
    standalone: bool,
    timing_observer: Option<SharedTimingObserver>,
}

enum ShardClaim {
    Selected {
        endpoint: String,
        shards: Vec<usize>,
        lease: Duration,
    },
    Membership {
        endpoint: String,
        node_ids: Vec<String>,
        lease: Duration,
        placement: MembershipPlacement,
    },
}

#[derive(Clone, Copy)]
enum MembershipPlacement {
    Balanced,
    Rendezvous,
}

impl ActorRuntimeBuilder {
    fn new() -> Self {
        Self {
            kinds: HashMap::new(),
            storage: None,
            node_id: unique_node_id(),
            shards: 4,
            ingress_capacity: 1024,
            mailbox_capacity: 256,
            activation_limit: 10_000,
            idle_timeout: Duration::from_secs(60),
            eviction_interval: Duration::from_secs(1),
            shard_claim: None,
            standalone: false,
            timing_observer: None,
        }
    }
    /// Explicit opt-in to SQLite's single-process development mode.
    /// It takes all logical shards and fences any older SQLite owner.
    /// Never use together with a clustered runtime sharing the same database.
    pub fn standalone(mut self) -> Self {
        self.standalone = true;
        self
    }

    /// Claim only these logical shards and renew the lease for this runtime.
    ///
    /// If renewal fails, durable calls fail with [`ActorError::OwnershipLost`]. When the
    /// experimental RPC server is enabled, it also stops accepting requests. SQLite remains
    /// single-host only.
    pub fn claim_shards(
        mut self,
        endpoint: impl Into<String>,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Self {
        self.shard_claim = Some(ShardClaim::Selected {
            endpoint: endpoint.into(),
            shards,
            lease,
        });
        self
    }

    /// Claim this node's exactly balanced share from a fixed membership list.
    ///
    /// The node ID set is sorted before assignment, so every node must provide
    /// the same unique IDs. This plans initial ownership only; it does not discover
    /// members, migrate state, or rebalance when membership changes.
    pub fn claim_balanced_shards(
        self,
        endpoint: impl Into<String>,
        node_ids: Vec<String>,
        lease: Duration,
    ) -> Self {
        self.claim_membership_shards(endpoint, node_ids, lease, MembershipPlacement::Balanced)
    }

    /// Claim this node's rendezvous-hashed share from a fixed membership list.
    ///
    /// The node ID set is sorted before assignment, so every node must provide
    /// the same unique IDs. This planner favors minimal shard movement when
    /// membership changes, but it does not migrate state or rebalance ownership.
    pub fn claim_rendezvous_shards(
        self,
        endpoint: impl Into<String>,
        node_ids: Vec<String>,
        lease: Duration,
    ) -> Self {
        self.claim_membership_shards(endpoint, node_ids, lease, MembershipPlacement::Rendezvous)
    }

    fn claim_membership_shards(
        mut self,
        endpoint: impl Into<String>,
        node_ids: Vec<String>,
        lease: Duration,
        placement: MembershipPlacement,
    ) -> Self {
        self.shard_claim = Some(ShardClaim::Membership {
            endpoint: endpoint.into(),
            node_ids,
            lease,
            placement,
        });
        self
    }
    /// Install a durable storage adapter.
    ///
    /// Any type implementing [`crate::storage::StorageAdapter`] can be used,
    /// including a [`crate::storage::StorageBackend`] or an application-defined
    /// provider. The runtime depends only on that contract.
    pub fn storage<A: crate::storage::StorageAdapter>(mut self, storage: A) -> Self {
        self.storage = Some(StorageBackend::new(storage));
        self
    }

    /// Install a durable storage adapter through a shared trait object.
    ///
    /// This supports adapters chosen or constructed at runtime without adding
    /// provider-specific branches to the runtime. The adapter must still satisfy
    /// the full storage and outbox contract.
    pub fn storage_shared(mut self, storage: Arc<dyn crate::storage::StorageAdapter>) -> Self {
        self.storage = Some(StorageBackend::from_shared(storage));
        self
    }
    /// Human-readable owner identity; each Runtime build gets a fresh ownership epoch.
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = id.into();
        self
    }
    /// Sets the number of physical scheduler tasks. Must be non-zero.
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }
    /// Sets each shard's bounded ingress channel capacity. Must be non-zero.
    pub fn ingress_capacity(mut self, cap: usize) -> Self {
        self.ingress_capacity = cap;
        self
    }
    /// Sets each actor activation's bounded mailbox capacity. Must be non-zero.
    pub fn mailbox_capacity(mut self, cap: usize) -> Self {
        self.mailbox_capacity = cap;
        self
    }
    /// Sets the maximum resident activations per shard. Must be non-zero.
    pub fn activation_limit(mut self, limit: usize) -> Self {
        self.activation_limit = limit;
        self
    }
    /// Enable optional request-stage observations.
    ///
    /// The observer runs inline on scheduler and storage tasks. It must be fast,
    /// non-blocking, and non-panicking. With no observer configured, stage timers
    /// are not started.
    pub fn timing_observer(mut self, observer: Arc<dyn TimingObserver>) -> Self {
        self.timing_observer = Some(observer);
        self
    }
    /// Sets the idle duration before an activation may be evicted. Must be non-zero.
    pub fn idle_timeout(mut self, duration: Duration) -> Self {
        self.idle_timeout = duration;
        self
    }
    /// Sets how often shards scan for idle activations. Must be non-zero.
    pub fn eviction_interval(mut self, duration: Duration) -> Self {
        self.eviction_interval = duration;
        self
    }

    /// Registers a durable actor kind. Each kind may be registered only once.
    pub fn register_durable<A: DurableActor>(mut self) -> Result<Self, ActorError> {
        if A::KIND.is_empty() {
            return Err(ActorError::InvalidKey);
        }
        if self
            .kinds
            .insert(A::KIND, Arc::new(DurableKind::<A>(PhantomData)))
            .is_some()
        {
            return Err(ActorError::DuplicateKind);
        }
        Ok(self)
    }
    /// Registers a session actor kind. Each kind may be registered only once.
    pub fn register_session<A: SessionActor>(mut self) -> Result<Self, ActorError> {
        if A::KIND.is_empty() {
            return Err(ActorError::InvalidKey);
        }
        if self
            .kinds
            .insert(A::KIND, Arc::new(SessionKind::<A>(PhantomData)))
            .is_some()
        {
            return Err(ActorError::DuplicateKind);
        }
        Ok(self)
    }
    /// Synchronous construction supports adapters that implement local shard claiming.
    /// Remote adapters should use [`Self::build_async`].
    pub fn build(self) -> Result<ActorRuntime, ActorError> {
        let shard_claim = self.resolved_shard_claim()?;
        self.validate(&shard_claim)?;
        let ownership = if self.has_durable() {
            let store = self.storage.as_ref().expect("validated");
            Some(match &shard_claim {
                Some((endpoint, shards, lease)) => {
                    store.claim_selected_local(&self.node_id, endpoint, shards, *lease)?
                }
                None if self.standalone => store.claim_all_local(&self.node_id)?,
                None => return Err(ActorError::InvalidConfiguration),
            })
        } else {
            None
        };
        Ok(self.spawn(ownership, shard_claim))
    }

    /// Asynchronous construction supports local and remote adapters. Remote adapters should
    /// claim explicit shard assignments instead of silently taking over all shards.
    pub async fn build_async(self) -> Result<ActorRuntime, ActorError> {
        let shard_claim = self.resolved_shard_claim()?;
        self.validate(&shard_claim)?;
        let ownership = if self.has_durable() {
            let store = self.storage.as_ref().expect("validated");
            Some(match &shard_claim {
                Some((endpoint, shards, lease)) => {
                    store
                        .claim_selected(&self.node_id, endpoint, shards, *lease)
                        .await?
                }
                None if self.standalone => store.claim_all(&self.node_id).await?,
                None => return Err(ActorError::InvalidConfiguration),
            })
        } else {
            None
        };
        Ok(self.spawn(ownership, shard_claim))
    }

    fn resolved_shard_claim(&self) -> Result<Option<(String, Vec<usize>, Duration)>, ActorError> {
        self.shard_claim
            .as_ref()
            .map(|claim| match claim {
                ShardClaim::Selected {
                    endpoint,
                    shards,
                    lease,
                } => Ok((endpoint.clone(), shards.clone(), *lease)),
                ShardClaim::Membership {
                    endpoint,
                    node_ids,
                    lease,
                    placement,
                } => Ok((
                    endpoint.clone(),
                    match placement {
                        MembershipPlacement::Balanced => {
                            balanced_initial_shards(node_ids, &self.node_id)?
                        }
                        MembershipPlacement::Rendezvous => {
                            rendezvous_initial_shards(node_ids, &self.node_id)?
                        }
                    },
                    *lease,
                )),
            })
            .transpose()
    }

    fn has_durable(&self) -> bool {
        self.kinds.values().any(|kind| kind.mode() == Mode::Durable)
    }

    fn validate(
        &self,
        shard_claim: &Option<(String, Vec<usize>, Duration)>,
    ) -> Result<(), ActorError> {
        if self.shards == 0
            || self.ingress_capacity == 0
            || self.mailbox_capacity == 0
            || self.activation_limit == 0
            || self.idle_timeout.is_zero()
            || self.eviction_interval.is_zero()
        {
            return Err(ActorError::InvalidConfiguration);
        }
        if self.standalone && self.shard_claim.is_some() {
            return Err(ActorError::InvalidConfiguration);
        }
        if shard_claim
            .as_ref()
            .is_some_and(|(_, shards, lease)| shards.is_empty() || *lease < Duration::from_secs(1))
        {
            return Err(ActorError::InvalidConfiguration);
        }
        if self.has_durable() && self.storage.is_none() {
            return Err(ActorError::MissingStorage);
        }
        Ok(())
    }

    fn spawn(
        self,
        ownership: Option<Ownership>,
        shard_claim: Option<(String, Vec<usize>, Duration)>,
    ) -> ActorRuntime {
        let lease_supervisor = match (&shard_claim, &self.storage, &ownership) {
            (Some((_, shards, lease)), Some(storage), Some(ownership)) => {
                Some((storage.clone(), ownership.clone(), shards.clone(), *lease))
            }
            _ => None,
        };
        let lease_release = lease_supervisor
            .as_ref()
            .map(|(storage, ownership, shards, lease)| LeaseRelease {
                storage: storage.clone(),
                ownership: ownership.clone(),
                shards: shards.clone(),
                timeout: (*lease / 3).min(MAX_LEASE_RELEASE_WAIT),
            });
        let kinds = Arc::new(self.kinds);
        let mut senders = Vec::with_capacity(self.shards);
        let mut metrics = Vec::with_capacity(self.shards);
        for _ in 0..self.shards {
            let (tx, rx) = mpsc::channel(self.ingress_capacity);
            let shard_metrics = Arc::new(ShardMetrics::default());
            let shard = Shard {
                rx,
                metrics: shard_metrics.clone(),
                kinds: kinds.clone(),
                storage: self.storage.clone(),
                ownership: ownership.clone(),
                timing_observer: self.timing_observer.clone(),
                slots: HashMap::new(),
                ready: VecDeque::new(),
                pending: FuturesUnordered::new(),
                mailbox_capacity: self.mailbox_capacity,
                activation_limit: self.activation_limit,
                idle_timeout: self.idle_timeout,
                eviction_interval: self.eviction_interval,
            };
            tokio::spawn(shard.run());
            senders.push(tx);
            metrics.push(shard_metrics);
        }
        let (shutdown_status, _) = watch::channel(ShutdownStatus::Running);
        let (lease_stop, lease_stop_rx) = watch::channel(false);
        let (ownership_status, _) = watch::channel(false);
        let ownership_lost = Arc::new(AtomicBool::new(false));
        let runtime = ActorRuntime {
            inner: Arc::new(RuntimeInner {
                senders,
                kinds,
                timing_observer: self.timing_observer,
                runtime_handle: tokio::runtime::Handle::current(),
                #[cfg(feature = "experimental-rpc")]
                ownership,
                stopping: AtomicBool::new(false),
                ownership_lost: ownership_lost.clone(),
                #[cfg(feature = "experimental-rpc")]
                ownership_status: ownership_status.clone(),
                lease_stop: lease_stop.clone(),
                #[cfg(feature = "experimental-rpc")]
                lease_managed: lease_supervisor.is_some(),
                lease_release,
                metrics,
                shutdown_status,
            }),
        };
        if let Some((storage, ownership, shards, lease)) = lease_supervisor {
            super::lease::supervise(
                storage,
                ownership,
                shards,
                lease,
                lease_stop_rx,
                ownership_lost,
                ownership_status,
            );
        }
        runtime
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ShutdownStatus {
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone)]
struct LeaseRelease {
    storage: StorageBackend,
    ownership: Ownership,
    shards: Vec<usize>,
    timeout: Duration,
}

struct RuntimeInner {
    senders: Vec<mpsc::Sender<Ingress>>,
    kinds: Arc<HashMap<&'static str, Arc<dyn ErasedKind>>>,
    timing_observer: Option<SharedTimingObserver>,
    runtime_handle: tokio::runtime::Handle,
    stopping: AtomicBool,
    #[cfg(feature = "experimental-rpc")]
    ownership: Option<Ownership>,
    ownership_lost: Arc<AtomicBool>,
    #[cfg(feature = "experimental-rpc")]
    ownership_status: watch::Sender<bool>,
    lease_stop: watch::Sender<bool>,
    #[cfg(feature = "experimental-rpc")]
    lease_managed: bool,
    lease_release: Option<LeaseRelease>,
    metrics: Vec<Arc<ShardMetrics>>,
    shutdown_status: watch::Sender<ShutdownStatus>,
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        if self.stopping.swap(true, Ordering::AcqRel) {
            return;
        }
        let senders = self.senders.clone();
        let state = self.shutdown_status.clone();
        let lease_stop = self.lease_stop.clone();
        let lease_release = self.lease_release.clone();
        let handle = self.runtime_handle.clone();
        state.send_replace(ShutdownStatus::Stopping);
        drop(handle.spawn(drain(senders, lease_stop, lease_release, state)));
    }
}

async fn drain(
    senders: Vec<mpsc::Sender<Ingress>>,
    lease_stop: watch::Sender<bool>,
    lease_release: Option<LeaseRelease>,
    state: watch::Sender<ShutdownStatus>,
) {
    let result = async {
        let mut completions = Vec::with_capacity(senders.len());
        for sender in &senders {
            let (tx, rx) = oneshot::channel();
            sender
                .send(Ingress::Stop(tx))
                .await
                .map_err(|_| ActorError::ShuttingDown)?;
            completions.push(rx);
        }
        for completion in completions {
            completion.await.map_err(|_| ActorError::ShuttingDown)?;
        }
        Ok::<(), ActorError>(())
    }
    .await;
    lease_stop.send_replace(true);
    if result.is_ok()
        && let Some(lease_release) = lease_release
    {
        let _ = tokio::time::timeout(
            lease_release.timeout,
            lease_release
                .storage
                .release_selected(lease_release.ownership, lease_release.shards),
        )
        .await;
    }
    state.send_replace(if result.is_ok() {
        ShutdownStatus::Stopped
    } else {
        ShutdownStatus::Failed
    });
}

/// Typed handles to the actor kinds registered on this runtime.
///
/// Clones share one shutdown. Dropping the last handle starts a best-effort
/// drain and lease release on the Tokio runtime that built it; callers that
/// need to observe completion must still call [`Self::shutdown`].
#[derive(Clone)]
pub struct ActorRuntime {
    inner: Arc<RuntimeInner>,
}

impl ActorRuntime {
    /// Creates a new builder with default capacities and timing.
    pub fn builder() -> ActorRuntimeBuilder {
        ActorRuntimeBuilder::new()
    }
    fn stage_timer(&self, stage: TimingStage) -> TimingGuard<'_> {
        let observer = self.inner.timing_observer.as_deref();
        TimingGuard {
            observer,
            stage,
            started: observer.map(|_| Instant::now()),
        }
    }
    #[cfg(feature = "experimental-rpc")]
    pub(crate) fn ownership(&self) -> Option<&Ownership> {
        self.inner.ownership.as_ref()
    }
    #[cfg(feature = "experimental-rpc")]
    pub(crate) fn ownership_status(&self) -> watch::Receiver<bool> {
        self.inner.ownership_status.subscribe()
    }
    #[cfg(feature = "experimental-rpc")]
    pub(crate) fn lease_managed(&self) -> bool {
        self.inner.lease_managed
    }
    /// Returns a typed reference to a durable actor with the supplied key.
    pub fn durable<A: DurableActor>(&self, key: impl Into<String>) -> DurableRef<A> {
        DurableRef {
            key: key.into(),
            runtime: self.clone(),
            _kind: PhantomData,
        }
    }
    /// Returns a typed reference to a session actor with the supplied key.
    pub fn session<A: SessionActor>(&self, key: impl Into<String>) -> SessionRef<A> {
        SessionRef {
            key: key.into(),
            runtime: self.clone(),
            _kind: PhantomData,
        }
    }
    fn check(&self, id: &ActorId, type_id: TypeId, mode: Mode) -> Result<(), ActorError> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Err(ActorError::ShuttingDown);
        }
        if mode == Mode::Durable && self.inner.ownership_lost.load(Ordering::Acquire) {
            return Err(ActorError::OwnershipLost);
        }
        if id.key.is_empty() || id.key.len() > 256 {
            return Err(ActorError::InvalidKey);
        }
        let kind = self
            .inner
            .kinds
            .get(id.kind)
            .ok_or(ActorError::UnregisteredKind)?;
        if kind.actor_type_id() != type_id || kind.mode() != mode {
            return Err(ActorError::TypeMismatch);
        }
        Ok(())
    }
    async fn submit(&self, env: Envelope) -> Result<(), ActorError> {
        let idx = shard_index(&env.id, self.inner.senders.len());
        let observer = self.inner.timing_observer.as_deref();
        let started = observer.map(|_| std::time::Instant::now());
        let result = self.inner.senders[idx].send(Ingress::Request(env)).await;
        if let (Some(observer), Some(started)) = (observer, started) {
            observer.observe(super::timing::TimingStage::IngressQueue, started.elapsed());
        }
        result.map_err(|_| ActorError::ShuttingDown)
    }
    /// Process-local telemetry; counters can change during the snapshot.
    /// `live_activations` counts resident slots, not all durable logical actors.
    pub fn metrics(&self) -> RuntimeMetrics {
        let mut total = RuntimeMetrics::default();
        for shard in &self.inner.metrics {
            total.add(shard.snapshot());
        }
        total
    }

    /// Reports whether a managed lease has been lost.
    ///
    /// Once true, new durable calls fail with [`ActorError::OwnershipLost`].
    /// This is a process-local health observation and is not a fencing authority.
    pub fn ownership_lost(&self) -> bool {
        self.inner.ownership_lost.load(Ordering::Acquire)
    }

    /// Stop accepting new commands and drain accepted turns. Concurrent callers
    /// observe the *same completion*: neither may return early. If a caller's
    /// future is cancelled, a separately owned shutdown task keeps draining.
    /// After a fatal close failure all waiters receive `ShuttingDown`.
    /// Dropping the last runtime handle also starts the drain, but only this
    /// method reports when it has completed.
    pub async fn shutdown(&self) -> Result<(), ActorError> {
        let mut status = self.inner.shutdown_status.subscribe();
        if !self.inner.stopping.swap(true, Ordering::AcqRel) {
            let senders = self.inner.senders.clone();
            let state = self.inner.shutdown_status.clone();
            let lease_stop = self.inner.lease_stop.clone();
            let lease_release = self.inner.lease_release.clone();
            state.send_replace(ShutdownStatus::Stopping);
            tokio::spawn(drain(senders, lease_stop, lease_release, state));
        }
        loop {
            match *status.borrow_and_update() {
                ShutdownStatus::Stopped => return Ok(()),
                ShutdownStatus::Failed => return Err(ActorError::ShuttingDown),
                ShutdownStatus::Running | ShutdownStatus::Stopping => {}
            }
            status
                .changed()
                .await
                .map_err(|_| ActorError::ShuttingDown)?;
        }
    }
}

fn shard_index(id: &ActorId, count: usize) -> usize {
    (crate::storage::stable_actor_hash(id.kind, &id.key) % count as u64) as usize
}

/// Typed reference to one durable actor identity.
pub struct DurableRef<A: DurableActor> {
    key: String,
    runtime: ActorRuntime,
    _kind: PhantomData<fn() -> A>,
}
impl<A: DurableActor> Clone for DurableRef<A> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            runtime: self.runtime.clone(),
            _kind: PhantomData,
        }
    }
}
impl<A: DurableActor> DurableRef<A> {
    /// Submits one idempotent durable command and waits for its committed reply.
    ///
    /// Retry ambiguous failures with the same request ID and command payload.
    pub async fn command(
        &self,
        request_id: impl Into<String>,
        cmd: A::Command,
    ) -> Result<A::Reply, ActorError> {
        let _timer = self.runtime.stage_timer(TimingStage::ClientResponse);
        let id = ActorId {
            kind: A::KIND,
            key: self.key.clone(),
        };
        self.runtime.check(&id, TypeId::of::<A>(), Mode::Durable)?;
        let rid = request_id.into();
        if rid.is_empty() || rid.len() > 128 {
            return Err(ActorError::InvalidKey);
        }
        let hash: [u8; 32] = Sha256::digest(encode(&cmd)?).into();
        let (tx, rx) = oneshot::channel();
        self.runtime
            .submit(Envelope {
                id,
                request: Request::Command(CommandRequest {
                    command: Some(Box::new(cmd)),
                    request_id: Some(rid),
                    hash: Some(hash),
                    queued_at: None,
                    reply: tx,
                }),
            })
            .await?;
        let response = rx.await.map_err(|_| ActorError::ShuttingDown)??;
        Ok(*response
            .downcast::<A::Reply>()
            .map_err(|_| ActorError::TypeMismatch)?)
    }
    /// Reads a fenced clone of the actor's current cached state.
    pub async fn read(&self) -> Result<A::State, ActorError> {
        let id = ActorId {
            kind: A::KIND,
            key: self.key.clone(),
        };
        self.runtime.check(&id, TypeId::of::<A>(), Mode::Durable)?;
        let (tx, rx) = oneshot::channel();
        self.runtime
            .submit(Envelope {
                id,
                request: Request::Read(tx),
            })
            .await?;
        let response = rx.await.map_err(|_| ActorError::ShuttingDown)??;
        Ok(*response
            .downcast::<A::State>()
            .map_err(|_| ActorError::TypeMismatch)?)
    }
}

/// Typed reference to one session actor identity.
pub struct SessionRef<A: SessionActor> {
    key: String,
    runtime: ActorRuntime,
    _kind: PhantomData<fn() -> A>,
}
impl<A: SessionActor> Clone for SessionRef<A> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            runtime: self.runtime.clone(),
            _kind: PhantomData,
        }
    }
}
impl<A: SessionActor> SessionRef<A> {
    /// Submits one volatile command and waits for its reply.
    pub async fn command(&self, cmd: A::Command) -> Result<A::Reply, ActorError> {
        let _timer = self.runtime.stage_timer(TimingStage::ClientResponse);
        let id = ActorId {
            kind: A::KIND,
            key: self.key.clone(),
        };
        self.runtime.check(&id, TypeId::of::<A>(), Mode::Session)?;
        let (tx, rx) = oneshot::channel();
        self.runtime
            .submit(Envelope {
                id,
                request: Request::Command(CommandRequest {
                    command: Some(Box::new(cmd)),
                    request_id: None,
                    hash: None,
                    queued_at: None,
                    reply: tx,
                }),
            })
            .await?;
        let response = rx.await.map_err(|_| ActorError::ShuttingDown)??;
        Ok(*response
            .downcast::<A::Reply>()
            .map_err(|_| ActorError::TypeMismatch)?)
    }
    /// Reads a clone of the actor's current in-memory state.
    pub async fn read(&self) -> Result<A::State, ActorError> {
        let id = ActorId {
            kind: A::KIND,
            key: self.key.clone(),
        };
        self.runtime.check(&id, TypeId::of::<A>(), Mode::Session)?;
        let (tx, rx) = oneshot::channel();
        self.runtime
            .submit(Envelope {
                id,
                request: Request::Read(tx),
            })
            .await?;
        let response = rx.await.map_err(|_| ActorError::ShuttingDown)??;
        Ok(*response
            .downcast::<A::State>()
            .map_err(|_| ActorError::TypeMismatch)?)
    }
}

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
fn unique_node_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "local:{}:{}:{}",
        std::process::id(),
        nanos,
        NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed)
    )
}
