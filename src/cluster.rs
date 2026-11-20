//! Single-host distributed *laboratory*: typed actor RPC + selective shard leases.
//! SQLite is shared via a local filesystem; NOT a multi-host or secure network service.
//! Request IDs are supplied by callers and must be reused across ambiguous timeouts.
use crate::{
    ActorError, ActorRuntime, DurableActor, ShardLocation, StorageBackend, storage::shard_for,
};
use futures::{FutureExt, future::BoxFuture};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{RwLock, Semaphore, oneshot},
    task::JoinHandle,
};

const MAX_FRAME: usize = 1_048_576;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const ROUTE_CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct CachedRoute {
    location: ShardLocation,
    expires_at: Instant,
}

#[derive(Serialize, Deserialize)]
enum Op {
    Command,
    Read,
}

#[derive(Serialize, Deserialize)]
struct WireRequest {
    op: Op,
    kind: String,
    key: String,
    request_id: String,
    payload: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
struct WireResponse {
    result: Result<Vec<u8>, ActorError>,
}

async fn write_frame(stream: &mut TcpStream, value: &[u8]) -> Result<(), ActorError> {
    if value.len() > MAX_FRAME {
        return Err(ActorError::ClusterProtocol("frame too large".into()));
    }
    stream
        .write_all(&(value.len() as u32).to_be_bytes())
        .await
        .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
    stream
        .write_all(value)
        .await
        .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
    Ok(())
}
async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, ActorError> {
    let mut head = [0u8; 4];
    stream
        .read_exact(&mut head)
        .await
        .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
    let size = u32::from_be_bytes(head) as usize;
    if size > MAX_FRAME {
        return Err(ActorError::ClusterProtocol("frame too large".into()));
    }
    let mut payload = vec![0u8; size];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
    Ok(payload)
}

async fn invoke(endpoint: &str, req: WireRequest) -> Result<Vec<u8>, ActorError> {
    invoke_with_timeout(endpoint, req, RPC_TIMEOUT).await
}

async fn invoke_with_timeout(
    endpoint: &str,
    req: WireRequest,
    timeout: Duration,
) -> Result<Vec<u8>, ActorError> {
    let encoded = serde_json::to_vec(&req).map_err(|e| ActorError::Codec(e.to_string()))?;
    let request_in_flight = Arc::new(AtomicBool::new(false));
    let marked = request_in_flight.clone();
    let exchange = async {
        let mut socket = TcpStream::connect(endpoint)
            .await
            .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
        // From here on a timeout may leave a complete request on the server,
        // even if writing or flushing did not finish locally.
        marked.store(true, Ordering::Release);
        write_frame(&mut socket, &encoded).await?;
        let reply = read_frame(&mut socket).await?;
        let response: WireResponse = serde_json::from_slice(&reply)
            .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
        response.result
    };
    match tokio::time::timeout(timeout, exchange).await {
        Ok(result) => result,
        Err(_) if request_in_flight.load(Ordering::Acquire) => Err(ActorError::UnknownOutcome),
        Err(_) => Err(ActorError::ClusterProtocol(
            "RPC timed out before the request was sent".into(),
        )),
    }
}

async fn invoke_command<A: DurableActor>(
    endpoint: &str,
    key: &str,
    request_id: &str,
    payload: &[u8],
) -> Result<A::Reply, ActorError> {
    let bytes = invoke(
        endpoint,
        WireRequest {
            op: Op::Command,
            kind: A::KIND.into(),
            key: key.into(),
            request_id: request_id.into(),
            payload: payload.to_vec(),
        },
    )
    .await?;
    serde_json::from_slice(&bytes).map_err(|e| ActorError::Codec(e.to_string()))
}

async fn invoke_read<A: DurableActor>(endpoint: &str, key: &str) -> Result<A::State, ActorError> {
    let bytes = invoke(
        endpoint,
        WireRequest {
            op: Op::Read,
            kind: A::KIND.into(),
            key: key.into(),
            request_id: String::new(),
            payload: Vec::new(),
        },
    )
    .await?;
    serde_json::from_slice(&bytes).map_err(|e| ActorError::Codec(e.to_string()))
}

fn should_refresh_route(error: &ActorError) -> bool {
    matches!(
        error,
        ActorError::NoOwner
            | ActorError::OwnershipLost
            | ActorError::UnknownOutcome
            | ActorError::ClusterProtocol(_)
    )
}

/// Client-side API with a short-lived route cache and one refresh attempt after routing failures.
#[derive(Clone)]
pub struct ClusterClient {
    runtime: ActorRuntime,
    storage: StorageBackend,
    node_id: String,
    routes: Arc<RwLock<HashMap<usize, CachedRoute>>>,
}
impl ClusterClient {
    pub fn new(
        runtime: ActorRuntime,
        storage: impl crate::storage::StorageAdapter,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            runtime,
            storage: StorageBackend::new(storage),
            node_id: node_id.into(),
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn route_for(&self, shard: usize) -> Result<ShardLocation, ActorError> {
        let now = Instant::now();
        if let Some(route) = self
            .routes
            .read()
            .await
            .get(&shard)
            .filter(|route| route.expires_at > now)
        {
            return Ok(route.location.clone());
        }
        self.refresh_route(shard).await
    }

    async fn refresh_route(&self, shard: usize) -> Result<ShardLocation, ActorError> {
        let location = self.storage.locate(shard).await?;
        let Some(location) = location else {
            self.routes.write().await.remove(&shard);
            return Err(ActorError::NoOwner);
        };

        let now = Instant::now();
        let mut routes = self.routes.write().await;
        if let Some(current) = routes
            .get(&shard)
            .filter(|current| current.expires_at > now && current.location.epoch > location.epoch)
        {
            return Ok(current.location.clone());
        }
        routes.insert(
            shard,
            CachedRoute {
                location: location.clone(),
                expires_at: now + ROUTE_CACHE_TTL,
            },
        );
        Ok(location)
    }

    async fn invalidate_route(&self, shard: usize, observed: &ShardLocation) {
        let mut routes = self.routes.write().await;
        if routes
            .get(&shard)
            .is_some_and(|route| route.location == *observed)
        {
            routes.remove(&shard);
        }
    }

    pub async fn command<A: DurableActor>(
        &self,
        key: &str,
        request_id: &str,
        cmd: A::Command,
    ) -> Result<A::Reply, ActorError> {
        if key.is_empty() || key.len() > 256 || request_id.is_empty() || request_id.len() > 128 {
            return Err(ActorError::InvalidKey);
        }
        let shard = shard_for(A::KIND, key);
        let placement = self.route_for(shard).await?;
        if placement.owner_id == self.node_id {
            let result = self
                .runtime
                .durable::<A>(key)
                .command(request_id, cmd)
                .await;
            if result.as_ref().is_err_and(should_refresh_route) {
                self.invalidate_route(shard, &placement).await;
            }
            return result;
        }
        let payload = serde_json::to_vec(&cmd).map_err(|e| ActorError::Codec(e.to_string()))?;
        let result = invoke_command::<A>(&placement.endpoint, key, request_id, &payload).await;
        let Err(error) = result else {
            return result;
        };
        if !should_refresh_route(&error) {
            return Err(error);
        }

        self.invalidate_route(shard, &placement).await;
        let refreshed = match self.refresh_route(shard).await {
            Ok(route) => route,
            Err(_) => return Err(error),
        };
        let retry = if refreshed.owner_id == self.node_id {
            self.runtime
                .durable::<A>(key)
                .command(request_id, cmd)
                .await
        } else {
            invoke_command::<A>(&refreshed.endpoint, key, request_id, &payload).await
        };
        if retry.as_ref().is_err_and(should_refresh_route) {
            self.invalidate_route(shard, &refreshed).await;
        }
        retry
    }

    pub async fn read<A: DurableActor>(&self, key: &str) -> Result<A::State, ActorError> {
        if key.is_empty() || key.len() > 256 {
            return Err(ActorError::InvalidKey);
        }
        let shard = shard_for(A::KIND, key);
        let placement = self.route_for(shard).await?;
        if placement.owner_id == self.node_id {
            let result = self.runtime.durable::<A>(key).read().await;
            if result.as_ref().is_err_and(should_refresh_route) {
                self.invalidate_route(shard, &placement).await;
            }
            return result;
        }
        let result = invoke_read::<A>(&placement.endpoint, key).await;
        let Err(error) = result else {
            return result;
        };
        if !should_refresh_route(&error) {
            return Err(error);
        }

        self.invalidate_route(shard, &placement).await;
        let refreshed = match self.refresh_route(shard).await {
            Ok(route) => route,
            Err(_) => return Err(error),
        };
        let retry = if refreshed.owner_id == self.node_id {
            self.runtime.durable::<A>(key).read().await
        } else {
            invoke_read::<A>(&refreshed.endpoint, key).await
        };
        if retry.as_ref().is_err_and(should_refresh_route) {
            self.invalidate_route(shard, &refreshed).await;
        }
        retry
    }
}

trait RemoteHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        runtime: &'a ActorRuntime,
        req: WireRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>, ActorError>>;
}
struct DurableHandler<A>(PhantomData<fn() -> A>);
impl<A> RemoteHandler for DurableHandler<A>
where
    A: DurableActor,
    A::Command: DeserializeOwned,
{
    fn handle<'a>(
        &'a self,
        runtime: &'a ActorRuntime,
        req: WireRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>, ActorError>> {
        async move {
            match req.op {
                Op::Command => {
                    let cmd: A::Command = serde_json::from_slice(&req.payload)
                        .map_err(|e| ActorError::Codec(e.to_string()))?;
                    let reply = runtime
                        .durable::<A>(&req.key)
                        .command(req.request_id, cmd)
                        .await?;
                    serde_json::to_vec(&reply).map_err(|e| ActorError::Codec(e.to_string()))
                }
                Op::Read => {
                    let state = runtime.durable::<A>(&req.key).read().await?;
                    serde_json::to_vec(&state).map_err(|e| ActorError::Codec(e.to_string()))
                }
            }
        }
        .boxed()
    }
}

pub struct ClusterServerBuilder {
    handlers: HashMap<String, Arc<dyn RemoteHandler>>,
}
impl ClusterServerBuilder {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }
    pub fn register<A>(mut self) -> Result<Self, ActorError>
    where
        A: DurableActor,
        A::Command: DeserializeOwned,
    {
        if self
            .handlers
            .insert(A::KIND.into(), Arc::new(DurableHandler::<A>(PhantomData)))
            .is_some()
        {
            return Err(ActorError::DuplicateKind);
        }
        Ok(self)
    }
    /// TCP listener must be bound before building the Runtime so its address can
    /// be entered into the shard directory via `claim_shards()`.
    /// The runtime owns renewal; this server stops accepting requests when that ownership is lost.
    pub fn serve(
        self,
        listener: TcpListener,
        runtime: ActorRuntime,
    ) -> Result<ClusterServer, ActorError> {
        if !listener
            .local_addr()
            .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?
            .ip()
            .is_loopback()
        {
            return Err(ActorError::InvalidConfiguration); // no auth / TLS in laboratory transport
        }
        if !runtime.lease_managed() || runtime.ownership().is_none() {
            return Err(ActorError::InvalidConfiguration);
        }
        let mut ownership_status = runtime.ownership_status();
        if *ownership_status.borrow_and_update() {
            return Err(ActorError::OwnershipLost);
        }
        let handlers = Arc::new(self.handlers);
        let permits = Arc::new(Semaphore::new(256));
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    changed = ownership_status.changed() => {
                        let ownership_lost = *ownership_status.borrow_and_update();
                        if changed.is_err() || ownership_lost {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((mut stream,_)) = accepted else { break; };
                        let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                        let runtime = runtime.clone();
                        let handlers = handlers.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            let serve_one = async {
                                let bytes = read_frame(&mut stream).await?;
                                let req:WireRequest = serde_json::from_slice(&bytes)
                                    .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
                                let result = match handlers.get(&req.kind) {
                                    Some(handler) => handler.handle(&runtime,req).await,
                                    None => Err(ActorError::UnregisteredKind),
                                };
                                let bytes = serde_json::to_vec(&WireResponse { result })
                                    .map_err(|e| ActorError::Codec(e.to_string()))?;
                                write_frame(&mut stream,&bytes).await
                            };
                            let _ = tokio::time::timeout(RPC_TIMEOUT,serve_one).await;
                        });
                    }
                }
            }
        });
        Ok(ClusterServer {
            stop: Some(stop_tx),
            task,
        })
    }
}
impl Default for ClusterServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClusterServer {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}
impl ClusterServer {
    pub async fn shutdown(mut self) {
        if let Some(sender) = self.stop.take() {
            let _ = sender.send(());
        }
        let _ = (&mut self.task).await;
    }
}
impl Drop for ClusterServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClusterClient, ClusterServerBuilder, Instant, Op, TcpListener, WireRequest,
        invoke_with_timeout,
    };
    use crate::{
        ActorError, ActorRuntime, CommitRequest, CommitResult, Decision, DurableActor,
        OutboxRecord, OutboxStore, Ownership, ShardLocation, SqliteStorage, StorageAdapter,
    };
    use async_trait::async_trait;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::sync::RwLock;

    #[derive(Clone)]
    struct RouteDirectory {
        location: Arc<RwLock<Option<ShardLocation>>>,
        locates: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OutboxStore for RouteDirectory {
        async fn pending_outbox(&self, _limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
            Ok(Vec::new())
        }

        async fn ack_outbox(&self, _id: i64) -> Result<(), ActorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl StorageAdapter for RouteDirectory {
        async fn claim_selected(
            &self,
            _node_id: &str,
            _endpoint: &str,
            _shards: &[usize],
            _lease: Duration,
        ) -> Result<Ownership, ActorError> {
            Err(ActorError::InvalidConfiguration)
        }

        async fn release_selected(
            &self,
            _ownership: Ownership,
            _shards: Vec<usize>,
        ) -> Result<(), ActorError> {
            Err(ActorError::InvalidConfiguration)
        }

        async fn locate(&self, _shard: usize) -> Result<Option<ShardLocation>, ActorError> {
            self.locates.fetch_add(1, Ordering::Relaxed);
            Ok(self.location.read().await.clone())
        }

        async fn check_fence(
            &self,
            _ownership: Ownership,
            _kind: &'static str,
            _key: String,
        ) -> Result<(), ActorError> {
            Err(ActorError::InvalidConfiguration)
        }

        async fn load(
            &self,
            _ownership: Ownership,
            _kind: &'static str,
            _key: String,
        ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
            Err(ActorError::InvalidConfiguration)
        }

        async fn receipt(
            &self,
            _ownership: Ownership,
            _kind: &'static str,
            _key: String,
            _request_id: String,
        ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
            Err(ActorError::InvalidConfiguration)
        }

        async fn commit(&self, _request: CommitRequest) -> Result<CommitResult, ActorError> {
            Err(ActorError::InvalidConfiguration)
        }
    }

    fn location(owner_id: &str, epoch: u64) -> ShardLocation {
        ShardLocation {
            shard: 7,
            owner_id: owner_id.to_owned(),
            endpoint: format!("{owner_id}:9000"),
            epoch,
            expires_at_ms: 0,
        }
    }

    #[tokio::test]
    async fn route_cache_expires_and_old_failures_do_not_evict_newer_routes() {
        let directory = RouteDirectory {
            location: Arc::new(RwLock::new(Some(location("owner-a", 1)))),
            locates: Arc::new(AtomicUsize::new(0)),
        };
        let runtime = ActorRuntime::builder().build().unwrap();
        let client = ClusterClient::new(runtime.clone(), directory.clone(), "client");

        let old_route = client.route_for(7).await.unwrap();
        assert_eq!(client.route_for(7).await.unwrap(), old_route);
        assert_eq!(directory.locates.load(Ordering::Relaxed), 1);

        {
            let mut routes = client.routes.write().await;
            routes.get_mut(&7).unwrap().expires_at = Instant::now() - Duration::from_secs(1);
        }
        *directory.location.write().await = Some(location("owner-b", 2));

        let new_route = client.route_for(7).await.unwrap();
        assert_eq!(new_route.owner_id, "owner-b");
        assert_eq!(directory.locates.load(Ordering::Relaxed), 2);

        client.invalidate_route(7, &old_route).await;
        assert_eq!(client.route_for(7).await.unwrap(), new_route);
        assert_eq!(directory.locates.load(Ordering::Relaxed), 2);

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rpc_timeout_after_connect_reports_unknown_outcome() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let stalled_server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let request = WireRequest {
            op: Op::Command,
            kind: "test.kind.v1".into(),
            key: "key".into(),
            request_id: "request-1".into(),
            payload: Vec::new(),
        };
        let result = invoke_with_timeout(&endpoint, request, Duration::from_millis(50)).await;
        assert!(
            matches!(result, Err(ActorError::UnknownOutcome)),
            "unexpected result: {result:?}"
        );
        stalled_server.abort();
    }

    struct LeaseProbe;

    impl DurableActor for LeaseProbe {
        const KIND: &'static str = "cluster.lease_probe.v1";
        type State = ();
        type Command = ();
        type Reply = ();

        fn initial(_key: &str) -> Self::State {}

        fn decide(
            _state: &Self::State,
            _command: Self::Command,
        ) -> Result<Decision<Self::State, Self::Reply>, ActorError> {
            Ok(Decision::new((), ()))
        }
    }

    #[tokio::test]
    async fn rpc_server_stops_when_runtime_loses_a_shard_lease() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lease-loss.db");
        let storage = SqliteStorage::open(&path).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let runtime = ActorRuntime::builder()
            .storage(storage)
            .node_id("lease-probe-owner")
            .claim_shards(endpoint, vec![0], Duration::from_secs(1))
            .register_durable::<LeaseProbe>()
            .unwrap()
            .build()
            .unwrap();
        let server = ClusterServerBuilder::new()
            .serve(listener, runtime.clone())
            .unwrap();

        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE shard_owners SET expires_at_ms=0 WHERE shard_id=0",
                [],
            )
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while !server.task.is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("server task should stop after lease renewal fails");

        server.shutdown().await;
        runtime.shutdown().await.unwrap();
    }
}
