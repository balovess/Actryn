//! Single-host distributed *laboratory*: typed actor RPC + selective shard leases.
//! SQLite is shared via a local filesystem; NOT a multi-host or secure network service.
//! Request IDs are supplied by callers and must be reused across ambiguous timeouts.
use crate::{ActorError, ActorRuntime, DurableActor, StorageBackend, storage::shard_for};
use futures::{FutureExt, future::BoxFuture};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot},
    task::JoinHandle,
};

const MAX_FRAME: usize = 1_048_576;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

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
    let encoded = serde_json::to_vec(&req).map_err(|e| ActorError::Codec(e.to_string()))?;
    let exchange = async {
        let mut socket = TcpStream::connect(endpoint)
            .await
            .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
        write_frame(&mut socket, &encoded).await?;
        let reply = read_frame(&mut socket).await?;
        let response: WireResponse = serde_json::from_slice(&reply)
            .map_err(|e| ActorError::ClusterProtocol(e.to_string()))?;
        response.result
    };
    tokio::time::timeout(RPC_TIMEOUT, exchange)
        .await
        .map_err(|_| {
            ActorError::ClusterProtocol("RPC outcome unknown: retry same request ID".into())
        })?
}

/// Client-side API. First version deliberately queries placement for every call,
/// so it cannot accidentally reuse an invalidated routing cache.
#[derive(Clone)]
pub struct ClusterClient {
    runtime: ActorRuntime,
    storage: StorageBackend,
    node_id: String,
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
        let placement = self
            .storage
            .locate(shard)
            .await?
            .ok_or(ActorError::NoOwner)?;
        if placement.owner_id == self.node_id {
            return self
                .runtime
                .durable::<A>(key)
                .command(request_id, cmd)
                .await;
        }
        let bytes = invoke(
            &placement.endpoint,
            WireRequest {
                op: Op::Command,
                kind: A::KIND.into(),
                key: key.into(),
                request_id: request_id.into(),
                payload: serde_json::to_vec(&cmd).map_err(|e| ActorError::Codec(e.to_string()))?,
            },
        )
        .await?;
        serde_json::from_slice(&bytes).map_err(|e| ActorError::Codec(e.to_string()))
    }

    pub async fn read<A: DurableActor>(&self, key: &str) -> Result<A::State, ActorError> {
        if key.is_empty() || key.len() > 256 {
            return Err(ActorError::InvalidKey);
        }
        let shard = shard_for(A::KIND, key);
        let placement = self
            .storage
            .locate(shard)
            .await?
            .ok_or(ActorError::NoOwner)?;
        if placement.owner_id == self.node_id {
            return self.runtime.durable::<A>(key).read().await;
        }
        let bytes = invoke(
            &placement.endpoint,
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
    use super::{ClusterServerBuilder, TcpListener};
    use crate::{ActorError, ActorRuntime, Decision, DurableActor, SqliteStorage};
    use std::time::Duration;

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
