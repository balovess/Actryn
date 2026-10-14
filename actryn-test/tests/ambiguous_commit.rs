use actryn::{ActorError, ActorRuntime, Decision, DurableActor, OutboxEffect, SqliteStorage};
use actryn_test::faults::LoseNextCommitResponse;
use serde::{Deserialize, Serialize};

struct Counter;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CounterState(u64);

#[derive(Clone, Deserialize, Serialize)]
struct Add(u64);

impl DurableActor for Counter {
    const KIND: &'static str = "actryn.test.ambiguous_commit.v1";
    type State = CounterState;
    type Command = Add;
    type Reply = u64;

    fn initial(_: &str) -> Self::State {
        CounterState(0)
    }

    fn decide(
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Decision<Self::State, Self::Reply>, ActorError> {
        let value = state
            .0
            .checked_add(command.0)
            .ok_or_else(|| ActorError::Rejected("counter overflow".into()))?;
        Ok(
            Decision::new(CounterState(value), value).with_effect(OutboxEffect::new(
                "actryn.test.counter.v1",
                "counter-1",
                b"incremented".to_vec(),
            )),
        )
    }
}

#[tokio::test]
async fn runtime_recovers_a_commit_whose_response_was_lost() {
    let directory = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(directory.path().join("ambiguous.db")).unwrap();
    let verification_storage = storage.clone();
    let runtime = ActorRuntime::builder()
        .storage(LoseNextCommitResponse::new(storage))
        .standalone()
        .register_durable::<Counter>()
        .unwrap()
        .build()
        .unwrap();
    let counter = runtime.durable::<Counter>("counter-1");

    assert!(matches!(
        counter.command("request-1", Add(1)).await,
        Err(ActorError::UnknownOutcome)
    ));
    assert_eq!(counter.command("request-1", Add(1)).await.unwrap(), 1);
    assert_eq!(counter.command("request-2", Add(1)).await.unwrap(), 2);
    assert_eq!(
        verification_storage
            .pending_outbox(10_000)
            .await
            .unwrap()
            .iter()
            .filter(|record| {
                record.request_id == "request-1" && record.origin_key == "counter-1"
            })
            .count(),
        1,
        "retry after a lost response must not add another outbox effect"
    );

    runtime.shutdown().await.unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_runtime_recovers_a_commit_whose_response_was_lost() {
    use std::time::Duration;

    use actryn::{LOGICAL_SHARDS, PostgresStorage};
    use deadpool_postgres::{Manager, Pool};
    use tokio_postgres::NoTls;

    let url = std::env::var("ACTOR_TEST_DATABASE_URL")
        .expect("ACTOR_TEST_DATABASE_URL must point to an isolated disposable database");
    let test_id = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let config = url.parse::<tokio_postgres::Config>().expect("database URL");
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(8)
        .build()
        .expect("connection pool");
    let storage = PostgresStorage::new(pool);
    storage.initialize_schema().await.unwrap();

    let mut selected = None;
    for shard in 0..LOGICAL_SHARDS {
        if storage.locate(shard).await.unwrap().is_none() {
            selected = Some(shard);
            break;
        }
    }
    let shard = selected.expect("dedicated test database has an unleased shard");
    let key = (0..100_000)
        .map(|index| format!("ambiguous-pg-{test_id}-{index}"))
        .find(|key| actryn::storage::shard_for(Counter::KIND, key) == shard)
        .expect("a key exists for the selected shard");
    let verification_storage = storage.clone();
    let counter_key = key.clone();
    let runtime = ActorRuntime::builder()
        .storage(LoseNextCommitResponse::new(storage))
        .node_id(format!("ambiguous-pg-node-{test_id}"))
        .claim_shards("127.0.0.1:55107", vec![shard], Duration::from_secs(30))
        .register_durable::<Counter>()
        .unwrap()
        .build_async()
        .await
        .unwrap();
    let counter = runtime.durable::<Counter>(key.clone());

    assert!(matches!(
        counter.command("request-1", Add(1)).await,
        Err(ActorError::UnknownOutcome)
    ));
    assert_eq!(counter.command("request-1", Add(1)).await.unwrap(), 1);
    assert_eq!(counter.command("request-2", Add(1)).await.unwrap(), 2);
    assert_eq!(
        verification_storage
            .pending_outbox(10_000)
            .await
            .unwrap()
            .iter()
            .filter(|record| {
                record.request_id == "request-1" && record.origin_key == counter_key
            })
            .count(),
        1,
        "retry after a lost response must not add another outbox effect"
    );

    runtime.shutdown().await.unwrap();
}
