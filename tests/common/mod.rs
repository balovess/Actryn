use std::time::{SystemTime, UNIX_EPOCH};

use actryn::{LOGICAL_SHARDS, PostgresStorage};
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::NoTls;

pub fn database_url() -> String {
    std::env::var("ACTOR_TEST_DATABASE_URL")
        .expect("ACTOR_TEST_DATABASE_URL must point to an isolated disposable database")
}

pub fn unique_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{now}", std::process::id())
}

pub fn pool(url: &str) -> Pool {
    pool_with_options(url, &unique_id(), None)
}

pub fn pool_with_options(url: &str, application_name: &str, options: Option<&str>) -> Pool {
    let mut config = url.parse::<tokio_postgres::Config>().expect("database URL");
    config.application_name(application_name);
    if let Some(options) = options {
        config.options(options);
    }
    Pool::builder(Manager::new(config, NoTls))
        .max_size(8)
        .build()
        .expect("connection pool")
}

pub async fn unleased_shard(storage: &PostgresStorage) -> usize {
    let base = unique_id()
        .bytes()
        .fold(0usize, |value, byte| value.wrapping_add(usize::from(byte)))
        % LOGICAL_SHARDS;
    for offset in 0..LOGICAL_SHARDS {
        let shard = (base + offset) % LOGICAL_SHARDS;
        if storage
            .locate(shard)
            .await
            .expect("read shard lease")
            .is_none()
        {
            return shard;
        }
    }
    panic!("no unleased shard is available in the dedicated test database");
}

pub fn key_on_shard(kind: &str, shard: usize) -> String {
    let run_id = unique_id();
    (0..100_000)
        .map(|index| format!("{run_id}-contract-key-{index}"))
        .find(|key| actryn::storage::shard_for(kind, key) == shard)
        .expect("a logical shard has a key in the search range")
}
